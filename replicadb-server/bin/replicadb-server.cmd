@echo off
setlocal EnableExtensions

set "PACKAGE_ROOT=%~dp0.."
set "SERVER_HOME=%REPLICADB_SERVER_HOME%"
if not defined SERVER_HOME set "SERVER_HOME=%USERPROFILE%\.replicadb"
set "RUN_DIR=%SERVER_HOME%\run"
set "LOG_DIR=%SERVER_HOME%\logs"
set "PID_FILE=%RUN_DIR%\server.pid"
set "MODE_FILE=%RUN_DIR%\server.mode"
set "READINESS_TIMEOUT=%REPLICADB_READINESS_TIMEOUT%"
if not defined READINESS_TIMEOUT set "READINESS_TIMEOUT=180"

if /I "%~1"=="help" goto :help
if /I "%~1"=="" goto :help
if /I "%~1"=="start" goto :start
if /I "%~1"=="status" goto :status
if /I "%~1"=="stop" goto :stop
echo Error: unknown command %~1 1>&2
goto :invalid

:help
echo Usage: replicadb-server ^<command^> [mode]
echo.
echo Commands:
echo   start local^|api^|worker  Start the server in an explicit mode
echo   stop                     Stop the managed server
echo   status                   Show managed server state
echo   help                     Show this help
exit /b 0

:start
if /I not "%~2"=="local" if /I not "%~2"=="api" if /I not "%~2"=="worker" (
    echo Error: start requires one mode: local, api, or worker 1>&2
    exit /b 2
)
if /I "%~2"=="api" if not defined DB_URL goto :missing_external
if /I "%~2"=="api" if not defined DB_USERNAME goto :missing_external
if /I "%~2"=="api" if not defined DB_PASSWORD goto :missing_external
if /I "%~2"=="worker" if not defined DB_URL goto :missing_external
if /I "%~2"=="worker" if not defined DB_USERNAME goto :missing_external
if /I "%~2"=="worker" if not defined DB_PASSWORD goto :missing_external
if /I "%~2"=="local" goto :local_credentials
goto :start_process

:local_credentials
if defined REPLICADB_BOOTSTRAP_ADMIN_USERNAME if defined REPLICADB_BOOTSTRAP_ADMIN_PASSWORD goto :start_process
for /f "delims=" %%U in ('powershell -NoProfile -Command "$u=Read-Host 'Initial admin username'; [Console]::WriteLine($u)"') do set "REPLICADB_BOOTSTRAP_ADMIN_USERNAME=%%U"
for /f "delims=" %%P in ('powershell -NoProfile -Command "$s=Read-Host 'Initial admin password' -AsSecureString; $b=[Runtime.InteropServices.Marshal]::SecureStringToBSTR($s); try { [Console]::WriteLine([Runtime.InteropServices.Marshal]::PtrToStringBSTR($b)) } finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($b) }"') do set "REPLICADB_BOOTSTRAP_ADMIN_PASSWORD=%%P"
if not defined REPLICADB_BOOTSTRAP_ADMIN_USERNAME if not defined REPLICADB_BOOTSTRAP_ADMIN_PASSWORD (
    echo Error: local mode requires administrator credentials 1>&2
    exit /b 1
)

:start_process
call :resolve_jar || exit /b 1
if not exist "%RUN_DIR%" mkdir "%RUN_DIR%"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
if exist "%PID_FILE%" (
    set /p EXISTING_PID=<"%PID_FILE%"
    powershell -NoProfile -Command "$p=Get-CimInstance Win32_Process -Filter 'ProcessId=%EXISTING_PID%' -OperationTimeoutSec 10 -ErrorAction SilentlyContinue; if ($p -and $p.CommandLine -like '*%JAR_FILE%*') { exit 0 } else { exit 1 }"
    if not errorlevel 1 goto :already_started
    del /q "%PID_FILE%" "%MODE_FILE%" 2>nul
)
if exist "%LOG_DIR%\server.log" for %%A in ("%LOG_DIR%\server.log") do if %%~zA GTR 10485760 move /y "%LOG_DIR%\server.log" "%LOG_DIR%\server.log.1" >nul
java.exe -version >nul 2>&1 || goto :java_missing
set "PROFILE=%~2"
if /I "%~2"=="local" set "PROFILE=api"
set "JAVA_ARGS=--spring.profiles.active=%PROFILE%"
if /I "%~2"=="local" set "JAVA_ARGS=%JAVA_ARGS% --replicadb.embedded-postgres.enabled=true"
for /f "delims=" %%P in ('powershell -NoProfile -Command "$p=Start-Process -FilePath java.exe -ArgumentList '-jar "%JAR_FILE%" %JAVA_ARGS%' -RedirectStandardOutput '%LOG_DIR%\server.log' -RedirectStandardError '%LOG_DIR%\server-error.log' -PassThru; $p.Id"') do echo %%P>"%PID_FILE%"
>"%MODE_FILE%" echo %~2
set "HEALTH_PORT=8080"
if /I "%~2"=="worker" set "HEALTH_PORT=%REPLICADB_WORKER_MANAGEMENT_PORT%"
where curl.exe >nul 2>&1 || goto :curl_missing
for /l %%A in (1,1,%READINESS_TIMEOUT%) do (
    curl.exe --silent --show-error --fail --connect-timeout 1 --max-time 2 "http://127.0.0.1:%HEALTH_PORT%/actuator/health" >nul 2>&1 && goto :health_ready
)
goto :health_failed

:health_ready
echo server started (mode=%~2)
exit /b 0

:health_failed
    call "%~f0" stop >nul 2>&1
    echo Error: server did not become healthy 1>&2
    exit /b 1

:curl_missing
echo Error: curl.exe is required for server readiness checks 1>&2
call "%~f0" stop >nul 2>&1
exit /b 1

:status
if not exist "%PID_FILE%" (
    echo server is stopped
    exit /b 3
)
set /p PID=<"%PID_FILE%"
powershell -NoProfile -Command "$p=Get-CimInstance Win32_Process -Filter 'ProcessId=%PID%' -OperationTimeoutSec 10 -ErrorAction SilentlyContinue; if ($p -and $p.CommandLine -like '*replicadb-server*.jar*') { exit 0 } else { exit 1 }"
if errorlevel 1 (
    del /q "%PID_FILE%" "%MODE_FILE%" 2>nul
    echo server is stopped
    exit /b 3
)
set /p MODE=<"%MODE_FILE%"
echo server is running (mode=%MODE% pid=%PID%)
exit /b 0

:stop
call "%~f0" status >nul 2>&1
if errorlevel 1 (
    del /q "%PID_FILE%" "%MODE_FILE%" 2>nul
    echo server is stopped
    exit /b 3
)
set /p PID=<"%PID_FILE%"
powershell -NoProfile -Command "$p=Get-CimInstance Win32_Process -Filter 'ProcessId=%PID%' -OperationTimeoutSec 10 -ErrorAction SilentlyContinue; if ($p -and $p.CommandLine -like '*replicadb-server*.jar*') { Stop-Process -Id %PID% -ErrorAction SilentlyContinue }"
del /q "%PID_FILE%" "%MODE_FILE%" 2>nul
echo server stopped
exit /b 0

:already_started
echo Error: server is already running 1>&2
exit /b 1

:missing_external
echo Error: %~2 requires DB_URL, DB_USERNAME and DB_PASSWORD 1>&2
exit /b 1

:java_missing
echo Error: Java 17 is required 1>&2
exit /b 1

:invalid
echo Usage: replicadb-server ^<command^> [mode] 1>&2
exit /b 2

:resolve_jar
set "SERVER_VERSION="
if exist "%PACKAGE_ROOT%\VERSION" set /p SERVER_VERSION=<"%PACKAGE_ROOT%\VERSION"
if not defined SERVER_VERSION set "SERVER_VERSION=%REPLICADB_SERVER_VERSION%"
if not defined SERVER_VERSION (
    echo Error: VERSION is missing from the server package 1>&2
    exit /b 1
)
set "JAR_FILE=%PACKAGE_ROOT%\lib\replicadb-server-%SERVER_VERSION%.jar"
if defined REPLICADB_SERVER_JAR set "JAR_FILE=%REPLICADB_SERVER_JAR%"
if not exist "%JAR_FILE%" (
    echo Error: server JAR is missing from the package 1>&2
    exit /b 1
)
exit /b 0
