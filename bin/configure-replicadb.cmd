:: Licensed to the Apache Software Foundation (ASF) under one or more
:: contributor license agreements.  See the NOTICE file distributed with
:: this work for additional information regarding copyright ownership.
:: The ASF licenses this file to You under the Apache License, Version 2.0
:: (the "License"); you may not use this file except in compliance with
:: the License.  You may obtain a copy of the License at
::
::
::     http://www.apache.org/licenses/LICENSE-2.0
::
:: Unless required by applicable law or agreed to in writing, software
:: distributed under the License is distributed on an "AS IS" BASIS,
:: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
:: See the License for the specific language governing permissions and
:: limitations under the License.

if not exist %bin% (
  echo Error: Environment variable bin not defined.
  echo This is generally because this script should not be invoked directly. Use replicadb instead.
  exit /b 1
)

if not defined REPLICADB_HOME (
  set REPLICADB_HOME=%bin%\..
)

if not defined REPLICADB_CONF_DIR (
  set REPLICADB_CONF_DIR=%REPLICADB_HOME%\conf
)

:: Add replicadb dependencies to classpath
set REPLICADB_CLASSPATH=

:: Where to find the main ReplicaDB jar
set REPLICADB_JAR_DIR=%REPLICADB_HOME%
call :add_dir_to_classpath %REPLICADB_JAR_DIR%

if exist "%REPLICADB_HOME%\lib" (
  call :add_dir_to_classpath %REPLICADB_HOME%\lib
)

call :add_dir_to_classpath %REPLICADB_CONF_DIR%

:: -----------------------------------------------------------------------------
::  Set JAVA_HOME or JRE_HOME if not already set
:: -----------------------------------------------------------------------------

:: Make sure prerequisite environment variables are set
if not "%JRE_HOME%" == "" goto gotJreHome
if not "%JAVA_HOME%" == "" goto gotJavaHome
echo Neither the JAVA_HOME nor the JRE_HOME environment variable is defined
echo At least one of these environment variable is needed to run this program
goto exit

:gotJavaHome
:: No JRE given, use JAVA_HOME as JRE_HOME
set "JRE_HOME=%JAVA_HOME%"

:gotJreHome
:: Check if we have a usable JRE
if not exist "%JRE_HOME%\bin\java.exe" goto noJreHome
goto okJava

:noJreHome
rem Needed at least a JRE
echo The JRE_HOME environment variable is not defined correctly
echo This environment variable is needed to run this program
goto exit

:okJava
:: Don't override _RUNJAVA if the user has set it previously
if not "%_RUNJAVA%" == "" goto gotRunJava
:: Set standard command for invoking Java.
:: Also note the quoting as JRE_HOME may contain spaces.
set _RUNJAVA="%JRE_HOME%\bin\java.exe"
:gotRunJava

goto :eof


:: -----------------------------------------------------------------------------
:: Function to add the given directory to the list of classpath directories
:: All jars under the given directory are added to the classpath
:: -----------------------------------------------------------------------------
:add_dir_to_classpath
if not "%1"=="" (
  set REPLICADB_CLASSPATH=!REPLICADB_CLASSPATH!;%1\*
)
goto :eof

