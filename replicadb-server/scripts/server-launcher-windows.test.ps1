$ErrorActionPreference = 'Stop'
$launcher = Join-Path $PSScriptRoot '..\bin\replicadb-server.cmd'

$help = & $launcher help
if ($LASTEXITCODE -ne 0 -or -not (($help -join "`n") -match 'start local\|api\|worker')) {
    throw 'launcher help contract failed'
}

& $launcher start invalid *> $null
if ($LASTEXITCODE -ne 2) {
    throw 'invalid mode contract failed'
}

$testHome = Join-Path $env:TEMP ('replicadb-launcher-' + [guid]::NewGuid())
try {
    $env:REPLICADB_SERVER_HOME = $testHome
    & $launcher start api *> $null
    if ($LASTEXITCODE -ne 1) {
        throw 'missing external metadata contract failed'
    }
} finally {
    Remove-Item Env:REPLICADB_SERVER_HOME -ErrorAction SilentlyContinue
    Remove-Item $testHome -Recurse -Force -ErrorAction SilentlyContinue
}

Write-Output 'Windows server launcher contract checks passed'
exit 0
