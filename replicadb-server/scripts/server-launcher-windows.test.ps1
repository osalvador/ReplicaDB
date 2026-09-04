$ErrorActionPreference = 'Stop'
$launcher = Join-Path $PSScriptRoot '..\bin\replicadb-server.cmd'

$help = & $launcher help
if ($LASTEXITCODE -ne 0 -or ($help -notmatch 'start local\|api\|worker')) {
    throw 'launcher help contract failed'
}

& $launcher start invalid *> $null
if ($LASTEXITCODE -ne 2) {
    throw 'invalid mode contract failed'
}

$home = Join-Path $env:TEMP ('replicadb-launcher-' + [guid]::NewGuid())
try {
    $env:REPLICADB_SERVER_HOME = $home
    & $launcher start api *> $null
    if ($LASTEXITCODE -ne 1) {
        throw 'missing external metadata contract failed'
    }
} finally {
    Remove-Item Env:REPLICADB_SERVER_HOME -ErrorAction SilentlyContinue
    Remove-Item $home -Recurse -Force -ErrorAction SilentlyContinue
}

Write-Output 'Windows server launcher contract checks passed'
