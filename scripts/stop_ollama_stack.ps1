Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Resolve-DockerPath {
    $cmd = Get-Command docker -ErrorAction SilentlyContinue
    if ($cmd -and $cmd.Source) {
        return $cmd.Source
    }
    $fallback = "C:\Program Files\Docker\Docker\resources\bin\docker.exe"
    if (Test-Path -LiteralPath $fallback) {
        return $fallback
    }
    return $null
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$composeFile = Join-Path $repoRoot "examples\fastapi_demo\docker-compose.yml"
$pidFile = Join-Path $repoRoot ".run\fastapi_demo.pid"

if (Test-Path -LiteralPath $pidFile) {
    $pidRaw = Get-Content -LiteralPath $pidFile -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($pidRaw -match '^\d+$') {
        $proc = Get-Process -Id ([int]$pidRaw) -ErrorAction SilentlyContinue
        if ($proc) {
            Write-Host "Stopping FastAPI process PID=$pidRaw ..."
            Stop-Process -Id ([int]$pidRaw) -Force
        } else {
            Write-Host "FastAPI process not running (PID file existed)."
        }
    }
    Remove-Item -LiteralPath $pidFile -Force -ErrorAction SilentlyContinue
} else {
    Write-Host "PID file not found. FastAPI may already be stopped."
}

Write-Host "Stopping Memgraph container..."
$dockerBin = Resolve-DockerPath
if ($dockerBin) {
    & $dockerBin compose -f $composeFile stop memgraph | Out-Host
} else {
    Write-Host "Docker CLI not found. Skip Memgraph stop."
}

Write-Host "Done."
