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

function Stop-ListenerOnPort {
    param([Parameter(Mandatory = $true)][int]$Port)
    $netstatPath = "C:\Windows\System32\netstat.exe"
    if (-not (Test-Path -LiteralPath $netstatPath)) {
        Write-Host "netstat is not available, skip port listener cleanup for $Port."
        return
    }
    $lines = & $netstatPath -ano | Select-String ":$Port" | Select-String "LISTENING"
    foreach ($line in $lines) {
        $parts = ($line.ToString() -split '\s+') | Where-Object { $_ -ne "" }
        if ($parts.Count -gt 0) {
            $targetPid = $parts[-1]
            if ($targetPid -match '^\d+$') {
                try {
                    Stop-Process -Id ([int]$targetPid) -Force -ErrorAction Stop
                    Write-Host "Stopped process on port $Port (PID=$targetPid)."
                } catch {
                    Write-Host "Could not stop PID=$targetPid on port ${Port}: $($_.Exception.Message)"
                }
            }
        }
    }
}

function Stop-ProcessByPidFile {
    param(
        [Parameter(Mandatory = $true)][string]$PidFilePath,
        [Parameter(Mandatory = $true)][string]$Name
    )
    if (-not (Test-Path -LiteralPath $PidFilePath)) {
        Write-Host "$Name PID file not found."
        return
    }

    $pidRaw = Get-Content -LiteralPath $PidFilePath -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($pidRaw -match '^\d+$') {
        $proc = Get-Process -Id ([int]$pidRaw) -ErrorAction SilentlyContinue
        if ($proc) {
            try {
                Write-Host "Stopping $Name process PID=$pidRaw ..."
                Stop-Process -Id ([int]$pidRaw) -Force -ErrorAction Stop
            } catch {
                Write-Host "Could not stop $Name PID=${pidRaw}: $($_.Exception.Message)"
            }
        } else {
            Write-Host "$Name process not running (PID file existed)."
        }
    }
    Remove-Item -LiteralPath $PidFilePath -Force -ErrorAction SilentlyContinue
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$composeFile = Join-Path $repoRoot "examples\fastapi_demo\docker-compose.yml"
$runDir = Join-Path $repoRoot ".run"

Stop-ProcessByPidFile -PidFilePath (Join-Path $runDir "bot.pid") -Name "TelegramBot"
Stop-ProcessByPidFile -PidFilePath (Join-Path $runDir "sheets_sync.pid") -Name "SheetsSync"
Stop-ProcessByPidFile -PidFilePath (Join-Path $runDir "orchestrator.pid") -Name "Orchestrator"
Stop-ProcessByPidFile -PidFilePath (Join-Path $runDir "fastapi_demo.pid") -Name "FastAPI"

Stop-ListenerOnPort -Port 8000

Write-Host "Stopping Memgraph container..."
$dockerBin = Resolve-DockerPath
if ($dockerBin) {
    try {
        & $dockerBin compose -f $composeFile stop memgraph memgraph-lab | Out-Host
    } catch {
        Write-Host "Docker stop returned an error: $($_.Exception.Message)"
    }
} else {
    Write-Host "Docker CLI not found. Skip Memgraph stop."
}

Write-Host "Done."
