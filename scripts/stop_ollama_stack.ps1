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

function Import-DotEnv {
    param([Parameter(Mandatory = $true)][string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        return
    }
    Get-Content -LiteralPath $Path | ForEach-Object {
        $line = $_.Trim()
        if ([string]::IsNullOrWhiteSpace($line) -or $line.StartsWith("#")) {
            return
        }
        $match = [regex]::Match($line, '^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$')
        if (-not $match.Success) {
            return
        }
        $key = $match.Groups[1].Value
        $value = $match.Groups[2].Value.Trim()
        if (($value.StartsWith('"') -and $value.EndsWith('"')) -or ($value.StartsWith("'") -and $value.EndsWith("'"))) {
            $value = $value.Substring(1, $value.Length - 2)
        }
        [System.Environment]::SetEnvironmentVariable($key, $value, "Process")
    }
}

function Test-DockerReady {
    param([Parameter(Mandatory = $true)][string]$DockerPath)
    try {
        & $DockerPath version *> $null
        return ($LASTEXITCODE -eq 0)
    } catch {
        return $false
    }
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

function Get-ChildProcessIds {
    param([Parameter(Mandatory = $true)][int]$ParentPid)
    $children = @(Get-CimInstance Win32_Process -Filter "ParentProcessId = $ParentPid" -ErrorAction SilentlyContinue)
    foreach ($child in $children) {
        [int]$child.ProcessId
        Get-ChildProcessIds -ParentPid ([int]$child.ProcessId)
    }
}

function Stop-ProcessTreeByPidFile {
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
        $rootPid = [int]$pidRaw
        $processIds = @(Get-ChildProcessIds -ParentPid $rootPid) + @($rootPid)
        foreach ($processId in ($processIds | Select-Object -Unique | Sort-Object -Descending)) {
            $proc = Get-Process -Id $processId -ErrorAction SilentlyContinue
            if ($proc) {
                try {
                    Write-Host "Stopping $Name process PID=$processId ..."
                    Stop-Process -Id $processId -Force -ErrorAction Stop
                } catch {
                    Write-Host "Could not stop $Name PID=${processId}: $($_.Exception.Message)"
                }
            }
        }
    }
    Remove-Item -LiteralPath $PidFilePath -Force -ErrorAction SilentlyContinue
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$composeFile = Join-Path $repoRoot "examples\fastapi_demo\docker-compose.yml"
$runDir = Join-Path $repoRoot ".run"
$envFile = Join-Path $repoRoot ".env"
Import-DotEnv -Path $envFile

Stop-ProcessTreeByPidFile -PidFilePath (Join-Path $runDir "bot.pid") -Name "TelegramBot"
Stop-ProcessTreeByPidFile -PidFilePath (Join-Path $runDir "vk_bot.pid") -Name "VkBot"
Stop-ProcessTreeByPidFile -PidFilePath (Join-Path $runDir "sheets_sync.pid") -Name "SheetsSync"
Stop-ProcessTreeByPidFile -PidFilePath (Join-Path $runDir "orchestrator.pid") -Name "Orchestrator"
Stop-ProcessTreeByPidFile -PidFilePath (Join-Path $runDir "fastapi_demo.pid") -Name "FastAPI"

Stop-ListenerOnPort -Port 8000

Write-Host "Stopping Memgraph containers..."
$dockerBin = Resolve-DockerPath
if ($dockerBin -and (Test-DockerReady -DockerPath $dockerBin)) {
    try {
        & $dockerBin compose -f $composeFile stop memgraph memgraph-lab | Out-Host
        if ($LASTEXITCODE -ne 0) {
            Write-Host "Docker compose stop returned exit code $LASTEXITCODE."
        }
    } catch {
        Write-Host "Docker stop returned an error: $($_.Exception.Message)"
    }
} elseif ($dockerBin) {
    Write-Host "Docker CLI found, but Docker API is not reachable. Skip container stop."
} else {
    Write-Host "Docker CLI not found. Skip Memgraph stop."
}

$stopOllama = ([System.Environment]::GetEnvironmentVariable("STOP_OLLAMA_ON_STOP", "Process") + "").ToLowerInvariant() -in @("1","true","yes","on")
if ($stopOllama) {
    Get-Process | Where-Object { $_.ProcessName -like "*ollama*" } | Stop-Process -Force -ErrorAction SilentlyContinue
    Write-Host "Ollama processes stopped because STOP_OLLAMA_ON_STOP=true."
}

Write-Host "Done."
