Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Resolve-ToolPath {
    param(
        [Parameter(Mandatory = $true)][string]$CommandName,
        [Parameter(Mandatory = $true)][string[]]$FallbackPaths
    )

    $cmd = Get-Command $CommandName -ErrorAction SilentlyContinue
    if ($cmd -and $cmd.Source) {
        return $cmd.Source
    }
    foreach ($candidate in $FallbackPaths) {
        if (Test-Path -LiteralPath $candidate) {
            return $candidate
        }
    }
    return $null
}

function Import-DotEnv {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        throw "Missing .env file: $Path"
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

function Ensure-EnvDefault {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Value
    )
    $current = [System.Environment]::GetEnvironmentVariable($Name, "Process")
    if ([string]::IsNullOrWhiteSpace($current)) {
        [System.Environment]::SetEnvironmentVariable($Name, $Value, "Process")
    }
}

function Stop-ListenerOnPort {
    param([Parameter(Mandatory = $true)][int]$Port)
    $netstatPath = Resolve-ToolPath -CommandName "netstat" -FallbackPaths @("C:\Windows\System32\netstat.exe")
    if (-not $netstatPath) {
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
        return
    }
    $oldPidRaw = Get-Content -LiteralPath $PidFilePath -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($oldPidRaw -match '^\d+$') {
        $oldProc = Get-Process -Id ([int]$oldPidRaw) -ErrorAction SilentlyContinue
        if ($oldProc) {
            Write-Host "Stopping old $Name process PID=$oldPidRaw ..."
            Stop-Process -Id ([int]$oldPidRaw) -Force
        }
    }
    Remove-Item -LiteralPath $PidFilePath -Force -ErrorAction SilentlyContinue
}

function Start-ManagedProcess {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$PythonPath,
        [Parameter(Mandatory = $true)][string]$Arguments,
        [Parameter(Mandatory = $true)][string]$WorkDir,
        [Parameter(Mandatory = $true)][string]$PidFilePath,
        [Parameter(Mandatory = $true)][string]$StdoutLogPath,
        [Parameter(Mandatory = $true)][string]$StderrLogPath
    )

    Stop-ProcessByPidFile -PidFilePath $PidFilePath -Name $Name
    if (Test-Path -LiteralPath $StdoutLogPath) { Remove-Item -LiteralPath $StdoutLogPath -Force }
    if (Test-Path -LiteralPath $StderrLogPath) { Remove-Item -LiteralPath $StderrLogPath -Force }

    $proc = Start-Process -FilePath $PythonPath `
        -ArgumentList $Arguments `
        -WorkingDirectory $WorkDir `
        -RedirectStandardOutput $StdoutLogPath `
        -RedirectStandardError $StderrLogPath `
        -PassThru

    $proc.Id | Set-Content -LiteralPath $PidFilePath -NoNewline
    Start-Sleep -Seconds 2
    if ($proc.HasExited) {
        throw "$Name process exited early with code $($proc.ExitCode). Check logs: $StdoutLogPath and $StderrLogPath"
    }
    return $proc
}

function Test-PythonModule {
    param(
        [Parameter(Mandatory = $true)][string]$PythonPath,
        [Parameter(Mandatory = $true)][string]$ModuleName
    )
    $probeCode = "import importlib.util, sys; sys.exit(0 if importlib.util.find_spec('$ModuleName') else 1)"
    $oldNativePref = $null
    $hasNativePref = $false
    try {
        if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
            $hasNativePref = $true
            $oldNativePref = $PSNativeCommandUseErrorActionPreference
            $PSNativeCommandUseErrorActionPreference = $false
        }
        & $PythonPath -c $probeCode | Out-Null
        return ($LASTEXITCODE -eq 0)
    } finally {
        if ($hasNativePref) {
            $PSNativeCommandUseErrorActionPreference = $oldNativePref
        }
    }
}

function Normalize-PathVariables {
    $pathUpper = [System.Environment]::GetEnvironmentVariable("PATH", "Process")
    $pathMixed = [System.Environment]::GetEnvironmentVariable("Path", "Process")
    if ($null -ne $pathUpper -and $null -ne $pathMixed) {
        $parts = @()
        foreach ($candidate in @($pathUpper, $pathMixed)) {
            if ([string]::IsNullOrWhiteSpace($candidate)) {
                continue
            }
            foreach ($segment in ($candidate -split ';')) {
                $trimmed = $segment.Trim()
                if ([string]::IsNullOrWhiteSpace($trimmed)) {
                    continue
                }
                if ($parts -notcontains $trimmed) {
                    $parts += $trimmed
                }
            }
        }
        $effective = ($parts -join ';')
        [System.Environment]::SetEnvironmentVariable("Path", $effective, "Process")
        [System.Environment]::SetEnvironmentVariable("PATH", $null, "Process")
        Remove-Item Env:PATH -ErrorAction SilentlyContinue
    }
}

function Normalize-ProxyVariables {
    $proxyVars = @(
        "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
        "http_proxy", "https_proxy", "all_proxy",
        "GIT_HTTP_PROXY", "GIT_HTTPS_PROXY"
    )
    foreach ($name in $proxyVars) {
        $value = [System.Environment]::GetEnvironmentVariable($name, "Process")
        if ($null -ne $value -and $value -match "127\.0\.0\.1:9") {
            [System.Environment]::SetEnvironmentVariable($name, $null, "Process")
            Remove-Item "Env:$name" -ErrorAction SilentlyContinue
        }
    }
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$composeFile = Join-Path $repoRoot "examples\fastapi_demo\docker-compose.yml"
$python = Join-Path $repoRoot "venv\Scripts\python.exe"
$envFile = Join-Path $repoRoot ".env"
$runDir = Join-Path $repoRoot ".run"
$pidFile = Join-Path $runDir "fastapi_demo.pid"
$stdoutLog = Join-Path $runDir "fastapi_demo.stdout.log"
$stderrLog = Join-Path $runDir "fastapi_demo.stderr.log"
$orchestratorPidFile = Join-Path $runDir "orchestrator.pid"
$orchestratorStdoutLog = Join-Path $runDir "orchestrator.stdout.log"
$orchestratorStderrLog = Join-Path $runDir "orchestrator.stderr.log"
$botPidFile = Join-Path $runDir "bot.pid"
$botStdoutLog = Join-Path $runDir "bot.stdout.log"
$botStderrLog = Join-Path $runDir "bot.stderr.log"
$sheetsPidFile = Join-Path $runDir "sheets_sync.pid"
$sheetsStdoutLog = Join-Path $runDir "sheets_sync.stdout.log"
$sheetsStderrLog = Join-Path $runDir "sheets_sync.stderr.log"

if (-not (Test-Path -LiteralPath $python)) {
    throw "Python from venv not found: $python"
}

$dockerBin = Resolve-ToolPath -CommandName "docker" -FallbackPaths @(
    "C:\Program Files\Docker\Docker\resources\bin\docker.exe"
)
if (-not $dockerBin) {
    throw "Docker is not installed or not in PATH. Install Docker Desktop or set DOCKER_BIN in .env."
}
[System.Environment]::SetEnvironmentVariable("DOCKER_BIN", $dockerBin, "Process")

Import-DotEnv -Path $envFile
Normalize-PathVariables
Normalize-ProxyVariables

Ensure-EnvDefault -Name "API_KEY" -Value "local"
Ensure-EnvDefault -Name "BASE_URL" -Value "http://127.0.0.1:11434/v1"
Ensure-EnvDefault -Name "EMBEDDING_BASE_URL" -Value "http://127.0.0.1:11434/v1"
Ensure-EnvDefault -Name "LLM_MODEL_NAME" -Value "qwen2.5:3b"
Ensure-EnvDefault -Name "EMBEDDER_MODEL_NAME" -Value "nomic-embed-text"
Ensure-EnvDefault -Name "MEMGRAPH_URI" -Value "bolt://127.0.0.1:7687"
Ensure-EnvDefault -Name "OLLAMA_AUTO_PULL" -Value "false"
Ensure-EnvDefault -Name "MEMGRAPH_LAB_ENABLED" -Value "true"
$disableLlm = ([System.Environment]::GetEnvironmentVariable("DISABLE_LLM_ANSWERS", "Process") + "").ToLowerInvariant() -in @("1","true","yes","on")
$autoPullModels = ([System.Environment]::GetEnvironmentVariable("OLLAMA_AUTO_PULL", "Process") + "").ToLowerInvariant() -in @("1","true","yes","on")
$memgraphLabEnabled = ([System.Environment]::GetEnvironmentVariable("MEMGRAPH_LAB_ENABLED", "Process") + "").ToLowerInvariant() -in @("1","true","yes","on")
$startupTimeoutRaw = [System.Environment]::GetEnvironmentVariable("FASTAPI_START_TIMEOUT_SEC", "Process")
if ([string]::IsNullOrWhiteSpace($startupTimeoutRaw)) {
    $startupTimeout = 120
} else {
    try {
        $startupTimeout = [int]$startupTimeoutRaw
    } catch {
        $startupTimeout = 120
    }
}
if ($startupTimeout -lt 10) { $startupTimeout = 120 }

if (-not $disableLlm) {
    $ollamaBin = Resolve-ToolPath -CommandName "ollama" -FallbackPaths @(
        (Join-Path $env:LOCALAPPDATA "Programs\Ollama\ollama.exe"),
        "C:\Program Files\Ollama\ollama.exe"
    )
    if (-not $ollamaBin) {
        throw "Ollama is not installed or not in PATH. Install Ollama or set OLLAMA_BIN in .env."
    }
    [System.Environment]::SetEnvironmentVariable("OLLAMA_BIN", $ollamaBin, "Process")
    Write-Host "Checking Ollama models..."
    $modelsRaw = & $ollamaBin list
    $llmModel = [System.Environment]::GetEnvironmentVariable("LLM_MODEL_NAME", "Process")
    $embModel = [System.Environment]::GetEnvironmentVariable("EMBEDDER_MODEL_NAME", "Process")
    $hasLlm = $null -ne ($modelsRaw | Select-String -SimpleMatch "$llmModel")
    $hasEmb = $null -ne ($modelsRaw | Select-String -SimpleMatch "$embModel")

    if (-not $hasLlm -or -not $hasEmb) {
        if (-not $autoPullModels) {
            throw "Required Ollama models are missing (LLM='$llmModel', EMB='$embModel'). Set OLLAMA_AUTO_PULL=true or run: ollama pull $llmModel ; ollama pull $embModel"
        }
    }

    if (-not $hasLlm) {
        Write-Host "Pulling LLM model $llmModel ..."
        & $ollamaBin pull $llmModel | Out-Host
    }
    if (-not $hasEmb) {
        Write-Host "Pulling embedder model $embModel ..."
        & $ollamaBin pull $embModel | Out-Host
    }
} else {
    Write-Host "DISABLE_LLM_ANSWERS=true -> skip Ollama checks."
}

Write-Host "Starting Memgraph..."
if ($memgraphLabEnabled) {
    Write-Host "Memgraph Lab is enabled -> attempting to start visual UI on http://127.0.0.1:3000 ..."
}
try {
    if ($memgraphLabEnabled) {
        try {
            & $dockerBin compose -f $composeFile up -d memgraph memgraph-lab | Out-Host
        } catch {
            Write-Host "Failed to start memgraph-lab. Starting Memgraph without UI. Error: $($_.Exception.Message)"
            & $dockerBin compose -f $composeFile up -d memgraph | Out-Host
        }
    } else {
        & $dockerBin compose -f $composeFile up -d memgraph | Out-Host
    }
} catch {
    throw "Failed to start Memgraph via Docker. Check Docker Desktop and permissions. Original error: $($_.Exception.Message)"
}

if (-not (Test-Path -LiteralPath $runDir)) {
    New-Item -ItemType Directory -Path $runDir | Out-Null
}

Write-Host "Starting FastAPI service..."
# Ensure port is free so readiness probes target the new process.
Stop-ListenerOnPort -Port 8000

$proc = Start-ManagedProcess `
    -Name "FastAPI" `
    -PythonPath $python `
    -Arguments "examples/fastapi_demo/server.py" `
    -WorkDir $repoRoot `
    -PidFilePath $pidFile `
    -StdoutLogPath $stdoutLog `
    -StderrLogPath $stderrLog

$ready = $false
for ($i = 0; $i -lt $startupTimeout; $i++) {
    if ($proc.HasExited) {
        throw "FastAPI process exited early with code $($proc.ExitCode). Check logs: $stdoutLog and $stderrLog"
    }
    Start-Sleep -Seconds 1
    try {
        $status = Invoke-RestMethod -Uri "http://127.0.0.1:8000/status" -Method Get -TimeoutSec 2
        if ($null -ne $status.is_indexing) {
            $ready = $true
            break
        }
    } catch {
        # keep waiting
    }
}

if (-not $ready) {
    throw "API did not become ready in time. Check logs: $stdoutLog and $stderrLog"
}

Write-Host "Starting orchestrator worker..."
Start-ManagedProcess `
    -Name "Orchestrator" `
    -PythonPath $python `
    -Arguments "-m apps.orchestrator.main" `
    -WorkDir $repoRoot `
    -PidFilePath $orchestratorPidFile `
    -StdoutLogPath $orchestratorStdoutLog `
    -StderrLogPath $orchestratorStderrLog | Out-Null

Write-Host "Starting sheets sync worker..."
Start-ManagedProcess `
    -Name "SheetsSync" `
    -PythonPath $python `
    -Arguments "-m apps.sheets_sync.main" `
    -WorkDir $repoRoot `
    -PidFilePath $sheetsPidFile `
    -StdoutLogPath $sheetsStdoutLog `
    -StderrLogPath $sheetsStderrLog | Out-Null

$tgToken = [System.Environment]::GetEnvironmentVariable("TELEGRAM_BOT_TOKEN", "Process")
if ([string]::IsNullOrWhiteSpace($tgToken)) {
    Write-Host "TELEGRAM_BOT_TOKEN is empty -> skip bot startup."
} else {
    if (-not (Test-PythonModule -PythonPath $python -ModuleName "aiogram")) {
        Write-Host "aiogram is not installed in venv -> skip bot startup."
        Remove-Item -LiteralPath $botPidFile -Force -ErrorAction SilentlyContinue
    } else {
        Write-Host "Starting telegram bot..."
        Start-ManagedProcess `
            -Name "TelegramBot" `
            -PythonPath $python `
            -Arguments "-m apps.bot.main" `
            -WorkDir $repoRoot `
            -PidFilePath $botPidFile `
            -StdoutLogPath $botStdoutLog `
            -StderrLogPath $botStderrLog | Out-Null
    }
}

Write-Host ""
Write-Host "Stack is ready."
Write-Host "API docs: http://127.0.0.1:8000/docs"
Write-Host "Status:   http://127.0.0.1:8000/status"
if ($memgraphLabEnabled) {
    Write-Host "Memgraph UI: http://127.0.0.1:3000"
}
Write-Host "Memgraph Bolt: bolt://127.0.0.1:7687"
Write-Host "PID file: $pidFile"
Write-Host "Logs:     $stdoutLog / $stderrLog"
Write-Host "Orchestrator PID: $orchestratorPidFile"
Write-Host "Orchestrator logs: $orchestratorStdoutLog / $orchestratorStderrLog"
Write-Host "Sheets PID: $sheetsPidFile"
Write-Host "Sheets logs: $sheetsStdoutLog / $sheetsStderrLog"
Write-Host "Bot PID: $botPidFile"
Write-Host "Bot logs: $botStdoutLog / $botStderrLog"
$sheetId = [System.Environment]::GetEnvironmentVariable("GOOGLE_SHEETS_SPREADSHEET_ID", "Process")
if (-not [string]::IsNullOrWhiteSpace($sheetId)) {
    Write-Host "Google Sheets: https://docs.google.com/spreadsheets/d/$sheetId/edit"
}
