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

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$composeFile = Join-Path $repoRoot "examples\fastapi_demo\docker-compose.yml"
$python = Join-Path $repoRoot "venv\Scripts\python.exe"
$envFile = Join-Path $repoRoot ".env"
$runDir = Join-Path $repoRoot ".run"
$pidFile = Join-Path $runDir "fastapi_demo.pid"
$stdoutLog = Join-Path $runDir "fastapi_demo.stdout.log"
$stderrLog = Join-Path $runDir "fastapi_demo.stderr.log"

if (-not (Test-Path -LiteralPath $python)) {
    throw "Python from venv not found: $python"
}

$ollamaBin = Resolve-ToolPath -CommandName "ollama" -FallbackPaths @(
    (Join-Path $env:LOCALAPPDATA "Programs\Ollama\ollama.exe"),
    "C:\Program Files\Ollama\ollama.exe"
)
if (-not $ollamaBin) {
    throw "Ollama is not installed or not in PATH. Install Ollama or set OLLAMA_BIN in .env."
}
[System.Environment]::SetEnvironmentVariable("OLLAMA_BIN", $ollamaBin, "Process")

$dockerBin = Resolve-ToolPath -CommandName "docker" -FallbackPaths @(
    "C:\Program Files\Docker\Docker\resources\bin\docker.exe"
)
if (-not $dockerBin) {
    throw "Docker is not installed or not in PATH. Install Docker Desktop or set DOCKER_BIN in .env."
}
[System.Environment]::SetEnvironmentVariable("DOCKER_BIN", $dockerBin, "Process")

Import-DotEnv -Path $envFile

Ensure-EnvDefault -Name "API_KEY" -Value "local"
Ensure-EnvDefault -Name "BASE_URL" -Value "http://127.0.0.1:11434/v1"
Ensure-EnvDefault -Name "EMBEDDING_BASE_URL" -Value "http://127.0.0.1:11434/v1"
Ensure-EnvDefault -Name "LLM_MODEL_NAME" -Value "qwen2.5:3b"
Ensure-EnvDefault -Name "EMBEDDER_MODEL_NAME" -Value "nomic-embed-text"
Ensure-EnvDefault -Name "MEMGRAPH_URI" -Value "bolt://127.0.0.1:7687"

Write-Host "Checking Ollama models..."
$modelsRaw = & $ollamaBin list
$llmModel = [System.Environment]::GetEnvironmentVariable("LLM_MODEL_NAME", "Process")
$embModel = [System.Environment]::GetEnvironmentVariable("EMBEDDER_MODEL_NAME", "Process")

if (-not ($modelsRaw | Select-String -SimpleMatch "$llmModel")) {
    Write-Host "Pulling LLM model $llmModel ..."
    & $ollamaBin pull $llmModel | Out-Host
}
if (-not ($modelsRaw | Select-String -SimpleMatch "$embModel")) {
    Write-Host "Pulling embedder model $embModel ..."
    & $ollamaBin pull $embModel | Out-Host
}

Write-Host "Starting Memgraph..."
try {
    & $dockerBin compose -f $composeFile up -d memgraph | Out-Host
} catch {
    throw "Failed to start Memgraph via Docker. Check Docker Desktop and permissions. Original error: $($_.Exception.Message)"
}

if (-not (Test-Path -LiteralPath $runDir)) {
    New-Item -ItemType Directory -Path $runDir | Out-Null
}

if (Test-Path -LiteralPath $pidFile) {
    $oldPidRaw = Get-Content -LiteralPath $pidFile -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($oldPidRaw -match '^\d+$') {
        $oldProc = Get-Process -Id ([int]$oldPidRaw) -ErrorAction SilentlyContinue
        if ($oldProc) {
            Write-Host "Stopping old API process PID=$oldPidRaw ..."
            Stop-Process -Id ([int]$oldPidRaw) -Force
        }
    }
}

if (Test-Path -LiteralPath $stdoutLog) { Remove-Item -LiteralPath $stdoutLog -Force }
if (Test-Path -LiteralPath $stderrLog) { Remove-Item -LiteralPath $stderrLog -Force }

Write-Host "Starting FastAPI service..."
$cmd = "`"$python`" examples/fastapi_demo/server.py > `"$stdoutLog`" 2> `"$stderrLog`""
$proc = Start-Process -FilePath "cmd.exe" `
    -ArgumentList "/c", $cmd `
    -WorkingDirectory $repoRoot `
    -PassThru

$proc.Id | Set-Content -LiteralPath $pidFile -NoNewline

$ready = $false
for ($i = 0; $i -lt 90; $i++) {
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

Write-Host ""
Write-Host "Stack is ready."
Write-Host "API docs: http://127.0.0.1:8000/docs"
Write-Host "Status:   http://127.0.0.1:8000/status"
Write-Host "PID file: $pidFile"
Write-Host "Logs:     $stdoutLog / $stderrLog"
