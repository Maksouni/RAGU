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
    param([Parameter(Mandatory = $true)][string]$Path)
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

function Test-DockerReady {
    param([Parameter(Mandatory = $true)][string]$DockerPath)
    try {
        & $DockerPath version *> $null
        return ($LASTEXITCODE -eq 0)
    } catch {
        return $false
    }
}

function Ensure-DockerReady {
    param(
        [Parameter(Mandatory = $true)][string]$DockerPath,
        [int]$TimeoutSec = 180
    )
    if (Test-DockerReady -DockerPath $DockerPath) {
        return
    }

    $dockerDesktop = "C:\Program Files\Docker\Docker\Docker Desktop.exe"
    if (Test-Path -LiteralPath $dockerDesktop) {
        Write-Host "Docker API is not ready, starting Docker Desktop..."
        Start-Process -FilePath $dockerDesktop -WindowStyle Hidden | Out-Null
    }

    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        if (Test-DockerReady -DockerPath $DockerPath) {
            return
        }
        Start-Sleep -Seconds 3
    }
    throw "Docker API is not reachable. Start Docker Desktop manually and rerun the script."
}

function Get-OllamaTags {
    param([Parameter(Mandatory = $true)][string]$ApiRoot)
    try {
        $resp = Invoke-RestMethod -Uri "$ApiRoot/api/tags" -Method Get -TimeoutSec 4
        if ($null -eq $resp.models) {
            return @()
        }
        return @($resp.models | ForEach-Object { "$($_.name)" })
    } catch {
        return @()
    }
}

function Test-OllamaModelPresent {
    param(
        [Parameter(Mandatory = $true)][string]$ModelName,
        [Parameter(Mandatory = $true)][string[]]$Tags
    )
    if ($Tags -contains $ModelName) {
        return $true
    }
    if ($ModelName.Contains(":")) {
        return $false
    }
    return $null -ne ($Tags | Where-Object { $_ -eq "$ModelName`:latest" })
}

function Ensure-OllamaApiReady {
    param(
        [Parameter(Mandatory = $true)][string]$OllamaBin,
        [Parameter(Mandatory = $true)][string]$ApiRoot
    )
    $tags = Get-OllamaTags -ApiRoot $ApiRoot
    if (@($tags).Count -gt 0) {
        return
    }

    Write-Host "Ollama API is not ready, starting ollama serve..."
    Start-Process -FilePath $OllamaBin -ArgumentList "serve" -WindowStyle Hidden | Out-Null
    for ($i = 0; $i -lt 45; $i++) {
        Start-Sleep -Seconds 1
        $tags = Get-OllamaTags -ApiRoot $ApiRoot
        if (@($tags).Count -gt 0) {
            return
        }
    }
    throw "Ollama API is not reachable at $ApiRoot/api/tags."
}

function Wait-HttpStatus {
    param(
        [Parameter(Mandatory = $true)][string]$Url,
        [int]$TimeoutSec = 120
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        try {
            $status = Invoke-RestMethod -Uri $Url -Method Get -TimeoutSec 2
            if ($null -ne $status.is_indexing) {
                return
            }
        } catch {
            Start-Sleep -Seconds 1
        }
    }
    throw "API did not become ready in time: $Url"
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$composeFile = Join-Path $repoRoot "examples\fastapi_demo\docker-compose.yml"
$envFile = Join-Path $repoRoot ".env"

$dockerBin = Resolve-ToolPath -CommandName "docker" -FallbackPaths @(
    "C:\Program Files\Docker\Docker\resources\bin\docker.exe"
)
if (-not $dockerBin) {
    throw "Docker is not installed or not in PATH."
}

$ollamaBin = Resolve-ToolPath -CommandName "ollama" -FallbackPaths @(
    (Join-Path $env:LOCALAPPDATA "Programs\Ollama\ollama.exe"),
    "C:\Program Files\Ollama\ollama.exe"
)
if (-not $ollamaBin) {
    throw "Ollama is not installed or not in PATH."
}

Import-DotEnv -Path $envFile
Ensure-EnvDefault -Name "LLM_PROVIDER" -Value "ollama"
Ensure-EnvDefault -Name "API_KEY" -Value "local"
Ensure-EnvDefault -Name "OLLAMA_HOST_BASE_URL" -Value "http://127.0.0.1:11434"
Ensure-EnvDefault -Name "OLLAMA_BASE_URL" -Value "http://host.docker.internal:11434/v1"
Ensure-EnvDefault -Name "LLM_MODEL_NAME" -Value "qwen2.5:3b"
Ensure-EnvDefault -Name "EMBEDDER_MODEL_NAME" -Value "nomic-embed-text"
Ensure-EnvDefault -Name "EMBEDDING_DIM" -Value "20"
Ensure-EnvDefault -Name "OLLAMA_AUTO_PULL" -Value "false"
Ensure-EnvDefault -Name "MEMGRAPH_LAB_ENABLED" -Value "true"
Ensure-EnvDefault -Name "VK_BOT_ENABLED" -Value "false"
Ensure-EnvDefault -Name "SHEETS_SYNC_ENABLED" -Value "false"
Ensure-EnvDefault -Name "FASTAPI_START_TIMEOUT_SEC" -Value "120"

$provider = ([System.Environment]::GetEnvironmentVariable("LLM_PROVIDER", "Process") + "").Trim().ToLowerInvariant()
if ($provider -ne "ollama") {
    throw "Unsupported LLM_PROVIDER. Use ollama."
}

Ensure-DockerReady -DockerPath $dockerBin

$ollamaApiRoot = ([System.Environment]::GetEnvironmentVariable("OLLAMA_HOST_BASE_URL", "Process") + "").Trim().TrimEnd("/")
Ensure-OllamaApiReady -OllamaBin $ollamaBin -ApiRoot $ollamaApiRoot

$disableLlm = ([System.Environment]::GetEnvironmentVariable("DISABLE_LLM_ANSWERS", "Process") + "").ToLowerInvariant() -in @("1","true","yes","on")
$autoPullModels = ([System.Environment]::GetEnvironmentVariable("OLLAMA_AUTO_PULL", "Process") + "").ToLowerInvariant() -in @("1","true","yes","on")
$tags = Get-OllamaTags -ApiRoot $ollamaApiRoot
$llmModel = [System.Environment]::GetEnvironmentVariable("LLM_MODEL_NAME", "Process")
$embModel = [System.Environment]::GetEnvironmentVariable("EMBEDDER_MODEL_NAME", "Process")
$hasLlm = Test-OllamaModelPresent -ModelName $llmModel -Tags $tags
$hasEmb = Test-OllamaModelPresent -ModelName $embModel -Tags $tags

if ((-not $hasEmb) -or ((-not $disableLlm) -and (-not $hasLlm))) {
    if (-not $autoPullModels) {
        throw "Required Ollama models are missing. Set OLLAMA_AUTO_PULL=true or run: ollama pull $llmModel ; ollama pull $embModel"
    }
}
if ((-not $disableLlm) -and (-not $hasLlm)) {
    & $ollamaBin pull $llmModel | Out-Host
}
if (-not $hasEmb) {
    & $ollamaBin pull $embModel | Out-Host
}

$composeProfileArgs = @()
if (([System.Environment]::GetEnvironmentVariable("MEMGRAPH_LAB_ENABLED", "Process") + "").ToLowerInvariant() -in @("1","true","yes","on")) {
    $composeProfileArgs += @("--profile", "lab")
}
if (([System.Environment]::GetEnvironmentVariable("SHEETS_SYNC_ENABLED", "Process") + "").ToLowerInvariant() -in @("1","true","yes","on")) {
    $composeProfileArgs += @("--profile", "sheets")
}
if (([System.Environment]::GetEnvironmentVariable("VK_BOT_ENABLED", "Process") + "").ToLowerInvariant() -in @("1","true","yes","on")) {
    $composeProfileArgs += @("--profile", "vk")
}

Write-Host "Starting RAGU compose stack..."
& $dockerBin compose -f $composeFile @composeProfileArgs up -d --build | Out-Host
if ($LASTEXITCODE -ne 0) {
    throw "docker compose up failed with exit code $LASTEXITCODE"
}

Wait-HttpStatus -Url "http://127.0.0.1:8000/status" -TimeoutSec ([int][System.Environment]::GetEnvironmentVariable("FASTAPI_START_TIMEOUT_SEC", "Process"))

Write-Host ""
Write-Host "Stack is ready."
Write-Host "API docs: http://127.0.0.1:8000/docs"
Write-Host "Status:   http://127.0.0.1:8000/status"
if (([System.Environment]::GetEnvironmentVariable("MEMGRAPH_LAB_ENABLED", "Process") + "").ToLowerInvariant() -in @("1","true","yes","on")) {
    Write-Host "Memgraph UI: http://127.0.0.1:3000"
}
Write-Host "Memgraph Bolt: bolt://127.0.0.1:7687"
Write-Host "Prepared IT seed is explicit: .\venv\Scripts\python.exe scripts\seed_demo_it_knowledge.py --api-base-url http://127.0.0.1:8000"
