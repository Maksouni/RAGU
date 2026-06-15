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

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$composeFile = Join-Path $repoRoot "examples\fastapi_demo\docker-compose.yml"
$envFile = Join-Path $repoRoot ".env"
Import-DotEnv -Path $envFile

$dockerBin = Resolve-DockerPath
if ($dockerBin) {
    Write-Host "Stopping RAGU compose stack..."
    & $dockerBin compose -f $composeFile --profile lab --profile sheets --profile vk stop | Out-Host
    if ($LASTEXITCODE -ne 0) {
        Write-Host "docker compose stop returned exit code $LASTEXITCODE."
    }
} else {
    Write-Host "Docker CLI not found. Skip compose stop."
}

$stopOllama = ([System.Environment]::GetEnvironmentVariable("STOP_OLLAMA_ON_STOP", "Process") + "").ToLowerInvariant() -in @("1","true","yes","on")
if ($stopOllama) {
    Get-Process | Where-Object { $_.ProcessName -like "*ollama*" } | Stop-Process -Force -ErrorAction SilentlyContinue
    Write-Host "Ollama processes stopped because STOP_OLLAMA_ON_STOP=true."
}

Write-Host "Done."
