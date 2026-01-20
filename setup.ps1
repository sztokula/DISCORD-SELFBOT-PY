$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

if (-not (Test-Path ".venv")) {
    python -m venv .venv
}

$python = Join-Path $root ".venv\\Scripts\\python.exe"
& $python -m pip install --upgrade pip
& $python -m pip install customtkinter httpx cryptography

if ($args -contains "-run") {
    & $python ".\\main.py"
}
