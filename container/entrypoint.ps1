param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]] $CommandArguments
)

$ErrorActionPreference = 'Stop'

function Ensure-Directory([string] $Path) {
    if ($Path) {
        New-Item -ItemType Directory -Path $Path -Force | Out-Null
    }
}

Ensure-Directory $env:INSTANCE_DIR
Ensure-Directory $env:BACKUP_DIR
Ensure-Directory $env:TQ_HOME_DIR
Ensure-Directory (Split-Path -Parent $env:SDL_FILE_PATH)

if (-not $CommandArguments -or $CommandArguments.Count -eq 0) {
    $CommandArguments = @('web')
}

$command = $CommandArguments[0]
$remaining = @($CommandArguments | Select-Object -Skip 1)

switch ($command) {
    'web' {
        python -m flask --app app.py init-db
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        python -m waitress --host=0.0.0.0 --port=$env:PORT app:app
    }
    'pipeline' {
        python -u C:\app\src\pipeline.py @remaining
    }
    'nightly' {
        python -u C:\app\nightly_label_check.py @remaining
    }
    'python' {
        python @remaining
    }
    'powershell' {
        powershell.exe -NoLogo -NoProfile @remaining
    }
    default {
        throw "Unknown container command '$command'. Use web, pipeline, nightly, python, or powershell."
    }
}

exit $LASTEXITCODE
