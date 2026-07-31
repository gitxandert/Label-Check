# escape=`

ARG WINDOWS_VERSION=ltsc2022

FROM mcr.microsoft.com/windows/servercore:${WINDOWS_VERSION} AS rust-builder

SHELL ["powershell.exe", "-NoLogo", "-NoProfile", "-Command", "$ErrorActionPreference = 'Stop'; $ProgressPreference = 'SilentlyContinue';"]

ARG MINGW_VERSION=16.1.0

ENV RUSTUP_HOME="C:\rustup" `
    CARGO_HOME="C:\cargo" `
    PATH="C:\cargo\bin;C:\mingw\bin;C:\Windows\system32;C:\Windows"

RUN Set-ExecutionPolicy Bypass -Scope Process -Force; `
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; `
    Invoke-Expression ((New-Object Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1')); `
    choco install mingw --version=$env:MINGW_VERSION --yes --no-progress; `
    if ($LASTEXITCODE -ne 0) { throw "MinGW installation failed with exit code $LASTEXITCODE." }; `
    $gcc = Get-ChildItem 'C:\ProgramData\chocolatey\lib\mingw\tools' -Filter gcc.exe -Recurse | Where-Object { $_.FullName -match 'mingw64.*\\bin\\gcc.exe$' } | Select-Object -First 1; `
    if (-not $gcc) { throw 'Chocolatey MinGW package did not provide gcc.exe.' }; `
    if (-not (Test-Path (Join-Path $gcc.DirectoryName 'ar.exe'))) { throw 'Chocolatey MinGW package did not provide ar.exe.' }; `
    New-Item -ItemType Directory -Path 'C:\mingw' -Force | Out-Null; `
    New-Item -ItemType Junction -Path 'C:\mingw\bin' -Target $gcc.DirectoryName | Out-Null; `
    Invoke-WebRequest 'https://win.rustup.rs/x86_64' -OutFile 'C:\rustup-init.exe'; `
    & 'C:\rustup-init.exe' -y --no-modify-path --default-host x86_64-pc-windows-gnu --default-toolchain stable-x86_64-pc-windows-gnu; `
    if ($LASTEXITCODE -ne 0) { throw "rustup installation failed with exit code $LASTEXITCODE." }; `
    Remove-Item 'C:\rustup-init.exe' -Force

ENV CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER="C:\mingw\bin\gcc.exe" `
    CC_x86_64_pc_windows_gnu="C:\mingw\bin\gcc.exe" `
    AR_x86_64_pc_windows_gnu="C:\mingw\bin\ar.exe"

WORKDIR C:\src\tq
COPY tq C:\src\tq

RUN rustup default stable-x86_64-pc-windows-gnu; `
    if ($LASTEXITCODE -ne 0) { throw "rustup default failed with exit code $LASTEXITCODE." }; `
    cargo test --locked --release --target x86_64-pc-windows-gnu; `
    if ($LASTEXITCODE -ne 0) { throw "TQ tests failed with exit code $LASTEXITCODE." }; `
    cargo build --locked --release --target x86_64-pc-windows-gnu; `
    if ($LASTEXITCODE -ne 0) { throw "TQ build failed with exit code $LASTEXITCODE." }


FROM mcr.microsoft.com/windows/servercore:${WINDOWS_VERSION} AS python-base

SHELL ["powershell.exe", "-NoLogo", "-NoProfile", "-Command", "$ErrorActionPreference = 'Stop'; $ProgressPreference = 'SilentlyContinue';"]

ARG PYTHON_VERSION=3.12.10
ARG ODBC_DRIVER_URL=https://go.microsoft.com/fwlink/?linkid=2358430

RUN Invoke-WebRequest "https://www.python.org/ftp/python/$env:PYTHON_VERSION/python-$env:PYTHON_VERSION-amd64.exe" -OutFile C:\python-installer.exe; `
    $pythonInstaller = Start-Process C:\python-installer.exe -ArgumentList '/quiet InstallAllUsers=1 PrependPath=0 Include_test=0 TargetDir=C:\Python312' -Wait -PassThru; `
    if ($pythonInstaller.ExitCode -ne 0) { throw "Python installation failed with exit code $($pythonInstaller.ExitCode)." }; `
    Remove-Item C:\python-installer.exe -Force; `
    Invoke-WebRequest 'https://aka.ms/vc14/vc_redist.x64.exe' -OutFile C:\vc-redist.exe; `
    $vcRuntime = Start-Process C:\vc-redist.exe -ArgumentList '/install /quiet /norestart' -Wait -PassThru; `
    if ($vcRuntime.ExitCode -notin 0, 1638, 3010) { throw "Visual C++ runtime installation failed with exit code $($vcRuntime.ExitCode)." }; `
    Remove-Item C:\vc-redist.exe -Force; `
    Invoke-WebRequest $env:ODBC_DRIVER_URL -OutFile C:\msodbcsql.msi; `
    $odbc = Start-Process msiexec.exe -ArgumentList '/i C:\msodbcsql.msi /qn /norestart IACCEPTMSODBCSQLLICENSETERMS=YES' -Wait -PassThru; `
    if ($odbc.ExitCode -notin 0, 3010) { throw "ODBC Driver installation failed with exit code $($odbc.ExitCode)." }; `
    Remove-Item C:\msodbcsql.msi -Force

ENV PATH="C:\Python312;C:\Python312\Scripts;C:\Windows\system32;C:\Windows" `
    PYTHONUNBUFFERED="1" `
    PYTHONDONTWRITEBYTECODE="1" `
    LABEL_CHECK_CONTAINER="true" `
    EASYOCR_FORCE_CPU="true" `
    EASYOCR_MODEL_DIR="C:\easyocr-models" `
    HOME="C:\labelcheck-profile" `
    TQ_HOME_DIR="C:\labelcheck-profile\.tq"

WORKDIR C:\app
COPY requirements.txt C:\app\requirements.txt
RUN python -m pip install --disable-pip-version-check --no-cache-dir --upgrade pip; `
    if ($LASTEXITCODE -ne 0) { throw "pip upgrade failed with exit code $LASTEXITCODE." }; `
    python -m pip install --disable-pip-version-check --no-cache-dir --requirement C:\app\requirements.txt; `
    if ($LASTEXITCODE -ne 0) { throw "Python dependency installation failed with exit code $LASTEXITCODE." }

COPY src C:\app\src
COPY tests C:\app\tests
COPY container C:\app\container
COPY nightly_label_check.py C:\app\nightly_label_check.py
COPY requirements-test.txt C:\app\requirements-test.txt

RUN New-Item -ItemType Directory -Path C:\easyocr-models -Force | Out-Null; `
    python -c "import easyocr; easyocr.Reader(['en'], gpu=False, model_storage_directory=r'C:\easyocr-models')"; `
    if ($LASTEXITCODE -ne 0) { throw "EasyOCR model download failed with exit code $LASTEXITCODE." }


FROM python-base AS test

RUN python -m pip install --disable-pip-version-check --no-cache-dir --requirement C:\app\requirements-test.txt; `
    if ($LASTEXITCODE -ne 0) { throw "Test dependency installation failed with exit code $LASTEXITCODE." }; `
    python -W error::ResourceWarning -m unittest discover --start-directory C:\app\tests --verbose; `
    if ($LASTEXITCODE -ne 0) { throw "Python tests failed with exit code $LASTEXITCODE." }


FROM python-base AS runtime

COPY --from=rust-builder C:\src\tq\target\x86_64-pc-windows-gnu\release\tq.exe C:\app\bin\tq.exe

RUN $hash = (Get-FileHash C:\app\bin\tq.exe -Algorithm SHA256).Hash; `
    Set-Content -Path C:\app\bin\tq.exe.sha256 -Value "$hash  tq.exe" -Encoding Ascii; `
    & C:\app\bin\tq.exe help; `
    if ($LASTEXITCODE -ne 1) { throw "tq.exe help returned unexpected exit code $LASTEXITCODE." }

ENV TQ_EXECUTABLE="C:\app\bin\tq.exe" `
    INSTANCE_DIR="D:\label-check-state\instance" `
    SDL_FILE_PATH="D:\label-check-state\Slide_Digitization_Log.xlsx" `
    BACKUP_DIR="D:\label-check-state\csv_backups" `
    SCANNER_INVENTORIES="D:\scanner_inventories" `
    LABEL_CHECK_BATCHES="D:\label_check_batches" `
    COPATH_CLONE="D:\copath_clone" `
    TQ_TRANSFER_LOG_DIR="D:\label_check_batches\transfer_logs" `
    PORT="5000"

WORKDIR C:\app\src
EXPOSE 5000

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 `
    CMD powershell -NoLogo -NoProfile -Command "try { $response = Invoke-WebRequest -UseBasicParsing http://localhost:5000/login; if ($response.StatusCode -ge 400) { exit 1 } } catch { exit 1 }"

ENTRYPOINT ["powershell.exe", "-NoLogo", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", "C:\\app\\container\\entrypoint.ps1"]
CMD ["web"]
