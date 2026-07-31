# Label-Check
A label corrector

## Pipeline API

The versioned pipeline API is available under `/api/v1` and requires HTTPS plus a
scoped personal access token. Create a token for an existing Label-Check user:

```bash
flask --app src/app.py api-token create USERNAME --label "integration name"
```

The plaintext token is shown once. It expires after 90 days by default. Use
`api-token list`, `api-token rotate TOKEN_ID`, and `api-token revoke TOKEN_ID` to
manage credentials.

Submit a job using server-visible input and output paths:

```bash
curl --request POST https://label-check.example/api/v1/pipeline/jobs \
  --header "Authorization: Bearer $LABEL_CHECK_TOKEN" \
  --header "Content-Type: application/json" \
  --header "Idempotency-Key: unique-client-request-id" \
  --data '{"input_dir":"/data/incoming","output_dir":"/data/output"}'
```

Inspect the returned job URL and its `/output` subresource to monitor execution.
The authenticated OpenAPI 3.1 contract is served at `/api/v1/openapi.json` and is
also checked in as `src/openapi.json`.

API traffic is limited per token to five submissions and 60 reads per minute.
When TLS terminates at one trusted reverse proxy, set
`API_TRUST_PROXY_HEADERS=true`; otherwise forwarded scheme headers are ignored.

## Windows container

The container is Windows-only. It runs the Flask application and Python pipeline
on Windows Server Core LTSC 2022 and builds `tq.exe` as a release binary for
`x86_64-pc-windows-gnu`. No Linux TQ binary is built.

### Prerequisites

- A Windows Server host capable of running LTSC 2022 Windows containers.
- Docker configured for Windows containers.
- This repository cloned with submodules:

  ```powershell
  git clone --recurse-submodules <label-check-repository-url>
  ```

- A gMSA credential specification available to Docker. The gMSA needs:
  - integrated-auth access to the CoPath SQL Server;
  - read access to
    `\\chp.clarian.org\app\Philips_Slide_Images\GT450_images`;
  - NTFS access to the configured local bind mounts.
- IT approval for `tq.exe` in Microsoft Defender. Container layers remain visible
  to host antivirus. Building or running the image does not bypass quarantine.
- An external reverse proxy that terminates HTTPS and forwards the original
  request scheme to port 5000.

Copy `.env.example` to `.env`, replace every placeholder, and create all host
directories before starting. `TQ_HOME_HOST` must contain `config.toml`.
`SSH_HOME_HOST` must contain either `id_ed25519` or `id_rsa`; mount only the
dedicated key needed by TQ. `LABEL_CHECK_STATE_HOST` must contain
`Slide_Digitization_Log.xlsx` before SDL-backed workflows are used. Grant the
container gMSA read/write access to the state, batch, CoPath clone, and TQ home
directories; inventory and SSH mounts need read access only.

The scanner image share is not a Docker bind mount. Windows Docker does not
accept a UNC path as a bind source. Inventory paths access the share directly
using the gMSA network identity.

### Build and test

```powershell
git submodule update --init --recursive
docker build --target test --tag label-check:test .
docker compose build
```

The Rust stage uses an isolated MinGW installation with absolute compiler and
linker paths. It runs:

```powershell
rustup default stable-x86_64-pc-windows-gnu
cargo build --locked --release --target x86_64-pc-windows-gnu
```

The Python/runtime stage never receives Rust, Cargo, MinGW, or GCC. EasyOCR is
CPU-only and its English models are downloaded into the image during build.

Before deployment, obtain the built binary hash and send it to IT with the
Defender detection record:

```powershell
docker run --rm --entrypoint powershell.exe label-check:latest `
  -NoProfile -Command "Get-Content C:\app\bin\tq.exe.sha256"
```

Do not deploy until Defender permits that artifact. Each unsigned rebuild has a
new hash and may require renewed approval. If Defender quarantines the unsigned
binary during a build on the deployment server, build on an approved Windows
builder first, record the hash, publish the image to the organization's private
registry, obtain IT approval, and only then pull it on the deployment server.

### Run

```powershell
docker compose up -d
docker compose ps
```

The default `web` command initializes persistent application state idempotently
and starts Waitress on port 5000. Other applications use the same image:

```powershell
docker compose run --rm label-check pipeline `
  --input-dir "\\chp.clarian.org\app\Philips_Slide_Images\GT450_images\SS12797" `
  --output-dir "D:\label_check_batches\SS12797\2026-07-31" `
  --end-at name --ocr-use-cpu

docker compose run --rm label-check nightly
docker compose run --rm label-check python C:\app\src\deidentify_anonymize.py --help
```

Schedule the `nightly` command externally; it performs one cycle and exits.
Application state, SDL workbook, backups, TQ configuration, and transfer logs
live on configured host mounts and survive container replacement.
