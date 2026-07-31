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

## Linux container on Docker Desktop

The `linux/amd64` image runs as a Linux container under Docker Desktop. It contains the Flask
application, Python pipeline, utilities, and a native Linux `tq` release binary.
Rust and Python compile in separate stages; the final image contains neither
Cargo nor GCC.

### Prerequisites

- Docker Desktop configured for Linux containers.
- Repository cloned with its pinned TQ submodule:

  ```powershell
  git clone --recurse-submodules <label-check-repository-url>
  ```

- Windows directories shared with Docker Desktop.
- The GT450 SMB share mapped to a Windows path Docker Desktop can bind. The
  example uses `Z:\GT450_images`; change it to the server's working mapping.
- A dedicated TQ configuration directory containing `config.toml` and a
  dedicated SSH directory containing `id_ed25519` or `id_rsa`.
- A one-line CoPath ODBC connection string stored outside this repository. A
  Linux container cannot inherit the Windows user's integrated login. Use a SQL
  login approved by IT, or separately configure Kerberos inside the container.
- An external reverse proxy terminating HTTPS and forwarding the request scheme
  to port 5000.

Copy `.env.example` to `.env`, replace all placeholders, and create the host
directories. `LABEL_CHECK_STATE_HOST` must contain
`Slide_Digitization_Log.xlsx` before SDL workflows run.

Example CoPath secret file content:

```text
DRIVER={ODBC Driver 18 for SQL Server};SERVER=IUHWCPTHDB3980;DATABASE=COPLIVE;UID=service-user;PWD=secret;TrustServerCertificate=yes;
```

### Build and test

```powershell
git submodule update --init --recursive
docker build --target test --tag label-check:test .
docker compose build
```

TQ builds and tests natively for Linux:

```text
cargo test --locked --release
cargo build --locked --release
```

EasyOCR is CPU-only and its English models are baked into the image. Obtain the
Linux binary hash when needed with:

```powershell
docker run --rm --entrypoint sha256sum label-check:latest /app/bin/tq
```

### Paths

Inside the container, Windows resources appear at stable Linux paths:

- GT450 images: `/data/gt450-images`
- scanner inventories: `/data/scanner-inventories`
- label-check batches: `/data/label-check-batches`
- CoPath clone: `/data/copath-clone`
- persistent application state: `/data/state`

Persisted UNC GT450 paths and `D:\label_check_batches` paths are translated to
these mounts. New pipeline output records Linux mount paths directly.

### Run

```powershell
docker compose up -d
docker compose ps
```

The default command initializes persistent state and starts Waitress on port
5000. Other applications use the same image:

```powershell
docker compose run --rm label-check pipeline `
  --input-dir /data/gt450-images/SS12797 `
  --output-dir /data/label-check-batches/SS12797/2026-07-31 `
  --end-at name --ocr-use-cpu

docker compose run --rm label-check nightly
docker compose run --rm label-check python /app/src/deidentify_anonymize.py --help
```

Schedule `nightly` externally; it performs one cycle and exits. State, SDL,
backups, TQ configuration, and transfer logs survive container replacement.
