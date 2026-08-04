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
- A dedicated, read-only SMB account that can access the GT450 image directory.
  Docker's Linux VM mounts this share directly; no Windows drive mapping is
  required.
- A dedicated TQ configuration directory containing `config.toml` and a
  dedicated SSH directory containing `id_ed25519` or `id_rsa`.
- Python 3.10 or newer and Microsoft ODBC Driver 18 for SQL Server installed on
  the signed-in Windows workstation for the CoPath worker.
- A one-line CoPath ODBC connection string stored outside this repository. The
  Windows worker uses native integrated authentication; it contains no username
  or password.
- An external reverse proxy terminating HTTPS and forwarding the request scheme
  to port 5000.

Copy `.env.example` to `.env`, replace all placeholders, and create the host
directories. `LABEL_CHECK_STATE_HOST` must contain
`Slide_Digitization_Log.xlsx` before SDL workflows run.

The GT450 CIFS password is stored in the gitignored `.env` file and in local
Docker volume metadata. Restrict Docker access to trusted administrators and do
not reuse a personal account. Because CIFS mount options are comma-delimited,
the dedicated account password must not contain a comma. Quote other special
characters according to Compose `.env` syntax.

Example CoPath secret file content:

```text
DRIVER={ODBC Driver 18 for SQL Server};SERVER=sql-server.example.org;DATABASE=COPLIVE;Trusted_Connection=yes;TrustServerCertificate=yes;
```

Every ODBC property must be separated by a semicolon. The connection string
does not contain the Windows username or password.

### Build and test

```powershell
git submodule update --init --recursive
docker build --target test --tag label-check:test .
docker compose build
```

The image installs PyTorch from its CPU-only wheel index. BuildKit caches pip
downloads, Python dependency layers, and the EasyOCR models independently from
the application source, so normal source edits do not repeat the large OCR
setup. Keep BuildKit enabled and avoid `--no-cache` unless diagnosing a build.

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

The GT450 mount is the named Docker volume `label-check-gt450-images`. It mounts
`//chp.clarian.org/app/Philips_Slide_Images/GT450_Images` through CIFS with
read-only permissions. The remaining paths are ordinary Windows bind mounts.

### Windows CoPath worker

CoPath queries are delegated to a worker that you start after signing into
Windows. Docker and the worker communicate only through
`LABEL_CHECK_STATE_HOST\copath-query`; no SQL credentials enter the container.
The queue contains accessions and report data. Restrict the entire
`LABEL_CHECK_STATE_HOST` directory to the signed-in Windows account, the Docker
Desktop service account, and administrators. Do not share it broadly or use a
world-writable network directory.

Create the worker environment once from the repository root in PowerShell:

```powershell
py -m venv .venv-copath-worker
.\.venv-copath-worker\Scripts\python.exe -m pip install --upgrade pip
.\.venv-copath-worker\Scripts\python.exe -m pip install -r requirements-windows-worker.txt
```

Start the worker manually after Windows sign-in, before preparing or retrying a
batch:

```powershell
.\.venv-copath-worker\Scripts\python.exe src\copath_windows_worker.py `
  --queue "$env:LABEL_CHECK_STATE_HOST\copath-query" `
  --connection-string-file "$env:COPATH_CONNECTION_STRING_FILE_HOST"
```

If those values are stored only in `.env`, substitute their actual Windows
paths in the command. The worker writes a heartbeat every five seconds, accepts
only validated accession lists (up to 10,000), and uses the signed-in user's
native Windows identity through `Trusted_Connection=yes`. Press Ctrl+C for a
clean shutdown; the heartbeat is removed and the renaming page reports the
worker offline instead of waiting for the full query timeout.

To verify the SQL identity during integration testing, have a database
administrator inspect the worker's SQL Server session while a batch query is
active, or temporarily run the equivalent approved identity query through the
same ODBC connection. It should show the signed-in personal domain account.

### Run

```powershell
docker compose up -d
docker compose ps
```

Verify that the SMB mount contains the expected scanner directories and is
read-only for the application user:

```powershell
docker compose exec label-check ls -la /data/gt450-images
docker inspect $(docker compose ps -q label-check) `
  --format '{{range .Mounts}}{{if eq .Destination "/data/gt450-images"}}{{println "RW:" .RW "Name:" .Name}}{{end}}{{end}}'
```

The inspection output must report `RW: false` and
`Name: label-check-gt450-images`.

Docker volume options are fixed when the volume is created. After changing the
SMB password or any `GT450_SMB_*` mount setting, recreate only this mount:

```powershell
docker compose down
docker volume rm label-check-gt450-images
docker compose up -d
```

Removing this Docker volume unmounts the share; it does not delete files from
the SMB server.

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

### GT450 troubleshooting

- **GT450 volume fails to mount:** verify `GT450_SMB_SERVER`, the dedicated
  account credentials and domain, SMB 3.0 connectivity, and access to the
  `app` share. Docker should fail container startup on a mount error instead of
  substituting an empty local directory. After changing mount options, remove
  and recreate `label-check-gt450-images` as described above.

### CoPath troubleshooting

- **Worker offline:** confirm the worker console is still running and that both
  Windows and Docker can access `LABEL_CHECK_STATE_HOST\copath-query`. Check the
  Windows clock if `worker.json` is present but considered stale.
- **Query timeout:** the default is 300 seconds. Inspect the worker console and
  SQL connectivity before increasing `COPATH_QUERY_TIMEOUT_SECONDS`.
- **ODBC or login failure:** confirm ODBC Driver 18 is installed, the connection
  file has semicolon-delimited properties including `Trusted_Connection=yes`,
  and the signed-in Windows account is authorized for CoPath.
- **Queue permission failure:** restore ACL access for the signed-in user and
  Docker Desktop. Keep `requests`, `processing`, `results`, `errors`, `work`,
  and `worker.json` beneath the configured queue root.
- **Crash recovery:** restart the worker. It returns claims older than ten
  minutes to the request queue and removes terminal artifacts after 24 hours.

### Optional direct Linux/Kerberos mode

The previous in-container query path remains available as a fallback. Set
`KRB5_CONFIG_HOST` and `COPATH_CONNECTION_STRING_FILE_HOST`, then merge the
direct-mode override:

```powershell
docker compose -f compose.yaml -f compose.direct.yaml up -d
docker compose -f compose.yaml -f compose.direct.yaml exec label-check `
  kinit YOUR_USERNAME@YOUR.AD.REALM
docker compose -f compose.yaml -f compose.direct.yaml exec label-check klist
```

In direct mode, use the SQL Server DNS name associated with its `MSSQLSvc`
service principal name. Renew `kinit` after ticket expiry or container
recreation. The base `docker compose up -d` deployment always uses the Windows
queue.
