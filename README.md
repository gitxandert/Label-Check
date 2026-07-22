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
