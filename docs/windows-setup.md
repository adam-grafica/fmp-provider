# Windows Setup (local installation, loopback-only)

No admin required. No services, no registry keys, no systemd.

## 1. Prereqs

- Node 18+: `winget install OpenJS.NodeJS.LTS` (verify: `node --version`)
- Git (to clone the private repo)

## 2. Install

```powershell
git clone <private-repo-url> fmp-provider
cd fmp-provider
git checkout traycer/FMP-PRODUCTION-READINESS-001
Copy-Item .env.example .env
# edit .env: set FMP_API_KEYS (generate one below)
node -e "console.log('sk-'+require('crypto').randomBytes(24).toString('hex'))"
.\install.ps1
```

Or dry-run first: `$env:DRY_RUN=1; .\install.ps1`

## 3. Run

```powershell
node server.js
node bin\fmp.js health
$env:FMP_API_KEY="sk-..."; node bin\fmp.js models
```

## 4. 9Router / harness use (same machine)

Point OpenAI-compatible clients at `http://127.0.0.1:2089/v1`
with the configured key. Anthropic-compatible clients use
`http://127.0.0.1:2089` + `anthropic-version: 2023-06-01`.

## 5. Docker (optional)

```powershell
docker compose up --build   # loopback-bound 127.0.0.1:2089
```

## 6. Uninstall / rollback

- Uninstall: stop node, `Remove-Item -Recurse <dir>`.
- Rollback: `git checkout main -- .` or restore backup.
