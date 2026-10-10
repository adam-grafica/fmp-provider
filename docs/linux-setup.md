# Linux Setup (local installation, loopback-only)

## 1. Prereqs

Node 18+ (`node --version`). No sudo needed except for the OPTIONAL
systemd unit.

## 2. Install

```bash
git clone <private-repo-url> fmp-provider
cd fmp-provider
git checkout traycer/FMP-PRODUCTION-READINESS-001
cp .env.example .env
# edit .env: set FMP_API_KEYS
node -e "console.log('sk-'+require('crypto').randomBytes(24).toString('hex'))"
./install.sh            # DRY_RUN=1 ./install.sh for dry-run
```

## 3. Run

```bash
node server.js
node bin/fmp.js doctor
node bin/fmp.js health
FMP_API_KEY=sk-... node bin/fmp.js models
```

## 4. 9Router registration (same machine, explicit step)

```bash
./register-9router.sh   # idempotent, backs up sqlite first
```

## 5. Docker (optional)

```bash
cp .env.example .env   # fill keys
docker compose up --build   # 127.0.0.1:2089 only
```

## 6. OPTIONAL systemd (Linux server use only)

`fmp-provider.service` is marked OPTIONAL — not needed for local dev.
Enable only on a trusted host, loopback bind, keys configured:

```bash
cp fmp-provider.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now fmp-provider
```

## 7. Uninstall / rollback

- Uninstall: stop node (`systemctl --user disable --now fmp-provider`
  if enabled), `rm -rf <dir>`.
- Rollback: `git checkout main -- .` or restore backup.
