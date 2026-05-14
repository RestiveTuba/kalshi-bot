# Credential Rotation Checklist

Before deploying live trading with real capital, rotate any credentials that may have appeared in local transcripts, chat history, screenshots, or prior commits.

Do not skip this. Run the checklist 1-2 days before planned live deployment.

Status:

- [ ] Not yet started
- [ ] In progress
- [ ] Completed

## Kalshi API Credentials

Priority: Critical

- [ ] Log in to the Kalshi console.
- [ ] Navigate to API settings.
- [ ] Revoke the current API key and private key.
- [ ] Generate a new API key pair.
- [ ] Download the new private key and store it securely.
- [ ] SSH into the server: `ssh root@147.182.132.170`.
- [ ] Update `/root/kalshi-bot/.env` with the new `KALSHI_API_KEY_ID`.
- [ ] Update `/root/kalshi-bot/kalshi_private_key.pem` with the new private key.
- [ ] Lock permissions: `chmod 600 /root/kalshi-bot/.env /root/kalshi-bot/kalshi_private_key.pem`.
- [ ] Restart services that use Kalshi credentials:
  `systemctl restart kalshi-market-maker kalshi-momentum kalshi-data-collector kalshi-dashboard`.
- [ ] Verify service health: `systemctl status kalshi-market-maker kalshi-momentum kalshi-data-collector kalshi-dashboard`.

## Coinbase API Credentials

Priority: High if any Coinbase private keys are configured

- [ ] Log in to Coinbase.
- [ ] Review active API keys.
- [ ] Revoke any key that may have been exposed.
- [ ] Create a new key with the minimum permissions needed.
- [ ] Update `/root/kalshi-bot/.env` if Coinbase keys are present.
- [ ] Lock permissions: `chmod 600 /root/kalshi-bot/.env`.
- [ ] Restart relevant services.
- [ ] Check data collector logs: `journalctl -u kalshi-data-collector -n 50 --no-pager`.

## Telegram Bot Token

Priority: Medium

- [ ] If Telegram alerts are enabled, revoke the exposed bot token.
- [ ] Create a new bot token through BotFather.
- [ ] Update `TELEGRAM_BOT_TOKEN` in `/root/kalshi-bot/.env`.
- [ ] Restart services that send alerts.
- [ ] Send or trigger a test alert.

## Anthropic API Key

Priority: Medium if used by monitoring or dashboard tools

- [ ] Revoke any exposed Anthropic API key.
- [ ] Create a new key in the Anthropic console.
- [ ] Update `/root/kalshi-bot/.env` if `ANTHROPIC_API_KEY` is present.
- [ ] Lock permissions: `chmod 600 /root/kalshi-bot/.env`.

## Review Local Transcript History

Priority: Critical

- [ ] Search local chat transcripts and notes for `KALSHI`, `API_KEY`, `.pem`, private key markers, Coinbase key formats, and Telegram token formats.
- [ ] Treat any secret found in transcripts as compromised.
- [ ] Rotate every matching credential before live trading.

## Verify Secrets Are Not In Git

Priority: Critical

Run on the server:

```bash
cd /root/kalshi-bot
git log --all --full-history -- .env kalshi_private_key.pem
git status -s | grep -E '\\.env|private_key.pem' || true
```

- [ ] Confirm `.env` and `kalshi_private_key.pem` do not appear in git history.
- [ ] Confirm `.env` and `kalshi_private_key.pem` are ignored by git.
- [ ] Rotate immediately if either file appears in history.

## Final Verification

Priority: Critical

- [ ] Run the health check if available: `/root/kalshi-bot/health_check.sh`.
- [ ] Verify all services are active:
  `systemctl is-active kalshi-market-maker kalshi-momentum kalshi-data-collector kalshi-dashboard`.
- [ ] Verify dashboard responds: `curl http://127.0.0.1:5000`.
- [ ] Check for auth errors:

```bash
journalctl -u kalshi-market-maker -n 100 --no-pager | grep -i 'error\\|unauthorized\\|forbidden' || true
journalctl -u kalshi-data-collector -n 100 --no-pager | grep -i 'error\\|unauthorized\\|forbidden' || true
```

- [ ] Do not deploy live capital until all critical items are complete.
