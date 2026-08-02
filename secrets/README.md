# Local Compose secrets

`compose.yaml` expects two untracked UTF-8, single-line files in this directory:

- `operator-api-key.txt`: at least 16 random characters;
- `token-signing-secret.txt`: at least 32 random bytes/characters.

Generate them without committing their contents. On PowerShell 7:

```powershell
python -c "import secrets; print(secrets.token_urlsafe(32), end='')" |
  Set-Content -NoNewline -Encoding utf8 secrets/operator-api-key.txt
python -c "import secrets; print(secrets.token_urlsafe(48), end='')" |
  Set-Content -NoNewline -Encoding utf8 secrets/token-signing-secret.txt
```

On a POSIX shell:

```bash
umask 077
python -c 'import secrets; print(secrets.token_urlsafe(32), end="")' > secrets/operator-api-key.txt
python -c 'import secrets; print(secrets.token_urlsafe(48), end="")' > secrets/token-signing-secret.txt
```

Override either location with `SAMSARIX_CHAT_OPERATOR_SECRET_PATH` or `SAMSARIX_CHAT_TOKEN_SECRET_PATH` in the host environment. Never reuse webhook or token-signing secrets as the operator API key.
