# Connect Claude Code to grafana.nearone.org

This document sets up the official [`grafana/mcp-grafana`](https://github.com/grafana/mcp-grafana) MCP server so Claude Code can query dashboards, Prometheus, Loki, Tempo, alerts, and on-call schedules during incidents and investigations.

First complete the [Prerequisites](#prerequisites-do-these-yourself--claude-cannot), then ask Claude to follow the instructions. In a Claude Code session say:

> Follow the instructions in `docs/grafana-mcp-setup.md` to set up the Grafana MCP server for me.

Claude will run through the steps below, verify each one, and stop with a clear error if anything is off.

---

## Prerequisites (do these yourself — Claude cannot)

### 1. Get a Grafana service account token

You can obtain the grafana service account token from 1Password. It's stored as
`Grafana MCP Token`. If the token is missing or doesn't work please reach out to
the SRE team for support.

### 2. Save the token to a local file

Do this in your own terminal (not in Claude Code's prompt). Either use an editor, or a one-liner with `read -rsp` — both avoid leaking the token to shell history.

**Editor:**

```bash
mkdir -p ~/.config/grafana
vim -n ~/.config/grafana/nearone-token   # -n disables swap files so the token doesn't linger on disk
# paste the token, save and quit
chmod 600 ~/.config/grafana/nearone-token
```

**One-liner:**

```bash
mkdir -p ~/.config/grafana && \
  umask 077 && \
  read -rsp "paste Grafana service account token (glsa_...): " TOK && \
  printf '%s' "$TOK" > ~/.config/grafana/nearone-token && \
  unset TOK && echo && echo saved
```

---

## Instructions for Claude

Execute the steps below in order. **Stop and report to the user if any step fails** — do not improvise around errors.

### Step 1: Verify prerequisites

Run each check. Abort with the stated message if a check fails.

```bash
# Token file present and non-empty
[ -s ~/.config/grafana/nearone-token ] || { echo "ABORT: ~/.config/grafana/nearone-token is missing or empty. See prerequisites."; exit 1; }

# Token file has correct format
grep -q '^glsa_' ~/.config/grafana/nearone-token || {
  echo "ABORT: token does not start with glsa_. You likely have an access policy token (glc_...) or a service account login (sa-1-...). See prerequisites section for how to tell them apart.";
  exit 1;
}
```

### Step 2: Install `uv` if missing

`uvx` is the recommended runtime for the MCP server (single binary, no daemon). Skip if already installed.

```bash
if ! command -v uvx >/dev/null && ! [ -x ~/.local/bin/uvx ]; then
  curl -LsSf https://astral.sh/uv/install.sh | sh
fi
```

Verify it's on `PATH`. If not, the user's shell init hasn't picked up `~/.local/bin` yet — tell them to either restart their shell or add `export PATH="$HOME/.local/bin:$PATH"` to their shell rc.

### Step 3: Verify the token actually works

**Do this before registering the MCP server.** Grafana will return 401 "Invalid API key" on every UI endpoint if the token is wrong, even though the MCP server itself will report `✓ Connected` in `claude mcp list` (the MCP handshake does not exercise the token).

```bash
TOKEN=$(cat ~/.config/grafana/nearone-token)
HTTP=$(curl -s -o /dev/null -w "%{http_code}" \
  -H "Authorization: Bearer $TOKEN" \
  https://grafana.nearone.org/api/user)
if [ "$HTTP" != "200" ]; then
  echo "ABORT: token rejected by Grafana (HTTP $HTTP). Token is malformed, expired, revoked, or the wrong type. Re-check the prerequisites."
  exit 1
fi
echo "token verified: /api/user returned 200"
unset TOKEN
```

### Step 4: Write the wrapper script

Create `~/.local/bin/mcp-grafana-nearone`:

```bash
#!/usr/bin/env bash
set -euo pipefail

TOKEN_FILE="${HOME}/.config/grafana/nearone-token"

if [[ ! -r "${TOKEN_FILE}" ]]; then
  echo "mcp-grafana-nearone: token file not found at ${TOKEN_FILE}" >&2
  exit 1
fi

export GRAFANA_URL="https://grafana.nearone.org"
GRAFANA_SERVICE_ACCOUNT_TOKEN="$(cat "${TOKEN_FILE}")"
export GRAFANA_SERVICE_ACCOUNT_TOKEN

exec uvx --from mcp-grafana mcp-grafana --disable-write "$@"
```

Then `chmod +x ~/.local/bin/mcp-grafana-nearone`.

**Why `--disable-write`:** this is a read-only deployment. Claude can query, inspect, and render but cannot modify dashboards, alerts, incidents, or annotations. If a teammate needs write access later, they drop the flag in their own wrapper.

### Step 5: Register the MCP server with Claude Code

Scope: **user** (available in every project, not tied to one repo).

```bash
# Remove any stale entry so re-runs are clean
claude mcp remove grafana --scope user 2>/dev/null || true

claude mcp add grafana --scope user -- ~/.local/bin/mcp-grafana-nearone
```

### Step 6: Verify the full stack

```bash
claude mcp list 2>&1 | grep -A1 grafana
```

Expect `grafana: ... ✓ Connected`. If it shows `✗ Failed to connect`, run `~/.local/bin/mcp-grafana-nearone </dev/null` interactively and report the error to the user.

### Step 7: Tell the user what's next

Report to the user, in their words:

> Grafana MCP is set up at user scope. **Restart Claude Code** (exit + relaunch) so the MCP tools register in a new session. Once restarted, you can ask me to query Prometheus, search dashboards, pull logs from Loki, etc.
>
> Token rotation later: overwrite `~/.config/grafana/nearone-token` with the new value. No other config changes needed.
>
> To uninstall: `claude mcp remove grafana --scope user && rm ~/.local/bin/mcp-grafana-nearone ~/.config/grafana/nearone-token`.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `claude mcp list` shows `✓ Connected` but tool calls return "Invalid API key" | Token is `glc_...` (access policy) or `sa-1-...` (login ID), not `glsa_...` | Generate a real service account token; see prerequisites |
| `claude mcp list` shows `✗ Failed to connect` | Wrapper script errored before stdio handshake — usually missing token file | `cat ~/.config/grafana/nearone-token` to confirm presence; run wrapper directly to see error |
| `uvx: command not found` after install | `~/.local/bin` not on `PATH` | `export PATH="$HOME/.local/bin:$PATH"` in shell rc, restart shell |
| 401 on `/api/user` during verification | Token expired, revoked, or wrong org | Regenerate; confirm it's a service account token not an access policy |
| Tools work but empty results on datasource queries | Service account has Viewer role but no datasource-level permissions | Admin needs to grant `datasources:query` scope or ensure Viewer role is org-wide |

## Datasources available on grafana.nearone.org

After setup you can query these directly (UIDs for reference):

- **Prometheus (default)**: `grafanacloud-nearone-prom`
- **Loki**: `grafanacloud-nearone-logs`
- **Tempo**: `grafanacloud-nearone-traces`
- **Postgres**: `telemetry_mainnet`, `telemetry_testnet`, `crt_benchmarks`
- **Validator-specific Prometheus**: `prom-mainnet-validators`, `pagoda-prom`
- **Google Cloud Monitoring**: `Bridge services`, `GCP Google stackdriver`
- Plus `grafana-github-datasource`, `Synthetic Monitoring`, `grafanacloud-ml-metrics`

## Security notes

- The token file is `chmod 600` — don't loosen those permissions.
- **Never paste a token into a Claude Code prompt.** Claude Code sends conversation content to Anthropic; anything you type there is no longer purely local. Use the `read -rsp` flow in the prerequisites.
- Rotate tokens if they're exposed (committed to git, pasted in Slack, etc.). Admin revokes the token in Grafana UI; you generate a new one and overwrite the file.

<!-- cspell:words glsa -->

