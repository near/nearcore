# Zulip MCP setup (macOS + Docker Desktop)

A Zulip MCP server for Claude Code that runs in a companion Docker container, so
your API key stays isolated — it lives only in the container's process env and
your macOS Keychain, never in Claude's conversation context or reachable by
Claude's own tools (when Claude runs in a different container).

**Know the limits.** The companion container is an isolation boundary against
Claude-in-a-container, **not an encryption vault**. Anyone with Docker access
on your Mac can trivially extract the key, e.g.:

```bash
docker compose exec zulip-mcp env | grep ZULIP_API_KEY
```

That includes you, anything else you run on your host, and Claude Code if you
run it natively on macOS with `docker` on `PATH`. Use a dedicated bot key
(see Prerequisites) so rotation is cheap, and see [Security notes](#security-notes)
for the full threat model.

## Prerequisites

- macOS with **Docker Desktop** installed and running
- **Claude Code** CLI installed (`claude --version`)
- A **Zulip API key**. Prefer a **dedicated bot's** key over your personal one —
  it bounds blast radius and is easy to rotate.
  - Bot key: Zulip → avatar → Personal settings → **Bots** → Add a new bot
    (Generic type), then copy its API key. Subscribe the bot to only the streams
    you want Claude to read.
  - Personal key: Zulip → avatar → Personal settings →
    Account & privacy → "Manage your API key".

## One-time setup

### 1. Clone the repo

```bash
git clone https://github.com/VanBarbascu/zulip-mcp.git
cd zulip-mcp
```

### 2. Store your API key in the macOS Keychain

```bash
security add-generic-password -s zulip-mcp -a you@nearone.org -w
```

Paste your API key at the prompt (it won't echo). The key is now in your login
keychain; it will not appear in any file on disk.

**Important:** also do the [Keychain ACL hardening](#required-require-confirmation-on-keychain-access)
below. Without it, the entry is silently readable by any process running as
your user — no safer than a chmod-600 file.

### 3. Create `.env` (no secrets)

```bash
cat > .env <<EOF
ZULIP_URL=https://near.zulipchat.com
ZULIP_EMAIL=you@nearone.org
ZULIP_NOTIFICATION_USER=you@nearone.org
EOF
```

Do **not** put `ZULIP_API_KEY` in `.env`. The start script injects it from
Keychain at launch.

### 4. Add the start script

Save as `start.sh` in the repo root:

```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

ZULIP_API_KEY="$(security find-generic-password -s zulip-mcp -w)" \
  docker compose up -d "$@"
```

```bash
chmod +x start.sh
```

### 5. Start the container

```bash
./start.sh
```

Verify:

```bash
curl -sI http://127.0.0.1:3000/sse | head -1   # → HTTP/1.1 200 OK
```

### 6. Register with Claude Code

Pick the URL based on where your Claude Code runs:

- **Native Claude Code on macOS**:
  ```bash
  claude mcp add --transport sse -s user zulip http://127.0.0.1:3000/sse
  ```

- **Claude Code inside a Docker dev container**:
  ```bash
  claude mcp add --transport sse -s user zulip http://host.docker.internal:3000/sse
  ```

Restart your Claude Code session.

### 7. Test

In Claude Code:

> get my recent Zulip messages

You should see messages come back.

## Recommended: block the write tools

VanBarbascu's server registers four write tools (`create_drafts`, `edit_draft`,
`delete_draft`, `send_notification`). Add this to `~/.claude/settings.json` to
stop Claude from calling them:

```json
{
  "permissions": {
    "allow": [
      "mcp__zulip__get_messages",
      "mcp__zulip__get_drafts"
    ],
    "deny": [
      "mcp__zulip__create_drafts",
      "mcp__zulip__edit_draft",
      "mcp__zulip__delete_draft",
      "mcp__zulip__send_notification"
    ]
  }
}
```

## Required: require confirmation on Keychain access

By default the `zulip-mcp` Keychain entry is silently readable by any process
running as your user (including Claude Code if you run it natively on macOS).
That makes the Keychain no safer than a chmod-600 file in your home directory —
the TouchID / password prompt below is what actually buys you protection over
storing the token in a file. Skip this and you've spent the Keychain's
ergonomics for none of its security:

1. Open Keychain Access — it's tucked away on Sequoia+:
   ```bash
   open /System/Library/CoreServices/Applications/Keychain\ Access.app
   ```
   (Or Spotlight → "Keychain Access".)
2. In the sidebar select **login**, then the **Passwords** category, and find
   the `zulip-mcp` entry.
3. Right-click → **Get Info** → **Access Control** tab.
4. Choose **Confirm before allowing access**.
5. Under **Always allow access by these applications**, select `security` and
   click **−** to remove it. (When you created the entry with
   `security add-generic-password`, the `security` binary was auto-added to the
   trusted list — apps in that list bypass the confirmation prompt, so the
   checkbox alone isn't enough.)
6. **Save Changes**.

Trade-off: you'll get a prompt every time `./start.sh` runs. That's the point —
so will any other process that tries to read the key.

## Daily use

The container has `restart: unless-stopped`, so it comes back on reboots.
Manual controls:

```bash
./start.sh            # bring up
./start.sh --build    # rebuild after code/Dockerfile changes
docker compose down   # stop
docker compose logs -f zulip-mcp
```

## Troubleshooting

**"Malformed API key"** — the value in the container is wrong. Check length
(should be 32):

```bash
docker compose exec zulip-mcp sh -c 'printf "len=%s\n" "${#ZULIP_API_KEY}"'
```

If it's not 32, redo step 2 — the Keychain entry probably has extra whitespace
or the wrong value.

**MCP won't connect in Claude Code** — check you used the right URL for your
Claude Code environment (step 6). Verify reachability:

```bash
# native local host
curl -sI http://127.0.0.1:3000/sse
# or from inside a dev container:
curl -sI http://host.docker.internal:3000/sse
```

**Tools not visible in Claude Code** — restart the session after `claude mcp add`.
Confirm with `claude mcp list`.

## Security notes

**What this setup protects against**

- API key is not in `~/.claude.json` or any other Claude-visible config.
- Claude Code running in a dev container cannot reach the key via its tools:
  no `security` binary to hit the Keychain, network-only access to the MCP
  container (no filesystem / process / env sharing).
- Port bound to `127.0.0.1:3000` — not exposed on your network.

**What it does not protect against**

- Anyone with Docker daemon access (you, or any process on your Mac with
  `docker` on `PATH`) can read the key:
  ```bash
  docker compose exec zulip-mcp env | grep ZULIP_API_KEY
  ```
- Claude Code running **natively on macOS** falls in that category — its Bash
  tool can call `docker exec` or `security find-generic-password` just like
  you can. The companion-container isolation only bites when Claude itself
  runs in a separate container without Docker socket access.
- Memory inspection of the running MCP process (via a debugger) would also
  expose the key. Same for any `docker inspect` of the container.

**Practical mitigations**

- Use a **dedicated bot key** so rotation is cheap and blast radius is
  bounded to streams the bot is subscribed to.
- Keep the **write-tool allowlist** above to prevent accidental sends.
- Lock down the Keychain entry with **Confirm before allowing access** (see
  required section above). Without it the Keychain adds nothing over a file,
  since any user-owned process can read it silently. The trade-off is a
  TouchID/password prompt on every `./start.sh`.
