# Meshtastic ↔ Discord Bridge Bot

A robust bridge that relays Meshtastic **text messages** to Discord with clean, information‑rich embeds, plus helpful commands for querying nodes, recent traffic, and network health. It stores data in SQLite so names, telemetry, and stats improve over time.

---

## ✨ Highlights

- **Human‑readable relays**: Messages show **LongName (node_id)** with hops/SNR/RSSI when available.
- **Daily Digest**: A 📊 summary embed once a day (messages, active nodes, new nodes, link quality, top talkers).
- **Useful commands**: `$nodes`, `$activenodes`, `$stats`, `$last`, `$find`, `$whois`, `$uptime`, `$txt`, `$send`, `$help`, `$status`.
- **Rate‑limit & dedup**: Prevents spam during bursts.
- **Name resolution**: Long/short names pulled from the DB; auto‑fallback to node_id until learned.
- **No per‑channel filters or allowlists** (by request) — simple global bridge toggle via `$bridge on|off`.

---

## 🧭 Architecture at a glance

- **MeshtasticInterface** (TCP/serial) → receives packets and pushes structured events
- **SQLite** (`meshtastic.db`) → nodes, telemetry, positions, messages
- **DiscordBot** → renders readable embeds, exposes commands, posts daily digest
- **CommandHandler** → command parsing, DB lookups, formatting

---

## ✅ Requirements

- **Python 3.10+** (3.11 recommended)
- `pip install -r requirements.txt`
  - `discord.py>=2.3.0`
  - `python-dotenv>=1.0.0`
  - `pypubsub>=4.0.0`
  - `meshtastic>=2.0.0`
- **Windows timezone note**: If daily digest timezone resolution fails, install `tzdata`:
  ```bash
  pip install tzdata
  ```

---

## 🚀 Quick Start

1. Unzip the project (or clone your working repo).
2. Create a **.env** file in the project root (see config below).
3. Install deps:
   ```bash
   python -m pip install -r requirements.txt
   ```
4. Run the bot:
   ```bash
   python bot.py
   ```
5. Invite your Discord bot to a server and make sure it can **read & send messages** in your target channel.

The bot will create `meshtastic.db` on first run and start learning node names and telemetry as it sees packets.

---

## 🔧 Configuration (.env)

```env
# Required
DISCORD_TOKEN=your_discord_bot_token
DISCORD_CHANNEL_ID=123456789012345678
MESHTASTIC_HOSTNAME=192.168.0.150   # or leave blank to use serial if your code is set up for it

# Optional – sane defaults provided
BRIDGE_ENABLED=1                   # 1: relay mesh->Discord, 0: disable
RELAY_RATE_LIMIT_PER_MIN=60        # token bucket per minute
DEDUP_WINDOW_SEC=20                # ignore duplicate texts within this window
VERBOSE_EMBEDS=1                   # 1: rich embeds, 0: plain strings
MAX_EMBED_FIELDS=25                # Discord embed field cap (safety)

# Daily Digest
DIGEST_ENABLED=1
DIGEST_HOUR_LOCAL=9                # hour in local timezone
DIGEST_TZ=America/Toronto          # IANA TZ name; install 'tzdata' on Windows if needed
DIGEST_HOURS=24                    # interval window for the digest
```

> **Tip:** Longnames show up as the bot learns them. Initially, some messages may fall back to `node_id` until node info is received and stored.

---

## 💬 Commands

| Command | What it does | Example |
|---|---|---|
| `$help` | Show command help + examples | `$help` |
| `$txt <message>` | Send a message to the primary mesh channel | `$txt Hello mesh!` |
| `$send "<name>" <message>` | Send to a specific node (fuzzy longname match) | `$send "John Base" Ping?` |
| `$nodes` | List all known nodes (paged embeds) | `$nodes` |
| `$activenodes` | Nodes heard in last 60 minutes (paged embeds) | `$activenodes` |
| `$stats [hours]` | Network stats embed for the last N hours | `$stats 12` |
| `$last [N]` | Show last N text messages (default 10, up to 50) | `$last 25` |
| `$find <text> [N]` | Search recent message text | `$find sos 20` |
| `$whois <name-or-id>` | Show a node’s details (name, id, hops, last heard) | `$whois !433d1b18` |
| `$uptime` | Show bot uptime | `$uptime` |
| `$status` | Bridge status/health snapshot | `$status` |
| `$bridge on|off` | Enable/disable mesh→Discord relay globally | `$bridge off` |

---

## 📅 Daily Digest

- Runs once per day at `DIGEST_HOUR_LOCAL` in `DIGEST_TZ`.
- Embed includes:
  - Total **Messages** in the window (`DIGEST_HOURS`, default 24h)
  - **Active nodes** and **New nodes**
  - **Avg SNR / Avg RSSI**
  - **Top 5 talkers** (by message count)
- To disable: set `DIGEST_ENABLED=0`

> Digest is posted only once per date (guards against loop frequency).

---

## 🧠 How names & metrics are resolved

- On every text message, the bot:
  - Stores message metadata (from/to node IDs, text, hops, SNR/RSSI) in SQLite.
  - Tries to resolve **long_name** or **short_name** from the `nodes` table.
  - Updates `last_heard` for the sender (and recipient if known).
- Node entries are inserted/updated when node info/telemetry packets are seen.

---

## 🗂️ Database

- File: `meshtastic.db` (SQLite)
- Main tables (typical):
  - `nodes` – node_id, long/short names, last_heard, hops, etc.
  - `telemetry` – recent node telemetry (battery/temp/etc.)
  - `positions` – location data when available
  - `messages` – text message logs with link metrics
- Helpers used by the bot for stats/digest:
  - `count_messages_since(since_iso)`
  - `top_talkers_since(since_iso, limit)`
  - `new_nodes_since(since_iso)`
  - `avg_link_quality_since(since_iso)`
  - `get_recent_messages(limit)`, `search_messages(query, limit)`
  - `get_node_by_id(node_id)`, `find_node_by_name(name)`

### Maintenance

A helper script is included:

```bash
python maintain_db.py --stats
python maintain_db.py --prune --older-than-days 30
```

> Always back up `meshtastic.db` before pruning.

---

## 🧪 Running tips

- **Discord permissions**: Ensure the bot can view & post in the `DISCORD_CHANNEL_ID` channel.
- **Meshtastic connection**: Verify `MESHTASTIC_HOSTNAME` (TCP) or adapt for serial if that’s your setup.
- **Busy meshes**: Tune `RELAY_RATE_LIMIT_PER_MIN` and `DEDUP_WINDOW_SEC` for your traffic patterns.
- **Windows TZ**: If the digest throws timezone errors, `pip install tzdata` or set `DIGEST_TZ=UTC`.

---

## 🛠️ Troubleshooting

- **Longnames still not appearing**  
  The bot hasn’t learned the node yet. Give it time to see nodeinfo/telemetry. It will fall back to `node_id` until learned.

- **Nothing posts to Discord**  
  Double‑check `DISCORD_TOKEN`, `DISCORD_CHANNEL_ID`, and channel permissions. Watch the console logs for errors.

- **Rate‑limit drops**  
  Logs will say “Rate limit reached; dropping message”. Raise `RELAY_RATE_LIMIT_PER_MIN` or lower traffic.

- **Digest not posting**  
  Confirm `DIGEST_ENABLED=1`, `DIGEST_TZ` is valid, and your system time is correct. On Windows, install `tzdata`.

- **DB locked / busy**  
  Avoid running multiple copies of the bot pointing at the same DB file.

---

## 🔄 Recent changes (this build)

- Longname‑first embeds for mesh→Discord relays
- Clean embeds for `$last`, `$find`, `$whois`, `$stats`, `$nodes`, `$activenodes`
- Daily Digest (messages, nodes, link quality, top talkers)
- `$bridge` toggle + rate limiting + dedup for robustness
- Safer DB updates and `last_heard` maintenance

---

## 🙌 Notes

- This bot is intentionally simple operationally (no per‑channel filters or allowlists).  
- If you want **topology snapshots**, **keyword watches**, **CSV exports**, or **quiet hours**, say the word and we’ll extend this build.

---

**Happy meshing!**
