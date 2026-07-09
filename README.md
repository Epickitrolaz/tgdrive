# tgdrive

### Mount a Telegram chat as a FUSE filesystem.

```
                    ┌───────────────┐
                    │  Telegram     │
                    │  Bot API      │
                    └──┬─────▲──────┘
                 POST  │     │  GET
              sendDoc  │     │ getFile
                 ┌─────▼─────┴─────────┐
                 │  telegram.py        │
                 │  TelegramClient     │
                 │                     │
                 │  upload: sequential │
                 │  download: parallel │
                 │  (8 workers)        │
                 └─────────┬───────────┘
                           │
              ┌────────────┴────────────┐
              │  fs.py                  │
              │  TgDriveFS (FUSE)       │
              │                         │
              │  Index in pinned msg    │
              │  Polls every 5s for     │
              │  remote changes         │
              └────────────┬────────────┘
                           │
              ┌────────────┴────────────┐
              │  cli.py                 │
              │  python -m tgdrive      │
              │  mountpoint /mnt/tgdrive│
              └─────────────────────────┘
```

**File upload flow** (on `flush()` / `fsync()`):

```
User writes → temp file → upload_chunks_from_file()
                           │
              ┌────────────▼────────────┐
              │  For each chunk:        │
              │  POST sendDocument      │
              │  (sequential, 15 MB)    │
              │  ↓                      │
              │  Telegram stores doc    │
              │  Returns file_id        │
              ├─────────────────────────┤
              │  Update pinned index    │
              │  (inline or doc-based)  │
              └─────────────────────────┘
```

**File read flow** (on `read()`):

```
User reads → read() → _ensure_loaded() (first time)
                        │
              ┌─────────▼──────────────┐
              │  For each needed chunk: │
              │  GET getFile → file_id  │
              │  GET /file/<path>       │
              │  (parallel, 8 workers)  │
              │  ↓                      │
              │  Write into temp file   │
              │  Cache to ~/.cache/     │
              └─────────────────────────┘
```

## Requirements

- Linux with FUSE support (`modprobe fuse`)
- Python 3.10+
- `libfuse2` or `libfuse-dev`
- A Telegram bot token from [@BotFather](https://t.me/BotFather)
- A Telegram chat ID

## Setup

```bash
# System dependencies (Debian/Ubuntu)
sudo apt install fuse libfuse2 python3 python3-venv python3-pip

# Clone and enter
git clone https://github.com/Epickitrolaz/tgdrive && cd tgdrive

# Virtual environment
python3 -m venv venv
source venv/bin/activate

# Install Python dependencies
pip install -r requirements.txt

# Mount (foreground, test first)
python -m tgdrive --token BOT_TOKEN --chat-id CHAT_ID /mnt/tgdrive --foreground

# Mount (background / daemonized)
python -m tgdrive --token BOT_TOKEN --chat-id CHAT_ID /mnt/tgdrive
```

## Performance

| Metric       | Speed    |
|--------------|----------|
| Upload       | 10.5 MB/s (sustained, sequential chunks) |
| Download     | 13 MB/s (sustained, 8 parallel workers)  |

Telegram applies per-chat rate limits (roughly 30–40 s cooldown after ~20 messages).
Large files amortize this well; workloads with many small files may be throttled to as little as 1MB/s.
Downloads are cached on disk at `~/.cache/tgdrive/` to avoid re-fetching.

## CLI

| Argument        | Env              | Default        | Description                       |
|-----------------|------------------|----------------|-----------------------------------|
| `--token`       | `TGDRIVE_TOKEN`  | —              | Telegram bot token (required)     |
| `--chat-id`     | `TGDRIVE_CHAT_ID`| —              | Chat ID (required)                |
| `--foreground`  | —                | `false`        | Run in foreground                 |
| `--debug`       | —                | `false`        | FUSE debug output                 |
| `--chunk-size`  | —                | 15 MB          | Upload chunk size in bytes        |
| `mountpoint`    | —                | `/mnt/tgdrive` | FUSE mount point                  |

## Architecture Notes

- **Uploads are sequential** — Telegram's sendDocument endpoint does not tolerate
  concurrent large uploads (causes write timeouts). Sequential also spreads
  requests, gentler on rate limits.
- **Downloads are parallel** — up to 8 concurrent `getFile`/`GET file` requests
  to saturate bandwidth.
- **Index** is stored in the pinned message. If it fits under 4000 chars, it is
  kept inline (edited in place, 1 API call). Otherwise it is uploaded as a
  `tgdrive_index.json` document and the pinned message becomes a "pointer".
- **Polling** — a background thread checks the pinned message fingerprint every
  5 seconds, so changes made from another machine are picked up automatically.
- **`release()` does NOT flush** — files must be explicitly `fsync()`ed or
  `flush()`ed before closing, or writes are lost.

## Credits

This project was AI-generated using [OpenCode](https://opencode.ai) with GLM 5.2.
The plan and architecture were designed by a human.
