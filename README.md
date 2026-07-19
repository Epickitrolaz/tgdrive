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
               │  1. Spool chunk body    │
               │     to a temp file on   │
               │     disk (TGDRIVE_TMP_  │
               │     DIR or /tmp)        │
               │  2. POST sendDocument   │
               │     (streams body from  │
               │     disk via requests)  │
               │  3. unlink() the spool  │
               │     file before next    │
               │     chunk               │
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

| Env                          | Default             | Description                                                                |
|------------------------------|---------------------|----------------------------------------------------------------------------|
| `TGDRIVE_TOKEN`              | —                   | Telegram bot token (alternative to `--token`)                              |
| `TGDRIVE_CHAT_ID`            | —                   | Chat ID (alternative to `--chat-id`)                                       |
| `TGDRIVE_CACHE_DIR`          | `~/.cache/tgdrive`  | Where downloaded chunks are cached on disk                                 |
| `TGDRIVE_TMP_DIR`            | `/tmp`              | Where per-handle write spools and chunk-upload spools live                 |
| `TGDRIVE_MAX_CACHE_SIZE`     | unlimited           | Max on-disk cache size (e.g. `10G`); oldest entries evicted past it       |
| `TGDRIVE_MAX_DISK_USAGE`     | disabled            | Max % used on the cache filesystem (e.g. `90`); triggers eviction          |
| `TGDRIVE_MIN_FREE_BYTES`     | `1G`                | Floor of free bytes kept when enforcing `TGDRIVE_MAX_DISK_USAGE`           |
| `TGDRIVE_CACHE_ENFORCE_INTERVAL` | `30`             | Seconds between background cache enforcement passes                        |

## Cache management

Downloaded chunks are stored in `~/.cache/tgdrive` (or `$TGDRIVE_CACHE_DIR`) so
re-reading the same file does not re-fetch from Telegram. To keep the cache
from filling the whole drive, tgdrive enforces two optional upper bounds,
both checked on every cache write and by a background thread (every
`--cache-enforce-interval` seconds):

* `--max-cache-size 10G` (or `TGDRIVE_MAX_CACHE_SIZE=10G`) - the cache is
  shrunk oldest-first whenever the total on-disk size exceeds this limit.
  Accepts `K`/`M`/`G`/`T` suffixes and bare bytes.
* `--max-disk-usage 90` (or `TGDRIVE_MAX_DISK_USAGE=90`) - if the filesystem
  hosting the cache directory is more than this percent full, or there is
  less than `TGDRIVE_MIN_FREE_BYTES` (default `1G`) of free space, the cache
  is shrunk oldest-first until the disk is back under the limit. This is the
  "full drive protection" mode and is recommended on long-running servers.

Either bound can be set independently; both can be set together. Example for
a long-running server with a 50 GB cache volume that should never go over
90% full:

```bash
python -m tgdrive \
    --token BOT_TOKEN --chat-id CHAT_ID \
    --max-cache-size 10G \
    --max-disk-usage 90 \
    --min-free-bytes 2G \
    --cache-enforce-interval 30 \
    /mnt/tgdrive
```

`--clear-cache` (or the absence of one) wipes the cache directory on
startup; use it when you want to start from scratch.

## Low-memory devices (Raspberry Pi, etc.)

By default tgdrive buffers each chunk body in RAM only long enough to wrap
it in an `io.BytesIO` for the upload POST. On a host with limited RAM
(e.g. a Pi Zero 2 W with 512 MB) the kernel can return `ENOSPC` ("No space
left on device") on the local spool before the bytes ever reach Telegram.
Two changes keep memory and disk usage bounded to a single chunk (~15 MB)
at any time:

- **Spool-to-disk uploads.** `upload_chunks_from_file` writes each chunk
  to a `tempfile.mkstemp(prefix="tgdrive-chunk-", suffix=".bin")` file
  in `TGDRIVE_TMP_DIR`, hands the path to `requests` (which streams the
  body from disk), then `os.unlink`s the file before reading the next
  chunk. No chunk body is held in RAM for longer than it takes to write
  it to the spool file.
- **Configurable spool location.** The per-handle write spool
  (`_Handle.tmp`) and the `truncate()` scratch file are both opened with
  `dir=TGDRIVE_TMP_DIR`. On a stock Pi the rootfs is small, so point
  this at a USB stick, external SSD, or a tmpfs sized to fit the largest
  file you intend to copy:

  ```bash
  # Example: use a tmpfs sized for a 2 GB worst-case file
  sudo mount -t tmpfs -o size=2G tmpfs /mnt/tgdrive-tmp
  sudo TGDRIVE_TMP_DIR=/mnt/tgdrive-tmp python -m tgdrive \
      --token BOT_TOKEN --chat-id CHAT_ID /mnt/tgdrive
  ```

  Make sure the target directory lives on a filesystem with at least as
  much free space as the largest file you plan to copy into the mount.
  Spool files are unlinked as soon as the upload completes (or the
  handle is released), so steady-state disk usage stays at zero.

## Architecture Notes

- **Uploads are sequential** — Telegram's sendDocument endpoint does not tolerate
  concurrent large uploads (causes write timeouts). Sequential also spreads
  requests, gentler on rate limits.
- **Downloads are parallel** — up to 8 concurrent `getFile`/`GET file` requests
  to saturate bandwidth.
- **Multi-user reads are parallel and deduplicated** — each FUSE read may span
  several chunks; a shared thread pool fetches the missing chunks concurrently.
  Two users opening the same media file at the same time share a single
  Telegram download per chunk (the chunk cache uses an in-flight Future map to
  coalesce concurrent misses on the same `file_id`), so the second user does
  not have to wait for the first user's stream to finish caching before
  playback starts.
- **Per-handle tmp files are serialized** — each open file handle owns its own
  spool buffer protected by a lock, so two concurrent reads on the same
  handle cannot corrupt each other's `seek`/`write`/`read` sequence. Different
  handles (different users) run fully in parallel.
- **Index** is stored in the pinned message. If it fits under 4000 chars, it is
  kept inline (edited in place, 1 API call). Otherwise it is uploaded as a
  `tgdrive_index.json` document and the pinned message becomes a "pointer".
- **Polling** — a background thread checks the pinned message fingerprint every
  5 seconds, so changes made from another machine are picked up automatically.
- **`release()` does NOT flush** — files must be explicitly `fsync()`ed or
  `flush()`ed before closing, or writes are lost.

## Credits

This project was AI-generated using [OpenCode](https://opencode.ai) with GLM 5.2 and Kimi K2.7.
The plan and architecture were designed by a human.
