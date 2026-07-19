"""FUSE filesystem backed by a Telegram chat.

Files are stored as Telegram document messages (20 MB chunks) and a JSON
directory index is kept in the chat's pinned message. Open file data is spooled
to temporary files and only uploaded on flush()/fsync(); release() does NOT flush.
"""

from __future__ import annotations

import errno
import hashlib
import json
import logging
import os
import stat
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from fuse import FUSE, FuseOSError, Operations

from .cache import ChunkCache, default_cache_dir
from .telegram import TelegramClient

log = logging.getLogger("tgdrive.fs")

DIR_MODE = 0o755
FILE_MODE = 0o644
ROOT_PATH = "/"
POLL_INTERVAL = 5.0


def _norm(path: str) -> str:
    if not path or path == ROOT_PATH:
        return ROOT_PATH
    # Collapse duplicate/trailing slashes.
    parts = [p for p in path.split("/") if p]
    return ROOT_PATH + "/".join(parts)


def _now() -> float:
    return time.time()


_DEFAULT_CACHE_DIR = os.path.join(os.path.expanduser("~"), ".cache", "tgdrive")
_DEFAULT_TMP_DIR = tempfile.gettempdir()


def _tmp_dir() -> str:
    """Directory for per-handle write spool files.

    On a stock Pi the rootfs is small, so a multi-hundred-MB write will
    fill ``/tmp`` and the kernel returns ``ENOSPC`` to ``cp`` ("No space
    left on device"). Point ``TGDRIVE_TMP_DIR`` at a drive with enough
    free space (a USB stick, an external SSD, or a tmpfs sized to fit
    the largest file you intend to copy) and tgdrive will spool writes
    there instead.
    """
    d = os.environ.get("TGDRIVE_TMP_DIR") or _DEFAULT_TMP_DIR
    os.makedirs(d, exist_ok=True)
    return d


def _cache_path(file_id: str) -> str:
    d = os.environ.get("TGDRIVE_CACHE_DIR") or _DEFAULT_CACHE_DIR
    os.makedirs(d, exist_ok=True)
    h = hashlib.sha256(file_id.encode()).hexdigest()
    return os.path.join(d, h)


class _Handle:
    """An open file handle.

    Holds a temporary file that is the single source of truth for this handle:
    for read-only handles it is filled once (lazily) from Telegram and reused
    for every read(); for writable handles it accumulates writes until flush().
    """

    __slots__ = (
        "fh",
        "path",
        "tmp",
        "size",
        "loaded",
        "dirty",
        "flushed_chunks",
        "uuid",
        "start_empty",
        "write_mode",
        "downloaded_parts",
        "lock",
    )

    def __init__(self, fh: int, path: str, start_empty: bool = False, write_mode: bool = False):
        self.fh = fh
        self.path = path
        # The spool file lives in TGDRIVE_TMP_DIR (or /tmp by default) so
        # writes don't fill the rootfs on a stock Pi. See _tmp_dir() for why.
        self.tmp = tempfile.TemporaryFile(dir=_tmp_dir())
        self.size = 0
        self.loaded = False
        self.dirty = False
        self.flushed_chunks: list[dict[str, Any]] | None = None
        self.uuid = uuid.uuid4().hex
        self.start_empty = start_empty
        self.write_mode = write_mode
        self.downloaded_parts: set[int] = set()
        # Serializes concurrent reads on the same handle so the tmp file's
        # seek/write/read sequence is not interleaved. Cross-handle reads run
        # in parallel; this lock only protects a single _Handle.tmp buffer.
        self.lock = threading.Lock()


class _CachingTempFile:
    """A write-only file-like wrapper that mirrors each chunk into the cache.

    The TelegramClient writes downloaded chunk bodies sequentially into a
    file object; this wrapper observes the writes, pairs them back to the
    originating ``file_id`` based on the running offset, and pushes each
    chunk body into the :class:`ChunkCache` so it can be served from disk
    next time without re-fetching from Telegram.
    """

    def __init__(self, real, cache: ChunkCache, chunks: list[dict[str, Any]]):
        self._real = real
        self._cache = cache
        # Pair each chunk with the offset the client will write it at. We
        # sort by part index to match download_chunks_to_file's ordering.
        ordered = sorted(
            (c for c in chunks if c.get("file_id")),
            key=lambda c: c.get("part", 0),
        )
        self._plan: list[tuple[int, int, str]] = []
        off = 0
        for c in ordered:
            size = int(c.get("size", 0))
            if size <= 0:
                continue
            self._plan.append((off, off + size, c["file_id"]))
            off += size
        self._buf = bytearray()
        self._write_pos = 0
        self._plan_idx = 0

    def write(self, data: bytes) -> int:
        if not data:
            return 0
        self._buf.extend(data)
        # Drain into the underlying file and into the cache, advancing through
        # _plan as we cross chunk boundaries.
        while self._buf and self._plan_idx < len(self._plan):
            start, end, fid = self._plan[self._plan_idx]
            chunk_pos = self._write_pos - start
            need = (end - start) - chunk_pos
            take = min(len(self._buf), need)
            if take <= 0:
                break
            piece = bytes(self._buf[:take])
            self._real.write(piece)
            try:
                self._cache.put(fid, piece)
            except Exception as e:  # pragma: no cover - defensive
                log.debug("cache put failed for %s: %s", fid, e)
            del self._buf[:take]
            self._write_pos += take
            if self._write_pos >= end:
                self._plan_idx += 1
        # If the client writes past the planned chunks (shouldn't happen but
        # is harmless), just mirror it to the real file.
        if self._buf:
            self._real.write(bytes(self._buf))
            self._write_pos += len(self._buf)
            self._buf.clear()
        return len(data)

    def seek(self, *args, **kwargs):
        return self._real.seek(*args, **kwargs)

    def truncate(self, *args, **kwargs):
        return self._real.truncate(*args, **kwargs)

    def flush(self):
        return self._real.flush()

    def close(self):
        return None

    def __getattr__(self, name):
        return getattr(self._real, name)


class TgDriveFS(Operations):
    """A FUSE filesystem mapping a Telegram chat to a virtual drive."""

    def __init__(
        self,
        token: str,
        chat_id: str | int,
        chunk_size: int | None = None,
        cache: ChunkCache | None = None,
    ):
        kwargs: dict[str, Any] = {}
        if chunk_size:
            kwargs["chunk_size"] = chunk_size
        self.tg = TelegramClient(token, chat_id, **kwargs)
        self.entries: dict[str, dict[str, Any]] = {}
        self._lock = threading.RLock()
        self._handles: dict[int, _Handle] = {}
        self._next_fh = 1
        self._fh_lock = threading.Lock()
        self._stop = threading.Event()
        self._fingerprint: str = ""
        self._uid = os.getuid()
        self._gid = os.getgid()
        # The on-disk chunk cache. The CLI hands us a fully configured instance
        # (with size / disk-usage limits); fall back to an unconstrained one
        # for tests and library users that don't care.
        if cache is None:
            cache = ChunkCache(cache_dir=os.environ.get("TGDRIVE_CACHE_DIR"))
        self.cache = cache
        self.cache.start()
        # Shared thread pool for parallel chunk downloads. A single FUSE
        # read() may span several chunks; running them concurrently (and
        # letting multiple users share the pool) is the difference between
        # "second user waits behind first user" and "both streams start
        # immediately". Eight workers matches the download parallelism that
        # already exists in TelegramClient.download_chunks().
        self._read_pool = ThreadPoolExecutor(
            max_workers=8, thread_name_prefix="tgdrive-read"
        )
        # Load initial index.
        self._load_index()
        # Start background poller.
        self._poller = threading.Thread(target=self._poll_loop, daemon=True, name="tgdrive-poll")
        self._poller.start()

    # -- index (de)serialization -------------------------------------------
    def _empty_index(self) -> dict[str, Any]:
        return {"version": 1, "entries": {}}

    def _load_index(self) -> None:
        with self._lock:
            try:
                idx = self.tg.read_pinned_index()
            except Exception as e:
                log.error("failed to load index: %s", e)
                idx = None
            if not isinstance(idx, dict) or "entries" not in idx:
                self.entries = {}
            else:
                self.entries = {str(k): dict(v) for k, v in idx.get("entries", {}).items()}
            self._fingerprint = self._compute_fingerprint()
            log.debug("loaded index with %d entries", len(self.entries))

    def _compute_fingerprint(self) -> str:
        try:
            meta = self.tg.get_pinned_meta()
        except Exception:
            meta = None
        if meta is None:
            return ""
        return json.dumps(meta, sort_keys=True, separators=(",", ":"))

    def _index_dict(self) -> dict[str, Any]:
        return {"version": 1, "entries": self.entries}

    def _persist(self) -> None:
        with self._lock:
            idx = self._index_dict()
            try:
                meta = self.tg.write_index(idx)
            except Exception as e:
                log.exception("failed to persist index (entries=%d): %s", len(self.entries), e)
                raise FuseOSError(errno.EIO)
            self._fingerprint = json.dumps(meta, sort_keys=True, separators=(",", ":"))

    # -- poller ------------------------------------------------------------
    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            try:
                remote_fp = self._compute_fingerprint()
                if remote_fp and remote_fp != self._fingerprint:
                    # Our own persist may have raced with this fetch and already
                    # updated the fingerprint; re-check before reloading to avoid
                    # a spurious (and potentially clobbering) reload.
                    if remote_fp != self._fingerprint:
                        log.info("detected remote index change, reloading")
                        self._load_index()
            except Exception as e:
                log.debug("poll error: %s", e)
            self._stop.wait(POLL_INTERVAL)

    def destroy(self, path: str | None = None) -> None:
        self._stop.set()
        try:
            self._read_pool.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass
        try:
            self.cache.stop()
        except Exception:
            pass

    # -- handle helpers ----------------------------------------------------
    def _new_fh(self, path: str, start_empty: bool = False, write_mode: bool = False) -> int:
        with self._fh_lock:
            fh = self._next_fh
            self._next_fh += 1
            self._handles[fh] = _Handle(fh, path, start_empty=start_empty, write_mode=write_mode)
            return fh

    def _get_handle(self, fh: int) -> _Handle:
        h = self._handles.get(fh)
        if h is None:
            raise FuseOSError(errno.EBADF)
        return h

    def _ensure_loaded(self, h: _Handle) -> None:
        """Populate the handle's temporary file from Telegram exactly once.

        After the first call the temporary file is reused for all subsequent reads on
        this handle, so a 100 MB file is downloaded a single time per open()
        instead of once per 128 KB read.
        """
        if h.loaded or h.dirty:
            return
        with self._lock:
            entry = self.entries.get(h.path)
            chunks = list(entry.get("chunks", [])) if entry else []
        if h.start_empty or not chunks:
            h.tmp.seek(0)
            h.tmp.truncate(0)
            h.size = 0
            h.loaded = True
            return
        size = int(entry.get("size", sum(int(c.get("size", 0)) for c in chunks))) if entry else 0
        log.info(
            "downloading %d chunk(s) (%.2f MB) for %s",
            len(chunks),
            size / 1e6,
            h.path,
        )
        t0 = time.time()
        last_pct = [-1]

        def on_progress(done: int, total: int, bytes_done: int, bytes_total: int) -> None:
            if total:
                pct = int(done * 100 / total)
            else:
                pct = 100
            if pct != last_pct[0] and pct % 10 == 0:
                last_pct[0] = pct
                rate = bytes_done / (time.time() - t0) / 1e6 if time.time() > t0 else 0
                log.info(
                    "  download %s: %d/%d chunks (%d%%, %.2f MB/s)",
                    h.path,
                    done,
                    total,
                    pct,
                    rate,
                )

        # Wrap the file-like object the TelegramClient writes into so that
        # each chunk body also lands in the on-disk cache and the cache's
        # LRU/size eviction policy applies on prefetch paths.
        cached_tmp = _CachingTempFile(h.tmp, self.cache, chunks)

        try:
            downloaded = self.tg.download_chunks_to_file(chunks, cached_tmp, on_progress=on_progress)
        except Exception as e:
            log.error("download failed for %s: %s", h.path, e)
            raise FuseOSError(errno.EIO)
        h.size = downloaded
        h.loaded = True
        elapsed = time.time() - t0
        rate = h.size / elapsed / 1e6 if elapsed > 0 else 0.0
        log.info(
            "downloaded %s (%d bytes) in %.2fs (%.2f MB/s)",
            h.path,
            h.size,
            elapsed,
            rate,
        )

    # -- attribute helpers -------------------------------------------------
    def _entry_for(self, path: str) -> dict[str, Any] | None:
        with self._lock:
            return self.entries.get(path)

    def _is_implicit_dir(self, path: str) -> bool:
        if path == ROOT_PATH:
            return True
        prefix = path + "/"
        with self._lock:
            for k in self.entries:
                if k.startswith(prefix):
                    return True
        return False

    def _dir_attr(self, mtime: float | None = None) -> dict[str, Any]:
        return {
            "st_mode": stat.S_IFDIR | DIR_MODE,
            "st_nlink": 2,
            "st_size": 4096,
            "st_mtime": mtime or _now(),
            "st_atime": mtime or _now(),
            "st_ctime": mtime or _now(),
            "st_uid": self._uid,
            "st_gid": self._gid,
        }

    def _file_attr(self, entry: dict[str, Any]) -> dict[str, Any]:
        mode = entry.get("mode", FILE_MODE)
        size = int(entry.get("size", 0))
        mtime = float(entry.get("mtime", _now()))
        atime = float(entry.get("atime", mtime))
        return {
            "st_mode": (stat.S_IFREG | (mode & 0o7777)) if not stat.S_ISREG(mode) else mode,
            "st_nlink": 1,
            "st_size": size,
            "st_mtime": mtime,
            "st_atime": atime,
            "st_ctime": mtime,
            "st_uid": int(entry.get("uid", self._uid)),
            "st_gid": int(entry.get("gid", self._gid)),
        }

    def _resolve_attr(self, path: str) -> dict[str, Any]:
        if path == ROOT_PATH:
            return self._dir_attr(_now())
        with self._lock:
            entry = self.entries.get(path)
            if entry is not None:
                if entry.get("type") == "dir":
                    return self._dir_attr(float(entry.get("mtime", _now())))
                return self._file_attr(entry)
        if self._is_implicit_dir(path):
            return self._dir_attr(_now())
        raise FuseOSError(errno.ENOENT)

    # -- FUSE operations ---------------------------------------------------
    def getattr(self, path: str, fh: int | None = None) -> dict[str, Any]:
        path = _norm(path)
        return self._resolve_attr(path)

    def readdir(self, path: str, fh: int | None = None) -> list[str]:
        path = _norm(path)
        names = {".", ".."}
        prefix = ROOT_PATH if path == ROOT_PATH else path + "/"
        plen = len(prefix)
        with self._lock:
            for k in self.entries:
                if path != ROOT_PATH and not k.startswith(prefix):
                    continue
                rest = k[plen:]
                if not rest:
                    continue
                child = rest.split("/", 1)[0]
                names.add(child)
        return list(names)

    def access(self, path: str, mode: int) -> int:
        path = _norm(path)
        try:
            self._resolve_attr(path)
        except FuseOSError as e:
            if mode & os.F_OK and e.errno == errno.ENOENT:
                raise
            return 0
        return 0

    def open(self, path: str, flags: int) -> int:
        path = _norm(path)
        with self._lock:
            entry = self.entries.get(path)
            if entry is None or entry.get("type") != "file":
                raise FuseOSError(errno.ENOENT)
        start_empty = bool(flags & os.O_TRUNC)
        write_mode = bool(flags & (os.O_WRONLY | os.O_RDWR))
        return self._new_fh(path, start_empty=start_empty, write_mode=write_mode)

    def create(self, path: str, mode: int, fi: Any | None = None) -> int:
        path = _norm(path)
        now = _now()
        entry = {
            "type": "file",
            "size": 0,
            "mtime": now,
            "atime": now,
            "mode": stat.S_IFREG | (mode & 0o7777),
            "uid": self._uid,
            "gid": self._gid,
            "chunks": [],
            "uuid": uuid.uuid4().hex,
        }
        with self._lock:
            self.entries[path] = entry
            self._persist()
        return self._new_fh(path, start_empty=True, write_mode=True)

    def read(self, path: str, size: int, offset: int, fh: int) -> bytes:
        path = _norm(path)
        h = self._get_handle(fh)

        # For dirty/written-to handles the temp file is authoritative.
        if h.dirty:
            self._ensure_loaded(h)
            with h.lock:
                if offset >= h.size:
                    return b""
                h.tmp.seek(offset)
                return h.tmp.read(size)

        # Read-only path: download only the chunks needed for this range,
        # fetching any missing chunks in parallel via the shared pool. Two
        # users opening the same file at the same time share a single
        # download per chunk (the cache's in-flight Future), so the second
        # user doesn't wait for the first user's stream to finish caching.
        with self._lock:
            entry = self.entries.get(h.path)
            if entry is None:
                raise FuseOSError(errno.ENOENT)
            chunks = list(entry.get("chunks", []))
            file_size = int(entry.get("size", 0))

        if offset >= file_size:
            return b""

        nread = min(size, file_size - offset)

        if chunks and nread:
            cs = self.tg.chunk_size
            start_part = offset // cs
            end_part = (offset + nread - 1) // cs
            # Chunks we still need bytes from. Already-loaded chunks are
            # skipped; the rest are submitted to the pool together.
            part_by_fid: dict[str, dict[str, Any]] = {}
            for c in chunks:
                fid = c.get("file_id")
                if not fid:
                    continue
                part_by_fid[fid] = c
            needed: list[dict[str, Any]] = []
            for part in range(start_part, end_part + 1):
                if part in h.downloaded_parts:
                    continue
                cm = next((c for c in chunks if c.get("part") == part), None)
                if cm is None:
                    continue
                # Cache hit short-circuits without going through the pool.
                fid = cm["file_id"]
                if self.cache.get(fid) is not None:
                    data = self.cache.get(fid)
                    with h.lock:
                        if part not in h.downloaded_parts:
                            h.tmp.seek(part * cs)
                            h.tmp.write(data)
                            h.downloaded_parts.add(part)
                    continue
                needed.append(cm)
            if needed:
                tg = self.tg
                cache = self.cache
                pool = self._read_pool

                def fetch_chunk(cm: dict[str, Any]) -> tuple[int, bytes]:
                    fid = cm["file_id"]

                    def fetcher() -> bytes:
                        return tg.download_file(fid)

                    return cm.get("part", 0), cache.get_or_fetch(fid, fetcher)

                futures = [pool.submit(fetch_chunk, cm) for cm in needed]
                results: list[tuple[int, bytes]] = []
                for fut in futures:
                    try:
                        results.append(fut.result())
                    except Exception as e:
                        log.error("read fetch failed for %s: %s", h.path, e)
                        raise FuseOSError(errno.EIO)
                with h.lock:
                    for part, data in results:
                        if part in h.downloaded_parts:
                            continue
                        h.tmp.seek(part * cs)
                        h.tmp.write(data)
                        h.downloaded_parts.add(part)

        with h.lock:
            h.tmp.seek(offset)
            return h.tmp.read(nread)

    def write(self, path: str, data: bytes, offset: int, fh: int) -> int:
        path = _norm(path)
        h = self._get_handle(fh)
        # Load existing content so offset-based writes merge correctly (unless
        # the handle was opened with O_TRUNC / via create(), in which case
        # _ensure_loaded is a no-op that leaves an empty temporary file).
        self._ensure_loaded(h)
        with self._lock:
            # Ensure file entry exists.
            if path not in self.entries:
                now = _now()
                self.entries[path] = {
                    "type": "file",
                    "size": 0,
                    "mtime": now,
                    "atime": now,
                    "mode": stat.S_IFREG | FILE_MODE,
                    "uid": self._uid,
                    "gid": self._gid,
                    "chunks": [],
                    "uuid": h.uuid,
                }
            entry = self.entries[path]
            entry["uuid"] = h.uuid
        end = offset + len(data)
        h.tmp.seek(offset)
        h.tmp.write(data)
        if end > h.size:
            h.size = end
        h.dirty = True
        h.flushed_chunks = None
        return len(data)

    def flush(self, path: str, fh: int) -> int:
        path = _norm(path)
        h = self._get_handle(fh)
        if not h.dirty:
            return 0
        # Delete any previously flushed chunks from this handle.
        with self._lock:
            entry = self.entries.get(path)
            old_chunks = []
            if entry:
                old_chunks = list(entry.get("chunks", []))
        if h.flushed_chunks:
            old_chunks = h.flushed_chunks
        if old_chunks:
            try:
                self.tg.delete_chunks(old_chunks)
            except Exception as e:
                log.warning("could not delete old chunks for %s: %s", path, e)
        now = _now()
        with self._lock:
            entry = self.entries.get(path)
            if entry is None:
                entry = {
                    "type": "file",
                    "size": 0,
                    "mtime": now,
                    "atime": now,
                    "mode": stat.S_IFREG | FILE_MODE,
                    "uid": self._uid,
                    "gid": self._gid,
                    "chunks": [],
                    "uuid": h.uuid,
                }
                self.entries[path] = entry
            entry["uuid"] = h.uuid
            mtime = float(entry.get("mtime", now))
            mode = int(entry.get("mode", stat.S_IFREG | FILE_MODE))
        try:
            chunks = self.tg.upload_chunks_from_file(path, h.tmp, h.size, mtime, mode, h.uuid)
        except Exception as e:
            log.error("upload failed for %s: %s", path, e)
            raise FuseOSError(errno.EIO)
        with self._lock:
            entry = self.entries.get(path)
            if entry is not None:
                entry["chunks"] = chunks
                entry["size"] = h.size
                entry["mtime"] = mtime
                entry["uuid"] = h.uuid
            # Persist atomically with the chunk/size update so a poller reload
            # cannot clobber the just-uploaded file metadata.
            self._persist()
        h.dirty = False
        h.loaded = True
        h.flushed_chunks = chunks
        return 0

    def fsync(self, path: str, datasync: int, fh: int) -> int:
        return self.flush(path, fh)

    def release(self, path: str, fh: int) -> int:
        with self._fh_lock:
            h = self._handles.pop(fh, None)
        if h is not None:
            try:
                h.tmp.close()
            except Exception:
                pass
        return 0

    def truncate(self, path: str, length: int, fh: int | None = None) -> int:
        path = _norm(path)
        if fh is not None and fh in self._handles:
            h = self._handles[fh]
            self._ensure_loaded(h)
            h.tmp.truncate(length)
            h.size = length
            h.dirty = True
            h.flushed_chunks = None
            with self._lock:
                entry = self.entries.get(path)
                if entry is not None:
                    entry["size"] = length
            return 0
        with self._lock:
            entry = self.entries.get(path)
            if entry is None:
                raise FuseOSError(errno.ENOENT)
            chunks = list(entry.get("chunks", []))
            mtime = float(entry.get("mtime", _now()))
            mode = int(entry.get("mode", stat.S_IFREG | FILE_MODE))
            file_uuid = entry.get("uuid", uuid.uuid4().hex)
        if length == 0:
            if chunks:
                try:
                    self.tg.delete_chunks(chunks)
                except Exception as e:
                    log.warning("delete chunks during truncate: %s", e)
            with self._lock:
                entry = self.entries.get(path)
                if entry is not None:
                    entry["chunks"] = []
                    entry["size"] = 0
                    entry["mtime"] = _now()
                self._persist()
            return 0
        with tempfile.TemporaryFile(dir=_tmp_dir()) as tmp:
            try:
                if chunks:
                    self.tg.download_chunks_to_file(chunks, tmp)
            except Exception as e:
                log.error("truncate read failed: %s", e)
                raise FuseOSError(errno.EIO)
            tmp.truncate(length)
            if chunks:
                try:
                    self.tg.delete_chunks(chunks)
                except Exception:
                    pass
            try:
                new_chunks = self.tg.upload_chunks_from_file(path, tmp, length, mtime, mode, file_uuid)
            except Exception as e:
                log.error("truncate upload failed: %s", e)
                raise FuseOSError(errno.EIO)
        with self._lock:
            entry = self.entries.get(path)
            if entry is not None:
                entry["chunks"] = new_chunks
                entry["size"] = length
                entry["mtime"] = _now()
            self._persist()
        return 0

    def unlink(self, path: str) -> int:
        path = _norm(path)
        with self._lock:
            entry = self.entries.pop(path, None)
            if entry is None:
                raise FuseOSError(errno.ENOENT)
            # Persist atomically with the pop so a poller reload cannot
            # resurrect the entry between the pop and the persist.
            self._persist()
        chunks = entry.get("chunks", [])
        if chunks:
            try:
                self.tg.delete_chunks(chunks)
            except Exception as e:
                log.warning("unlink delete chunks: %s", e)
        return 0

    def rename(self, old: str, new: str) -> int:
        old = _norm(old)
        new = _norm(new)
        with self._lock:
            entry = self.entries.pop(old, None)
            if entry is None:
                raise FuseOSError(errno.ENOENT)
            entry["mtime"] = _now()
            self.entries[new] = entry
            # Update any open handles.
            for h in self._handles.values():
                if h.path == old:
                    h.path = new
            self._persist()
        return 0

    def mkdir(self, path: str, mode: int) -> int:
        path = _norm(path)
        now = _now()
        with self._lock:
            if path in self.entries:
                raise FuseOSError(errno.EEXIST)
            if not self._is_implicit_dir_parent(path):
                raise FuseOSError(errno.ENOENT)
            self.entries[path] = {
                "type": "dir",
                "mtime": now,
                "atime": now,
                "mode": stat.S_IFDIR | (mode & 0o7777),
                "uid": self._uid,
                "gid": self._gid,
            }
            self._persist()
        return 0

    def _is_implicit_dir_parent(self, path: str) -> bool:
        if path == ROOT_PATH:
            return True
        parent = path.rsplit("/", 1)[0] or ROOT_PATH
        if parent == ROOT_PATH:
            return True
        with self._lock:
            if parent in self.entries:
                return self.entries[parent].get("type") == "dir"
        return self._is_implicit_dir(parent)

    def rmdir(self, path: str) -> int:
        path = _norm(path)
        prefix = path + "/"
        with self._lock:
            if path not in self.entries:
                # Implicit dir with no explicit entry: nothing to remove.
                if self._is_implicit_dir(path):
                    return 0
                raise FuseOSError(errno.ENOENT)
            for k in self.entries:
                if k.startswith(prefix):
                    raise FuseOSError(errno.ENOTEMPTY)
            self.entries.pop(path, None)
            self._persist()
        return 0

    def utimens(self, path: str, times: tuple[float, float] | None = None) -> int:
        path = _norm(path)
        if times is None:
            atime = mtime = _now()
        else:
            atime, mtime = times
        with self._lock:
            entry = self.entries.get(path)
            if entry is None:
                if self._is_implicit_dir(path):
                    # Create an explicit dir entry to hold the times.
                    self.entries[path] = {
                        "type": "dir",
                        "mtime": mtime,
                        "atime": atime,
                        "mode": stat.S_IFDIR | DIR_MODE,
                        "uid": self._uid,
                        "gid": self._gid,
                    }
                    self._persist()
                    return 0
                raise FuseOSError(errno.ENOENT)
            entry["atime"] = atime
            entry["mtime"] = mtime
            self._persist()
        return 0

    def chmod(self, path: str, mode: int) -> int:
        path = _norm(path)
        with self._lock:
            entry = self.entries.get(path)
            if entry is None:
                raise FuseOSError(errno.ENOENT)
            if entry.get("type") == "dir":
                entry["mode"] = stat.S_IFDIR | (mode & 0o7777)
            else:
                entry["mode"] = stat.S_IFREG | (mode & 0o7777)
            self._persist()
        return 0

    def chown(self, path: str, uid: int, gid: int) -> int:
        path = _norm(path)
        with self._lock:
            entry = self.entries.get(path)
            if entry is None:
                raise FuseOSError(errno.ENOENT)
            if uid != -1:
                entry["uid"] = uid
            if gid != -1:
                entry["gid"] = gid
            self._persist()
        return 0

    def statfs(self, path: str) -> dict[str, int]:
        # Report a generous, fictitious filesystem. Keys must match the
        # statvfs struct field names (f_-prefixed) used by fusepy.
        block_size = 20 * 1024 * 1024  # 20 MB blocks (chunk size)
        total = 50 * 1024 * block_size
        free = total // 2
        return {
            "f_bsize": block_size,
            "f_frsize": block_size,
            "f_blocks": total // block_size,
            "f_bfree": free // block_size,
            "f_bavail": free // block_size,
            "f_files": 1000000,
            "f_ffree": 1000000,
            "f_favail": 1000000,
            "f_flag": 0,
        }


def mount(
    token: str,
    chat_id: str | int,
    mountpoint: str,
    foreground: bool = False,
    debug: bool = False,
    chunk_size: int | None = None,
    cache: ChunkCache | None = None,
) -> None:
    fs = TgDriveFS(token, chat_id, chunk_size=chunk_size, cache=cache)
    log.info("mounting tgdrive at %s (foreground=%s)", mountpoint, foreground)
    FUSE(
        fs,
        mountpoint,
        foreground=foreground or debug,
        nothreads=not debug,
        debug=debug,
        allow_root=False,
        fsname="tgdrive",
        subtype="tgdrive",
    )
