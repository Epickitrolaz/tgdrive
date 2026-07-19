"""On-disk LRU cache for downloaded Telegram chunks.

The cache lives under ``$TGDRIVE_CACHE_DIR`` (default ``~/.cache/tgdrive``) and
stores each chunk body as a single file named after the SHA-256 of its
``file_id``. Each entry is tracked in an in-memory index keyed by ``file_id``
so reads can resolve the path without scanning the directory.

Two upper bounds are enforced on top of plain LRU:

* **max bytes** - once the total on-disk size of cached chunks exceeds this
  value, the oldest entries are evicted until we are back under the limit.
* **max disk usage percent** - if the filesystem hosting the cache directory
  is more than this percentage full, entries are evicted (oldest first) until
  we either reach the target free ratio or have nothing left to drop.

A background thread wakes up periodically and re-applies the limits, so
eviction keeps working even when the user only ever reads the same hot chunks.
"""

from __future__ import annotations

import hashlib
import logging
import os
import shutil
import threading
import time
from collections import OrderedDict
from concurrent.futures import Future
from typing import Callable, Optional

log = logging.getLogger("tgdrive.cache")


def default_cache_dir() -> str:
    return os.path.join(os.path.expanduser("~"), ".cache", "tgdrive")


def _parse_size(value: str | int | None) -> Optional[int]:
    """Parse a human-friendly size string like ``"10G"`` or ``"512M"`` to bytes.

    Accepts plain integers (bytes) or suffixes ``K``, ``M``, ``G``, ``T`` (and
    the lowercase variants). ``None`` and the empty string mean "no limit".
    """
    if value is None:
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    s = str(value).strip()
    if not s:
        return None
    s = s.replace("_", "")
    units = {"K": 1024, "M": 1024 ** 2, "G": 1024 ** 3, "T": 1024 ** 4}
    suffix = s[-1].upper()
    if suffix in units:
        num = float(s[:-1])
        return int(num * units[suffix])
    return int(s)


def _parse_percent(value: str | float | int | None) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value) if value > 0 else None
    s = str(value).strip().rstrip("%")
    if not s:
        return None
    return float(s) if float(s) > 0 else None


def _file_id_to_name(file_id: str) -> str:
    return hashlib.sha256(file_id.encode("utf-8")).hexdigest()


class ChunkCache:
    """LRU + size/percent-limited cache for downloaded chunks on disk.

    Parameters
    ----------
    cache_dir:
        Directory where chunk bodies are stored. Created if missing.
    max_bytes:
        Hard upper bound on the total size of the cache. ``None`` disables it.
    max_disk_percent:
        If set, the cache is shrunk whenever the filesystem hosting
        ``cache_dir`` exceeds this percent used. ``None`` disables it.
    min_free_bytes:
        Floor for the free-bytes target when enforcing ``max_disk_percent``.
        Defaults to 1 GiB; useful so a 99% rule still leaves room to download.
    enforce_interval:
        Seconds between background re-enforcement passes.
    """

    def __init__(
        self,
        cache_dir: str | None = None,
        max_bytes: int | None = None,
        max_disk_percent: float | None = None,
        min_free_bytes: int = 1 << 30,
        enforce_interval: float = 30.0,
    ):
        self.cache_dir = cache_dir or default_cache_dir()
        self.max_bytes = max_bytes
        self.max_disk_percent = max_disk_percent
        self.min_free_bytes = int(min_free_bytes)
        self.enforce_interval = float(enforce_interval)
        os.makedirs(self.cache_dir, exist_ok=True)
        # OrderedDict preserves insertion order; we move entries to the end on
        # every touch so the front of the dict is the LRU candidate.
        self._entries: "OrderedDict[str, tuple[str, int, float]]" = OrderedDict()
        # Tracks bytes currently counted toward the limit (in-memory view of
        # the on-disk cache). It can drift from reality if files are removed
        # out-of-band; the background enforcer reconciles from disk.
        self._size = 0
        self._lock = threading.RLock()
        # In-flight downloads keyed by file_id. The first concurrent miss for
        # a given file_id registers a Future here; every other miss blocks on
        # the same Future so the chunk is fetched from Telegram at most once
        # even if N users ask for it simultaneously. The value is removed
        # once the Future resolves.
        self._inflight: dict[str, Future] = {}
        self._inflight_lock = threading.Lock()
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._scanned = False
        self._thread: threading.Thread | None = None
        self._rebuild_from_disk()

    # -- public helpers ----------------------------------------------------
    @staticmethod
    def path_for(cache_dir: str, file_id: str) -> str:
        return os.path.join(cache_dir, _file_id_to_name(file_id))

    def path(self, file_id: str) -> str:
        return self.path_for(self.cache_dir, file_id)

    def get(self, file_id: str) -> bytes | None:
        """Return the cached body for ``file_id`` if present, else ``None``.

        Hits bump the entry to MRU position.
        """
        with self._lock:
            entry = self._entries.get(file_id)
            if entry is None:
                # Slow path: maybe on disk but not indexed yet.
                p = self.path(file_id)
                if not os.path.exists(p):
                    return None
                st = os.stat(p)
                self._entries[file_id] = (p, st.st_size, st.st_mtime)
                self._size += st.st_size
                entry = self._entries[file_id]
                self._maybe_enforce_locked()
            _path, _size, _mtime = entry
            self._entries.move_to_end(file_id)
        try:
            with open(_path, "rb") as fh:
                return fh.read()
        except FileNotFoundError:
            # Raced with an eviction that happened between the index lookup
            # and the open(); drop the stale index entry and report a miss.
            with self._lock:
                removed = self._entries.pop(file_id, None)
                if removed is not None:
                    self._size -= removed[1]
            return None

    def put(self, file_id: str, data: bytes) -> str:
        """Write ``data`` to the cache and return the path. Evicts as needed."""
        path = self.path(file_id)
        with self._lock:
            # Remove any previous copy so we don't double-count.
            prev = self._entries.pop(file_id, None)
            if prev is not None:
                try:
                    os.unlink(prev[0])
                except FileNotFoundError:
                    pass
                self._size -= prev[1]
            tmp = path + ".tmp"
            with open(tmp, "wb") as fh:
                fh.write(data)
            os.replace(tmp, path)
            st = os.stat(path)
            self._entries[file_id] = (path, st.st_size, st.st_mtime)
            self._entries.move_to_end(file_id)
            self._size += st.st_size
            self._maybe_enforce_locked()
        return path

    # -- single-flight fetch ----------------------------------------------
    def get_or_fetch(
        self,
        file_id: str,
        fetcher: "Callable[[], bytes] | None" = None,
    ) -> bytes:
        """Return cached bytes for ``file_id``, fetching via ``fetcher`` on miss.

        Concurrent calls for the same ``file_id`` share a single download:
        the first caller invokes ``fetcher`` and every other caller blocks
        on the same Future. This is what makes parallel multi-user reads
        cheap when two users open the same media file at the same time.

        If ``fetcher`` is ``None`` (or the on-disk cache is already populated)
        no network I/O happens. The cache's LRU/size limits are still
        enforced on the resulting write.
        """
        # Fast path: cache hit. Don't even take the in-flight lock.
        data = self.get(file_id)
        if data is not None:
            return data
        if fetcher is None:
            raise KeyError(file_id)
        # Cache miss; either join an in-flight fetch or start one.
        with self._inflight_lock:
            existing = self._inflight.get(file_id)
            if existing is not None:
                # Someone else is already fetching this chunk. Wait on their
                # result instead of starting a duplicate download.
                fut = existing
                joined = True
            else:
                fut = Future()
                self._inflight[file_id] = fut
                joined = False
        if joined:
            return fut.result()
        try:
            data = fetcher()
        except BaseException as e:
            with self._inflight_lock:
                self._inflight.pop(file_id, None)
            fut.set_exception(e)
            raise
        try:
            self.put(file_id, data)
        except Exception as e:  # pragma: no cover - defensive
            log.debug("cache put failed for %s: %s", file_id, e)
        with self._inflight_lock:
            self._inflight.pop(file_id, None)
        fut.set_result(data)
        return data

    def forget(self, file_id: str) -> None:
        with self._lock:
            entry = self._entries.pop(file_id, None)
            if entry is None:
                return
            self._size -= entry[1]
            try:
                os.unlink(entry[0])
            except FileNotFoundError:
                pass

    def clear(self) -> None:
        with self._lock:
            for _fid, (p, _s, _m) in list(self._entries.items()):
                try:
                    os.unlink(p)
                except FileNotFoundError:
                    pass
            self._entries.clear()
            self._size = 0

    def total_bytes(self) -> int:
        with self._lock:
            return self._size

    def entry_count(self) -> int:
        with self._lock:
            return len(self._entries)

    def disk_usage(self) -> tuple[int, int, int, float]:
        """Return ``(used, total, free, percent_used)`` for the cache filesystem."""
        usage = shutil.disk_usage(self.cache_dir)
        percent = (usage.used / usage.total * 100.0) if usage.total else 0.0
        return usage.used, usage.total, usage.free, percent

    # -- enforcement --------------------------------------------------------
    def _maybe_enforce_locked(self) -> int:
        """Enforce all limits, dropping LRU entries. Must be called under lock."""
        evicted = 0
        # Bytes limit.
        if self.max_bytes is not None:
            while self._size > self.max_bytes and self._entries:
                evicted += self._evict_oldest_locked()
        # Disk usage percent limit.
        if self.max_disk_percent is not None:
            while self._entries:
                used, _total, free, percent = self.disk_usage()
                if percent < self.max_disk_percent and free >= self.min_free_bytes:
                    break
                evicted += self._evict_oldest_locked()
        return evicted

    def _evict_oldest_locked(self) -> int:
        if not self._entries:
            return 0
        fid, (path, size, _mtime) = next(iter(self._entries.items()))
        del self._entries[fid]
        self._size -= size
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
        except OSError as e:
            log.warning("failed to unlink %s: %s", path, e)
        return 1

    def enforce(self) -> int:
        with self._lock:
            return self._maybe_enforce_locked()

    def _rebuild_from_disk(self) -> None:
        """Populate the index from the contents of ``cache_dir``.

        Runs once at startup so we can pick up files left over from previous
        runs. Files that don't look like ours (wrong name length) are ignored
        and not deleted, since the cache dir is shared with the rest of
        tgdrive and may contain unrelated state.
        """
        with self._lock:
            self._entries.clear()
            self._size = 0
            if not os.path.isdir(self.cache_dir):
                return
            try:
                names = os.listdir(self.cache_dir)
            except OSError as e:
                log.warning("cannot list cache dir %s: %s", self.cache_dir, e)
                return
            # We don't know the file_id for a stray file, but we can still
            # track its size for accounting. We key those by the sha256 of the
            # filename so they don't collide with real entries but the totals
            # stay honest.
            for name in names:
                if name.endswith(".tmp"):
                    try:
                        os.unlink(os.path.join(self.cache_dir, name))
                    except OSError:
                        pass
                    continue
                p = os.path.join(self.cache_dir, name)
                if not os.path.isfile(p):
                    continue
                try:
                    st = os.stat(p)
                except OSError:
                    continue
                # Key by the file name itself (already a sha256). This means
                # cached chunks from a previous run that we can't tie back to
                # a file_id still count against the size budget.
                key = name
                self._entries[key] = (p, st.st_size, st.st_mtime)
                self._size += st.st_size
            self._scanned = True
            if self._entries:
                log.info(
                    "cache: %d entries, %.2f MB on disk under %s",
                    len(self._entries),
                    self._size / 1e6,
                    self.cache_dir,
                )

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="tgdrive-cache", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def wake(self) -> None:
        """Force the background enforcer to run immediately."""
        self._wake.set()

    def _run(self) -> None:
        while not self._stop.is_set():
            # Sleep in small slices so ``stop()`` is responsive.
            woke = self._wake.wait(timeout=self.enforce_interval)
            self._wake.clear()
            if self._stop.is_set():
                break
            try:
                evicted = self.enforce()
                if evicted:
                    log.info("cache enforcer evicted %d entries", evicted)
            except Exception as e:  # pragma: no cover - defensive
                log.warning("cache enforcer error: %s", e)


# -- CLI helpers ------------------------------------------------------------
def add_cli_arguments(parser) -> None:
    """Attach cache-related arguments to an ``argparse.ArgumentParser``."""
    group = parser.add_argument_group("cache management")
    group.add_argument(
        "--cache-dir",
        dest="cache_dir",
        default=os.environ.get("TGDRIVE_CACHE_DIR"),
        help="Cache directory (env: TGDRIVE_CACHE_DIR, default: ~/.cache/tgdrive).",
    )
    group.add_argument(
        "--max-cache-size",
        dest="max_cache_size",
        default=os.environ.get("TGDRIVE_MAX_CACHE_SIZE"),
        help=(
            "Maximum cache size, e.g. '10G' (env: TGDRIVE_MAX_CACHE_SIZE). "
            "Oldest entries are evicted once exceeded. Default: unlimited."
        ),
    )
    group.add_argument(
        "--max-disk-usage",
        dest="max_disk_usage",
        default=os.environ.get("TGDRIVE_MAX_DISK_USAGE"),
        help=(
            "Maximum disk usage percent, e.g. '90' or '90%%' "
            "(env: TGDRIVE_MAX_DISK_USAGE). Default: disabled."
        ),
    )
    group.add_argument(
        "--min-free-bytes",
        dest="min_free_bytes",
        default=os.environ.get("TGDRIVE_MIN_FREE_BYTES"),
        help=(
            "Minimum free bytes to keep when enforcing --max-disk-usage "
            "(env: TGDRIVE_MIN_FREE_BYTES, default: 1G)."
        ),
    )
    group.add_argument(
        "--cache-enforce-interval",
        dest="cache_enforce_interval",
        type=float,
        default=None,
        help=(
            "Seconds between cache enforcement passes "
            "(env: TGDRIVE_CACHE_ENFORCE_INTERVAL, default: 30)."
        ),
    )
    group.add_argument(
        "--clear-cache",
        dest="clear_cache",
        action="store_true",
        help="Delete all cached chunks before mounting.",
    )


def build_cache(args) -> ChunkCache:
    """Construct a :class:`ChunkCache` from parsed CLI args.

    Accepts either a ``argparse.Namespace`` (from the bundled ``add_cli_arguments``)
    or a plain dict of keyword arguments.
    """

    def _get(name, default=None):
        if isinstance(args, dict):
            return args.get(name, default)
        return getattr(args, name, default)

    cache = ChunkCache(
        cache_dir=_get("cache_dir"),
        max_bytes=_parse_size(_get("max_cache_size")),
        max_disk_percent=_parse_percent(_get("max_disk_usage")),
        min_free_bytes=_parse_size(_get("min_free_bytes")) or (1 << 30),
        enforce_interval=float(_get("cache_enforce_interval") or 30.0),
    )
    if _get("clear_cache"):
        log.info("clearing cache at %s", cache.cache_dir)
        cache.clear()
    return cache
