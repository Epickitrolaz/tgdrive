import json
import logging
import os
import stat
import threading
import time
import uuid
from collections import OrderedDict
from errno import ENOENT, EBADF, EACCES, EEXIST, ENOTEMPTY

from fuse import FuseOSError, Operations, LoggingMixIn

from .bot import TgBot, CAPTION_PREFIX, CHUNK_SIZE

log = logging.getLogger(__name__)


class TgDriveFS(LoggingMixIn, Operations):
    def __init__(self, token, chat_id, cache_dir, foreground=False):
        self.bot = TgBot(token, chat_id)
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)

        self.files = OrderedDict()
        self.by_uuid = {}
        self.fd_map = {}
        self.next_fd = 1
        self._mutex = threading.RLock()
        self._running = True
        self._state_path = os.path.join(cache_dir, "state.json")
        self._chunks_dir = os.path.join(cache_dir, "chunks")
        os.makedirs(self._chunks_dir, exist_ok=True)

        self._load_state()
        self._discover_existing_files()
        log.info("Filesystem initialized with %d files", len(self.files))

        self._poll_thread = threading.Thread(
            target=self._poll_loop, daemon=True
        )
        self._poll_thread.start()

    def _discover_existing_files(self):
        try:
            updates = self.bot.get_updates(timeout=2)
            for update in updates:
                self._process_message(update.get("message", {}))
            self._save_state()
        except Exception as e:
            log.warning("Initial discovery error: %s", e)

    def _load_state(self):
        if os.path.exists(self._state_path):
            try:
                with open(self._state_path) as f:
                    state = json.load(f)
                with self._mutex:
                    self.files = OrderedDict(state.get("files", {}))
                    for name, meta in list(self.files.items()):
                        self.by_uuid[meta["uuid"]] = name
                        parts = meta.get("parts", [])
                        parts[:] = [
                            p if isinstance(p, dict) else None for p in parts
                        ]
                log.info("Loaded %d files from local state", len(self.files))
            except Exception as e:
                log.warning("Failed to load state: %s", e)

    def _save_state(self):
        with self._mutex:
            state = {
                "files": list(self.files.items()),
                "version": 2,
            }
            tmp = self._state_path + ".tmp"
            try:
                with open(tmp, "w") as f:
                    json.dump(state, f)
                os.replace(tmp, self._state_path)
            except Exception as e:
                log.error("Failed to save state: %s", e)

    def _poll_loop(self):
        while self._running:
            try:
                updates = self.bot.get_updates(timeout=30)
                changed = False
                for update in updates:
                    msg = update.get("message", {})
                    if self._process_message(msg):
                        changed = True
                if changed:
                    self._save_state()
            except Exception as e:
                log.debug("Poll error: %s", e)

    def _process_message(self, msg):
        caption = msg.get("caption", "")
        if not caption.startswith(CAPTION_PREFIX):
            return False

        try:
            meta = json.loads(caption[len(CAPTION_PREFIX):])
        except (json.JSONDecodeError, KeyError):
            return False

        file_uuid = meta["uuid"]
        part_idx = meta["part"]
        filename = meta["name"]
        file_size = meta["size"]
        total_parts = meta.get("total", 1)
        mtime = meta.get("mtime", time.time())
        mode = meta.get("mode", 0o644 | stat.S_IFREG)

        doc = msg.get("document", {})
        chunk_file_id = doc.get("file_id")
        chunk_size = doc.get("file_size", 0)

        chunk_info = {
            "msg_id": msg["message_id"],
            "file_id": chunk_file_id,
            "size": chunk_size,
            "index": part_idx,
        }

        with self._mutex:
            changed = False
            if filename not in self.files:
                self.files[filename] = {
                    "uuid": file_uuid,
                    "size": file_size,
                    "mtime": mtime,
                    "mode": mode,
                    "parts": [None] * total_parts,
                }
                self.by_uuid[file_uuid] = filename
                changed = True

            existing = self.files[filename]
            parts = existing["parts"]
            if part_idx < len(parts):
                if parts[part_idx] is None:
                    parts[part_idx] = chunk_info
                    changed = True
                    if all(p is not None for p in parts):
                        existing["size"] = file_size
                        existing["mtime"] = mtime

            return changed

    def _get_chunk_path(self, file_id):
        return os.path.join(self._chunks_dir, file_id.replace("/", "_"))

    def _ensure_file_cached(self, filename):
        meta = self.files.get(filename)
        if not meta:
            raise FuseOSError(ENOENT)

        cache_path = os.path.join(self.cache_dir, meta["uuid"])
        if os.path.exists(cache_path) and os.path.getsize(cache_path) == meta["size"]:
            return cache_path

        data = self.bot.download_file_data(meta["parts"])
        tmp = cache_path + ".tmp"
        with open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, cache_path)
        return cache_path

    def getattr(self, path, fh=None):
        stripped = path.lstrip("/")
        if not stripped:
            now = time.time()
            uid = os.getuid()
            gid = os.getgid()
            return {
                "st_mode": stat.S_IFDIR | 0o755,
                "st_nlink": 2,
                "st_size": 4096,
                "st_ctime": now,
                "st_mtime": now,
                "st_atime": now,
                "st_uid": uid,
                "st_gid": gid,
            }

        with self._mutex:
            meta = self.files.get(stripped)
            if meta:
                return {
                    "st_mode": meta["mode"],
                    "st_nlink": 1,
                    "st_size": meta["size"],
                    "st_ctime": meta["mtime"],
                    "st_mtime": meta["mtime"],
                    "st_atime": meta["mtime"],
                    "st_uid": os.getuid(),
                    "st_gid": os.getgid(),
                }

        raise FuseOSError(ENOENT)

    def readdir(self, path, fh):
        stripped = path.lstrip("/")
        if stripped:
            raise FuseOSError(ENOENT)

        entries = [".", ".."]
        with self._mutex:
            entries.extend(self.files.keys())
        return entries

    def open(self, path, fi):
        filename = path.lstrip("/")
        with self._mutex:
            if filename not in self.files:
                raise FuseOSError(ENOENT)
            fd = self.next_fd
            self.next_fd += 1
            self.fd_map[fd] = {
                "filename": filename,
                "dirty": False,
                "read_progress": 0,
            }
        return fd

    def read(self, path, size, offset, fh):
        filename = path.lstrip("/")
        with self._mutex:
            meta = self.files.get(filename)
            if not meta:
                raise FuseOSError(ENOENT)

        cache_path = os.path.join(self.cache_dir, meta["uuid"])
        if not os.path.exists(cache_path) or os.path.getsize(cache_path) != meta["size"]:
            self._ensure_file_cached(filename)

        with open(cache_path, "rb") as f:
            f.seek(offset)
            return f.read(size)

    def create(self, path, mode, fi=None):
        filename = path.lstrip("/")
        file_uuid = str(uuid.uuid4())

        with self._mutex:
            if filename in self.files:
                raise FuseOSError(EEXIST)
            self.files[filename] = {
                "uuid": file_uuid,
                "size": 0,
                "mtime": time.time(),
                "mode": stat.S_IFREG | (mode & 0o7777),
                "parts": [],
            }
            self.by_uuid[file_uuid] = filename

            fd = self.next_fd
            self.next_fd += 1
            cache_path = os.path.join(self.cache_dir, file_uuid)
            open(cache_path, "wb").close()
            self.fd_map[fd] = {
                "filename": filename,
                "uuid": file_uuid,
                "dirty": True,
                "cache_path": cache_path,
                "mode": "w",
            }

        return fd

    def write(self, path, data, offset, fh):
        with self._mutex:
            h = self.fd_map.get(fh)
            if not h:
                raise FuseOSError(EBADF)

            cache_path = h["cache_path"]
            h["dirty"] = True

        with open(cache_path, "r+b") as f:
            f.seek(offset)
            f.write(data)

        return len(data)

    def truncate(self, path, length, fh=None):
        filename = path.lstrip("/")
        with self._mutex:
            if filename not in self.files:
                raise FuseOSError(ENOENT)

            if fh and fh in self.fd_map:
                h = self.fd_map[fh]
                with open(h["cache_path"], "r+b") as f:
                    f.truncate(length)

            self.files[filename]["size"] = length
            self.files[filename]["mtime"] = time.time()

    def flush(self, path, fh):
        with self._mutex:
            h = self.fd_map.get(fh)
            if not h:
                return
            if not h.get("dirty"):
                return
            filename = h["filename"]
            cache_path = h["cache_path"]
            meta = self.files.get(filename)
            if not meta:
                return

        if os.path.exists(cache_path):
            with open(cache_path, "rb") as f:
                data = f.read()

            file_uuid = meta["uuid"]
            mtime = time.time()
            mode = meta["mode"]

            for old_part in meta.get("parts", []):
                if old_part is not None:
                    try:
                        self.bot.delete_message(old_part["msg_id"])
                    except Exception:
                        pass

            new_parts = self.bot.upload_file_chunks(
                file_uuid, data, filename, mode, mtime
            )

            with self._mutex:
                if filename in self.files:
                    self.files[filename]["parts"] = new_parts
                    self.files[filename]["size"] = len(data)
                    self.files[filename]["mtime"] = mtime
                    self._save_state()

    def release(self, path, fh):
        with self._mutex:
            self.fd_map.pop(fh, None)

    def unlink(self, path):
        filename = path.lstrip("/")
        with self._mutex:
            meta = self.files.pop(filename, None)
            if not meta:
                raise FuseOSError(ENOENT)

            self.by_uuid.pop(meta["uuid"], None)

            for part in meta.get("parts", []):
                if part is not None:
                    try:
                        self.bot.delete_message(part["msg_id"])
                    except Exception:
                        pass

            cache_path = os.path.join(self.cache_dir, meta["uuid"])
            if os.path.exists(cache_path):
                os.remove(cache_path)

            self._save_state()

    def rename(self, old, new):
        old_name = old.lstrip("/")
        new_name = new.lstrip("/")
        with self._mutex:
            if old_name not in self.files:
                raise FuseOSError(ENOENT)

            self.files[new_name] = self.files.pop(old_name)
            self.files[new_name]["mtime"] = time.time()
            self.by_uuid[self.files[new_name]["uuid"]] = new_name
            self._save_state()

    def utimens(self, path, times):
        filename = path.lstrip("/")
        with self._mutex:
            if filename in self.files:
                if times:
                    self.files[filename]["mtime"] = times[1]
                else:
                    self.files[filename]["mtime"] = time.time()

    def chmod(self, path, mode):
        filename = path.lstrip("/")
        with self._mutex:
            if filename in self.files:
                self.files[filename]["mode"] = stat.S_IFREG | (mode & 0o7777)

    def mkdir(self, path, mode):
        raise FuseOSError(ENOTEMPTY)

    def rmdir(self, path):
        stripped = path.lstrip("/")
        if not stripped:
            raise FuseOSError(ENOTEMPTY)
        raise FuseOSError(ENOENT)

    def chown(self, path, uid, gid):
        pass

    def statfs(self, path):
        return {
            "f_bsize": 512,
            "f_blocks": 2 ** 32,
            "f_bfree": 2 ** 32,
            "f_bavail": 2 ** 32,
            "f_files": 1000000,
            "f_ffree": 1000000,
            "f_favail": 1000000,
            "f_namemax": 255,
        }

    def destroy(self, private_data):
        self._running = False
        self._save_state()

    def lock(self, path, fh, cmd, lock):
        return 0

    def fsync(self, path, datasync, fh):
        return 0

    def access(self, path, amode):
        stripped = path.lstrip("/")
        if not stripped:
            return
        with self._mutex:
            if stripped not in self.files:
                raise FuseOSError(EACCES)

