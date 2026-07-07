import logging
import os
import stat
import threading
import time
import uuid
from collections import OrderedDict
from errno import ENOENT, EBADF, EACCES, EEXIST, ENOTEMPTY, ENOTDIR

from fuse import FuseOSError, Operations, LoggingMixIn

from .bot import TgBot, CHUNK_SIZE

log = logging.getLogger(__name__)

POLL_INTERVAL = 5


class TgDriveFS(LoggingMixIn, Operations):
    def __init__(self, token, chat_id, foreground=False):
        self.bot = TgBot(token, chat_id)
        self._idx_msg_id = None
        self._idx_file_id = None
        self._idx_doc_msg_id = None
        self._idx_data = OrderedDict()
        self._idx_gen = 0
        self._fd_map = {}
        self._next_fd = 1
        self._mutex = threading.RLock()
        self._running = True

        self._init_index()

        self._poll_thread = threading.Thread(
            target=self._poll_loop, daemon=True
        )
        self._poll_thread.start()

        log.info(
            "Filesystem initialized with %d entries", len(self._idx_data)
        )

    def _init_index(self):
        msg_id, file_id, doc_msg_id, gen, data = self.bot.get_index_file()
        if msg_id is not None:
            with self._mutex:
                self._idx_msg_id = msg_id
                self._idx_file_id = file_id
                self._idx_doc_msg_id = doc_msg_id
                self._idx_gen = gen
                self._idx_data = OrderedDict(data)
            log.info("Loaded directory index from file %s", file_id)
        else:
            with self._mutex:
                msg_id, file_id, doc_msg_id = self.bot.create_index_file({})
                self._idx_msg_id = msg_id
                self._idx_file_id = file_id
                self._idx_doc_msg_id = doc_msg_id
                self._idx_data = OrderedDict()
            log.info("Created new empty directory index file")

    def _replace_index(self):
        with self._mutex:
            old_msg_id = self._idx_msg_id
            old_doc_msg_id = self._idx_doc_msg_id
            self._idx_gen += 1
            try:
                self._idx_msg_id, self._idx_file_id, self._idx_doc_msg_id = \
                    self.bot.update_index_file(
                        old_msg_id, old_doc_msg_id, self._idx_gen,
                        dict(self._idx_data)
                    )
            except Exception:
                log.warning("Failed to update directory index", exc_info=True)

    def _poll_loop(self):
        while self._running:
            time.sleep(POLL_INTERVAL)
            try:
                msg_id, file_id, doc_msg_id, gen, data = \
                    self.bot.get_index_file()
                if msg_id is None:
                    continue
                with self._mutex:
                    if gen > self._idx_gen:
                        self._idx_gen = gen
                        self._idx_msg_id = msg_id
                        self._idx_file_id = file_id
                        self._idx_doc_msg_id = doc_msg_id
                        self._idx_data = OrderedDict(data)
                        log.info(
                            "Directory index updated from file %s (gen %d)",
                            file_id, gen,
                        )
            except Exception as e:
                log.debug("Poll error: %s", e)

    def _is_dir(self, path):
        if path in self._idx_data:
            return stat.S_ISDIR(self._idx_data[path]["mode"])
        prefix = path + "/"
        return any(k.startswith(prefix) for k in self._idx_data)

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
            meta = self._idx_data.get(stripped)
            if meta:
                return {
                    "st_mode": meta["mode"],
                    "st_nlink": 1 if stat.S_ISREG(meta["mode"]) else 2,
                    "st_size": meta.get("size", 4096),
                    "st_ctime": meta["mtime"],
                    "st_mtime": meta["mtime"],
                    "st_atime": meta["mtime"],
                    "st_uid": os.getuid(),
                    "st_gid": os.getgid(),
                }
            if self._is_dir(stripped):
                now = time.time()
                return {
                    "st_mode": stat.S_IFDIR | 0o755,
                    "st_nlink": 2,
                    "st_size": 4096,
                    "st_ctime": now,
                    "st_mtime": now,
                    "st_atime": now,
                    "st_uid": os.getuid(),
                    "st_gid": os.getgid(),
                }
        raise FuseOSError(ENOENT)

    def readdir(self, path, fh):
        stripped = path.lstrip("/")
        entries = [".", ".."]
        prefix = (stripped + "/") if stripped else ""
        seen = set()
        with self._mutex:
            if stripped and not self._is_dir(stripped):
                raise FuseOSError(ENOENT)
            for key in self._idx_data:
                if not key.startswith(prefix):
                    continue
                rest = key[len(prefix):]
                if "/" in rest:
                    subdir = rest.split("/", 1)[0]
                    if subdir not in seen:
                        seen.add(subdir)
                        entries.append(subdir)
                elif rest:
                    if rest not in seen:
                        seen.add(rest)
                        entries.append(rest)
        return entries

    def open(self, path, flags):
        filename = path.lstrip("/")
        with self._mutex:
            meta = self._idx_data.get(filename)
            if not meta:
                raise FuseOSError(ENOENT)
            if stat.S_ISDIR(meta["mode"]):
                raise FuseOSError(EACCES)
            fd = self._next_fd
            self._next_fd += 1
            writable = bool(flags & (os.O_WRONLY | os.O_RDWR))
            self._fd_map[fd] = {
                "filename": filename,
                "parts": list(meta.get("parts", [])),
                "file_size": meta["size"],
                "chunks": {},
            }
            if writable:
                self._fd_map[fd]["buffer"] = bytearray()
                self._fd_map[fd]["dirty"] = False
        return fd

    def read(self, path, size, offset, fh):
        with self._mutex:
            h = self._fd_map.get(fh)
            if not h:
                raise FuseOSError(EBADF)
            parts = h["parts"]
            chunks = h["chunks"]
            file_size = h["file_size"]

        if offset >= file_size:
            return b""

        result = bytearray()
        while size > 0 and offset < file_size:
            part_idx = offset // CHUNK_SIZE
            if part_idx >= len(parts) or parts[part_idx] is None:
                break
            if part_idx not in chunks:
                chunks[part_idx] = self.bot.download_chunk_data(
                    parts[part_idx]["file_id"]
                )
            chunk_data = chunks[part_idx]
            chunk_off = offset % CHUNK_SIZE
            available = min(size, len(chunk_data) - chunk_off)
            result.extend(
                chunk_data[chunk_off : chunk_off + available]
            )
            offset += available
            size -= available

        return bytes(result)

    def create(self, path, mode, fi=None):
        filename = path.lstrip("/")
        file_uuid = str(uuid.uuid4())
        with self._mutex:
            if filename in self._idx_data:
                raise FuseOSError(EEXIST)
            parent = filename.rsplit("/", 1)[0] if "/" in filename else ""
            if parent and not self._is_dir(parent):
                raise FuseOSError(ENOENT)
            self._idx_data[filename] = {
                "uuid": file_uuid,
                "size": 0,
                "mtime": time.time(),
                "mode": stat.S_IFREG | (mode & 0o7777),
                "parts": [],
            }
            fd = self._next_fd
            self._next_fd += 1
            self._fd_map[fd] = {
                "filename": filename,
                "buffer": bytearray(),
                "dirty": False,
            }
        return fd

    def write(self, path, data, offset, fh):
        with self._mutex:
            h = self._fd_map.get(fh)
            if not h or "buffer" not in h:
                raise FuseOSError(EBADF)
            h["dirty"] = True
            buf = h["buffer"]
            needed = offset + len(data)
            if needed > len(buf):
                buf.extend(b"\x00" * (needed - len(buf)))
            buf[offset : offset + len(data)] = data
            return len(data)

    def truncate(self, path, length, fh=None):
        filename = path.lstrip("/")
        with self._mutex:
            meta = self._idx_data.get(filename)
            if not meta:
                raise FuseOSError(ENOENT)
            if fh is not None and fh in self._fd_map:
                h = self._fd_map[fh]
                if "buffer" in h:
                    buf = h["buffer"]
                    if length < len(buf):
                        h["buffer"] = buf[:length]
                    elif length > len(buf):
                        h["buffer"].extend(
                            b"\x00" * (length - len(buf))
                        )
                    h["dirty"] = True
            meta["size"] = length
            meta["mtime"] = time.time()

    def flush(self, path, fh):
        with self._mutex:
            h = self._fd_map.get(fh)
            if not h or not h.get("dirty"):
                return
            filename = h["filename"]
            meta = self._idx_data.get(filename)
            if not meta:
                return
            data = bytes(h["buffer"])

        for part in meta.get("parts", []):
            if part is not None:
                try:
                    self.bot.delete_message(part["msg_id"])
                except Exception:
                    pass

        new_parts = self.bot.upload_file_chunks(
            meta["uuid"], data, filename, meta["mode"], time.time()
        )

        with self._mutex:
            if filename in self._idx_data:
                meta = self._idx_data[filename]
                meta["parts"] = new_parts
                meta["size"] = len(data)
                meta["mtime"] = time.time()
                self._replace_index()

        h["dirty"] = False

    def release(self, path, fh):
        with self._mutex:
            self._fd_map.pop(fh, None)

    def unlink(self, path):
        filename = path.lstrip("/")
        with self._mutex:
            meta = self._idx_data.pop(filename, None)
            if not meta:
                raise FuseOSError(ENOENT)

        for part in meta.get("parts", []):
            if part is not None:
                try:
                    self.bot.delete_message(part["msg_id"])
                except Exception:
                    pass

        with self._mutex:
            self._replace_index()

    def rename(self, old, new):
        old_name = old.lstrip("/")
        new_name = new.lstrip("/")
        with self._mutex:
            if old_name not in self._idx_data:
                raise FuseOSError(ENOENT)
            meta = self._idx_data.pop(old_name)
            meta["mtime"] = time.time()
            self._idx_data[new_name] = meta
            if stat.S_ISDIR(meta["mode"]):
                prefix = old_name + "/"
                to_move = [k for k in self._idx_data if k.startswith(prefix)]
                for k in to_move:
                    v = self._idx_data.pop(k)
                    self._idx_data[new_name + k[len(old_name):]] = v
            self._replace_index()

    def utimens(self, path, times):
        filename = path.lstrip("/")
        with self._mutex:
            if filename in self._idx_data:
                if times:
                    self._idx_data[filename]["mtime"] = times[1]
                else:
                    self._idx_data[filename]["mtime"] = time.time()
                self._replace_index()

    def chmod(self, path, mode):
        filename = path.lstrip("/")
        with self._mutex:
            if filename in self._idx_data:
                self._idx_data[filename]["mode"] = (
                    stat.S_IFREG | (mode & 0o7777)
                )
                self._replace_index()

    def mkdir(self, path, mode):
        stripped = path.lstrip("/")
        if not stripped:
            raise FuseOSError(EEXIST)
        with self._mutex:
            if stripped in self._idx_data:
                raise FuseOSError(EEXIST)
            parent = stripped.rsplit("/", 1)[0] if "/" in stripped else ""
            if parent and not self._is_dir(parent):
                raise FuseOSError(ENOENT)
            self._idx_data[stripped] = {
                "mode": stat.S_IFDIR | (mode & 0o7777),
                "mtime": time.time(),
                "size": 4096,
            }
            self._replace_index()

    def rmdir(self, path):
        stripped = path.lstrip("/")
        if not stripped:
            raise FuseOSError(EEXIST)
        with self._mutex:
            meta = self._idx_data.get(stripped)
            if not meta:
                raise FuseOSError(ENOENT)
            if not stat.S_ISDIR(meta["mode"]):
                raise FuseOSError(ENOTDIR)
            prefix = stripped + "/"
            for key in self._idx_data:
                if key.startswith(prefix):
                    raise FuseOSError(ENOTEMPTY)
            del self._idx_data[stripped]
            self._replace_index()

    def chown(self, path, uid, gid):
        pass

    def statfs(self, path):
        return {
            "f_bsize": 512,
            "f_blocks": 4000000,
            "f_bfree": 4000000,
            "f_bavail": 4000000,
            "f_files": 100000,
            "f_ffree": 100000,
            "f_favail": 100000,
            "f_namemax": 255,
        }

    def destroy(self, private_data):
        self._running = False

    def lock(self, path, fh, cmd, lock):
        return 0

    def fsync(self, path, datasync, fh):
        return 0

    def access(self, path, amode):
        stripped = path.lstrip("/")
        if not stripped:
            return
        with self._mutex:
            if stripped in self._idx_data:
                return
            if self._is_dir(stripped):
                return
        raise FuseOSError(EACCES)
