"""Telegram Bot API client for tgdrive.

Handles uploading/downloading file chunks as document messages, and managing
a JSON directory index kept in the chat's pinned message.
"""

from __future__ import annotations

import io
import json
import logging
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from typing import Any

import requests

log = logging.getLogger("tgdrive.telegram")

# Telegram Bot API: getFile only works for files up to 20 MB. Keep chunks below
# that limit; testing showed 15 MB uploads were fastest without write timeouts.
CHUNK_SIZE = 15 * 1024 * 1024
API_BASE = "https://api.telegram.org"
INDEX_MAGIC = "tgdrive"
INDEX_VERSION = 1
# A pinned text message may hold at most 4096 characters; keep a small margin.
# Staying below this keeps the index inline (1 API call to edit) instead of
# spilling to a document (3 API calls), which materially reduces rate limiting.
MAX_INLINE_INDEX = 4000


class TelegramError(Exception):
    def __init__(self, message: str, code: int = 0):
        super().__init__(message)
        self.code = code


class TelegramClient:
    """Low level Telegram Bot API wrapper with retry/backoff plus index management."""

    def __init__(self, token: str, chat_id: str | int, chunk_size: int = CHUNK_SIZE):
        self.token = token
        self.chat_id = chat_id
        self.chunk_size = chunk_size
        self._session = requests.Session()
        self._lock = threading.RLock()
        self._me: dict[str, Any] | None = None

    # -- low level HTTP -----------------------------------------------------
    def _api_url(self, method: str) -> str:
        return f"{API_BASE}/bot{self.token}/{method}"

    def _file_url(self, file_path: str) -> str:
        return f"{API_BASE}/file/bot{self.token}/{file_path}"

    @staticmethod
    def _rewind_files(files: dict[str, Any] | None) -> None:
        """Reset file-like objects so retries send the full body.

        ``requests`` consumes the underlying stream on each POST, so a retry
        after a rate-limit/5xx would otherwise upload an empty file body
        (Telegram then returns "Bad Request: file must be non-empty").
        """
        if not files:
            return
        for value in files.values():
            items = value if isinstance(value, (tuple, list)) else [value]
            for item in items:
                seek = getattr(item, "seek", None)
                if callable(seek):
                    try:
                        seek(0)
                    except Exception:
                        pass

    def _request(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        files: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        timeout: float = 120.0,
        max_retries: int = 6,
    ) -> Any:
        """Call a Bot API method with exponential backoff retry."""
        url = self._api_url(method)
        last_err: Exception | None = None
        for attempt in range(max_retries):
            # File objects may have been consumed by a previous attempt; rewind
            # them so every retry sends the complete body.
            self._rewind_files(files)
            try:
                resp = self._session.post(
                    url, params=params, files=files, data=data, timeout=timeout
                )
                try:
                    payload = resp.json()
                except ValueError:
                    payload = None
                if resp.status_code == 429 or (
                    payload
                    and not payload.get("ok")
                    and payload.get("error_code") == 429
                ):
                    retry_after = 1.0
                    if payload and "parameters" in payload:
                        retry_after = float(
                            payload["parameters"].get("retry_after", retry_after)
                        )
                    log.warning("rate limited, sleeping %.1fs", retry_after)
                    time.sleep(retry_after + 0.5)
                    last_err = TelegramError("rate limited", 429)
                    continue
                if resp.status_code >= 500:
                    backoff = min(30.0, (2 ** attempt) * 0.5)
                    log.warning("server error %s, retrying in %.1fs", resp.status_code, backoff)
                    time.sleep(backoff)
                    last_err = TelegramError(f"server error {resp.status_code}", resp.status_code)
                    continue
                if payload and not payload.get("ok"):
                    err = TelegramError(
                        payload.get("description", "api error"),
                        payload.get("error_code", 0),
                    )
                    # Non-retryable API errors.
                    raise err
                if payload is None:
                    raise TelegramError(f"non-json response {resp.status_code}", resp.status_code)
                return payload.get("result")
            except (requests.RequestException, ConnectionError) as e:
                backoff = min(30.0, (2 ** attempt) * 0.5)
                log.warning("network error %s, retrying in %.1fs", e, backoff)
                time.sleep(backoff)
                last_err = e
                continue
        raise TelegramError(f"request failed after {max_retries} attempts: {last_err}")

    def get_me(self) -> dict[str, Any]:
        if self._me is None:
            self._me = self._request("getMe")
        return self._me

    # -- message helpers ----------------------------------------------------
    def send_document(
        self,
        data: bytes,
        filename: str,
        caption: str | None = None,
        disable_notification: bool = True,
    ) -> dict[str, Any]:
        files = {"document": (filename, io.BytesIO(data))}
        params: dict[str, Any] = {"chat_id": self.chat_id}
        if caption is not None:
            params["caption"] = caption
            params["parse_mode"] = "HTML"
        if disable_notification:
            params["disable_notification"] = True
        return self._request("sendDocument", params=params, files=files)

    def send_text(self, text: str, disable_notification: bool = True) -> dict[str, Any]:
        params: dict[str, Any] = {"chat_id": self.chat_id, "text": text}
        if disable_notification:
            params["disable_notification"] = True
        return self._request("sendMessage", params=params)

    def delete_message(self, message_id: int) -> bool:
        return bool(self._request("deleteMessage", params={"chat_id": self.chat_id, "message_id": message_id}))

    def delete_messages(self, message_ids: list[int]) -> None:
        # deleteMessages requires 2+ ids; fall back to one-by-one otherwise.
        ids = [m for m in message_ids if m]
        if not ids:
            return
        if len(ids) == 1:
            self.delete_message(ids[0])
            return
        # Bot API deleteMessages may not be available on all servers; try then fallback.
        try:
            self._request(
                "deleteMessages",
                params={"chat_id": self.chat_id, "message_ids": json.dumps(ids)},
            )
        except TelegramError:
            for mid in ids:
                try:
                    self.delete_message(mid)
                except TelegramError:
                    pass

    def edit_message_text(self, message_id: int, text: str) -> dict[str, Any]:
        return self._request(
            "editMessageText",
            params={"chat_id": self.chat_id, "message_id": message_id, "text": text},
        )

    def pin_message(self, message_id: int) -> bool:
        return bool(
            self._request(
                "pinChatMessage",
                params={
                    "chat_id": self.chat_id,
                    "message_id": message_id,
                    "disable_notification": True,
                },
            )
        )

    def unpin_message(self, message_id: int) -> bool:
        return bool(
            self._request(
                "unpinChatMessage",
                params={"chat_id": self.chat_id, "message_id": message_id},
            )
        )

    def get_chat(self) -> dict[str, Any]:
        return self._request("getChat", params={"chat_id": self.chat_id})

    def get_pinned_message(self) -> dict[str, Any] | None:
        chat = self.get_chat()
        return chat.get("pinned_message")

    # -- file download ------------------------------------------------------
    def get_file_path(self, file_id: str) -> str:
        result = self._request("getFile", params={"file_id": file_id})
        return result["file_path"]

    def download_file(self, file_id: str, timeout: float = 300.0) -> bytes:
        file_path = self.get_file_path(file_id)
        url = self._file_url(file_path)
        last_err: Exception | None = None
        for attempt in range(6):
            try:
                resp = self._session.get(url, timeout=timeout)
                if resp.status_code >= 500:
                    time.sleep(min(30.0, (2 ** attempt) * 0.5))
                    last_err = TelegramError(f"download server error {resp.status_code}", resp.status_code)
                    continue
                if resp.status_code != 200:
                    raise TelegramError(f"download failed {resp.status_code}", resp.status_code)
                return resp.content
            except requests.RequestException as e:
                time.sleep(min(30.0, (2 ** attempt) * 0.5))
                last_err = e
        raise TelegramError(f"download failed after retries: {last_err}")

    # -- index management ---------------------------------------------------
    def _make_inline_index(self, index: dict[str, Any]) -> str:
        return json.dumps({"tgdrive": "index", "v": INDEX_VERSION, "index": index}, separators=(",", ":"))

    def _make_pointer(self, message_id: int, file_id: str) -> str:
        return json.dumps(
            {"tgdrive": "pointer", "v": INDEX_VERSION, "message_id": message_id, "file_id": file_id},
            separators=(",", ":"),
        )

    def read_pinned_index(self) -> dict[str, Any] | None:
        """Return the index dict from the pinned message, or None if absent/invalid."""
        pinned = self.get_pinned_message()
        if not pinned:
            return None
        text = pinned.get("caption") or pinned.get("text")
        if not text:
            return None
        try:
            meta = json.loads(text)
        except (ValueError, TypeError):
            return None
        if not isinstance(meta, dict) or meta.get("tgdrive") != INDEX_MAGIC and meta.get("tgdrive") not in ("index", "pointer"):
            return None
        kind = meta.get("tgdrive")
        if kind == "index":
            return meta.get("index")
        if kind == "pointer":
            file_id = meta.get("file_id")
            if not file_id:
                return None
            try:
                data = self.download_file(file_id)
                return json.loads(data.decode("utf-8"))
            except (TelegramError, ValueError) as e:
                log.error("failed to download index document: %s", e)
                return None
        return None

    def get_pinned_meta(self) -> dict[str, Any] | None:
        """Return raw meta dict of the pinned message (for detecting changes)."""
        pinned = self.get_pinned_message()
        if not pinned:
            return None
        text = pinned.get("caption") or pinned.get("text")
        if not text:
            return None
        try:
            meta = json.loads(text)
        except (ValueError, TypeError):
            return None
        if not isinstance(meta, dict) or meta.get("tgdrive") not in ("index", "pointer"):
            return None
        return meta

    def write_index(self, index: dict[str, Any]) -> None:
        """Persist the index to the chat's pinned message.

        If it fits inline, edit the existing pinned text message (or send a new
        one). Otherwise upload a document, point the pinned message at it, and
        delete the previous index document.
        """
        with self._lock:
            inline_meta = {"tgdrive": "index", "v": INDEX_VERSION, "index": index}
            inline = json.dumps(inline_meta, separators=(",", ":"))
            pinned = self.get_pinned_message()
            prev_meta = None
            if pinned:
                text = pinned.get("caption") or pinned.get("text")
                if text:
                    try:
                        prev_meta = json.loads(text)
                    except (ValueError, TypeError):
                        prev_meta = None
            is_ours = isinstance(prev_meta, dict) and prev_meta.get("tgdrive") in ("index", "pointer")

            if len(inline) <= MAX_INLINE_INDEX:
                # Inline storage.
                if is_ours and pinned.get("text") is not None:
                    try:
                        self.edit_message_text(pinned["message_id"], inline)
                    except TelegramError as e:
                        log.warning("edit pinned index failed, sending new: %s", e)
                        msg = self.send_text(inline)
                        self.pin_message(msg["message_id"])
                else:
                    msg = self.send_text(inline)
                    self.pin_message(msg["message_id"])
                # Clean up a previous pointer document if we shrank.
                if is_ours and prev_meta.get("tgdrive") == "pointer":
                    try:
                        self.delete_message(prev_meta["message_id"])
                    except TelegramError:
                        pass
                return inline_meta

            # Document storage.
            data = json.dumps(index, separators=(",", ":")).encode("utf-8")
            doc = self.send_document(data, "tgdrive_index.json", caption='{"tgdrive":"index_doc","v":%d}' % INDEX_VERSION)
            new_msg_id = doc["message_id"]
            new_file_id = doc["document"]["file_id"]
            pointer_meta = {"tgdrive": "pointer", "v": INDEX_VERSION, "message_id": new_msg_id, "file_id": new_file_id}
            pointer = json.dumps(pointer_meta, separators=(",", ":"))
            if is_ours and pinned.get("text") is not None:
                try:
                    self.edit_message_text(pinned["message_id"], pointer)
                except TelegramError as e:
                    log.warning("edit pinned pointer failed, sending new: %s", e)
                    msg = self.send_text(pointer)
                    self.pin_message(msg["message_id"])
            else:
                msg = self.send_text(pointer)
                self.pin_message(msg["message_id"])
            # Delete previous index document if any.
            if is_ours and prev_meta.get("tgdrive") == "pointer":
                try:
                    self.delete_message(prev_meta["message_id"])
                except TelegramError:
                    pass
            return pointer_meta

    # -- chunk upload/download ---------------------------------------------
    def upload_chunks(
        self,
        path: str,
        data: bytes,
        mtime: float,
        mode: int,
        uuid: str,
        on_progress=None,
    ) -> list[dict[str, Any]]:
        """Split data into chunks and upload each as a document. Return chunk metadata.

        Uploads are sequential: Telegram's document upload endpoint does not
        tolerate multiple concurrent large (20 MB) uploads well (it triggers
        write timeouts), so parallelism here is counterproductive. Sequential
        uploads also spread requests in time, which is gentler on rate limits
        than a burst of concurrent sendDocument calls.
        """
        # A 0-byte file is represented as an empty chunk list; Telegram rejects
        # empty document uploads ("file must be non-empty").
        if len(data) == 0:
            log.info("uploading %s (0 bytes, 0 chunk(s))", path)
            return []
        total = max(1, (len(data) + self.chunk_size - 1) // self.chunk_size)
        chunks: list[dict[str, Any]] = []
        offset = 0
        part = 0
        filename = path.rsplit("/", 1)[-1] or "file"
        log.info("uploading %s (%d bytes, %d chunk(s))", path, len(data), total)
        t0 = time.time()
        try:
            while offset < len(data):
                piece = data[offset : offset + self.chunk_size]
                caption = json.dumps(
                    {
                        "tgdrive": "chunk",
                        "v": INDEX_VERSION,
                        "uuid": uuid,
                        "part": part,
                        "total": total,
                        "filename": filename,
                        "path": path,
                        "size": len(data),
                        "mtime": mtime,
                        "mode": mode,
                    },
                    separators=(",", ":"),
                )
                if len(caption) > 1024:
                    # Caption limit is 1024 chars; trim non-essential fields.
                    caption = json.dumps(
                        {
                            "tgdrive": "chunk",
                            "v": INDEX_VERSION,
                            "uuid": uuid,
                            "part": part,
                            "total": total,
                            "filename": filename,
                            "size": len(data),
                        },
                        separators=(",", ":"),
                    )
                chunk_name = f"{filename}.part{part}"
                ct0 = time.time()
                msg = self.send_document(piece, chunk_name, caption=caption)
                doc = msg.get("document", {})
                chunks.append(
                    {
                        "message_id": msg["message_id"],
                        "file_id": doc.get("file_id"),
                        "size": len(piece),
                        "part": part,
                    }
                )
                log.debug(
                    "uploaded chunk %d/%d (%d bytes) in %.2fs",
                    part + 1,
                    total,
                    len(piece),
                    time.time() - ct0,
                )
                offset += self.chunk_size
                part += 1
                if on_progress:
                    try:
                        on_progress(part, total)
                    except Exception:
                        pass
        except Exception:
            # Clean up any chunks that did upload so a partial failure does not
            # leave orphaned documents in the chat, then re-raise.
            if chunks:
                log.warning("cleaning up %d/%d uploaded chunks after failure", len(chunks), total)
                try:
                    self.delete_chunks(chunks)
                except Exception:
                    pass
            elapsed = time.time() - t0
            log.error("upload of %s failed after %.2fs", path, elapsed)
            raise
        elapsed = time.time() - t0
        rate = len(data) / elapsed / 1e6 if elapsed > 0 else 0.0
        log.info(
            "uploaded %s (%d bytes) in %.2fs (%.2f MB/s)",
            path,
            len(data),
            elapsed,
            rate,
        )
        return chunks

    def upload_chunks_from_file(
        self,
        path: str,
        fileobj,
        size: int,
        mtime: float,
        mode: int,
        uuid: str,
        on_progress=None,
    ) -> list[dict[str, Any]]:
        """Upload chunks read from a seekable file object without buffering the whole file."""
        if size == 0:
            log.info("uploading %s (0 bytes, 0 chunk(s))", path)
            return []
        total = max(1, (size + self.chunk_size - 1) // self.chunk_size)
        chunks: list[dict[str, Any]] = []
        part = 0
        filename = path.rsplit("/", 1)[-1] or "file"
        log.info("uploading %s (%d bytes, %d chunk(s))", path, size, total)
        t0 = time.time()
        try:
            fileobj.seek(0)
            while part < total:
                piece = fileobj.read(self.chunk_size)
                if not piece:
                    break
                caption = json.dumps(
                    {
                        "tgdrive": "chunk",
                        "v": INDEX_VERSION,
                        "uuid": uuid,
                        "part": part,
                        "total": total,
                        "filename": filename,
                        "path": path,
                        "size": size,
                        "mtime": mtime,
                        "mode": mode,
                    },
                    separators=(",", ":"),
                )
                if len(caption) > 1024:
                    caption = json.dumps(
                        {
                            "tgdrive": "chunk",
                            "v": INDEX_VERSION,
                            "uuid": uuid,
                            "part": part,
                            "total": total,
                            "filename": filename,
                            "size": size,
                        },
                        separators=(",", ":"),
                    )
                chunk_name = f"{filename}.part{part}"
                ct0 = time.time()
                msg = self.send_document(piece, chunk_name, caption=caption)
                doc = msg.get("document", {})
                chunks.append(
                    {
                        "message_id": msg["message_id"],
                        "file_id": doc.get("file_id"),
                        "size": len(piece),
                        "part": part,
                    }
                )
                log.debug(
                    "uploaded chunk %d/%d (%d bytes) in %.2fs",
                    part + 1,
                    total,
                    len(piece),
                    time.time() - ct0,
                )
                part += 1
                if on_progress:
                    try:
                        on_progress(part, total)
                    except Exception:
                        pass
        except Exception:
            if chunks:
                log.warning("cleaning up %d/%d uploaded chunks after failure", len(chunks), total)
                try:
                    self.delete_chunks(chunks)
                except Exception:
                    pass
            elapsed = time.time() - t0
            log.error("upload of %s failed after %.2fs", path, elapsed)
            raise
        elapsed = time.time() - t0
        rate = size / elapsed / 1e6 if elapsed > 0 else 0.0
        log.info(
            "uploaded %s (%d bytes) in %.2fs (%.2f MB/s)",
            path,
            size,
            elapsed,
            rate,
        )
        return chunks

    def download_chunks(
        self,
        chunks: list[dict[str, Any]],
        on_progress=None,
        max_workers: int = 8,
    ) -> bytes:
        """Download all chunks concurrently and concatenate them in order.

        Chunks are downloaded in parallel (up to ``max_workers`` at once) to
        amortize per-connection latency and saturate bandwidth. Progress is
        reported via ``on_progress(done, total, bytes_done, bytes_total)``.
        """
        ordered = [c for c in sorted(chunks, key=lambda c: c.get("part", 0)) if c.get("file_id")]
        n = len(ordered)
        if n == 0:
            return b""
        total_bytes = sum(int(c.get("size", 0)) for c in ordered)
        results: list[bytes | None] = [None] * n
        done = 0
        bytes_done = 0
        done_lock = threading.Lock()

        def fetch(idx: int) -> None:
            nonlocal done, bytes_done
            ch = ordered[idx]
            t0 = time.time()
            data = self.download_file(ch["file_id"])
            with done_lock:
                results[idx] = data
                done += 1
                bytes_done += len(data)
                log.debug(
                    "chunk %d/%d (%d bytes) downloaded in %.2fs",
                    done,
                    n,
                    len(data),
                    time.time() - t0,
                )
                if on_progress:
                    try:
                        on_progress(done, n, bytes_done, total_bytes)
                    except Exception:
                        pass

        workers = max(1, min(max_workers, n))
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="tg-dl") as pool:
            futs = [pool.submit(fetch, i) for i in range(n)]
            for f in futs:
                f.result()
        return b"".join(b for b in results if b is not None)

    def download_chunks_to_file(
        self,
        chunks: list[dict[str, Any]],
        fileobj,
        on_progress=None,
        max_workers: int = 8,
    ) -> int:
        """Download chunks into a seekable file object without concatenating them in RAM."""
        ordered = [c for c in sorted(chunks, key=lambda c: c.get("part", 0)) if c.get("file_id")]
        n = len(ordered)
        fileobj.seek(0)
        fileobj.truncate(0)
        if n == 0:
            return 0
        offsets: list[int] = []
        offset = 0
        for ch in ordered:
            offsets.append(offset)
            offset += int(ch.get("size", 0))
        total_bytes = offset
        done = 0
        bytes_done = 0

        def fetch(idx: int) -> tuple[int, bytes, float]:
            t0 = time.time()
            return idx, self.download_file(ordered[idx]["file_id"]), time.time() - t0

        workers = max(1, min(max_workers, n))
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="tg-dl") as pool:
            pending = {pool.submit(fetch, i) for i in range(n)}
            while pending:
                done_futs, pending = wait(pending, return_when=FIRST_COMPLETED)
                for fut in done_futs:
                    idx, data, elapsed = fut.result()
                    fileobj.seek(offsets[idx])
                    fileobj.write(data)
                    done += 1
                    bytes_done += len(data)
                    log.debug(
                        "chunk %d/%d (%d bytes) downloaded in %.2fs",
                        done,
                        n,
                        len(data),
                        elapsed,
                    )
                    if on_progress:
                        try:
                            on_progress(done, n, bytes_done, total_bytes)
                        except Exception:
                            pass
        fileobj.flush()
        return total_bytes

    def delete_chunks(self, chunks: list[dict[str, Any]]) -> None:
        ids = [c.get("message_id") for c in chunks if c.get("message_id")]
        self.delete_messages(ids)
