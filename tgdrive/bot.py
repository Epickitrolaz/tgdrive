import json
import time
import logging
import requests

API_BASE = "https://api.telegram.org/bot{token}/{method}"
FILE_BASE = "https://api.telegram.org/file/bot{token}/{file_path}"
CHUNK_SIZE = 20 * 1024 * 1024
CAPTION_PREFIX = "TGDRIVE:v1:"
REQUEST_TIMEOUT = 600

log = logging.getLogger(__name__)


class TgBot:
    def __init__(self, token, chat_id):
        self.token = token
        self.chat_id = str(chat_id)
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "tgdrive/2.0"
        self._offset = 0

    def _call(self, method, data=None, files=None, params=None):
        url = API_BASE.format(token=self.token, method=method)
        for attempt in range(3):
            try:
                r = self.session.post(
                    url, data=data, files=files, params=params,
                    timeout=REQUEST_TIMEOUT
                )
                r.raise_for_status()
                result = r.json()
                if not result.get("ok"):
                    desc = result.get("description", "?")
                    if "message to edit not found" in desc:
                        raise MessageNotFoundError(desc)
                    if "message to delete not found" in desc:
                        raise MessageNotFoundError(desc)
                    raise Exception(f"API error: {desc}")
                return result["result"]
            except (requests.Timeout, requests.ConnectionError) as e:
                if attempt == 2:
                    raise
                log.warning("Retry %d for %s: %s", attempt + 1, method, e)
                time.sleep(2 ** attempt)
        return None

    def send_document(self, data, filename, caption=""):
        return self._call(
            "sendDocument",
            data={"chat_id": self.chat_id, "caption": caption},
            files={"document": (filename, data)},
        )

    def delete_message(self, msg_id):
        try:
            return self._call(
                "deleteMessage",
                data={"chat_id": self.chat_id, "message_id": msg_id},
            )
        except MessageNotFoundError:
            return None

    def send_message(self, text):
        return self._call(
            "sendMessage", data={"chat_id": self.chat_id, "text": text}
        )

    def edit_message_text(self, msg_id, text):
        return self._call(
            "editMessageText",
            data={
                "chat_id": self.chat_id,
                "message_id": msg_id,
                "text": text,
            },
        )

    def forward_message(self, from_chat_id, msg_id):
        return self._call(
            "forwardMessage",
            data={
                "chat_id": self.chat_id,
                "from_chat_id": from_chat_id,
                "message_id": msg_id,
            },
        )

    def get_file(self, file_id):
        return self._call("getFile", data={"file_id": file_id})

    def download_file(self, file_path):
        url = FILE_BASE.format(token=self.token, file_path=file_path)
        for attempt in range(3):
            try:
                r = self.session.get(url, timeout=REQUEST_TIMEOUT)
                r.raise_for_status()
                return r.content
            except Exception as e:
                if attempt == 2:
                    raise
                log.warning("Download retry %d: %s", attempt + 1, e)
                time.sleep(2 ** attempt)
        raise Exception(f"Download failed for {file_path}")

    def get_updates(self, timeout=30):
        params = {
            "offset": self._offset,
            "timeout": timeout,
            "allowed_updates": json.dumps(["message"]),
        }
        try:
            result = self._call("getUpdates", params=params)
            if result:
                for u in result:
                    self._offset = u["update_id"] + 1
                return result
        except Exception:
            pass
        return []

    def upload_chunk(self, data, filename, caption):
        return self.send_document(data, filename, caption)

    def download_chunk_data(self, file_id):
        info = self.get_file(file_id)
        return self.download_file(info["file_path"])

    def get_chat(self):
        return self._call("getChat", data={"chat_id": self.chat_id})

    def upload_file_chunks(self, file_uuid, data, filename, mode, mtime):
        total_size = len(data)
        total_parts = (total_size + CHUNK_SIZE - 1) // CHUNK_SIZE
        parts = []

        for i in range(total_parts):
            start = i * CHUNK_SIZE
            end = min(start + CHUNK_SIZE, total_size)
            chunk_data = data[start:end]
            caption_data = {
                "uuid": file_uuid,
                "part": i,
                "total": total_parts,
                "name": filename,
                "size": total_size,
                "mtime": mtime,
                "mode": mode,
            }
            caption = CAPTION_PREFIX + json.dumps(caption_data)
            msg = self.upload_chunk(
                chunk_data, f"{file_uuid}.part{i}", caption
            )
            parts.append({
                "msg_id": msg["message_id"],
                "file_id": msg["document"]["file_id"],
                "size": len(chunk_data),
                "index": i,
            })

        return parts

    def download_file_data(self, parts):
        total_size = sum(p["size"] for p in parts if p)
        result = bytearray(total_size)

        for p in parts:
            if p is None:
                continue
            data = self.download_chunk_data(p["file_id"])
            start = p["index"] * CHUNK_SIZE
            end = start + len(data)
            result[start:end] = data

        return bytes(result)


class MessageNotFoundError(Exception):
    pass

