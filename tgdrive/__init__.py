"""tgdrive: a FUSE filesystem backed by a Telegram chat.

Mounts a Telegram chat as a virtual drive. Files are stored as Telegram
document messages (in 20 MB chunks) and a JSON directory index is kept in the
chat's pinned message.
"""

__version__ = "1.0.0"
