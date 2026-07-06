import argparse
import logging
import os
import sys

from fuse import FUSE


def main():
    parser = argparse.ArgumentParser(
        description="Mount a Telegram channel as a FUSE filesystem"
    )
    parser.add_argument(
        "--token", default=os.environ.get("TGDRIVE_TOKEN"),
        help="Telegram bot token (or TGDRIVE_TOKEN env var)"
    )
    parser.add_argument(
        "--chat-id", default=os.environ.get("TGDRIVE_CHAT_ID"),
        help="Telegram channel/chat ID (or TGDRIVE_CHAT_ID env var)"
    )
    parser.add_argument(
        "--foreground", action="store_true",
        help="Run in foreground (don't daemonize)"
    )
    parser.add_argument(
        "--debug", action="store_true",
        help="Enable debug logging"
    )
    parser.add_argument(
        "mountpoint", nargs="?", default="/mnt/tgdrive",
        help="Mount point (default: /mnt/tgdrive)"
    )

    args = parser.parse_args()

    if not args.token:
        print("Error: --token or TGDRIVE_TOKEN env var is required", file=sys.stderr)
        sys.exit(1)
    if not args.chat_id:
        print("Error: --chat-id or TGDRIVE_CHAT_ID env var is required", file=sys.stderr)
        sys.exit(1)

    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    log = logging.getLogger("tgdrive")
    log.info("Mounting Telegram Drive on %s", args.mountpoint)
    log.info("Chat ID: %s", args.chat_id)

    if not os.path.isdir(args.mountpoint):
        os.makedirs(args.mountpoint, exist_ok=True)

    from .fs import TgDriveFS

    fs = TgDriveFS(
        token=args.token,
        chat_id=args.chat_id,
    )

    fuse_kwargs = {
        "foreground": args.foreground,
        "allow_other": False,
        "default_permissions": True,
        "fsname": "tgdrive",
        "subtype": "tgdrive",
        "direct_io": True,
        "nonempty": True,
    }

    FUSE(fs, args.mountpoint, **fuse_kwargs)


if __name__ == "__main__":
    main()
