"""Command line interface for tgdrive."""

from __future__ import annotations

import argparse
import logging
import os
import sys

from .fs import mount


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tgdrive",
        description="Mount a Telegram chat as a FUSE filesystem.",
    )
    parser.add_argument(
        "--token",
        dest="token",
        default=os.environ.get("TGDRIVE_TOKEN"),
        help="Telegram bot token (env: TGDRIVE_TOKEN).",
    )
    parser.add_argument(
        "--chat-id",
        dest="chat_id",
        default=os.environ.get("TGDRIVE_CHAT_ID"),
        help="Telegram chat id (env: TGDRIVE_CHAT_ID).",
    )
    parser.add_argument(
        "--foreground",
        dest="foreground",
        action="store_true",
        default=False,
        help="Run in the foreground (do not daemonize).",
    )
    parser.add_argument(
        "--debug",
        dest="debug",
        action="store_true",
        default=False,
        help="Enable FUSE debug output and foreground mode.",
    )
    parser.add_argument(
        "--chunk-size",
        dest="chunk_size",
        type=int,
        default=None,
        help="Chunk size in bytes (default: 20 MB).",
    )
    parser.add_argument(
        "mountpoint",
        nargs="?",
        default="/mnt/tgdrive",
        help="Mountpoint path (default: /mnt/tgdrive).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if not args.token:
        parser.error("--token is required (or set TGDRIVE_TOKEN)")
    if not args.chat_id:
        parser.error("--chat-id is required (or set TGDRIVE_CHAT_ID)")

    try:
        chat_id = int(args.chat_id)
    except (TypeError, ValueError):
        chat_id = args.chat_id

    mountpoint = args.mountpoint
    if not os.path.isdir(mountpoint):
        try:
            os.makedirs(mountpoint, exist_ok=True)
        except OSError as e:
            print(f"tgdrive: cannot create mountpoint {mountpoint}: {e}", file=sys.stderr)
            return 1

    try:
        mount(
            token=args.token,
            chat_id=chat_id,
            mountpoint=mountpoint,
            foreground=args.foreground,
            debug=args.debug,
            chunk_size=args.chunk_size,
        )
    except KeyboardInterrupt:
        print("tgdrive: interrupted", file=sys.stderr)
    except RuntimeError as e:
        print(f"tgdrive: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
