"""Send a message to Telegram."""
from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .scraper import Listing


def send_telegram(
    token: str,
    chat_id: str,
    listing: "Listing",
    logger=None,
) -> bool:
    """Send one listing to Telegram. Sync wrapper around async."""
    try:
        return asyncio.run(_send_async(token, chat_id, listing, logger))
    except Exception as e:
        if logger:
            logger.error(f"Telegram send failed: {e}")
        return False


async def _send_async(
    token: str,
    chat_id: str,
    listing: "Listing",
    logger=None,
) -> bool:
    try:
        from telegram import Bot
    except ImportError:
        if logger:
            logger.error("python-telegram-bot is required. pip install python-telegram-bot")
        return False

    title_s = listing.title[:80] + ("..." if len(listing.title) > 80 else "")
    body = f"{title_s}\n{listing.price or 'No price'} | {listing.location or 'No location'}\n{listing.url}"
    if listing.description:
        body += f"\n\n{listing.description[:200]}{'...' if len(listing.description) > 200 else ''}"

    try:
        bot = Bot(token=token)
        await bot.send_message(chat_id=chat_id, text=body)
        if logger:
            logger.info(f"Sent to Telegram: {listing.title[:50]}...")
        return True
    except Exception as e:
        if logger:
            logger.error(f"Telegram error: {e}")
        return False
