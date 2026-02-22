"""Main loop: fetch RSS, dedupe, notify Telegram."""
from __future__ import annotations

import json
import logging
import sys
import time
from pathlib import Path

from .config import load_config
from .scraper import fetch_listings
from .telegram_notify import send_telegram

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m/%d %H:%M:%S",
)
logger = logging.getLogger("craigslist_monitor")

STATE_FILE = Path(__file__).resolve().parent.parent / "seen_ids.json"


def load_seen() -> set[str]:
    """Load set of seen listing IDs from state file."""
    if not STATE_FILE.exists():
        return set()
    try:
        with open(STATE_FILE, "r") as f:
            data = json.load(f)
        return set(data.get("ids", []))
    except Exception:
        return set()


def save_seen(seen: set[str], max_size: int = 5000) -> None:
    """Persist seen IDs. Keep only last max_size to avoid huge file."""
    ids = list(seen)
    if len(ids) > max_size:
        ids = ids[-max_size:]
    try:
        with open(STATE_FILE, "w") as f:
            json.dump({"ids": ids}, f)
    except Exception as e:
        logger.warning("Could not save seen_ids: %s", e)


def run_once(config: dict) -> set[str]:
    """Fetch listings, send new ones to Telegram, return updated seen set."""
    c = config.get("craigslist", {})
    tg = config.get("telegram", {})
    site = c.get("site", "philadelphia")
    category = c.get("category", "sss")
    query = c.get("query", "")
    min_price = int(c.get("min_price", 0) or 0)
    max_price = int(c.get("max_price", 0) or 0)
    token = (tg.get("telegram_token") or "").strip()
    chat_id = (tg.get("telegram_chat_id") or "").strip()

    if not token or not chat_id:
        logger.warning("telegram_token or telegram_chat_id missing; skipping Telegram")
        token = ""
        chat_id = ""

    seen = load_seen()
    try:
        listings = fetch_listings(site, category, query, min_price, max_price)
    except Exception as e:
        logger.error("Fetch failed: %s", e)
        return seen

    new_count = 0
    for li in listings:
        if li.id in seen:
            continue
        seen.add(li.id)
        new_count += 1
        if token and chat_id:
            send_telegram(token, chat_id, li, logger=logger)
        else:
            logger.info("New: %s %s", li.title[:60], li.url)

    if new_count:
        save_seen(seen)
        logger.info("New listings this run: %d", new_count)
    return seen


def main() -> None:
    """Load config and run loop."""
    config_path = Path(__file__).resolve().parent.parent / "config.toml"
    if len(sys.argv) > 1:
        config_path = Path(sys.argv[1])

    try:
        config = load_config(config_path)
    except FileNotFoundError as e:
        logger.error("%s", e)
        sys.exit(1)

    m = config.get("monitor", {})
    interval = int(m.get("interval_minutes", 30) or 30)
    interval_sec = max(60, interval * 60)

    logger.info("Craigslist monitor started (site=%s, query=%s, interval=%dm)",
                config.get("craigslist", {}).get("site"),
                config.get("craigslist", {}).get("query"),
                interval)

    while True:
        try:
            run_once(config)
        except KeyboardInterrupt:
            logger.info("Stopped by user")
            break
        except Exception as e:
            logger.exception("Run error: %s", e)
        logger.info("Sleeping %d seconds...", interval_sec)
        time.sleep(interval_sec)


if __name__ == "__main__":
    main()
