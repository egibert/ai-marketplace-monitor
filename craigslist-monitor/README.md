# Craigslist Monitor

**Separate project from ai-marketplace-monitor.** This monitors Craigslist listings and sends new matches to Telegram. Run it in its own terminal; it does not share code or config with the Facebook marketplace monitor.

- **Same Telegram** – Use the same bot token and chat_id as ai-marketplace-monitor so alerts from both go to one place.
- **Separate codebase** – No dependency on ai-marketplace-monitor. No stepping on each other's toes.
- **RSS-based** – Uses Craigslist search RSS feeds (no browser, no Playwright).

## Setup

```bash
cd craigslist-monitor
python -m venv .venv
source .venv/bin/activate   # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
cp config.example.toml config.toml
# Edit config.toml: site, query, telegram_token, telegram_chat_id
```

## Run

From the `craigslist-monitor` directory:

```bash
cd craigslist-monitor
python -m craigslist_monitor
```

Or with a custom config path:

```bash
python -m craigslist_monitor /path/to/config.toml
```

Runs in a loop: fetches RSS, dedupes by seen IDs, sends new listings to Telegram. Use a **separate terminal** for ai-marketplace-monitor (Facebook).

## Config

- **site** – Craigslist subdomain (e.g. `philadelphia`, `pittsburgh`).
- **category** – Category code (e.g. `sss` = all for sale, `mca` = mobile homes).
- **query** – Search query (e.g. `mobile home`).
- **telegram_token** / **telegram_chat_id** – Same as in ai-marketplace-monitor to get alerts in one chat.
- **interval_minutes** – How often to check (default 30).

## Project layout

```
craigslist-monitor/
  README.md
  requirements.txt
  config.example.toml
  craigslist_monitor/
    __init__.py
    config.py
    scraper.py
    telegram_notify.py
    main.py
```
