"""Fetch Craigslist search results via RSS."""
from __future__ import annotations

import re
import urllib.parse
from dataclasses import dataclass

import feedparser
import requests

USER_AGENT = "CraigslistMonitor/0.1 (RSS reader; +https://github.com)"


@dataclass
class Listing:
    id: str
    title: str
    url: str
    price: str
    location: str
    description: str
    published: str


def build_rss_url(site: str, category: str, query: str, min_price: int = 0, max_price: int = 0) -> str:
    """Build Craigslist search RSS URL."""
    base = f"https://{site}.craigslist.org/search/{category}"
    params: list[str] = []
    if query:
        params.append(f"query={urllib.parse.quote(query)}")
    if min_price and min_price > 0:
        params.append(f"min_price={min_price}")
    if max_price and max_price > 0:
        params.append(f"max_price={max_price}")
    params.append("format=rss")
    return f"{base}?{'&'.join(params)}"


def fetch_listings(
    site: str,
    category: str,
    query: str,
    min_price: int = 0,
    max_price: int = 0,
) -> list[Listing]:
    """Fetch listings from Craigslist RSS. Returns list of Listing."""
    url = build_rss_url(site, category, query, min_price, max_price)
    try:
        r = requests.get(url, timeout=30, headers={"User-Agent": USER_AGENT})
        r.raise_for_status()
        resp = feedparser.parse(r.content)
    except requests.RequestException as e:
        raise RuntimeError(f"Failed to fetch RSS: {e}") from e
    out: list[Listing] = []
    for e in getattr(resp, "entries", []):
        link = (e.get("link") or "").strip()
        if not link:
            continue
        # Use link as stable id (Craigslist URLs are unique per listing)
        lid = link
        title = (e.get("title") or "").strip()
        summary = (e.get("summary") or "").strip()
        # Strip HTML tags from summary
        summary_plain = re.sub(r"<[^>]+>", " ", summary).strip()
        published = (e.get("published") or e.get("updated") or "").strip()
        # Try to get price from title (e.g. "$5000")
        price = ""
        loc = ""
        if title:
            m = re.search(r"\$[\d,]+", title)
            if m:
                price = m.group(0)
            # Location often in parentheses at end
            loc_m = re.search(r"\(([^)]+)\)\s*$", title)
            if loc_m:
                loc = loc_m.group(1).strip()
        out.append(
            Listing(
                id=lid,
                title=title or "No title",
                url=link,
                price=price,
                location=loc,
                description=summary_plain[:500] if summary_plain else "",
                published=published,
            )
        )
    return out
