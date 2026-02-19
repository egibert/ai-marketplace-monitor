"""
MySQL comparison module for ai-marketplace-monitor.

When using Ollama locally, this module can query your MySQL database to fetch
comparison data (Zillow sales comps by zip→county→region, similar fb_listings)
and inject it into the AI prompt. It can also insert accepted listings into
fb_listings.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from logging import Logger
from typing import Any, Dict, List, Optional, Tuple

from .listing import Listing
from .utils import cache, hilight


@dataclass
class MySQLConfig:
    """Configuration for MySQL connection and comparison."""

    host: str = "localhost"
    port: int = 3306
    user: str = ""
    password: str = ""
    database: str = ""
    enabled: bool = True

    # Optional: custom query with placeholders {title}, {price}, {location}, {item_name}
    comparison_query: Optional[str] = None
    # Built-in: fetch similar by title/price (e.g. "fb_listings")
    comparison_table: Optional[str] = None

    title_column: str = "title"
    price_column: Optional[str] = "price"  # use "asking_price" for fb_listings

    max_rows: int = 10
    output_format: str = "full"

    # --- Sales comps (Zillow sold properties): zip → county → region
    use_sales_comps: bool = False
    sales_max_rows: int = 10
    sales_table: str = "sales"
    properties_table: str = "properties"
    zip_county_table: str = "zip_county"
    counties_table: str = "counties"
    # Year tolerance for age filter (e.g. ±5 years)
    year_tolerance: int = 5

    # --- Insert accepted listings into fb_listings
    insert_into_fb: bool = True
    fb_listings_table: str = "fb_listings"
    # When true (default), insert every evaluated listing into fb_listings. Set false to insert only when listing passes threshold.
    insert_all_evaluated: bool = True
    # Optional: also insert a row into this table for price history (e.g. fb_listing_history). Columns: external_id, asking_price, recorded_at.
    fb_listing_history_table: Optional[str] = None

    # Connection timeout (seconds); avoid hanging if MySQL is unreachable
    connection_timeout: int = 10

    # When true (default), resolve city/state -> zip via Geoapify Geocoding API when no zip in listing text.
    geocode_fallback: bool = True
    # Required for city/state -> zip. Get a key from https://www.geoapify.com/ (Geocoding API).
    geocode_geoapify_api_key: Optional[str] = None
    # Sleep this many seconds after each geocode API call.
    geocode_rate_limit_seconds: float = 1.0

    # --- Average lot rent (when not in listing description): query by zip → county → region
    lot_rent_table: Optional[str] = None  # e.g. lot_rents
    lot_rent_zip_column: str = "zip"
    lot_rent_county_column: str = "county_id"
    lot_rent_region_column: str = "region_id"
    lot_rent_value_column: str = "avg_rent"  # or "rent"

    def connection_dict(self) -> Dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "connection_timeout": self.connection_timeout,
        }


@dataclass
class ComparisonResult:
    """Result of a MySQL comparison for one listing."""

    summary: str  # Human-readable summary for the AI prompt
    rows: List[Dict[str, Any]] = field(default_factory=list)
    raw_text: str = ""  # Optional raw dump for custom output
    # One-line price comparison: "Vs Zillow: X% below average (computed using zip). Vs Facebook: ..." (or "no comps"/"no data")
    concise_price_line: str = ""
    # Which geographic scope was used for Zillow comps: "zip", "county", or "region"
    sales_scope: Optional[str] = None
    # When lot rent not in listing: "Average lot rent (zip 16428): $400" or ""
    average_lot_rent_line: str = ""


def _parse_price(price_str: str) -> Optional[float]:
    """Extract numeric price from listing price string (e.g. '$180', '$30,000 - $32,000'). Uses first number found for ranges."""
    if not price_str or price_str == "**unspecified**":
        return None
    # Find all numbers (with optional commas: 30,000 or 30000)
    numbers = re.findall(r"[\d,]+(?:\.\d+)?", price_str)
    for n in numbers:
        cleaned = n.replace(",", "").strip()
        if cleaned:
            try:
                return float(cleaned)
            except ValueError:
                continue
    return None


def _parse_beds_baths_year(listing: Listing) -> Tuple[Optional[int], Optional[float], Optional[int]]:
    """Extract beds, baths, and year_built from listing title and description."""
    text = f"{listing.title or ''} {listing.description or ''}".lower()
    beds: Optional[int] = None
    baths: Optional[float] = None
    year_built: Optional[int] = None
    m = re.search(r"(\d+)\s*bed", text)
    if m:
        try:
            beds = int(m.group(1))
        except ValueError:
            pass
    m = re.search(r"(\d+(?:\.\d+)?)\s*bath", text)
    if m:
        try:
            baths = float(m.group(1))
        except ValueError:
            pass
    m = re.search(r"(?:built|year|yr)\s*[:\s]*(\d{4})", text)
    if not m:
        m = re.search(r"\b(19\d{2}|20[0-2]\d)\b", text)
    if m:
        try:
            year_built = int(m.group(1))
        except (ValueError, IndexError):
            pass
    return (beds, baths, year_built)


def _safe_table(name: str) -> bool:
    return bool(name and re.match(r"^[a-zA-Z0-9_]+$", name))


def _lot_rent_in_listing(listing: Listing) -> bool:
    """True if listing title or description mentions lot rent / space rent with a dollar amount."""
    text = f"{listing.title or ''} {listing.description or ''}".lower()
    if not re.search(r"lot\s+rent|space\s+rent|lot\s+:\s*\$|space\s+:\s*\$", text):
        return False
    # Has a dollar amount near rent (e.g. "lot rent $400", "rent: 350", "space rent 500/mo")
    if re.search(r"(?:lot\s+rent|space\s+rent|rent)\s*[:\s]*\$?\s*[\d,]+", text):
        return True
    if re.search(r"\$[\d,]+(?:\s*/\s*mo|\s*per\s*month)?(?:\s+lot|\s+space|\s+rent)", text):
        return True
    return False


class MySQLCompare:
    """
    Query MySQL for comparison data based on the current listing.
    Use this when running with Ollama to enrich the AI context.
    """

    def __init__(self, config: MySQLConfig, logger: Optional[Logger] = None) -> None:
        self.config = config
        self.logger = logger
        self._client: Any = None

    def _get_client(self):  # noqa: ANN201
        """Lazy MySQL connection (requires mysql-connector-python or PyMySQL)."""
        if self._client is not None:
            return self._client
        conn_dict = self.config.connection_dict()
        timeout = conn_dict.pop("connection_timeout", 10)
        try:
            import mysql.connector  # type: ignore

            self._client = mysql.connector.connect(
                **conn_dict,
                connection_timeout=timeout,
            )
            if self.logger:
                self.logger.debug(
                    f"""{hilight("[MySQL]", "info")} Connected to {self.config.host}:{self.config.database}"""
                )
            return self._client
        except ImportError:
            try:
                import pymysql  # type: ignore

                conn_dict = self.config.connection_dict()
                timeout = conn_dict.pop("connection_timeout", 10)
                self._client = pymysql.connect(
                    **conn_dict,
                    connect_timeout=timeout,
                    cursorclass=pymysql.cursors.DictCursor,
                )
                if self.logger:
                    self.logger.debug(
                        f"""{hilight("[MySQL]", "info")} Connected via PyMySQL to {self.config.host}:{self.config.database}"""
                    )
                return self._client
            except ImportError as e:
                if self.logger:
                    self.logger.warning(
                        f"""{hilight("[MySQL]", "fail")} Install mysql-connector-python or PyMySQL for DB comparison: {e}"""
                    )
                raise

    def close(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None

    def _geocode_city_state_to_zip(self, city: str, state: str) -> Optional[str]:
        """Resolve city, state to zip via Geoapify Geocoding API. Results are cached. Rate-limited."""
        if not city and not state:
            return None
        query = f"{city or ''},{state or ''},USA".strip(" ,")
        if not query or query == ",USA":
            return None
        normalized = re.sub(r"\s+", " ", query.lower()).strip()
        cache_key = ("geocode_zip", normalized)
        try:
            cached = cache.get(cache_key)
            if cached is not None:
                return str(cached) if cached else None
        except Exception:
            pass
        api_key = getattr(self.config, "geocode_geoapify_api_key", None) or ""
        if not api_key.strip():
            if self.logger:
                self.logger.warning(
                    f"""{hilight("[MySQL]", "fail")} geocode_geoapify_api_key is not set; cannot resolve city/state to zip"""
                )
            return None
        try:
            import requests  # type: ignore

            url = "https://api.geoapify.com/v1/geocode/search"
            params = {
                "text": query,
                "format": "json",
                "filter": "countrycode:us",
                "limit": 10,
                "apiKey": api_key,
            }
            headers = {"Accept": "application/json"}
            resp = requests.get(url, params=params, headers=headers, timeout=10)
            if self.logger and self.logger.isEnabledFor(10):
                self.logger.debug(
                    f"""{hilight("[MySQL]", "info")} Geocode Geoapify: {repr(query)} -> status={resp.status_code}"""
                )
            resp.raise_for_status()
            data = resp.json()
            results = (data or {}).get("results") or []
            if not results:
                if self.logger and self.logger.isEnabledFor(10):
                    self.logger.debug(
                        f"""{hilight("[MySQL]", "info")} Geocode Geoapify: {repr(query)} -> no results"""
                    )
                time.sleep(max(0, self.config.geocode_rate_limit_seconds))
                return None
            for feature in results:
                postcode = (feature.get("postcode") or "").strip()
                if postcode and re.match(r"^\d{5}(?:-\d{4})?$", postcode):
                    zip_code = postcode[:5]
                    try:
                        cache.set(cache_key, zip_code, tag="geocode_zip")
                    except Exception:
                        pass
                    if self.logger and self.logger.isEnabledFor(10):
                        self.logger.debug(
                            f"""{hilight("[MySQL]", "info")} Geocode Geoapify: {repr(query)} -> zip={zip_code} (from result with postcode)"""
                        )
                    time.sleep(max(0, self.config.geocode_rate_limit_seconds))
                    return zip_code
            if self.logger and self.logger.isEnabledFor(10):
                self.logger.debug(
                    f"""{hilight("[MySQL]", "info")} Geocode Geoapify: {repr(query)} -> no result had US postcode (checked {len(results)} results)"""
                )
        except Exception as e:
            if self.logger:
                self.logger.warning(
                    f"""{hilight("[MySQL]", "fail")} Geoapify Geocoding API failed for {repr(query)}: {e}"""
                )
        time.sleep(max(0, self.config.geocode_rate_limit_seconds))
        return None

    def _drain_cursor(self, cursor: Any) -> None:
        """Consume any remaining rows so the cursor is clean for the next execute (avoids 'Unread result' errors)."""
        try:
            while cursor.fetchone():
                pass
        except Exception:
            pass

    def _resolve_location(self, cursor: Any, listing: Listing) -> Tuple[Optional[str], Optional[int], Optional[int]]:
        """Resolve listing.location to (zip, county_id, region_id). Zip from regex in text, else Geoapify Geocoding API (city/state -> zip)."""
        loc = (listing.location or "").strip()
        zip_match = re.search(r"\b(\d{5})(?:-\d{4})?\b", loc)
        zip_code = zip_match.group(1) if zip_match else None
        if self.logger and self.logger.isEnabledFor(10):
            self.logger.debug(
                f"""{hilight("[MySQL]", "info")} Zillow comps: resolving location from listing.location={repr(loc)[:80]} -> zip_from_regex={zip_code}"""
            )
        if not zip_code:
            city, state, _ = self._parse_location_parts(loc)
            if (city or state) and self.config.geocode_fallback:
                zip_code = self._geocode_city_state_to_zip(city or "", state or "")
                if self.logger and self.logger.isEnabledFor(10):
                    self.logger.debug(
                        f"""{hilight("[MySQL]", "info")} Zillow comps: after Geoapify geocode city={repr(city)} state={repr(state)} -> zip={zip_code}"""
                    )
        if not zip_code:
            if self.logger and self.logger.isEnabledFor(10):
                self.logger.debug(
                    f"""{hilight("[MySQL]", "info")} Zillow comps: no zip resolved; cannot query comps"""
                )
            return (None, None, None)
        if not _safe_table(self.config.zip_county_table) or not _safe_table(self.config.counties_table):
            return (zip_code, None, None)
        county_id: Optional[int] = None
        try:
            cursor.execute(
                f"SELECT county_id FROM `{self.config.zip_county_table}` WHERE zip = %s LIMIT 1",
                (zip_code,),
            )
            row = cursor.fetchone()
            self._drain_cursor(cursor)
            county_id = int(row["county_id"]) if row and isinstance(row, dict) else (int(row[0]) if row else None)
        except Exception:
            self._drain_cursor(cursor)
            county_id = None
        if county_id is None and _safe_table(self.config.properties_table):
            try:
                cursor.execute(
                    f"SELECT county_id FROM `{self.config.properties_table}` WHERE zip = %s AND county_id IS NOT NULL LIMIT 1",
                    (zip_code,),
                )
                row = cursor.fetchone()
                self._drain_cursor(cursor)
                county_id = int(row["county_id"]) if row and isinstance(row, dict) else (int(row[0]) if row else None)
            except Exception:
                self._drain_cursor(cursor)
        if county_id is None:
            if self.logger and self.logger.isEnabledFor(10):
                self.logger.debug(
                    f"""{hilight("[MySQL]", "info")} Zillow comps: zip={zip_code}, county_id=None (could not resolve county), region_id=None"""
                )
            return (zip_code, None, None)
        region_id: Optional[int] = None
        try:
            cursor.execute(
                f"SELECT region_id FROM `{self.config.counties_table}` WHERE id = %s LIMIT 1",
                (county_id,),
            )
            row = cursor.fetchone()
            self._drain_cursor(cursor)
            region_id = int(row["region_id"]) if row and isinstance(row, dict) else (int(row[0]) if row else None)
        except Exception:
            self._drain_cursor(cursor)
            region_id = None
        if region_id is None and _safe_table(self.config.properties_table):
            try:
                cursor.execute(
                    f"SELECT region_id FROM `{self.config.properties_table}` WHERE county_id = %s AND region_id IS NOT NULL LIMIT 1",
                    (county_id,),
                )
                row = cursor.fetchone()
                self._drain_cursor(cursor)
                region_id = int(row["region_id"]) if row and isinstance(row, dict) else (int(row[0]) if row else None)
            except Exception:
                self._drain_cursor(cursor)
        if self.logger and self.logger.isEnabledFor(10):
            self.logger.debug(
                f"""{hilight("[MySQL]", "info")} Zillow comps: resolved location zip={zip_code}, county_id={county_id}, region_id={region_id}"""
            )
        return (zip_code, county_id, region_id)

    def _fetch_sales_comps(
        self,
        cursor: Any,
        listing: Listing,
    ) -> Optional[ComparisonResult]:
        """Query sales JOIN properties: zip first, then county, then region; filter by beds, baths, year."""
        if not self.config.use_sales_comps or not _safe_table(self.config.sales_table) or not _safe_table(self.config.properties_table):
            return None
        zip_code, county_id, region_id = self._resolve_location(cursor, listing)
        beds, baths, year_built = _parse_beds_baths_year(listing)
        year_lo = (year_built - self.config.year_tolerance) if year_built is not None and self.config.year_tolerance >= 0 else None
        year_hi = (year_built + self.config.year_tolerance) if year_built is not None and self.config.year_tolerance >= 0 else None
        if self.logger and self.logger.isEnabledFor(10):
            self.logger.debug(
                f"""{hilight("[MySQL]", "info")} Zillow comps: parsed listing beds={beds}, baths={baths}, year_built={year_built}; """
                f"""age filter: year_tolerance={self.config.year_tolerance} -> year_built BETWEEN {year_lo} AND {year_hi} (applied={year_built is not None and self.config.year_tolerance >= 0})"""
            )
        s_t, p_t = self.config.sales_table, self.config.properties_table
        conditions: List[str] = []
        params: List[Any] = []
        if beds is not None:
            conditions.append(f"p.beds = %s")
            params.append(beds)
        if baths is not None:
            conditions.append(f"p.baths = %s")
            params.append(baths)
        if year_built is not None and self.config.year_tolerance >= 0:
            conditions.append(f"p.year_built BETWEEN %s AND %s")
            params.append(year_built - self.config.year_tolerance)
            params.append(year_built + self.config.year_tolerance)
        where_extra = (" AND " + " AND ".join(conditions)) if conditions else ""
        limit = self.config.sales_max_rows

        tries: List[Tuple[str, str, List[Any]]] = []
        if zip_code:
            tries.append(("zip", "p.zip = %s", [zip_code]))
        if county_id is not None:
            tries.append(("county", "p.county_id = %s", [county_id]))
        if region_id is not None:
            tries.append(("region", "p.region_id = %s", [region_id]))
        if self.logger and self.logger.isEnabledFor(10) and not tries:
            self.logger.debug(
                f"""{hilight("[MySQL]", "info")} Zillow comps: no location to query (zip={zip_code}, county_id={county_id}, region_id={region_id}); skipping comps"""
            )

        for scope, scope_where, scope_params in tries:
            try:
                q = (
                    f"SELECT s.sale_price, s.sale_date, p.beds, p.baths, p.square_feet, p.year_built, p.city, p.state, p.zip "
                    f"FROM `{s_t}` s JOIN `{p_t}` p ON s.property_id = p.id "
                    f"WHERE {scope_where}{where_extra} ORDER BY s.sale_date DESC LIMIT %s"
                )
                cursor.execute(q, scope_params + params + [limit])
                rows = cursor.fetchall()
            except Exception as e:
                if self.logger and self.logger.isEnabledFor(10):
                    self.logger.debug(
                        f"""{hilight("[MySQL]", "info")} Zillow comps: scope={scope} with filters (beds/baths/year): query failed: {e}"""
                    )
                self._drain_cursor(cursor)
                continue
            if rows and not isinstance(rows[0], dict):
                cols = cursor.column_names if hasattr(cursor, "column_names") else []
                rows = [dict(zip(cols, r)) for r in rows]
            if self.logger and self.logger.isEnabledFor(10):
                self.logger.debug(
                    f"""{hilight("[MySQL]", "info")} Zillow comps: scope={scope} with filters (beds/baths/year): {len(rows) if rows else 0} rows"""
                )
            if rows:
                summary = f"Recent sold comps ({scope}):\n" + self._rows_to_summary(rows)
                return ComparisonResult(summary=summary, rows=rows, raw_text="\n".join(str(r) for r in rows), sales_scope=scope)

        # Fallback: same zip/county/region but without beds/baths/year filter (area comps)
        if where_extra:
            if self.logger and self.logger.isEnabledFor(10):
                self.logger.debug(
                    f"""{hilight("[MySQL]", "info")} Zillow comps: no rows with beds/baths/year filter; trying fallback without age/size filters (area comps)"""
                )
            for scope, scope_where, scope_params in tries:
                try:
                    q = (
                        f"SELECT s.sale_price, s.sale_date, p.beds, p.baths, p.square_feet, p.year_built, p.city, p.state, p.zip "
                        f"FROM `{s_t}` s JOIN `{p_t}` p ON s.property_id = p.id "
                        f"WHERE {scope_where} ORDER BY s.sale_date DESC LIMIT %s"
                    )
                    cursor.execute(q, scope_params + [limit])
                    rows = cursor.fetchall()
                except Exception as e:
                    if self.logger and self.logger.isEnabledFor(10):
                        self.logger.debug(
                            f"""{hilight("[MySQL]", "info")} Zillow comps: fallback scope={scope} (no filters): query failed: {e}"""
                        )
                    self._drain_cursor(cursor)
                    continue
                if rows and not isinstance(rows[0], dict):
                    cols = cursor.column_names if hasattr(cursor, "column_names") else []
                    rows = [dict(zip(cols, r)) for r in rows]
                if self.logger and self.logger.isEnabledFor(10):
                    self.logger.debug(
                        f"""{hilight("[MySQL]", "info")} Zillow comps: fallback scope={scope} (no beds/baths/year): {len(rows) if rows else 0} rows"""
                    )
                if rows:
                    summary = f"Recent sold comps ({scope}, area):\n" + self._rows_to_summary(rows)
                    return ComparisonResult(summary=summary, rows=rows, raw_text="\n".join(str(r) for r in rows), sales_scope=scope)

        if self.logger and self.logger.isEnabledFor(10):
            self.logger.debug(
                f"""{hilight("[MySQL]", "info")} Zillow comps: no comps found (tried zip/county/region with and without beds/baths/year filter). """
                f"""Location: zip={zip_code}, county_id={county_id}, region_id={region_id}; filters used: beds={beds}, baths={baths}, year_built={year_built} (tolerance={self.config.year_tolerance})"""
            )
        return ComparisonResult(
            summary="No recent sales comps found for this location (zip → county → region).",
            rows=[],
            raw_text="",
        )

    def fetch_comparison(
        self,
        listing: Listing,
        item_name: str = "",
    ) -> Optional[ComparisonResult]:
        """
        Query MySQL: sales comps (zip→county→region) and/or fb_listings comparison.
        Returns combined summary for the AI prompt.
        """
        if not self.config.enabled:
            return None
        if not self.config.use_sales_comps and not self.config.comparison_query and not self.config.comparison_table:
            return None
        if self.logger:
            self.logger.info(f"""{hilight("[MySQL]", "info")} Fetching comparison for listing {hilight((listing.title or "")[:50])}...""")
        try:
            client = self._get_client()
        except Exception as e:
            if self.logger:
                self.logger.warning(f"""{hilight("[MySQL]", "fail")} Connection failed (check host/user/database): {e}""")
            return None

        cursor = client.cursor(dictionary=True) if hasattr(client, "cursor") else client.cursor()
        combined_parts: List[str] = []
        sales_res: Optional[ComparisonResult] = None
        fb_res: Optional[ComparisonResult] = None
        average_lot_rent_line: str = ""

        try:
            if self.config.comparison_query:
                res = self._run_custom_query(cursor, listing, item_name)
                if res:
                    combined_parts.append(res.summary)
            elif self.config.use_sales_comps:
                sales_res = self._fetch_sales_comps(cursor, listing)
                if sales_res:
                    combined_parts.append(sales_res.summary)
                if self.config.comparison_table:
                    fb_res = self._run_builtin_comparison(cursor, listing, item_name)
                    if fb_res and fb_res.summary:
                        combined_parts.append(fb_res.summary)
            elif self.config.comparison_table:
                fb_res = self._run_builtin_comparison(cursor, listing, item_name)
                if fb_res:
                    combined_parts.append(fb_res.summary)
            if getattr(self.config, "lot_rent_table", None) and _safe_table(getattr(self.config, "lot_rent_table", "")):
                if not _lot_rent_in_listing(listing):
                    average_lot_rent_line = self._get_average_lot_rent(cursor, listing)
        except Exception as e:
            if self.logger:
                self.logger.warning(f"""{hilight("[MySQL]", "fail")} Query failed: {e}""")
            return None
        finally:
            try:
                cursor.close()
            except Exception:
                pass

        if not combined_parts:
            return None
        if self.logger:
            self.logger.info(f"""{hilight("[MySQL]", "succ")} Comparison done for listing.""")

        # Build one-line price comparison: Vs Zillow: X% above/below average. Vs Facebook: Y% above/below average.
        listing_price = _parse_price(listing.price)
        concise_parts: List[str] = []
        sales_scope: Optional[str] = getattr(sales_res, "sales_scope", None) if sales_res else None
        if sales_res and sales_res.rows and listing_price is not None:
            prices = [float(r["sale_price"]) for r in sales_res.rows if r.get("sale_price") is not None]
            if prices:
                avg_z = sum(prices) / len(prices)
                pct = ((listing_price - avg_z) / avg_z) * 100
                scope_txt = f" (computed using {sales_scope})" if sales_scope else ""
                concise_parts.append(f"Vs Zillow: {abs(pct):.0f}% {'below' if pct < 0 else 'above'} average when compared to recently sold Zillow listings{scope_txt}.")
            else:
                if self.logger and self.logger.isEnabledFor(10):
                    self.logger.debug(
                        f"""{hilight("[MySQL]", "info")} Vs Zillow: no comps (sales_res has {len(sales_res.rows)} rows but no valid sale_price in any row)"""
                    )
                concise_parts.append("Vs Zillow: no comps.")
        elif self.config.use_sales_comps:
            if self.logger and self.logger.isEnabledFor(10):
                nrows = len(sales_res.rows) if sales_res else 0
                reason = "no listing price" if listing_price is None else f"no sales comps returned (rows={nrows})"
                self.logger.debug(
                    f"""{hilight("[MySQL]", "info")} Vs Zillow: no comps -> {reason}"""
                )
            concise_parts.append("Vs Zillow: no comps.")
        if fb_res and fb_res.rows and listing_price is not None:
            price_col = self.config.price_column or "asking_price"
            prices = [float(r[price_col]) for r in fb_res.rows if r.get(price_col) is not None]
            if prices:
                avg_f = sum(prices) / len(prices)
                pct = ((listing_price - avg_f) / avg_f) * 100
                concise_parts.append(f"Vs Facebook: {abs(pct):.0f}% {'below' if pct < 0 else 'above'} average when compared to similar Facebook Marketplace listings.")
            else:
                concise_parts.append("Vs Facebook: no data.")
        elif self.config.comparison_table:
            concise_parts.append("Vs Facebook: no data.")
        concise_price_line = " ".join(concise_parts) if concise_parts else ""

        return ComparisonResult(
            summary="\n\n".join(combined_parts),
            rows=[],
            raw_text="",
            concise_price_line=concise_price_line,
            sales_scope=sales_scope,
            average_lot_rent_line=average_lot_rent_line,
        )

    def _run_custom_query(
        self,
        cursor: Any,
        listing: Listing,
        item_name: str,
    ) -> Optional[ComparisonResult]:
        # Replace placeholders for safe display; for real param binding use %s and pass args
        q = self.config.comparison_query
        # Simple placeholder replacement (user must use safe placeholders)
        title = (listing.title or "").replace("%", "%%")[:200]
        price = listing.price or ""
        location = (listing.location or "").replace("%", "%%")[:100]
        q = q.replace("{title}", title).replace("{price}", price).replace("{location}", location).replace("{item_name}", item_name or "")
        # Limit to max_rows via LIMIT if not present
        if "LIMIT" not in q.upper():
            q = q.rstrip(" ;") + f" LIMIT {self.config.max_rows}"
        cursor.execute(q)
        rows = cursor.fetchall()
        if isinstance(rows, list) and rows and not isinstance(rows[0], dict):
            # mysql.connector returns list of dicts with cursor(dictionary=True)
            cols = cursor.column_names if hasattr(cursor, "column_names") else []
            rows = [dict(zip(cols, r)) for r in rows]
        summary = self._rows_to_summary(rows)
        raw = "\n".join(str(r) for r in rows[: self.config.max_rows])
        return ComparisonResult(summary=summary, rows=rows, raw_text=raw)

    def _run_builtin_comparison(
        self,
        cursor: Any,
        listing: Listing,
        item_name: str,
    ) -> Optional[ComparisonResult]:
        table = self.config.comparison_table
        if not table or not re.match(r"^[a-zA-Z0-9_]+$", table):
            return None
        title_col = self.config.title_column
        price_col = self.config.price_column
        if not re.match(r"^[a-zA-Z0-9_]+$", title_col):
            title_col = "title"
        if price_col is not None and not re.match(r"^[a-zA-Z0-9_]+$", price_col):
            price_col = "price"
        price_val = _parse_price(listing.price)
        title_like = f"%{(listing.title or '')[:30]}%"
        if price_col and price_val is not None:
            cursor.execute(
                f'SELECT * FROM `{table}` WHERE `{title_col}` LIKE %s AND `{price_col}` <= %s ORDER BY `{price_col}` DESC LIMIT %s',
                (title_like, price_val * 1.5, self.config.max_rows),
            )
        else:
            cursor.execute(
                f'SELECT * FROM `{table}` WHERE `{title_col}` LIKE %s LIMIT %s',
                (title_like, self.config.max_rows),
            )
        rows = cursor.fetchall()
        if rows and not isinstance(rows[0], dict):
            cols = cursor.column_names if hasattr(cursor, "column_names") else []
            rows = [dict(zip(cols, r)) for r in rows]
        summary = self._rows_to_summary(rows)
        raw = "\n".join(str(r) for r in rows)
        return ComparisonResult(summary=summary, rows=rows, raw_text=raw)

    def _rows_to_summary(self, rows: List[Dict[str, Any]]) -> str:
        if not rows:
            return "No similar listings found in the database."
        lines = []
        for i, r in enumerate(rows[: self.config.max_rows], 1):
            parts = [f"{k}: {v}" for k, v in list(r.items())[:6]]
            lines.append(f"  {i}. " + " | ".join(str(p) for p in parts))
        return "Similar or related listings from your database:\n" + "\n".join(lines)

    def _parse_location_parts(self, location: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Parse listing.location into (city, state, zip) if possible."""
        loc = (location or "").strip()
        zip_match = re.search(r"\b(\d{5})(?:-\d{4})?\b", loc)
        zip_code = zip_match.group(1) if zip_match else None
        city, state = None, None
        parts = [p.strip() for p in re.split(r",|\s{2,}", loc) if p.strip()]
        if len(parts) >= 2 and len(parts[-1]) == 2 and parts[-1].isalpha():
            state = parts[-1]
            city = parts[0] if parts else None
        elif len(parts) == 1 and not zip_code:
            city = parts[0]
        return (city, state, zip_code)

    def _get_average_lot_rent(self, cursor: Any, listing: Listing) -> str:
        """Look up average lot rent by zip → county → region. Return e.g. 'Average lot rent (zip 16428): $400' or ''."""
        table = getattr(self.config, "lot_rent_table", None)
        if not table or not _safe_table(table):
            return ""
        zip_code, county_id, region_id = self._resolve_location(cursor, listing)
        val_col = getattr(self.config, "lot_rent_value_column", "avg_rent") or "avg_rent"
        if not _safe_table(val_col):
            val_col = "avg_rent"
        for scope, col, key in [
            ("zip", getattr(self.config, "lot_rent_zip_column", "zip"), zip_code),
            ("county", getattr(self.config, "lot_rent_county_column", "county_id"), county_id),
            ("region", getattr(self.config, "lot_rent_region_column", "region_id"), region_id),
        ]:
            if not col or not _safe_table(col) or key is None:
                continue
            try:
                cursor.execute(
                    f"SELECT `{val_col}` FROM `{table}` WHERE `{col}` = %s LIMIT 1",
                    (key,),
                )
                row = cursor.fetchone()
                if row:
                    val = row.get(val_col, row[0]) if isinstance(row, dict) else row[0]
                    if val is not None:
                        try:
                            num = float(val)
                            label = f"{scope} {key}" if scope != "zip" else f"zip {key}"
                            return f"Average lot rent ({label}): ${num:,.0f}"
                        except (TypeError, ValueError):
                            pass
            except Exception:
                pass
        return ""

    def insert_fb_listing(self, listing: Listing) -> bool:
        """Insert listing into fb_listings (external_id, title, description, asking_price, url, beds, baths, county_id, region_id when available). Optionally insert into fb_listing_history for price history."""
        if not self.config.enabled or not _safe_table(self.config.fb_listings_table):
            return False
        if not self.config.insert_into_fb:
            return False
        if self.logger:
            self.logger.info(
                f"""{hilight("[MySQL]", "info")} Inserting listing {hilight(listing.id)} into {self.config.fb_listings_table}..."""
            )
        try:
            client = self._get_client()
        except Exception as e:
            if self.logger:
                self.logger.warning(f"""{hilight("[MySQL]", "fail")} Insert failed (connection): {e}""")
            return False

        cursor = client.cursor(dictionary=True) if hasattr(client, "cursor") else client.cursor()
        table = self.config.fb_listings_table
        asking_price = _parse_price(listing.price)
        city, state, zip_code = self._parse_location_parts(listing.location or "")
        beds, baths, _ = _parse_beds_baths_year(listing)
        county_id, region_id = None, None
        try:
            zip_code_res, county_id, region_id = self._resolve_location(cursor, listing)
            if zip_code and not zip_code_res:
                zip_code_res = zip_code
            if zip_code_res:
                zip_code = zip_code_res
        except Exception:
            pass

        values = (
            listing.id,
            (listing.title or "")[:500],
            (listing.description or "")[:10000],
            asking_price,
            (city or "")[:200] or None,
            (state or "")[:10] or None,
            (zip_code or "")[:10] or None,
            (listing.post_url or "")[:2000] or None,
            beds,
            baths,
            county_id,
            region_id,
        )
        update_with_updated_at = (
            "title = VALUES(title), description = VALUES(description), "
            "asking_price = VALUES(asking_price), city = VALUES(city), state = VALUES(state), "
            "zip = VALUES(zip), url = VALUES(url), beds = VALUES(beds), baths = VALUES(baths), "
            "county_id = VALUES(county_id), region_id = VALUES(region_id), updated_at = NOW()"
        )
        update_without_updated_at = (
            "title = VALUES(title), description = VALUES(description), "
            "asking_price = VALUES(asking_price), city = VALUES(city), state = VALUES(state), "
            "zip = VALUES(zip), url = VALUES(url), beds = VALUES(beds), baths = VALUES(baths), "
            "county_id = VALUES(county_id), region_id = VALUES(region_id)"
        )
        insert_with_posted = (
            f"""INSERT INTO `{table}` (external_id, title, description, asking_price, city, state, zip, url, beds, baths, county_id, region_id, posted_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON DUPLICATE KEY UPDATE """
        )
        insert_without_posted = (
            f"""INSERT INTO `{table}` (external_id, title, description, asking_price, city, state, zip, url, beds, baths, county_id, region_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE """
        )
        try:
            last_err = None
            for insert_sql in [
                insert_with_posted + update_with_updated_at,
                insert_with_posted + update_without_updated_at,
                insert_without_posted + update_with_updated_at,
                insert_without_posted + update_without_updated_at,
            ]:
                try:
                    cursor.execute(insert_sql, values)
                    break
                except Exception as e:
                    last_err = e
                    err_msg = str(e).lower()
                    if "unknown column" not in err_msg and "posted_date" not in err_msg and "updated_at" not in err_msg:
                        raise
                    continue
            else:
                raise last_err
            client.commit()
            if self.logger:
                self.logger.info(f"""{hilight("[MySQL]", "succ")} Inserted/updated fb_listing {listing.id}.""")
            history_table = getattr(self.config, "fb_listing_history_table", None)
            if history_table and _safe_table(history_table) and asking_price is not None:
                try:
                    cursor.execute(
                        f"""INSERT INTO `{history_table}` (external_id, asking_price, recorded_at) VALUES (%s, %s, NOW())""",
                        (listing.id, asking_price),
                    )
                    client.commit()
                    if self.logger:
                        self.logger.info(f"""{hilight("[MySQL]", "succ")} Recorded price in {history_table}.""")
                except Exception as he:
                    if self.logger:
                        self.logger.warning(f"""{hilight("[MySQL]", "fail")} fb_listing_history insert failed: {he}""")
                    try:
                        client.rollback()
                    except Exception:
                        pass
            return True
        except Exception as e:
            if self.logger:
                self.logger.warning(
                    f"""{hilight("[MySQL]", "fail")} Insert fb_listing failed: {e} (table={table}, external_id={listing.id})"""
                )
            try:
                client.rollback()
            except Exception:
                pass
            return False
        finally:
            try:
                cursor.close()
            except Exception:
                pass
