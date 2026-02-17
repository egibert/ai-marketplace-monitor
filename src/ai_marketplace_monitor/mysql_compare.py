"""
MySQL comparison module for ai-marketplace-monitor.

When using Ollama locally, this module can query your MySQL database to fetch
comparison data (Zillow sales comps by zip→county→region, similar fb_listings)
and inject it into the AI prompt. It can also insert accepted listings into
fb_listings.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from logging import Logger
from typing import Any, Dict, List, Optional, Tuple

from .listing import Listing
from .utils import hilight


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
    insert_into_fb: bool = False
    fb_listings_table: str = "fb_listings"
    # When true, insert every evaluated listing (not only accepted). Use to populate fb_listings.
    insert_all_evaluated: bool = False

    # Connection timeout (seconds); avoid hanging if MySQL is unreachable
    connection_timeout: int = 10

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


def _parse_price(price_str: str) -> Optional[float]:
    """Extract numeric price from listing price string (e.g. '$180' or '€ 200')."""
    if not price_str or price_str == "**unspecified**":
        return None
    cleaned = re.sub(r"[^\d.,]", "", price_str.replace(",", ""))
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
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

    def _resolve_location(self, cursor: Any, listing: Listing) -> Tuple[Optional[str], Optional[int], Optional[int]]:
        """Resolve listing.location to (zip, county_id, region_id) using zip_county and counties."""
        loc = (listing.location or "").strip()
        zip_match = re.search(r"\b(\d{5})(?:-\d{4})?\b", loc)
        zip_code = zip_match.group(1) if zip_match else None
        if self.logger:
            self.logger.info(
                f"""{hilight("[MySQL-debug]", "info")} Location: listing.location={hilight(repr(loc)[:80])} -> zip={hilight(str(zip_code))}"""
            )
        if not zip_code:
            return (None, None, None)
        if not _safe_table(self.config.zip_county_table) or not _safe_table(self.config.counties_table):
            if self.logger:
                self.logger.info(f"""{hilight("[MySQL-debug]", "info")} No zip_county/counties table config, skipping county/region lookup.""")
            return (zip_code, None, None)
        try:
            cursor.execute(
                f"SELECT county_id FROM `{self.config.zip_county_table}` WHERE zip = %s LIMIT 1",
                (zip_code,),
            )
            row = cursor.fetchone()
            county_id = int(row["county_id"]) if row and isinstance(row, dict) else (int(row[0]) if row else None)
        except Exception as e:
            county_id = None
            if self.logger:
                self.logger.info(f"""{hilight("[MySQL-debug]", "fail")} zip_county lookup for zip={zip_code} failed: {e}""")
        if county_id is None:
            if self.logger:
                self.logger.info(f"""{hilight("[MySQL-debug]", "info")} Zip {zip_code} not in {self.config.zip_county_table} or query returned no row.""")
            return (zip_code, None, None)
        try:
            cursor.execute(
                f"SELECT region_id FROM `{self.config.counties_table}` WHERE id = %s LIMIT 1",
                (county_id,),
            )
            row = cursor.fetchone()
            region_id = int(row["region_id"]) if row and isinstance(row, dict) else (int(row[0]) if row else None)
        except Exception as e:
            region_id = None
            if self.logger:
                self.logger.info(f"""{hilight("[MySQL-debug]", "fail")} counties lookup for county_id={county_id} failed: {e}""")
        if self.logger:
            self.logger.info(
                f"""{hilight("[MySQL-debug]", "succ")} Resolved: zip={zip_code} county_id={county_id} region_id={region_id}"""
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
        if self.logger:
            self.logger.info(
                f"""{hilight("[MySQL-debug]", "info")} Sales comps: parsed beds={beds} baths={baths} year_built={year_built} """
                f"""| location zip={zip_code} county_id={county_id} region_id={region_id}"""
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

        if not tries and self.logger:
            self.logger.info(
                f"""{hilight("[MySQL-debug]", "fail")} Sales comps: no zip/county/region to query (listing.location may have no 5-digit zip)."""
            )

        for scope, scope_where, scope_params in tries:
            q = (
                f"SELECT s.sale_price, s.sale_date, p.beds, p.baths, p.square_feet, p.year_built, p.city, p.state, p.zip "
                f"FROM `{s_t}` s JOIN `{p_t}` p ON s.property_id = p.id "
                f"WHERE {scope_where}{where_extra} ORDER BY s.sale_date DESC LIMIT %s"
            )
            cursor.execute(q, scope_params + params + [limit])
            rows = cursor.fetchall()
            if rows and not isinstance(rows[0], dict):
                cols = cursor.column_names if hasattr(cursor, "column_names") else []
                rows = [dict(zip(cols, r)) for r in rows]
            if self.logger:
                self.logger.info(
                    f"""{hilight("[MySQL-debug]", "info")} Sales comps: scope={scope} (params={scope_params + params}) -> {len(rows)} rows"""
                )
            if rows:
                summary = f"Recent sold comps ({scope}):\n" + self._rows_to_summary(rows)
                return ComparisonResult(summary=summary, rows=rows, raw_text="\n".join(str(r) for r in rows))

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
                res = self._run_builtin_comparison(cursor, listing, item_name)
                if res:
                    combined_parts.append(res.summary)
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
        return ComparisonResult(
            summary="\n\n".join(combined_parts),
            rows=[],
            raw_text="",
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
        if self.logger:
            self.logger.info(
                f"""{hilight("[MySQL-debug]", "info")} FB comparison: table={table} title_col={title_col} price_col={price_col} """
                f"""| title_like={hilight(repr(title_like)[:60])} price_val={price_val}"""
            )
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
        if self.logger:
            self.logger.info(
                f"""{hilight("[MySQL-debug]", "info")} FB comparison: table {table} -> {len(rows)} rows"""
            )
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

    def insert_fb_listing(self, listing: Listing) -> bool:
        """Insert listing into fb_listings (external_id, title, description, asking_price, url, beds, baths, county_id, region_id when available)."""
        if not self.config.enabled or not self.config.insert_into_fb or not _safe_table(self.config.fb_listings_table):
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

        try:
            cursor.execute(
                f"""INSERT INTO `{table}` (external_id, title, description, asking_price, city, state, zip, url, beds, baths, county_id, region_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE title = VALUES(title), description = VALUES(description),
                    asking_price = VALUES(asking_price), city = VALUES(city), state = VALUES(state),
                    zip = VALUES(zip), url = VALUES(url), beds = VALUES(beds), baths = VALUES(baths),
                    county_id = VALUES(county_id), region_id = VALUES(region_id), updated_at = NOW()""",
                (
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
                ),
            )
            client.commit()
            if self.logger:
                self.logger.info(f"""{hilight("[MySQL]", "succ")} Inserted/updated fb_listing {listing.id}.""")
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
