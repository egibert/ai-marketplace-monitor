"""
MySQL comparison module for ai-marketplace-monitor.

When using Ollama locally, this module can query your MySQL database to fetch
comparison data (e.g. similar listings, price history) and inject it into
the AI prompt or response for better evaluation and custom output.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from logging import Logger
from typing import Any, Dict, List, Optional

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
    # Example: "SELECT * FROM listings WHERE title LIKE %s AND price <= %s"
    comparison_query: Optional[str] = None
    # Or use built-in: fetch similar by title/price (requires table name)
    comparison_table: Optional[str] = None  # e.g. "marketplace_listings"

    # Max rows to return and include in prompt
    max_rows: int = 10
    # How to include DB in notification: "full" (summary), "short" (one line), "none"
    output_format: str = "full"

    def connection_dict(self) -> Dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database,
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
    # Remove currency symbols and spaces, keep digits and one dot/comma
    cleaned = re.sub(r"[^\d.,]", "", price_str.replace(",", ""))
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


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
        try:
            import mysql.connector  # type: ignore

            self._client = mysql.connector.connect(**self.config.connection_dict())
            if self.logger:
                self.logger.debug(
                    f"""{hilight("[MySQL]", "info")} Connected to {self.config.host}:{self.config.database}"""
                )
            return self._client
        except ImportError:
            try:
                import pymysql  # type: ignore

                self._client = pymysql.connect(
                    **self.config.connection_dict(),
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

    def fetch_comparison(
        self,
        listing: Listing,
        item_name: str = "",
    ) -> Optional[ComparisonResult]:
        """
        Query MySQL for data to compare with this listing.
        Returns a summary string and optional rows for the AI prompt.
        """
        if not self.config.enabled or (not self.config.comparison_query and not self.config.comparison_table):
            return None
        try:
            client = self._get_client()
        except Exception as e:
            if self.logger:
                self.logger.warning(f"""{hilight("[MySQL]", "fail")} Connection failed: {e}""")
            return None

        cursor = client.cursor(dictionary=True) if hasattr(client, "cursor") else client.cursor()

        try:
            if self.config.comparison_query:
                return self._run_custom_query(cursor, listing, item_name)
            if self.config.comparison_table:
                return self._run_builtin_comparison(cursor, listing, item_name)
        except Exception as e:
            if self.logger:
                self.logger.warning(f"""{hilight("[MySQL]", "fail")} Query failed: {e}""")
            return None
        finally:
            try:
                cursor.close()
            except Exception:
                pass

        return None

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
        price_val = _parse_price(listing.price)
        title_like = f"%{(listing.title or '')[:30]}%"
        if price_val is not None:
            cursor.execute(
                f'SELECT * FROM `{table}` WHERE title LIKE %s AND price <= %s ORDER BY price DESC LIMIT %s',
                (title_like, price_val * 1.5, self.config.max_rows),
            )
        else:
            cursor.execute(
                f'SELECT * FROM `{table}` WHERE title LIKE %s LIMIT %s',
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
