"""
Ollama backend with extra steps: MySQL comparison and configurable output.

Use provider = "ollama_mysql" in your [ai.xxx] section to:
  - Query your local MySQL DB for comparison data (similar listings, price history)
  - Inject that context into the AI prompt so Ollama can consider it
  - Optionally customize the output (comment format, extra fields) for notifications
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from .ai import AIResponse, OllamaBackend, OllamaConfig
from .listing import Listing
from .marketplace import TItemConfig, TMarketplaceConfig
from .mysql_compare import MySQLCompare, MySQLConfig
from .utils import hilight


@dataclass
class OllamaMySQLConfig(OllamaConfig):
    """Ollama config with optional MySQL block (from [ai.xxx.mysql] in TOML)."""

    mysql: Optional[Dict[str, Any]] = None

    def handle_provider(self: "OllamaMySQLConfig") -> None:
        # Accept "ollama_mysql" so config loader does not require changing AIServiceProvider
        if self.provider and self.provider.lower() == "ollama_mysql":
            return
        super().handle_provider()

    def get_mysql_config(self: "OllamaMySQLConfig") -> Optional[MySQLConfig]:
        if not self.mysql or not isinstance(self.mysql, dict):
            return None
        m = self.mysql
        return MySQLConfig(
            host=m.get("host", "localhost"),
            port=int(m.get("port", 3306)),
            user=m.get("user", ""),
            password=m.get("password", ""),
            database=m.get("database", ""),
            enabled=m.get("enabled", True),
            comparison_query=m.get("comparison_query"),
            comparison_table=m.get("comparison_table"),
            title_column=m.get("title_column", "title"),
            price_column=m.get("price_column") if "price_column" in m else "price",
            max_rows=int(m.get("max_rows", 10)),
            output_format=m.get("output_format", "full"),
            use_sales_comps=m.get("use_sales_comps", False),
            sales_max_rows=int(m.get("sales_max_rows", 10)),
            sales_table=m.get("sales_table", "sales"),
            properties_table=m.get("properties_table", "properties"),
            zip_county_table=m.get("zip_county_table", "zip_county"),
            counties_table=m.get("counties_table", "counties"),
            year_tolerance=int(m.get("year_tolerance", 5)),
            insert_into_fb=m.get("insert_into_fb", False),
            fb_listings_table=m.get("fb_listings_table", "fb_listings"),
            insert_all_evaluated=m.get("insert_all_evaluated", False),
            connection_timeout=int(m.get("connection_timeout", 10)),
        )


class OllamaMySQLBackend(OllamaBackend):
    """
    Same as OllamaBackend but:
    1. Before calling the model, queries MySQL (if configured) and appends
       comparison data to the prompt.
    2. After evaluation, appends DB comparison summary to the AI comment
       so notifications show it (custom output).
    """

    @classmethod
    def get_config(cls: type["OllamaMySQLBackend"], **kwargs: Any) -> OllamaMySQLConfig:
        mysql = kwargs.pop("mysql", None)
        return OllamaMySQLConfig(mysql=mysql, **kwargs)

    def __init__(self: "OllamaMySQLBackend", config: OllamaMySQLConfig, logger: Any = None) -> None:
        super().__init__(config, logger)
        self._mysql: Optional[MySQLCompare] = None
        mysql_cfg = config.get_mysql_config() if hasattr(config, "get_mysql_config") else None
        if mysql_cfg and mysql_cfg.enabled and (
            mysql_cfg.use_sales_comps
            or mysql_cfg.comparison_query
            or mysql_cfg.comparison_table
            or mysql_cfg.insert_into_fb
        ):
            self._mysql = MySQLCompare(mysql_cfg, logger)

    def connect(self: "OllamaMySQLBackend") -> None:
        """Ensure a request timeout is set so Ollama calls don't hang forever."""
        if getattr(self.config, "timeout", None) is None:
            self.config.timeout = 120
        super().connect()

    def get_prompt(
        self: "OllamaMySQLBackend",
        listing: Listing,
        item_config: TItemConfig,
        marketplace_config: TMarketplaceConfig,
    ) -> str:
        prompt = super().get_prompt(listing, item_config, marketplace_config)
        if self._mysql is None:
            return prompt
        mysql_cfg = getattr(self.config, "get_mysql_config", lambda: None)() if hasattr(self.config, "get_mysql_config") else None
        comparison = self._mysql.fetch_comparison(listing, item_name=item_config.name)
        if comparison and comparison.summary:
            prompt += "\n\n--- Comparison data from your database (use this to compare prices/conditions): ---\n"
            prompt += comparison.summary
            prompt += "\n--- End of comparison data ---\n"
            if mysql_cfg and (mysql_cfg.use_sales_comps or mysql_cfg.comparison_table):
                prompt += (
                    "\nIn your brief recommendation (after 'Rating <1-5>:'), state clearly: "
                    "(1) Versus Zillow sold comps: is this a good deal, fair, or overpriced? One short sentence. "
                    "(2) Versus other Facebook listings: good deal, fair, or overpriced? One short sentence. "
                    "Keep the total summary under 60 words."
                )
        return prompt

    def evaluate(
        self: "OllamaMySQLBackend",
        listing: Listing,
        item_config: TItemConfig,
        marketplace_config: TMarketplaceConfig,
    ) -> AIResponse:
        if self.logger:
            self.logger.info(
                f"""{hilight("[AI]", "info")} Evaluating listing {hilight((listing.title or "")[:50])} (Ollama may take 30–120s)..."""
            )
        response = super().evaluate(listing, item_config, marketplace_config)
        if self._mysql is None:
            return response
        mysql_cfg = getattr(self.config, "get_mysql_config", lambda: None)() if hasattr(self.config, "get_mysql_config") else None
        if mysql_cfg and mysql_cfg.insert_into_fb and mysql_cfg.insert_all_evaluated:
            self._mysql.insert_fb_listing(listing)
        comparison = self._mysql.fetch_comparison(listing, item_name=item_config.name)
        if response.comment != AIResponse.NOT_EVALUATED:
            comment = response.comment
            if comparison and comparison.summary:
                out_fmt = (mysql_cfg and getattr(mysql_cfg, "output_format", None)) or "full"
                if out_fmt != "none":
                    db_text = comparison.summary.replace("\n", " ").strip()
                    if out_fmt == "short":
                        db_text = db_text[:120] + ("..." if len(db_text) > 120 else "")
                    comment = comment + " | DB: " + db_text
            if listing.post_url:
                comment = comment + "\nListing URL: " + listing.post_url
            if comment != response.comment:
                response = AIResponse(name=response.name, score=response.score, comment=comment)
        return response

    def on_listing_accepted(
        self: "OllamaMySQLBackend",
        listing: Listing,
        item_config: Any,
        marketplace_config: Any,
    ) -> None:
        """Called by the monitor when a listing passes the rating threshold. Inserts into fb_listings if configured."""
        if self._mysql is None:
            return
        self._mysql.insert_fb_listing(listing)

    def __del__(self: "OllamaMySQLBackend") -> None:
        if getattr(self, "_mysql", None) is not None:
            self._mysql.close()
