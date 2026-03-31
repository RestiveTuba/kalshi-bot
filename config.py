from __future__ import annotations
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    # Kalshi credentials
    kalshi_api_key_id: str = ""
    kalshi_private_key_path: str = "./kalshi_private_key.pem"
    use_demo_api: bool = True

    # Safety flags — all three required to go live
    paper_trading: bool = True
    live_trading_confirmed: str = ""
    live_trading_amount_confirmed: str = ""

    # Anthropic
    anthropic_api_key: str = ""
    claude_model: str = "claude-sonnet-4-20250514"

    # Optional news enrichment
    tavily_api_key: str = ""

    # Risk parameters
    min_confidence: float = 0.55
    min_edge_pct: float = 0.05
    max_position_pct: float = 0.02
    daily_loss_limit_usd: float = 50.0
    min_trade_usd: float = 2.0
    max_trade_usd: float = 100.0

    # Scheduler
    scan_interval: int = 60  # seconds
    top_markets_count: int = 20

    # RSS feeds (comma-separated)
    rss_feeds: str = (
        "https://feeds.npr.org/1001/rss.xml,"
        "https://rss.nytimes.com/services/xml/rss/nyt/Politics.xml"
    )

    # Database
    db_path: str = "./kalshi_bot.db"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    @property
    def base_url(self) -> str:
        if self.use_demo_api:
            return "https://demo-api.kalshi.co/trade-api/v2/"
        return "https://trading-api.kalshi.com/trade-api/v2/"

    @property
    def ws_url(self) -> str:
        if self.use_demo_api:
            return "wss://demo-api.kalshi.co/trade-api/ws/v2"
        return "wss://trading-api.kalshi.com/trade-api/ws/v2"

    @property
    def is_live(self) -> bool:
        return (
            not self.paper_trading
            and self.live_trading_confirmed == "yes"
            and self.live_trading_amount_confirmed == "yes"
        )

    @property
    def rss_feed_list(self) -> list[str]:
        return [f.strip() for f in self.rss_feeds.split(",") if f.strip()]


settings = Settings()
