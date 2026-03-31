from __future__ import annotations
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    kalshi_api_key_id: str = ""
    kalshi_private_key_path: str = "./kalshi_private_key.pem"
    use_demo_api: bool = False          # ← PRODUCTION by default now

    paper_trading: bool = True          # still safe until you explicitly go live
    live_trading_confirmed: str = ""
    live_trading_amount_confirmed: str = ""

    anthropic_api_key: str = ""
    claude_model: str = "claude-sonnet-4-20250514"

    tavily_api_key: str = ""

    # Risk parameters
    min_confidence: float = 0.60        # slightly tighter than before
    min_edge_pct: float = 0.05
    max_position_pct: float = 0.02      # 2% of portfolio per market
    daily_loss_limit_usd: float = 50.0
    min_trade_usd: float = 2.0
    max_trade_usd: float = 100.0

    scan_interval: int = 60             # seconds between scans
    top_markets_count: int = 20

    # Crypto + macro news feeds for better signal on our target markets
    rss_feeds: str = (
        "https://feeds.feedburner.com/CoinDesk,"                        # crypto
        "https://cointelegraph.com/rss,"                                # crypto
        "https://decrypt.co/feed,"                                      # crypto
        "https://feeds.npr.org/1001/rss.xml,"                          # general
        "https://rss.nytimes.com/services/xml/rss/nyt/Economy.xml"     # macro/Fed
    )

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
