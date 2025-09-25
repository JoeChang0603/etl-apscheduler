"""Job that snapshots per-portfolio account summaries every minute."""

import asyncio
from datetime import datetime
from typing import Any, Dict

from configs.env_config import Env
from src.bot.discord import DiscordAlerter
from src.mongo.base import MongoClient
from src.snapshot.factory import SnapshotFactory
from src.utils.logger.config import LogLevel
from src.utils.logger.logger import Logger
from src.utils.logger_factory import log_exception
from src.utils.model_parser import model_parser


interval = 0.5 # minute
mongo = MongoClient()
portfolio_col = mongo.DATA_DB.portfolio
accouont_summary_col = mongo.DATA_DB.account_summary_1_minute


async def run(logger: Logger) -> None:
    """Snapshot account summaries for active portfolios and persist results.

    :param logger: Logger instance for diagnostics and error reporting.
    """

    ALERT_BOT = DiscordAlerter(
        webhook_url=Env.ETL_TOTAL_USD_VALUE_ALERT,
        username="ETL Alerts",
        format_as_code=True, code_lang="text"
    )
    await ALERT_BOT.start()
    
    portfolios =  await portfolio_col.find({"status": "active"}).to_list()
    current_time = datetime.utcnow().replace(microsecond=0)
    
    tasks = [asyncio.create_task(_account_summary_processing(p, current_time, logger, ALERT_BOT)) for p in portfolios]
    await asyncio.gather(*tasks, return_exceptions=True)
    await ALERT_BOT.flush(timeout=3.0) 
    await ALERT_BOT.shutdown()

#################### Private Function ############################

async def _account_summary_processing(portfolio: Dict[str, Any], current_time: datetime, logger: Logger, alert_bot: DiscordAlerter):
    """Fetch snapshot data for a single portfolio and persist it.

    :param portfolio: Portfolio document containing credentials and thresholds.
    :param current_time: Timestamp applied to the snapshot document.
    :param logger: Logger used for diagnostics.
    :param alert_bot: Discord alerter for threshold breaches.
    """
    portfolio_name = portfolio.get("portfolio")
    try:
        logger.info(f"portfolio docs: {portfolio_name}")

        obj = SnapshotFactory(portfolio, current_time, interval, logger).get_handler()
        data = await obj.snapshot_account_summary()

        total_usd_value = float(data.total_usd_value)
        threshold = portfolio.get("position_threshold")

        if threshold and total_usd_value <= threshold:
            await alert_bot.trigger(
                key=f"usd_under:{portfolio_name}", 
                    message=(
                    f"Total_Usd_Value Under {threshold}\n"
                    f"💼 Portfolio     : {portfolio_name}\n"
                    f"💰 Current Value : {total_usd_value:,.2f}\n"
                    f"🕒 Time(UTC)     : {current_time}"
                ),
                severity=LogLevel.CRITICAL,
            )
        
        key = {"portfolio" : portfolio_name, "current_time": current_time}
        update = {
            "$set": model_parser(data)  
        }
        accouont_summary_col.update_one(key, update, upsert= True)

        logger.info(f"portfolio docs: {data}")
    except Exception as e:
        log_exception(logger,e, context=portfolio_name)
