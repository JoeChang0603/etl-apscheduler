"""Job that aggregates portfolio performance metrics and persists them."""

from datetime import datetime, timedelta
from typing import Any

import pandas as pd
from pymongo import UpdateOne

from src.mongo.base import MongoClient
from src.utils.logger.logger import Logger
from src.utils.logger_factory import log_exception
from utils.bson_utils import bsonify_row
from utils.misc import datetime_to_str


mongo = MongoClient()
portfolio_col = mongo.DATA_DB.portfolio
account_summary_col = mongo.DATA_DB.account_summary_1_minute
portfolio_performance_col = mongo.MART_DB.portfolio_performance

async def run(logger: Logger) -> None:
    """Aggregate latest portfolio metrics and upsert performance documents.

    :param logger: Logger instance for diagnostics and error reporting.
    """
    current_time = datetime.utcnow().replace(microsecond=0)

    try:
        result = await _master_portfolio_aggregate(current_time)
        logger.info(f"Portfolio Aggregation: {result.to_string()}")

        latest_performance = await _get_latest_portfolio_performance()
        data = await _vectorized_process(result, latest_performance, current_time, logger)

        ops = []
        for rec in data.to_dict("records"):
            logger.info(rec)
            doc = bsonify_row(rec)
            key = {"portfolio": doc["portfolio"], "current_time": doc["current_time"]}
            ops.append(UpdateOne(key, {"$set": doc}, upsert=True))

        if ops:
            await portfolio_performance_col.bulk_write(ops, ordered=False)


    except Exception as e:
        log_exception(logger, e)


#################### Private Function ############################


async def _master_portfolio_aggregate(current_time: datetime) -> pd.DataFrame:
    """Return the latest account summary snapshot joined with portfolio info.

    :param current_time: Timestamp applied to the aggregated records.
    :return: DataFrame containing per-portfolio totals and adjustments.
    """
    query = {
        "status": "active"
    }
    portfolio_df =  pd.DataFrame(await portfolio_col.find(query).to_list())

    pipeline = [
            {"$sort": {"portfolio": 1, "current_time": -1}},  # 先依 portfolio 排，再按 current_time 降序
            {
                "$group": {
                    "_id": "$portfolio",
                    "doc": {"$first": "$$ROOT"}  # 每組 portfolio 保留 current_time 最大的那筆
                }
            },
            {"$replaceRoot": {"newRoot": "$doc"}}
    ]
    account_summary_df = pd.DataFrame(await account_summary_col.aggregate(pipeline).to_list())
    

    # Portfolio Aggregation
    df = pd.merge(portfolio_df, account_summary_df, on="portfolio", how="inner").loc[
        :,
        [
            "portfolio",
            "current_time",
            "tw_time",
            "total_usd_value",
            "transfer_adjustment",
        ],
    ]
    num_cols = ["total_usd_value", "transfer_adjustment"]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce").round(10)
    df['current_time'] = current_time
    df['tw_time'] = datetime_to_str(current_time - timedelta(hours=8))

    return df

async def _get_latest_portfolio_performance() -> dict[str, dict[str, Any]]:
    """Fetch the most recent performance document for each portfolio.

    :return: Mapping of portfolio ids to their latest performance record.
    """
    pipeline = [
        {"$sort": {"portfolio": 1, "current_time": -1}},  # 利用複合索引
        {"$group": {"_id": "$portfolio", "doc": {"$first": "$$ROOT"}}},
        {"$replaceRoot": {"newRoot": "$doc"}},
        {"$project": {"_id": 0}}
    ]
    cursor = portfolio_performance_col.aggregate(pipeline)

    return {doc["portfolio"]: doc async for doc in cursor}

async def _vectorized_process(
    latest_account_summary: pd.DataFrame,
    latest_perf_dict: dict,
    current_time: datetime,
    logger: Logger
) -> pd.DataFrame:
    """Compute vectorised portfolio metrics using the latest account summary.

    :param latest_account_summary: DataFrame from account summary aggregation.
    :param latest_perf_dict: Latest performance document per portfolio.
    :param current_time: UTC timestamp applied to the generated records.
    :param logger: Logger used for exception reporting.
    :return: DataFrame with updated performance metrics ready for persistence.
    """
    try:
        # 將 dict 轉成 DataFrame
        latest_perf_df = pd.DataFrame(latest_perf_dict.values())

        if latest_perf_df.empty:
            portfolio = latest_account_summary["portfolio"].values[0]
            total_usd_value = latest_account_summary["total_usd_value"].values[0]
            transfer_adjustment = latest_account_summary["transfer_adjustment"].values[0]
            net_value = total_usd_value + transfer_adjustment

            # 建立單筆 DataFrame（可與後續邏輯保持一致）
            latest_perf_df = pd.DataFrame([{
                "portfolio" : portfolio,
                "history_high": 0,
                "nav": 100,
                "total_usd_value": net_value,
                "crr": 0,
                "mdd": 0
            }])

        # 合併資料
        df = pd.merge(latest_account_summary, latest_perf_df, on="portfolio", how="left", suffixes=("", "_prev"))

        # 處理 fallback 值（若沒有歷史紀錄）
        df["history_high"] = df["history_high"].fillna(0)
        df["nav"] = df["nav"].fillna(100)
        df["total_usd_value_prev"] = df["total_usd_value_prev"].fillna(df["total_usd_value"])
        df["crr"] = df["crr"].fillna(0)
        df["mdd"] = df["mdd"].fillna(0)

        # 計算
        df["net_value"] = df["total_usd_value"] + df["transfer_adjustment"]
        df["current_return"] = (df["net_value"] - df["total_usd_value_prev"]) / df["total_usd_value_prev"]
        df["crr_new"] = (1 + df["crr"]) * (1 + df["current_return"]) - 1
        df["nav_new"] = df["nav"] * (1 + df["current_return"])
        df["history_high_new"] = df[["nav_new", "history_high"]].max(axis=1)
        df["cd"] = df["nav_new"] / df["history_high_new"] - 1
        df["mdd_new"] = df[["cd", "mdd"]].min(axis=1)

        # 加入時間欄位
        df["current_time"] = current_time
        df["tw_time"] = (current_time + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")

        # 選擇要儲存的欄位
        return df[
            [
                "portfolio",
                "total_usd_value",
                "history_high_new",
                "nav_new",
                "current_return",
                "crr_new",
                "cd",
                "mdd_new",
                "current_time",
                "tw_time",
            ]
        ].rename(
            columns={
                "history_high_new": "history_high",
                "nav_new": "nav",
                "crr_new": "crr",
                "mdd_new": "mdd",
            }
        )
    except Exception as e:
        log_exception(logger, e, context=latest_account_summary['portfolio'])
