from datetime import datetime, timedelta
from typing import Any
from pymongo import UpdateOne

from src.utils.logger.logger import Logger
from src.utils.logger_factory import log_exception
from src.mongo.base import MongoClient
from utils.misc import datetime_to_str
from utils.bson_utils import bsonify_row

import pandas as pd


mongo = MongoClient(is_test=True)
portfolio_col = mongo.DATA_DB.portfolio
account_summary_col = mongo.DATA_DB.account_summary_1_minute
portfolio_performance_col = mongo.MART_DB.portfolio_performance

async def run(logger: Logger):
    current_time = datetime.utcnow().replace(microsecond=0)

    try:
        result = await _master_portfolio_aggregate(current_time)
        logger.info(f"Portfolio Aggregation: {result.to_string()}")

        latest_performance = await _get_latest_portfolio_performance()
        data = await _vectorized_process(result, latest_performance, current_time)

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


async def _master_portfolio_aggregate(current_time: datetime):
    query = {
        "frequency": "1m",
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
    current_time: datetime
) -> pd.DataFrame:
    """
    透過 vectorized 操作，計算各 portfolio 的績效指標（NAV、CRR、MDD...）

    :param latest_account_summary: 由 account_summary 聚合出的最新資料（含 total_usd_value、transfer_adjustment 等欄位）
    :param latest_perf_dict:       每個 portfolio 對應的最新績效紀錄（由 get_latest_portfolio_performance 回傳）
    :param current_time:           當下統一處理時間戳記（UTC）

    :return: DataFrame，欄位包含：
        - portfolio
        - total_usd_value
        - history_high
        - nav
        - current_return
        - crr
        - cd
        - mdd
        - current_time
        - tw_time
    """
    
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