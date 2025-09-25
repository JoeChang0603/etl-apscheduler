"""Flexible MongoDB data loader supporting insert, update, and merge operations."""

from __future__ import annotations

import polars as pl
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple
from pymongo import ReplaceOne, UpdateOne
from datetime import datetime, timedelta

from src.mongo.base import MongoClient
#from src.utils.logger.logger import Logger
#from src.utils.logger_factory import log_exception


mongo = MongoClient()
portfolio_col = mongo.DATA_DB.portfolio
account_summary_col = mongo.DATA_DB.account_summary_1_minute
master_portfolio_performance_col = mongo.MART_DB.master_portfolio_performance

async def run(
    *,
    is_master: bool = False,
    source_is_test: bool = True,
    target_is_test: bool = False,
    start_time: datetime,
    end_time: datetime,
    #logger: Logger,
) -> None:
    
    mongo = MongoClient(source_is_test)
    portfolio_col = mongo.DATA_DB.portfolio
    account_summary_col = mongo.DATA_DB.account_summary_1_minute
    if is_master:
        performance_col = mongo.MART_DB.master_portfolio_performance
    else:
        performance_col = mongo.MART_DB.portfolio_performance

    ops: Dict[str, datetime] = {}
    if start_time is not None:
        ops["$gte"] = start_time
    if end_time is not None:
        ops["$lte"] = end_time
    query = {
        "current_time": ops
    }
    hist_df = await _fetch_hist_account_summary(query, account_summary_col, portfolio_col, is_master)
    lastest_data = await _get_lastest_performance(start_time, performance_col, "master_portfolio" if is_master else "portfolio")
    
    processed_batches = []     # 儲存每分鐘回傳的 processed_df

    number = 2
    for ts in (
        hist_df.select("current_time")
                .unique()
                .sort("current_time")["current_time"]      # 變成 Series
    ):
        # 2‑1. 擷取該分鐘所有列
        minute_df = hist_df.filter(pl.col("current_time") == ts)
        #print(minute_df)

        # 2‑2. 丟進先前寫好的函式
        processed_df, lastest_data = _process_batch(minute_df, lastest_data, "master_portfolio" if is_master else "portfolio")

        # 2‑3. 收集結果
        processed_batches.append(processed_df)
        # if number == 3:
        #     break
        # number+=1
    # === 3. 合併所有分鐘，得到完整結果 ===============
    full_processed_df = pl.concat(processed_batches, how="vertical")
    print(full_processed_df)

    full_processed_df = full_processed_df.with_columns(
        pl.col("current_time").cast(pl.Datetime("ms"))          # 轉成毫秒，to_dicts() → python datetime.datetime
    )

    mongo = MongoClient(target_is_test)
    master_portfolio_performance_col = mongo.MART_DB.master_portfolio_performance
    print(master_portfolio_performance_col)
    master_portfolio_performance_col.insert_many(
        full_processed_df.to_dicts(),
        ordered=False,                     # 允許並行
        bypass_document_validation=True,   # 若集合有 schema validation，可略過
        comment="daily_performance_import" # 方便 profiler / log 檢索
    )

#################### Private Function ############################

async def _fetch_hist_account_summary(query, account_summary_col, portfolio_col, is_master):

    # Fetch source data
    projection = {"_id": 0}
    account_task = account_summary_col.find(query, projection).to_list(length=None)
    portfolio_task = portfolio_col.find({}, projection).to_list(length=None)

    docs_account, docs_portfolio = await asyncio.gather(account_task, portfolio_task)
    
    account_summary_df = pl.from_dicts(docs_account) if docs_account else pl.DataFrame()
    portfolio_df = pl.from_dicts(docs_portfolio) if docs_portfolio else pl.DataFrame()

    base = (
        portfolio_df.join(account_summary_df, on="portfolio", how="inner")
        .select(
            "portfolio", "master_portfolio", "composite", "current_time", "tw_time",
            "total_usd_value", "transfer_adjustment",
        )
    )

    if is_master:
        # Integrate Composite
        map_df = _integrate_composite(base)

        # Aggregation
        aug = pl.concat([base, map_df], how="vertical")

        return (
            aug
            .with_columns([
                pl.col("total_usd_value").round(10),
                pl.col("transfer_adjustment").round(10),
            ])
            .group_by(["master_portfolio", "current_time", "tw_time"])
            .agg([
                pl.sum("total_usd_value").alias("total_usd_value"),
                pl.sum("transfer_adjustment").alias("transfer_adjustment"),
            ])
            .filter(~((pl.col("total_usd_value") == 0) & (pl.col("transfer_adjustment") == 0)))
        )
    else:
        return (
            base.select(
                "portfolio", "current_time", "tw_time",
                "total_usd_value", "transfer_adjustment",
            )
            .with_columns([
                pl.col("total_usd_value").round(10),
                pl.col("transfer_adjustment").round(10),
            ])
            .filter(~((pl.col("total_usd_value") == 0) & (pl.col("transfer_adjustment") == 0)))
        )


async def _get_lastest_performance(start_time, performance_col, pk):
    latest = await performance_col.find_one(
        {"current_time": {"$lt": start_time}},
        {"_id": 0, "current_time": 1},
        sort=[("current_time", -1)],
    )

    latest_doc = await (
        performance_col
        .find({"current_time": latest['current_time']}, {"_id": 0})
        .to_list(length=None)
    )
    latest_stats = pl.from_dicts(latest_doc)
    latest_stats = latest_stats.rename({
        "history_high": "latest_history_high",
        "nav"         : "latest_nav",
        "total_usd_value": "latest_total_usd_value",
        "crr"         : "latest_crr",
        "mdd"         : "latest_mdd",
    })

    cols_keep = [
        pk,
        "latest_history_high",
        "latest_nav",
        "latest_total_usd_value",
        "latest_crr",
        "latest_mdd",
    ]
    return latest_stats.select(cols_keep)

def _process_batch(src_df: pl.DataFrame, latest_stats: pl.DataFrame, pk: str):
        # ── 1. net_value ───────────────────────────
        cur = src_df.with_columns(
            (pl.col("total_usd_value") + pl.col("transfer_adjustment")).alias("net_value")
        )   

        # Join & init new row data
        cur = cur.join(latest_stats, on=pk, how="left")
        cur = cur.with_columns([
            pl.col("latest_nav").fill_null(100),
            pl.col("latest_history_high").fill_null(100),
            pl.col("latest_total_usd_value").fill_null(pl.col("net_value")),
            pl.col("latest_crr").fill_null(0),
            pl.col("latest_mdd").fill_null(0),
        ])

        cur = cur.with_columns([
            pl.when(pl.col("latest_total_usd_value") != 0)
            .then((pl.col("net_value") - pl.col("latest_total_usd_value"))
                    / pl.col("latest_total_usd_value"))
            .otherwise(0)
            .alias("current_return")
        ])

        # ── 2. crr & nav ───────────────────────────
        cur = cur.with_columns([
            ((1 + pl.col("latest_crr")) * (1 + pl.col("current_return")) - 1).alias("crr"),
            (pl.col("latest_nav") * (1 + pl.col("current_return"))).alias("nav"),
        ])

        # ── 3. history_high ───────────────────────────
        cur = cur.with_columns(
            pl.max_horizontal("nav", "latest_history_high").alias("history_high")
        )

        # ── 4. cd / mdd ──────────────────────────────
        cur = cur.with_columns(
            pl.when(pl.col("history_high") != 0)
            .then(pl.col("nav") / pl.col("history_high") - 1)
            .otherwise(0)
            .alias("cd")
        )

        cur = cur.with_columns(
            pl.min_horizontal("cd", "latest_mdd").alias("mdd")
        )

        latest_stats = cur.select(
            pk,
            pl.col("history_high").alias("latest_history_high"),
            pl.col("nav").alias("latest_nav"),
            pl.col("total_usd_value").alias("latest_total_usd_value"),
            pl.col("crr").alias("latest_crr"),
            pl.col("mdd").alias("latest_mdd"),
        )
        
        processed_df = cur.select(
            pk,
            "total_usd_value",
            "history_high",
            "nav",
            "current_return",
            "crr",
            "cd",
            "mdd",
            "current_time",
            "tw_time",
        )

        return processed_df, latest_stats


def _integrate_composite(df: pl.DataFrame):
    pairs = (
        df
        .filter(pl.col("composite").is_not_null() & (pl.col("composite") != ""))
        .select(["composite", "portfolio"])
        .unique()
    )
    grouped = pairs.group_by("composite").agg(pl.col("portfolio").unique())
    aggr_list = dict(zip(grouped["composite"], grouped["portfolio"]))


    map_rows = [(group, p) for group, lst in aggr_list.items() for p in lst]
    map_df = pl.DataFrame(map_rows, schema=["new_master", "portfolio"], orient="row")
    return (
            df.join(map_df, on="portfolio", how="inner")
                .with_columns([
                    pl.col("new_master").alias("master_portfolio"),
                    pl.col("new_master").alias("portfolio"),  
                ])
                .drop("new_master")
        )


if __name__ == "__main__":
    import asyncio
    asyncio.run(
        run(
            source_is_test=False,
            target_is_test=True,
            is_master=False,
            start_time=datetime(2025, 9, 16, 00, 33, 0),
            end_time=datetime(2025, 9, 24, 22, 59, 0),
        )
    )
