"""Job for deleting duplicate documents based on grouping fields."""

from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from src.mongo.base import MongoClient
from src.utils.logger.logger import Logger
from src.utils.logger_factory import log_exception
from src.utils.misc import normalize_datetime


mongo = MongoClient()


async def run(
    *,
    db_name: str,
    coll_name: str,
    time_field: str,
    group_fields: Sequence[str],
    time_start: Optional[Union[str, datetime]] = None,
    time_end: Optional[Union[str, datetime]] = None,
    extra_match: Optional[Dict[str, Any]] = None,
    sort_tiebreak: Sequence[Tuple[str, int]] = (),
    allow_disk_use: bool = True,
    batch_delete_size: int = 1000,
    logger: Logger,
) -> None:
    """Remove duplicate documents by grouping on ``group_fields``.

    The newest document in each group (based on ``time_field`` and
    ``sort_tiebreak``) is kept; the rest are deleted in batches.

    :param db_name: Mongo database name.
    :param coll_name: Collection name inside the database.
    :param time_field: Field used for primary sorting (typically a timestamp).
    :param group_fields: Fields that define which documents are duplicates.
    :param time_start: Optional inclusive lower bound for ``time_field``.
    :param time_end: Optional inclusive upper bound for ``time_field``.
    :param extra_match: Extra `$match` conditions applied before grouping.
    :param sort_tiebreak: Additional sort keys `(field, direction)` for ties.
    :param allow_disk_use: Whether aggregation may spill to disk.
    :param batch_delete_size: Number of `_id`s removed per delete batch.
    :param logger: Injected logger instance.
    """

    if batch_delete_size <= 0:
        raise ValueError("batch_delete_size must be greater than 0")

    try:
        db = mongo.client[db_name]
        col = db[coll_name]

        match: Dict[str, Any] = dict(extra_match or {})
        norm_start = normalize_datetime(time_start)
        norm_end = normalize_datetime(time_end)

        if norm_start is not None or norm_end is not None:
            rng: Dict[str, Any] = {}
            if norm_start is not None:
                rng["$gte"] = norm_start
            if norm_end is not None:
                rng["$lte"] = norm_end
            match[time_field] = rng

        group_id = {field: f"${field}" for field in group_fields}
        sort_stage = {time_field: -1}
        for key, direction in sort_tiebreak:
            sort_stage[key] = direction

        pipeline: List[Dict[str, Any]] = []
        if match:
            pipeline.append({"$match": match})
        pipeline.extend(
            [
                {"$sort": sort_stage},
                {
                    "$group": {
                        "_id": group_id,
                        "keep": {"$first": "$_id"},
                        "dups": {"$push": "$_id"},
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "removeIds": {"$slice": ["$dups", 1, {"$size": "$dups"}]},
                    }
                },
            ]
        )

        cursor = col.aggregate(pipeline, allowDiskUse=allow_disk_use)
        total_deleted = 0
        groups = 0
        buffer: List[Any] = []

        async for doc in cursor:
            groups += 1
            ids: List[Any] = doc.get("removeIds", [])
            if not ids:
                continue
            buffer.extend(ids)
            if len(buffer) >= batch_delete_size:
                res = await col.delete_many({"_id": {"$in": buffer}})
                total_deleted += res.deleted_count
                buffer.clear()

        if buffer:
            res = await col.delete_many({"_id": {"$in": buffer}})
            total_deleted += res.deleted_count

        logger.info(f"Aggregation pipeline: {pipeline}")
        logger.info(
            f"Duplicate cleanup summary => matched_groups={groups}, deleted_ids={total_deleted}"
        )
    except Exception as exc:  # pragma: no cover - defensive guard
        log_exception(logger, exc, context=f"OP_delete_duplicate_data:{db_name}.{coll_name}")
        raise
