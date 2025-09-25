"""Flexible MongoDB data loader supporting insert, update, and merge operations."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pymongo import ReplaceOne, UpdateOne

from src.mongo.base import MongoClient
from src.utils.logger.logger import Logger
from src.utils.logger_factory import log_exception


mongo = MongoClient()


async def run(
    *,
    mode: str,
    db_name: str,
    coll_name: str,
    documents: Sequence[Dict[str, Any]] | None = None,
    update_ops: Sequence[Dict[str, Any]] | None = None,
    key_fields: Sequence[str] | None = None,
    merge_strategy: str = "set",
    merge_upsert: bool = True,
    allow_disk_use: bool = True,  # reserved for future aggregation-based workflows
    logger: Logger,
) -> None:
    """Perform MongoDB insert/update/merge operations based on ``mode``.

    :param mode: One of ``insert``, ``update``, ``merge``.
    :param db_name: Target Mongo database name.
    :param coll_name: Target collection within the database.
    :param documents: Payload for insert/merge operations.
    :param update_ops: Explicit update specifications used when ``mode='update'``.
        Each item must contain at least ``filter`` and ``update`` keys, and may
        optionally include ``upsert`` (bool) and ``array_filters`` (list).
    :param key_fields: Fields that uniquely identify a document for merge.
    :param merge_strategy: ``set`` (default) uses ``$set`` to merge fields; ``replace``
        performs a full document replacement using ``ReplaceOne``.
    :param merge_upsert: Whether merges should upsert when no document matches.
    :param allow_disk_use: Reserved flag (aligns signature with other jobs).
    :param logger: Logger injected by the scheduler.
    """

    mode_normalized = mode.lower().strip()
    if mode_normalized not in {"insert", "update", "merge"}:
        raise ValueError("mode must be one of: 'insert', 'update', 'merge'")

    try:
        db = mongo.client[db_name]
        col = db[coll_name]

        if mode_normalized == "insert":
            if not documents:
                raise ValueError("'documents' is required for insert mode")
            payload = list(documents)
            if not payload:
                logger.info("Insert request received with 0 documents; nothing to do")
                return
            result = await col.insert_many(payload, ordered=False)
            logger.info(f"Inserted {len(result.inserted_ids)} documents into {db_name}.{coll_name}")
            return

        if mode_normalized == "update":
            if not update_ops:
                raise ValueError("'update_ops' is required for update mode")
            bulk_ops: List[UpdateOne] = []
            for idx, spec in enumerate(update_ops):
                if "filter" not in spec or "update" not in spec:
                    raise ValueError(
                        f"update_ops[{idx}] must include both 'filter' and 'update' keys"
                    )
                bulk_ops.append(
                    UpdateOne(
                        spec["filter"],
                        spec["update"],
                        upsert=spec.get("upsert", False),
                        array_filters=spec.get("array_filters"),
                    )
                )
            if not bulk_ops:
                logger.info("Update request received with 0 operations; nothing to do")
                return
            result = await col.bulk_write(bulk_ops, ordered=False)
            logger.info(
                f"Update summary => matched={result.matched_count}, modified={result.modified_count}, "
                f"upserts={len(result.upserted_ids)}"
            )
            return

        # merge mode
        if not documents:
            raise ValueError("'documents' is required for merge mode")
        if not key_fields:
            raise ValueError("'key_fields' must be provided for merge mode")

        merge_ops: List[Any] = []
        for idx, doc in enumerate(documents):
            missing = [field for field in key_fields if field not in doc]
            if missing:
                raise ValueError(
                    f"documents[{idx}] is missing key fields required for merge: {missing}"
                )
            filter_doc = {field: doc[field] for field in key_fields}
            if merge_strategy == "replace":
                merge_ops.append(ReplaceOne(filter_doc, doc, upsert=merge_upsert))
            elif merge_strategy == "set":
                merge_ops.append(
                    UpdateOne(filter_doc, {"$set": doc}, upsert=merge_upsert)
                )
            else:
                raise ValueError("merge_strategy must be 'set' or 'replace'")

        if not merge_ops:
            logger.info("Merge request received with 0 documents; nothing to do")
            return

        result = await col.bulk_write(merge_ops, ordered=False)
        logger.info(
            f"Merge summary => matched={result.matched_count}, modified={result.modified_count}, "
            f"upserts={len(result.upserted_ids)}"
        )

    except Exception as exc:  # pragma: no cover - defensive guard
        log_exception(logger, exc, context=f"OP_insert_data:{db_name}.{coll_name}")
        raise
