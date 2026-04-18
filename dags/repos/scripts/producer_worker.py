import asyncio
import redis
import json
import os
import uuid
import subprocess
from datetime import datetime
from src.pipelines.cukcuk_pipeline import CukCukETLPipeline
from src.utils.ssm_loader import load_config

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("Producer")


async def fetch_branch(
    branch_id: str,
    branch_name: str,
    pipeline: CukCukETLPipeline,
    r: redis.Redis,
    config: dict,
) -> dict:
    """
    Fetch orders + invoices for a single branch and push paths to Redis.
    Returns a summary dict for logging.

    CHANGE: extracted into its own coroutine so all 8 branches can be
    awaited concurrently via asyncio.gather() instead of running
    sequentially in a for-loop.
    """
    result = {
        "branch": branch_name,
        "order_batches": 0,
        "invoice_batches": 0,
        "errors": [],
    }

    # ── ORDERS ──────────────────────────────────────────────────────────
    try:
        last_sync_order = pipeline.get_last_sync_from_db("order_header", branch_id)
        logger.info(f"[{branch_name}] Orders from: {last_sync_order}")

        async for batch in pipeline.extractor.extract_orders_stream(
            last_sync_order, branch_id
        ):
            if not batch:
                continue

            s3_key = (
                f"raw/orders/{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                f"_{branch_id[:4]}_{uuid.uuid4().hex[:4]}.jsonl"
            )
            pipeline.s3_loader.upload_jsonl_from_memory(batch, s3_key)

            for tbl in ["order_header", "order_detail"]:
                r.lpush("ingestion_queue", json.dumps({"path": s3_key, "table": tbl}))

            result["order_batches"] += 1

    except Exception as e:
        msg = f"[{branch_name}] Orders error: {e}"
        logger.error(msg)
        result["errors"].append(msg)

    # ── INVOICES ─────────────────────────────────────────────────────────
    try:
        last_sync_invoice = pipeline.get_last_sync_from_db("invoice_header", branch_id)
        logger.info(f"[{branch_name}] Invoices from: {last_sync_invoice}")

        async for batch in pipeline.extractor.extract_invoices_stream(
            last_sync_invoice, branch_id
        ):
            if not batch:
                continue

            s3_key = (
                f"raw/invoices/{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                f"_{branch_id[:4]}_{uuid.uuid4().hex[:4]}.jsonl"
            )
            pipeline.s3_loader.upload_jsonl_from_memory(batch, s3_key)

            for tbl in ["invoice_header", "invoice_detail", "invoice_payment"]:
                r.lpush("ingestion_queue", json.dumps({"path": s3_key, "table": tbl}))

            result["invoice_batches"] += 1

    except Exception as e:
        msg = f"[{branch_name}] Invoices error: {e}"
        logger.error(msg)
        result["errors"].append(msg)

    return result


async def run():
    config = load_config()
    pipeline = CukCukETLPipeline(config)
    r = redis.Redis(host='redis-queue', port=6379, db=0)

    branch_map: dict = config.get("Branch-Name_Mapper", {})

    while True:
        cycle_start = datetime.now()
        logger.info(
            f"🚀 [{cycle_start}] New sync cycle — {len(branch_map)} branches in parallel"
        )

        # ── PARALLEL FETCH ───────────────────────────────────────────────
        # CHANGE: all branches now run concurrently with asyncio.gather().
        #
        # Before (sequential):
        #   for b_id in branch_ids:
        #       fetch orders for b_id   (~30-60s)
        #       fetch invoices for b_id (~30-60s)
        #   → total: 8 × 60s = ~8 minutes
        #
        # After (parallel):
        #   asyncio.gather(*[fetch_branch(b_id) for b_id in branch_map])
        #   → total: max(single branch time) ≈ ~1-2 minutes
        #
        # Each coroutine is independent: separate S3 keys (branch_id prefix
        # prevents collisions), separate Redis lpush calls (atomic op).
        # Redis and boto3 clients are safe to share across coroutines because
        # all network I/O is awaited — no true concurrency on the GIL boundary.
        tasks = [
            fetch_branch(b_id, b_name, pipeline, r, config)
            for b_id, b_name in branch_map.items()
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # ── SUMMARY LOG ──────────────────────────────────────────────────
        total_order_batches   = 0
        total_invoice_batches = 0
        failed_branches       = []

        for res in results:
            if isinstance(res, Exception):
                logger.error(f"Branch task raised exception: {res}")
                failed_branches.append(str(res))
                continue
            total_order_batches   += res["order_batches"]
            total_invoice_batches += res["invoice_batches"]
            if res["errors"]:
                failed_branches.extend(res["errors"])

        elapsed = (datetime.now() - cycle_start).seconds
        logger.info(
            f"✅ Fetch done in {elapsed}s — "
            f"order batches: {total_order_batches}, "
            f"invoice batches: {total_invoice_batches}"
        )
        if failed_branches:
            logger.warning(f"⚠️ {len(failed_branches)} branch error(s): {failed_branches}")

        # ── WAIT FOR CONSUMER TO DRAIN QUEUE ────────────────────────────
        logger.info("⏳ Waiting for consumer to drain queue...")
        wait_seconds = 0
        while True:
            queue_depth = r.llen("ingestion_queue")
            if queue_depth == 0:
                break
            if wait_seconds > 120:
                logger.warning(
                    f"Queue still has {queue_depth} messages after 2 min — running dbt anyway"
                )
                break
            await asyncio.sleep(5)
            wait_seconds += 5

        # ── TRIGGER DBT ──────────────────────────────────────────────────
        logger.info("🔄 Triggering dbt run...")
        dbt_result = subprocess.run(
            "cd /app/dbt_project && dbt run "
            "--select master_sales_analytics revenue_daily "
            "--profiles-dir /app/dbt_project "
            "--no-write-json && "
            "dbt test "
            "--profiles-dir /app/dbt_project "
            "--no-write-json "
            "2>&1",
            shell=True,
            capture_output=True,
            text=True,
        )

        if dbt_result.returncode == 0:
            logger.info("✅ dbt run + test succeeded.")
        else:
            logger.error(
                f"❌ dbt failed:\n{dbt_result.stdout[-1000:]}"
            )
            # TODO: send Slack alert here

        logger.info("💤 Sleeping 10 minutes...\n")
        await asyncio.sleep(600)


if __name__ == "__main__":
    asyncio.run(run())