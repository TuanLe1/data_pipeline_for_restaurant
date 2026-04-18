import sys
import os, json
import argparse
import asyncio
import logging
import uuid
from datetime import datetime

# ==============================================================================
# 1. SETUP ĐƯỜNG DẪN IMPORT
# ==============================================================================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# ==============================================================================
# 2. IMPORT MODULES
# ==============================================================================
try:
    from src.pipelines.cukcuk_pipeline import CukCukETLPipeline
    from src.utils.ssm_loader import load_config

except ImportError as e:
    print(f"❌ Import Error: {e}")
    print(f"Current sys.path: {sys.path}")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ETL_Runner")


# ==============================================================================
# 3. MAIN FUNCTION
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(description="Run CukCuk ETL Pipeline via CLI")

    parser.add_argument(
        "--phase",
        required=True,
        choices=['master', 'trans'],
        help="Phase to run: 'master' or 'trans'"
    )
    parser.add_argument(
        "--step",
        required=True,
        choices=['extract', 'load'],
        help="Step to run: 'extract' or 'load'"
    )
    parser.add_argument(
        "--date",
        help="Target date in YYYY-MM-DD format (Required for 'trans' phase)"
    )

    args = parser.parse_args()

    logger.info("🔧 Initializing Configuration...")
    try:
        config = load_config()
        pipeline = CukCukETLPipeline(config)

    except Exception as e:
        logger.critical(f"❌ Failed to initialize Pipeline: {e}")
        sys.exit(1)

    try:
        # 1. NHÁNH MASTER DATA
        if args.phase == 'master':
            if args.step == 'extract':
                logger.info("🚀 [Master] Extracting directly to S3...")
                asyncio.run(pipeline.sync_master_to_s3())

            elif args.step == 'load':
                logger.info("🚀 [Master] Loading from S3 to ClickHouse...")
                pipeline.load_master_from_s3_to_clickhouse()

        # 2. NHÁNH TRANSACTION DATA
        elif args.phase == 'trans':
            if args.step == 'extract':
                if not args.date:
                    logger.critical("❌ --date is required for trans phase")
                    sys.exit(1)

                logger.info(f"🚀 Backfill data for {args.date}...")

                import redis
                r = redis.Redis(host='redis-queue', port=6379, db=0)

                # ============================================================
                # THAY ĐỔI 1: Tách riêng Orders và Invoices, loop qua tất cả
                # branches thay vì chỉ xử lý một branch.
                #
                # VERSION CŨ (2 bugs):
                #   Bug A — dùng chung s3_key cho cả 4 tables:
                #     async for batch in pipeline.extractor.extract_orders_stream(
                #         from_date=args.date          # ← thiếu branch_id
                #     ):
                #         for target_table in ["order_header", "order_detail",
                #                              "invoice_header", "invoice_detail"]:
                #             r.lpush(...)             # ← order data → invoice table
                #
                #   Bug B — extract_orders_stream() thiếu argument branch_id
                #     → TypeError khi chạy
                #
                # VERSION MỚI:
                #   - Loop qua tất cả branches từ config
                #   - Truyền branch_id vào từng stream call
                #   - Tách orders và invoices thành 2 loop độc lập
                #   - Mỗi loại data push đúng tables của nó
                # ============================================================

                async def run_backfill():
                    # THAY ĐỔI 1a: lấy toàn bộ branch_map từ config
                    # thay vì dùng self.branch_id (chỉ là branch đầu tiên)
                    branch_map = config.get("Branch-Name_Mapper", {})

                    if not branch_map:
                        logger.error("❌ Branch-Name_Mapper is empty in config")
                        return

                    total_order_batches   = 0
                    total_invoice_batches = 0

                    for branch_id, branch_name in branch_map.items():

                        # ── ORDERS ──────────────────────────────────────────
                        logger.info(
                            f"📦 [Backfill Orders] {branch_name} ({branch_id})..."
                        )

                        async for batch in pipeline.extractor.extract_orders_stream(
                            from_date=args.date,
                            branch_id=branch_id,    # THAY ĐỔI 1b: truyền branch_id
                        ):
                            if not batch:
                                logger.info(f"☕ Orders {branch_name}: empty batch.")
                                continue

                            # THAY ĐỔI 1c: prefix riêng cho orders, gồm branch_id
                            # để tránh overwrite giữa các branches
                            s3_key = (
                                f"raw/backfill/orders/{args.date}/"
                                f"{branch_id}_{uuid.uuid4()}.jsonl"
                            )
                            pipeline.s3_loader.upload_jsonl_from_memory(batch, s3_key)

                            # THAY ĐỔI 1d: chỉ push đúng 2 tables của orders
                            for target_table in ["order_header", "order_detail"]:
                                r.lpush("ingestion_queue", json.dumps({
                                    "path":  s3_key,
                                    "table": target_table,
                                }))

                            total_order_batches += 1

                        # ── INVOICES ─────────────────────────────────────────
                        logger.info(
                            f"🧾 [Backfill Invoices] {branch_name} ({branch_id})..."
                        )

                        async for batch in pipeline.extractor.extract_invoices_stream(
                            from_date=args.date,
                            branch_id=branch_id,    # THAY ĐỔI 1e: truyền branch_id
                        ):
                            if not batch:
                                logger.info(f"☕ Invoices {branch_name}: empty batch.")
                                continue

                            # THAY ĐỔI 1f: prefix riêng cho invoices, gồm branch_id
                            s3_key = (
                                f"raw/backfill/invoices/{args.date}/"
                                f"{branch_id}_{uuid.uuid4()}.jsonl"
                            )
                            pipeline.s3_loader.upload_jsonl_from_memory(batch, s3_key)

                            # THAY ĐỔI 1g: chỉ push đúng 3 tables của invoices
                            for target_table in [
                                "invoice_header",
                                "invoice_detail",
                                "invoice_payment",
                            ]:
                                r.lpush("ingestion_queue", json.dumps({
                                    "path":  s3_key,
                                    "table": target_table,
                                }))

                            total_invoice_batches += 1

                    logger.info(
                        f"✅ Backfill done — "
                        f"orders: {total_order_batches} batch(es), "
                        f"invoices: {total_invoice_batches} batch(es) queued."
                    )

                asyncio.run(run_backfill())

        logger.info("✅ Job finished successfully.")

    except Exception as e:
        logger.error(f"❌ Job runtime failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()