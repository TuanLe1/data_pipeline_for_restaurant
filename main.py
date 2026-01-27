import argparse
import logging
import json
import os
import sys
import time
import random
import asyncio
import pytz

from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor, wait
from src.utils.ssm_loader import SSMParameterLoader
from src.pipelines.cukcuk_pipeline import CukCukETLPipeline
from src.utils.slack_alert import SlackAlert


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(processName)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def load_config(config_path="configs/config.json"):
    if not os.path.exists(config_path):
        logger.error(f"Config file not found: {config_path}")
        sys.exit(1)
    
    with open(config_path, "r") as f:
        config = json.load(f)
    try:
        ssm_loader = SSMParameterLoader()
        config = ssm_loader.merge_ssm_config(config)
        
    except Exception as e:
        logger.critical(f"STOPPING: Could not load configuration from AWS SSM. {e}")
        sys.exit(1)

    return config

def worker_master_data(config):
    """
    worker runner for Master Data (Phase 1).
    """
    try:
        logger.info("🎬 [Phase 1 Worker] Starting...")
        pipeline = CukCukETLPipeline(config)
        asyncio.run(pipeline.run_master_data_sync())
    except Exception as e:
        logger.error(f"❌ Master Data Failed: {e}")
        raise e

def worker_process_day(config, target_date):
    """
    worker runner for processing Transactions for a specific day (Phase 2).
    """
    try:
        sleep_time = random.uniform(1.0, 2.0)
        time.sleep(sleep_time)
        pipeline = CukCukETLPipeline(config)
        pipeline.run_single_day_process(target_date)
        
    except Exception as e:
        logger.error(f"Worker Crash on {target_date}: {e}")
        raise e

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, required=False, help="YYYY-MM-DD")
    # Mặc định chạy 1 ngày (Daily Job) để tối ưu tốc độ
    parser.add_argument("--days", type=int, default=1, help="Số ngày chạy lùi (Default: 1)")
    parser.add_argument("--workers", type=int, default=4, help="Số tiến trình chạy song song")
    return parser.parse_args()

def main():
    start_time = time.time()
    args = parse_args()
    config = load_config()
    slack = SlackAlert(config)
    vn_tz = pytz.timezone('Asia/Ho_Chi_Minh')
    now_vn = datetime.now(vn_tz)
    end_date = now_vn.replace(hour=0, minute=0, second=0, microsecond=0)
    
    date_list = []
    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d")
        date_list.append(vn_tz.localize(target_date))
    else:
        current = end_date - timedelta(days=args.days)
        while current < end_date:
            date_list.append(current)
            current += timedelta(days=1)

    logger.info(f"BATCH JOB STARTED. Parallel Workers: {args.workers}")
    logger.info(f"Dates to process: {[d.strftime('%Y-%m-%d') for d in date_list]}")

    try:
        logger.info("\n>>> [PHASE 1] SPAWNING MASTER DATA WORKER...")
        
        with ProcessPoolExecutor(max_workers=1) as executor:
            future = executor.submit(worker_master_data, config)
            future.result() 
        
        logger.info("✅ Phase 1 Complete.")

        logger.info(f"\n>>> [PHASE 2] SPAWNING {args.workers} WORKERS...")
        failed_workers = []
        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            futures = {executor.submit(worker_process_day, config, d): d for d in date_list}
            done, _ = wait(futures)
            for f in done:
                date_processed = futures[f]
                try:
                    f.result()
                except Exception as e:
                    err_msg = f"Date {date_processed.date()} failed: {e}"
                    failed_workers.append(err_msg)
        
        if failed_workers:
            raise Exception(f"Phase 2 had failures: {failed_workers}")
        elapsed_seconds = int(time.time() - start_time)
        duration_str = str(timedelta(seconds=elapsed_seconds))
        
        logger.info("🎉 ALL JOBS FINISHED SUCCESSFULLY.")
        
        msg = f"✅ ETL Daily Run Success.\nProcessed: {len(date_list)} days.\nRange: {date_list[0].date()} -> {date_list[-1].date()}"
        slack.send_success(message=msg, duration=duration_str)

    except KeyboardInterrupt:
        logger.warning("⚠️ Job cancelled by user.")
        slack.send_error("Job cancelled by user manually.")
        sys.exit(1)
        
    except Exception as e:
        logger.critical(f"🔥 CRITICAL FAILURE: {e}")
        slack.send_error(error_msg=str(e), error_obj=e)
        sys.exit(1)

if __name__ == "__main__":
    main()