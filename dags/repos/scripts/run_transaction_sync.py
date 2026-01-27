import sys
import os
import argparse
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# Hack path để import src
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# 👇 Bây giờ import này sẽ hoạt động NGON LÀNH
from src.utils.ssm_loader import load_config
from src.pipelines.cukcuk_pipeline import CukCukETLPipeline

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, type=str)
    args = parser.parse_args()
    
    target_date = datetime.strptime(args.date, "%Y-%m-%d")
    logging.info(f"🚀 [Script] Starting Transaction Sync for date: {args.date}")

    try:
        config = load_config() # Gọi hàm từ ssm_loader
        logging.info("✅ Config loaded successfully.")
    except Exception as e:
        logging.error(f"Failed to load config: {e}")
        sys.exit(1)

    pipeline = CukCukETLPipeline(config)
    pipeline.run_single_day_process(target_date)

if __name__ == "__main__":
    main()