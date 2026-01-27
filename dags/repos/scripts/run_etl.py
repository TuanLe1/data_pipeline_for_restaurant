import sys
import os
import argparse
import asyncio
import logging
from datetime import datetime

# ==============================================================================
# 1. SETUP ĐƯỜNG DẪN IMPORT (QUAN TRỌNG)
# ==============================================================================
# Mục tiêu: Thêm thư mục 'repos' vào sys.path để có thể import 'src'
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__)) # .../dags/repos/scripts
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)              # .../dags/repos

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# ==============================================================================
# 2. IMPORT MODULES
# ==============================================================================
try:
    # Import Pipeline Logic
    from src.pipelines.cukcuk_pipeline import CukCukETLPipeline
    
    # Import Config Loader từ file ssm_loader.py bạn đã cung cấp
    from src.utils.ssm_loader import load_config
    
except ImportError as e:
    print(f"❌ Import Error: {e}")
    print(f"Current sys.path: {sys.path}")
    sys.exit(1)

# Config Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ETL_Runner")

# ==============================================================================
# 3. MAIN FUNCTION
# ==============================================================================
def main():
    # --- A. Parse Arguments ---
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

    # --- B. Load Config & Init Pipeline ---
    logger.info("🔧 Initializing Configuration...")
    try:
        # Gọi hàm load_config thực tế từ ssm_loader.py
        # Lưu ý: Hàm này sẽ tự tìm config trong /opt/airflow/config/config.json 
        # hoặc đường dẫn tương đối ../../../configs/config.json
        config = load_config() 
        
        # Init Pipeline Class
        pipeline = CukCukETLPipeline(config)
        
    except Exception as e:
        logger.critical(f"❌ Failed to initialize Pipeline: {e}")
        sys.exit(1)

    # --- C. Router Logic ---
    try:
        # 1. NHÁNH MASTER DATA
        if args.phase == 'master':
            if args.step == 'extract':
                logger.info("🚀 Starting MASTER EXTRACT...")
                asyncio.run(pipeline.extract_master_to_disk())
                
            elif args.step == 'load':
                logger.info("🚀 Starting MASTER LOAD...")
                pipeline.load_master_from_disk_to_s3()

        # 2. NHÁNH TRANSACTION DATA
        elif args.phase == 'trans':
            if not args.date:
                logger.error("❌ Argument --date is required for transaction phase.")
                sys.exit(1)
            
            target_date = datetime.strptime(args.date, "%Y-%m-%d")
            
            if args.step == 'extract':
                logger.info(f"🚀 Starting TRANS EXTRACT for {args.date}...")
                asyncio.run(pipeline.extract_trans_to_disk(target_date))
                
            elif args.step == 'load':
                logger.info(f"🚀 Starting TRANS LOAD for {args.date}...")
                pipeline.load_trans_from_disk_to_s3(target_date)

        logger.info("✅ Job finished successfully.")

    except Exception as e:
        logger.error(f"❌ Job runtime failed: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()