import argparse
import json
import logging
import os
import sys
import subprocess
import asyncio
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor
from src.utils.ssm_loader import SSMParameterLoader

# Thêm đường dẫn src vào path để Python tìm thấy modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import Class Pipeline Mới (Đã update ở bước trước)
from src.pipelines.cukcuk_pipeline import CukCukETLPipeline

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(processName)s) %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# --- CONFIGURATION PATHS ---
LOCAL_CONFIG_PATH = "/home/tuanle/DE-lab/data_pipeline_for_restaurant/configs/config.json"
DOCKER_CONFIG_PATH = "configs/config.json"

def load_config():
    """
    Load configuration logic (Hybrid):
    1. Xác định đường dẫn file JSON (Local vs Docker).
    2. Đọc file JSON (Base config).
    3. Kết nối AWS SSM để lấy Secrets đè lên config (Secure config).
    """
    # --- BƯỚC 1: Tìm file config ---
    if os.path.exists(LOCAL_CONFIG_PATH):
        config_path = LOCAL_CONFIG_PATH
    elif os.path.exists(DOCKER_CONFIG_PATH):
        config_path = DOCKER_CONFIG_PATH
    else:
        config_path = "configs/config.json" # Fallback

    logger.info(f"📂 Loading base config from file: {config_path}")
    
    if not os.path.exists(config_path):
        logger.error(f"❌ Config file not found at: {config_path}")
        sys.exit(1)
        
    # --- BƯỚC 2: Load JSON cơ bản ---
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    # --- BƯỚC 3: Merge với AWS SSM Secrets ---
    try:
        logger.info("🔐 Fetching secrets from AWS SSM Parameter Store...")
        
        # Khởi tạo loader (Region thường lấy từ env var hoặc config file, default ap-southeast-1)
        # Lưu ý: Cần đảm bảo môi trường chạy (Local/EC2) đã có AWS Credentials (hoặc IAM Role)
        ssm_loader = SSMParameterLoader()
        
        # Gọi hàm merge (giả sử logic cũ của bạn là merge_ssm_config)
        # Nó sẽ tìm các key trong SSM và ghi đè lên các key tương ứng trong dict config
        config = ssm_loader.merge_ssm_config(config)
        
        logger.info("✅ Configuration loaded and merged with SSM secrets.")
        
    except Exception as e:
        # Lỗi SSM là lỗi nghiêm trọng (thiếu pass DB, API Key...), nên dừng luôn
        logger.critical(f"⛔ STOPPING: Could not load configuration from AWS SSM. {e}")
        sys.exit(1)

    return config

def run_dbt_transformation():
    """
    Kích hoạt dbt run sau khi Python ETL hoàn tất.
    """
    logger.info("🚀 [PHASE 3] Triggering dbt transformation (ELT)...")
    
    dbt_project_dir = "./dbt_project"
    dbt_profiles_dir = "./dbt_project" 
    
    if not os.path.exists(dbt_project_dir):
        logger.warning(f"⚠️ dbt directory '{dbt_project_dir}' not found. Skipping transformation.")
        return

    try:
        # 1. Dùng sys.executable để gọi module dbt (An toàn hơn gọi lệnh 'dbt' trực tiếp)
        cmd = [
            "dbt", "run",
            "--project-dir", dbt_project_dir,
            "--profiles-dir", dbt_profiles_dir
        ]
        
        # 2. Bỏ capture_output=True để thấy log dbt chạy theo thời gian thực
        # check=True sẽ tự raise lỗi nếu dbt fail
        subprocess.run(cmd, check=True)
        
        logger.info("✅ dbt Transformation Completed Successfully!")
        
    except subprocess.CalledProcessError:
        # Không cần in e.stderr vì nó đã in thẳng ra màn hình rồi
        logger.error("❌ dbt Transformation Failed! Please check the logs above.")
        # raise e # (Optional) Uncomment nếu muốn pipeline dừng hẳn và báo lỗi exit code 1

def worker_process(target_date_str, config):
    """
    Hàm này chạy trong một Process riêng biệt (Multiprocessing).
    Mỗi Process sẽ tự khởi tạo Pipeline và chạy cho 1 ngày cụ thể.
    """
    try:
        # Convert string -> datetime
        target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
        
        # Init Pipeline mới cho process này
        pipeline = CukCukETLPipeline(config)
        
        # Gọi hàm xử lý Transaction cho 1 ngày
        pipeline.run_single_day_process(target_date)
        return True
    except Exception as e:
        logger.error(f"❌ Worker failed for {target_date_str}: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="CukCuk Restaurant Data Pipeline (ELT)")
    parser.add_argument("--days", type=int, default=1, help="Number of days to backfill (default: 1 - Yesterday)")
    parser.add_argument("--workers", type=int, default=2, help="Number of parallel workers (default: 2)")
    parser.add_argument("--skip-dbt", action="store_true", help="Skip dbt transformation step")
    
    args = parser.parse_args()
    
    # ---------------------------------------------------------
    # 1. Load Config
    # ---------------------------------------------------------
    try:
        config = load_config()
    except Exception as e:
        logger.critical(f"Failed to load config: {e}")
        sys.exit(1)

    end_date = datetime.now()
    dates_to_process = []
    for i in range(1, args.days + 1):
        d = end_date - timedelta(days=i)
        dates_to_process.append(d.strftime("%Y-%m-%d"))

    # =========================================================================
    # 🚀 KÍCH HOẠT CHẠY SONG SONG (PARALLEL EXECUTION)
    # =========================================================================
    
    logger.info("🚀 STARTING PARALLEL EXTRACTION...")

    # 1. Kích hoạt Worker cho Transaction (Phase 2) - NHƯNG KHÔNG CHỜ (Non-blocking)
    # -------------------------------------------------------------------------
    executor = ProcessPoolExecutor(max_workers=args.workers)
    transaction_futures = []
    
    if dates_to_process:
        logger.info(f"⚡ [Phase 2] Submitting {len(dates_to_process)} transaction jobs to workers...")
        for date_str in dates_to_process:
            # Submit job vào pool và giữ lại cái "future" (biên lai) để check sau
            future = executor.submit(worker_process, date_str, config)
            transaction_futures.append((date_str, future))
    else:
        logger.info("[Phase 2] No dates to process.")

    # 2. Trong lúc Worker đang cày Transaction, Main Thread chạy Master Data (Phase 1)
    # -------------------------------------------------------------------------
    # Thay vì ngồi chơi xơi nước chờ worker, Main Thread sẽ tải Product/Customer
    logger.info("⚡ [Phase 1] Main Thread starting Master Data Sync...")
    
    # Init pipeline riêng cho Master Data
    pipeline_master = CukCukETLPipeline(config)
    try:
        # Chạy AsyncIO ngay tại đây
        asyncio.run(pipeline_master.run_master_data_sync())
    except Exception as e:
        logger.error(f"❌ Phase 1 (Master Data) Failed: {e}")
        # Tùy bạn: Có thể dừng luôn hoặc vẫn cho Phase 2 chạy tiếp
    
    # 3. Bây giờ mới ngồi chờ các Worker (Phase 2) báo cáo kết quả
    # -------------------------------------------------------------------------
    logger.info("⏳ [Phase 1 Done] Waiting for Transaction Workers (Phase 2) to finish...")
    
    phase2_success = True
    for date_str, future in transaction_futures:
        try:
            # Lúc này code sẽ block cho đến khi worker xong việc
            future.result() 
        except Exception as exc:
            logger.error(f"❌ Transaction job for {date_str} generated an exception: {exc}")
            phase2_success = False

    # Dọn dẹp Executor
    executor.shutdown()

    logger.info("✅ All Extract & Load tasks (Phase 1 & 2) finished.")

    # ---------------------------------------------------------
    # 4. PHASE 3: Trigger dbt (Transform)
    # ---------------------------------------------------------
    if not args.skip_dbt:
        # Chỉ chạy dbt khi dữ liệu đã về đủ (hoặc tùy logic của bạn)
        run_dbt_transformation()
    else:
        logger.info("⏩ Skipping dbt transformation as requested.")

if __name__ == "__main__":
    main()