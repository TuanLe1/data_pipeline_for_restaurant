import os
import sys
import logging

# ==============================================================================
# 1. SETUP ĐƯỜNG DẪN ĐỂ IMPORT SRC
# ==============================================================================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from src.loaders.clickhouse_loader import ClickHouseLoader
from src.utils.ssm_loader import load_config
import clickhouse_connect

# Cấu hình log
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("DB_Initializer")

def init():
    logger.info("🏗️ Bắt đầu quá trình khởi tạo cấu trúc Database...")
    
    try:
        # 2. Load cấu hình từ file json
        config = load_config()
        ch_conf = config.get("ClickHouse-Config", {})
        target_db = ch_conf.get("database", "restaurant_db") # Lấy tên DB đích (vd: restaurant_db)
        
        # ======================================================================
        # CHỖ UPDATE 1: KẾT NỐI VÀO DATABASE 'default' TRƯỚC
        # ======================================================================
        logger.info(f"🔗 Đang kết nối tạm thời vào database 'default' để khởi tạo...")
        client = clickhouse_connect.get_client(
            host=ch_conf.get("host"),
            port=ch_conf.get("port"),
            username=ch_conf.get("user"),
            password=ch_conf.get("password"),
            database='default'  # <--- KHÔNG kết nối vào restaurant_db ở đây
        )
        
        # ======================================================================
        # CHỖ UPDATE 2: TẠO DATABASE NẾU CHƯA CÓ
        # ======================================================================
        logger.info(f"🏠 Đang tạo Database '{target_db}' (nếu chưa có)...")
        client.command(f'CREATE DATABASE IF NOT EXISTS {target_db}')
        client.close()

        logger.info(f"📍 Đang kết nối trực tiếp vào database: {target_db}")
        new_client = clickhouse_connect.get_client(
            host=ch_conf.get("host"),
            port=ch_conf.get("port"),
            username=ch_conf.get("user"),
            password=ch_conf.get("password"),
            database=target_db  # <--- Ép mọi request sau này vào đây
        )

        loader = ClickHouseLoader(config)
        tables = config.get("Target-Schema", {}).keys()

        for table_name in tables:
            logger.info(f"🔨 Đang tạo bảng: {target_db}.{table_name}")
            # Truyền cái new_client đã có "hộ khẩu" restaurant_db vào
            loader.create_table_if_not_exists(new_client, table_name)
            
        logger.info(f"✅ Hoàn tất! Database '{target_db}' đã thực sự có bảng.")
        new_client.close()

    except Exception as e:
        logger.critical(f"💥 Lỗi khởi tạo DB: {e}")
        sys.exit(1)

if __name__ == "__main__":
    init()