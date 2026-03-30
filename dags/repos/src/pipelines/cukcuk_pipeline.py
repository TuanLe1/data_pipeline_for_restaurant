import os
import json
import shutil
import logging
import asyncio
from datetime import datetime, timedelta

from src.extractors.cukcuk.extractor import CukCukExtractor
from src.loaders.snowflake_loader import SnowflakeLoader

logger = logging.getLogger(__name__)

TEMP_DATA_DIR = "/tmp/restaurant_temp_data"

class CukCukETLPipeline:
    def __init__(self, config: dict):
        self.config = config
        self.extractor = CukCukExtractor(config)
        self.loader = SnowflakeLoader(config)

    # =================================================================
    # MASTER DATA (Product, Customer)
    # =================================================================
    async def extract_master_to_disk(self):
        logger.info("⬇️ [Master] Extracting Products & Customers to Disk...")
        path = f"{TEMP_DATA_DIR}/master"
        os.makedirs(path, exist_ok=True)
        
        prods = await self.extractor.extract_products()
        custs = await self.extractor.extract_customers()
        
        with open(f"{path}/products.json", 'w', encoding='utf-8') as f: 
            json.dump(prods, f, ensure_ascii=False)
        with open(f"{path}/customers.json", 'w', encoding='utf-8') as f: 
            json.dump(custs, f, ensure_ascii=False)

    def load_master_from_disk_to_snowflake(self):
        logger.info("🚀 [Master] Loading Master Data to Snowflake...")
        path = f"{TEMP_DATA_DIR}/master"
        
        with open(f"{path}/products.json", 'r', encoding='utf-8') as f: prods = json.load(f)
        with open(f"{path}/customers.json", 'r', encoding='utf-8') as f: custs = json.load(f)
            
        self.loader.load_extracted_data(prods, 'products')
        self.loader.load_extracted_data(custs, 'customers')

    # =================================================================
    # TRANSACTION DATA (Orders, Invoices)
    # =================================================================
    async def extract_trans_to_disk(self, target_date: datetime):
        day_str = target_date.strftime("%Y-%m-%d")
        path = f"{TEMP_DATA_DIR}/{day_str}"
        os.makedirs(path, exist_ok=True)
        
        start = target_date.strftime("%Y-%m-%dT%H:%M:%S")
        end = (target_date + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S")
        
        # Gọi Extractor (Bản mới trả về Dictionary)
        order_data = await self.extractor.extract_orders(start, end)
        invoice_data = await self.extractor.extract_invoices(start, end)
        
        # Lưu file Header & Detail riêng biệt
        with open(f"{path}/orders_split.json", 'w', encoding='utf-8') as f: 
            json.dump(order_data, f, ensure_ascii=False)
        with open(f"{path}/invoices_split.json", 'w', encoding='utf-8') as f: 
            json.dump(invoice_data, f, ensure_ascii=False)

    def load_trans_from_disk_to_snowflake(self, target_date: datetime):
        day_str = target_date.strftime("%Y-%m-%d")
        path = f"{TEMP_DATA_DIR}/{day_str}"
        
        logger.info(f"🚀 [Trans] Loading Data for {day_str} to Snowflake...")

        # 1. Load Orders (Header + Detail)
        if os.path.exists(f"{path}/orders_split.json"):
            with open(f"{path}/orders_split.json", 'r', encoding='utf-8') as f:
                order_data = json.load(f)
            self.loader.load_extracted_data(order_data, 'orders')

        # 2. Load Invoices (Header + Detail + Payment)
        if os.path.exists(f"{path}/invoices_split.json"):
            with open(f"{path}/invoices_split.json", 'r', encoding='utf-8') as f:
                invoice_data = json.load(f)
            self.loader.load_extracted_data(invoice_data, 'invoices')
        
        # Dọn dẹp folder sau khi nạp xong
        if os.path.exists(path):
            shutil.rmtree(path)
            logger.info(f"🧹 Cleaned up {path}")