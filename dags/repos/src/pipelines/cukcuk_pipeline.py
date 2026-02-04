import os
import json
import shutil
import logging
import asyncio
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

from src.extractors.cukcuk.extractor import CukCukExtractor
from src.transformers.generic_transformer import GenericTransformer
from src.loaders.s3_iceberg_loader import S3IcebergLoader

logger = logging.getLogger(__name__)

# Thư mục dùng chung (Shared Volume giữa các Task)
TEMP_DATA_DIR = "/opt/airflow/temp_data"

class CukCukETLPipeline:
    def __init__(self, config: dict):
        self.config = config
        self.partition_conf = config.get("Partition-Column", {})
        
        # Init Modules
        self.extractor = CukCukExtractor(config)
        self.transformer = GenericTransformer(config)
        self.loader = S3IcebergLoader(config)

    def _transform_and_get_df(self, raw_data: list, table_name: str) -> pd.DataFrame:
        if not raw_data:
            return pd.DataFrame()
        return self.transformer.transform(raw_data, table_name)
    
    def _load_dfs_to_s3(self, dfs_map: dict, time_col: str = None):
        """Hàm chung để đẩy danh sách DataFrame lên S3."""
        if not dfs_map:
            logger.warning("⚠️ No DataFrames to upload.")
            return

        logger.info(f"🚀 Uploading {len(dfs_map)} tables to S3...")
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for table_name, df in dfs_map.items():
                if not df.empty:
                    futures.append(executor.submit(
                        self._save_dataframe_worker, 
                        df, table_name, time_col
                    ))
            
            for f in futures: f.result()
        
        logger.info("✅ Upload S3 Finished.")
    
    def _save_dataframe_worker(self, df: pd.DataFrame, table_name: str, time_col: str):
        try:
            part_cols = self.partition_conf.get(table_name)
            self.loader.save_table(df, table_name, partition_cols=part_cols, time_col=time_col)
        except Exception as e:
            logger.error(f"❌ Failed writing table {table_name}: {e}")
            raise e

    async def _fetch_master_data_async(self):
        task_prod = self.extractor.extract_products()
        task_cust = self.extractor.extract_customers()
        return await asyncio.gather(task_prod, task_cust)

    async def _fetch_transactions_async(self, start_str, end_dt):
        task_orders = self.extractor.extract_orders(start_str, end_dt)
        task_invoices = self.extractor.extract_invoices(start_str, end_dt)
        return await asyncio.gather(task_orders, task_invoices)

    def _transform_transactions(self, raw_orders, raw_invoices) -> dict:
        """Helper để biến đổi JSON Transaction -> DataFrame Map"""
        dfs = {}

        # --- Transform Orders ---
        if raw_orders:
            dfs["order_header"] = self._transform_and_get_df(raw_orders, "order_header")
            
            # Flatten Details
            details = []
            for o in raw_orders:
                details.extend(o.get("line_items", []) or [])
            dfs["order_detail"] = self._transform_and_get_df(details, "order_detail")

        # --- Transform Invoices ---
        if raw_invoices:
            dfs["invoice_header"] = self._transform_and_get_df(raw_invoices, "invoice_header")
            
            # Flatten Details & Payments
            inv_details = []
            inv_payments = []
            for inv in raw_invoices:
                parent_info = { 
                    "RefID": inv.get("RefId"), 
                    "RefDate": inv.get("RefDate"),
                    "BranchId": inv.get("BranchId"),
                    "BranchName": inv.get("BranchName"),
                    "CustomerId": inv.get("CustomerId")
                }
                
                for d in inv.get("invoice_details", []) or []:
                    d.update(parent_info)
                    inv_details.append(d)
                
                for p in inv.get("invoice_payments", []) or []:
                    p.update(parent_info)
                    inv_payments.append(p)
            
            dfs["invoice_detail"] = self._transform_and_get_df(inv_details, "invoice_detail")
            dfs["invoice_payment"] = self._transform_and_get_df(inv_payments, "invoice_payment")
            
        return dfs

    async def extract_master_to_disk(self):
        """Bước 1: Tải API -> Lưu JSON xuống Disk"""
        logger.info("⬇️ [Master] Extracting to Disk...")
        path = f"{TEMP_DATA_DIR}/master"
        os.makedirs(path, exist_ok=True)
        
        prods, custs = await self._fetch_master_data_async()
        
        # Ghi file (Thêm ensure_ascii=False để đọc tiếng Việt không bị lỗi font)
        with open(f"{path}/products.json", 'w', encoding='utf-8') as f: 
            json.dump(prods, f, ensure_ascii=False)
        with open(f"{path}/customers.json", 'w', encoding='utf-8') as f: 
            json.dump(custs, f, ensure_ascii=False)
        logger.info(f"✅ [Master] Saved to {path}")

    def load_master_from_disk_to_s3(self):
        """Bước 2: Đọc Disk -> Transform -> S3"""
        logger.info("🚀 [Master] Loading to S3...")
        path = f"{TEMP_DATA_DIR}/master"
        
        with open(f"{path}/products.json", 'r', encoding='utf-8') as f: prods = json.load(f)
        with open(f"{path}/customers.json", 'r', encoding='utf-8') as f: custs = json.load(f)
            
        dfs = {
            "product": self._transform_and_get_df(prods, "product"),
            "customer": self._transform_and_get_df(custs, "customer")
        }
        self._load_dfs_to_s3(dfs, time_col=None)
        logger.info("✅ [Master] Finished.")

    async def extract_trans_to_disk(self, target_date: datetime):
        day_str = target_date.strftime("%Y-%m-%d")
        path = f"{TEMP_DATA_DIR}/{day_str}"
        os.makedirs(path, exist_ok=True)
        
        logger.info(f"⬇️ [Trans {day_str}] Extracting...")
        
        start = target_date.strftime("%Y-%m-%dT%H:%M:%S%z")
        end = (target_date + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S%z")
        
        orders, invoices = await self._fetch_transactions_async(start, end)
        
        with open(f"{path}/orders.json", 'w', encoding='utf-8') as f: 
            json.dump(orders, f, ensure_ascii=False)
        with open(f"{path}/invoices.json", 'w', encoding='utf-8') as f: 
            json.dump(invoices, f, ensure_ascii=False)
        logger.info(f"✅ [Trans {day_str}] Saved.")

    def load_trans_from_disk_to_s3(self, target_date: datetime):
        day_str = target_date.strftime("%Y-%m-%d")
        path = f"{TEMP_DATA_DIR}/{day_str}"
        logger.info(f"🚀 [Trans {day_str}] Loading...")
        
        # Kiểm tra file tồn tại trước khi đọc
        if not os.path.exists(f"{path}/orders.json"):
            logger.warning(f"⚠️ No data found for {day_str}. Skipping.")
            return

        with open(f"{path}/orders.json", 'r', encoding='utf-8') as f: orders = json.load(f)
        with open(f"{path}/invoices.json", 'r', encoding='utf-8') as f: invoices = json.load(f)
        
        # Transform
        dfs_map = self._transform_transactions(orders, invoices)
        
        # Load
        self._load_dfs_to_s3(dfs_map, time_col="report_date")
        
        # Dọn dẹp Disk
        if os.path.exists(path):
            shutil.rmtree(path)
            logger.info(f"🧹 Cleaned up {path}")