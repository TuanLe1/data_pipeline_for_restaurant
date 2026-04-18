import os
import json
import shutil
import logging
import asyncio
import pandas as pd
import clickhouse_connect
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

from src.extractors.cukcuk.extractor import CukCukExtractor
from src.transformers.generic_transformer import GenericTransformer
from src.loaders.clickhouse_loader import ClickHouseLoader
from src.loaders.s3_loader import S3Loader
from src.utils.ssm_loader import load_config

logger = logging.getLogger(__name__)

# Thư mục dùng chung (Shared Volume giữa các Task Airflow)
TEMP_DATA_DIR = "/opt/airflow/temp_data"

class CukCukETLPipeline:
    def __init__(self, config: dict):
        self.config = config
        
        # Init Modules
        self.extractor = CukCukExtractor(config)
        self.transformer = GenericTransformer(config)
        self.loader = ClickHouseLoader(config)
        self.s3_loader = S3Loader(config)

    def get_last_sync_from_db(self, table_name: str, branch_id: str = None) -> str:
        """
        Lấy mốc thời gian đồng bộ cuối cùng.
        Nếu truyền branch_id: Lấy MAX date của riêng chi nhánh đó.
        Nếu không truyền: Lấy MAX date toàn bảng (dùng cho Master data).
        """
        client = None
        try:
            ch_conf = self.config.get("ClickHouse-Config", {})
            db_name = ch_conf.get("database", "restaurant_db")
            
            # 1. Mapping cột thời gian
            time_column_map = {
                "order_header": "order_date",
                "invoice_header": "ref_date",
                "product": "report_date",
                "customer": "report_date"
            }
            target_col = time_column_map.get(table_name, "report_date")

            client = clickhouse_connect.get_client(
                host=ch_conf.get("host"),
                port=ch_conf.get("port"),
                username=ch_conf.get("user"),
                password=ch_conf.get("password"),
                database=db_name
            )
            
            # 2. Xây dựng Query có điều kiện Filter theo Branch
            query = f"SELECT MAX({target_col}) FROM {db_name}.{table_name}"
            
            # 👇 CẢI TIẾN: Chỉ lấy dữ liệu của chi nhánh đang cần fetch
            if branch_id:
                query += f" WHERE branch_id = '{branch_id}'"
            
            result = client.query(query).result_rows[0][0]
            
            # 3. Trả về kết quả nếu tìm thấy
            if result and not pd.isna(result):
                if isinstance(result, datetime):
                    return result.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                else:
                    # Nếu là kiểu Date (2026-04-13), thêm đuôi Time để API nhận dạng được
                    return f"{result}T00:00:00.000Z"

        except Exception as e:
            logger.warning(f"⚠️ Table {table_name} (Branch: {branch_id}) empty or error: {e}")
        finally:
            if client: client.close()
            
        
        fallback_dt = datetime.utcnow() - timedelta(days=3)
        return fallback_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    
    def _transform_and_get_df(self, raw_data: list, table_name: str) -> pd.DataFrame:
        """Helper biến đổi dữ liệu thô sang DataFrame chuẩn hóa."""
        if not raw_data:
            return pd.DataFrame()
        return self.transformer.transform(raw_data, table_name)
    
    def _load_dfs_to_clickhouse(self, dfs_map: dict):
        """Hàm song song hóa việc nạp danh sách DataFrame vào ClickHouse."""
        if not dfs_map:
            logger.warning("⚠️ No DataFrames to load into ClickHouse.")
            return

        logger.info(f"🚀 Loading {len(dfs_map)} tables into ClickHouse...")
        
        # Sử dụng ThreadPoolExecutor để nạp nhiều bảng cùng lúc (Concurrent Ingestion)
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for table_name, df in dfs_map.items():
                if not df.empty:
                    futures.append(executor.submit(
                        self._save_dataframe_worker, 
                        df, table_name
                    ))
            
            # Đảm bảo tất cả các luồng nạp dữ liệu hoàn tất
            for f in futures: 
                f.result()
        
        logger.info("✅ ClickHouse Load Finished successfully.")
    
    def _save_dataframe_worker(self, df: pd.DataFrame, table_name: str):
        """Worker thực hiện gọi loader để insert vào DB."""
        try:
            # ClickHouse tự xử lý Deduplicate qua ReplacingMergeTree dựa trên PK
            self.loader.save_table(df, table_name)
        except Exception as e:
            logger.error(f"❌ Failed writing table {table_name}: {e}")
            raise e

    async def _fetch_master_data_async(self):
        """Fetch song song danh mục sản phẩm và khách hàng."""
        task_prod = self.extractor.extract_products()
        task_cust = self.extractor.extract_customers()
        return await asyncio.gather(task_prod, task_cust)

    async def _fetch_transactions_async(self, start_str, end_dt):
        """Fetch song song đơn hàng và hóa đơn."""
        task_orders = self.extractor.extract_orders(start_str, end_dt)
        task_invoices = self.extractor.extract_invoices(start_str, end_dt)
        return await asyncio.gather(task_orders, task_invoices)

    def _transform_transactions(self, raw_orders, raw_invoices) -> dict:
        """Bóc tách và biến đổi dữ liệu Transaction (Header + Detail)."""
        dfs = {}

        # --- Transform Orders ---
        if raw_orders:
            dfs["order_header"] = self._transform_and_get_df(raw_orders, "order_header")
            
            # Flatten Details (Món ăn trong đơn)
            details = []
            for o in raw_orders:
                details.extend(o.get("line_items", []) or [])
            dfs["order_detail"] = self._transform_and_get_df(details, "order_detail")

        # --- Transform Invoices ---
        if raw_invoices:
            dfs["invoice_header"] = self._transform_and_get_df(raw_invoices, "invoice_header")
            
            # Flatten Details & Payments (Chi tiết hóa đơn & Cổng thanh toán)
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
    
    async def sync_master_to_s3(self):
        """
        Chiến thuật: Full Snapshot (API -> RAM -> S3).
        Lý do: Master Data không có timestamp/id tăng dần để filter.
        """
        logger.info("🚀 [Master Sync] Bắt đầu lấy bản chụp toàn bộ (Full Snapshot) danh mục...")
        
        # 1. Fetch toàn bộ dữ liệu từ API (Dữ liệu nằm hoàn toàn trong RAM)
        # Chúng ta không truyền last_sync vì API không hỗ trợ filter cho các bảng này.
        try:
            master_data = {
                "product": await self.extractor.extract_products(),
                "customer": await self.extractor.extract_customers()
            }

            # 2. Upload thẳng từ RAM lên S3 (Bronze Layer)
            for table, data in master_data.items():
                if data and len(data) > 0:
                    s3_key = f"raw/master/{table}_latest.jsonl"
                    
                    # Tận dụng function upload từ memory để không ghi file xuống Disk
                    self.s3_loader.upload_jsonl_from_memory(data, s3_key)
                    
                    logger.info(f"✅ Đã đẩy {len(data)} bản ghi {table} lên S3 (Full Snapshot): {s3_key}")
                else:
                    logger.warning(f"⚠️ Không nhận được dữ liệu {table} từ API.")

        except Exception as e:
            logger.error(f"❌ Lỗi trong quá trình Sync Master Data: {str(e)}")
            raise e

    def load_master_from_s3_to_clickhouse(self):
        logger.info("🚀 [Master Load] Đang nạp dữ liệu từ S3 vào ClickHouse...")
        
        tables = ["product", "customer"]
        errors = [] # Dùng danh sách để gom lỗi
        
        for table in tables:
            try:
                s3_path = f"raw/master/{table}_latest.jsonl"
                self.loader.load_from_s3(
                    s3_path=s3_path,
                    target_table=table
                )
                logger.info(f"✅ Đã nạp thành công bảng {table} từ S3.")
                
            except Exception as e:
                logger.error(f"❌ Lỗi nạp bảng {table} vào ClickHouse: {str(e)}")
                errors.append(str(e)) # Lưu lỗi lại
        
        # NẾU CÓ LỖI: Raise exception để Airflow báo Đỏ (Fail) cho Tuan biết
        if errors:
            raise Exception(f"❌ Master Load hoàn tất nhưng có {len(errors)} bảng bị lỗi: {errors}")

        logger.info("✅ Hoàn tất quy trình Master Load.")