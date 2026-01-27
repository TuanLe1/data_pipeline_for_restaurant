import logging
import asyncio
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

from src.extractors.cukcuk.extractor import CukCukExtractor
from src.transformers.generic_transformer import GenericTransformer
from src.loaders.s3_iceberg_loader import S3IcebergLoader

logger = logging.getLogger(__name__)

class CukCukETLPipeline:
    def __init__(self, config: dict):
        self.config = config
        self.partition_conf = config.get("Partition-Column", {})
        
        # Init Modules
        self.extractor = CukCukExtractor(config)
        self.transformer = GenericTransformer(config)
        self.loader = S3IcebergLoader(config)

    def _transform_and_get_df(self, raw_data: list, table_name: str) -> pd.DataFrame:
        """
        Helper: Chuyển List[Dict] -> Cleaned DataFrame bằng GenericTransformer
        """
        if not raw_data:
            return pd.DataFrame()
        return self.transformer.transform(raw_data, table_name)

    def _save_dataframe(self, df: pd.DataFrame, table_name: str, time_col: str = "report_date"):
        """
        Helper: Ghi DataFrame xuống S3 (I/O Bound) - Dùng cho ThreadPool
        """
        if df.empty:
            return
        try:
            # Lấy cấu hình partition từ config (nếu có), ví dụ: ['year', 'month']
            part_cols = self.partition_conf.get(table_name)
            
            self.loader.save_table(df, table_name, partition_cols=part_cols, time_col=time_col)
            # Log success được handle bên trong loader rồi, nhưng log thêm ở đây nếu cần
        except Exception as e:
            logger.error(f"❌ Failed writing table {table_name}: {e}")
            raise e

    # ==========================================================================
    # PHASE 1: MASTER DATA (Product, Customer)
    # ==========================================================================
    async def _fetch_master_data_async(self):
        task_prod = self.extractor.extract_products()
        task_cust = self.extractor.extract_customers()
        return await asyncio.gather(task_prod, task_cust)

    async def run_master_data_sync(self):
        """Chạy đồng bộ danh mục (Snapshot)"""
        logger.info("\n>>> [PHASE 1] SYNC MASTER DATA...")
        try:
            # 1. Fetch Async
            raw_prods, raw_custs = await self._fetch_master_data_async()
            
            # 2. Transform (In-Memory)
            df_prod = self._transform_and_get_df(raw_prods, "product")
            df_cust = self._transform_and_get_df(raw_custs, "customer")

            # 3. Load Parallel to S3
            logger.info("🚀 Writing Master Data to S3 in PARALLEL...")
            with ThreadPoolExecutor(max_workers=2) as executor:
                tasks = [
                    executor.submit(self._save_dataframe, df_prod, "product"),
                    executor.submit(self._save_dataframe, df_cust, "customer")
                ]
                for t in tasks: t.result()

            logger.info("✅ Master Data Synced.")
        except Exception as e:
            logger.error(f"❌ Master Data Failed: {e}")
            raise e

    # ==========================================================================
    # PHASE 2: TRANSACTIONS (Orders, Invoices)
    # ==========================================================================
    async def _fetch_transactions_async(self, start_str, end_dt):
        """Fetch Orders và Invoices cùng lúc"""
        task_orders = self.extractor.extract_orders(start_str, end_dt)
        task_invoices = self.extractor.extract_invoices(start_str, end_dt)
        return await asyncio.gather(task_orders, task_invoices)

    def run_single_day_process(self, target_date: datetime):
        """
        Xử lý toàn bộ dữ liệu Transaction của 1 ngày cụ thể.
        Hàm này bóc tách (flatten) JSON lồng nhau thành các bảng riêng biệt.
        """
        day_str = target_date.strftime("%Y-%m-%d")
        
        # Tạo khung thời gian từ 00:00:00 ngày này đến 00:00:00 ngày hôm sau
        iter_from = target_date.strftime("%Y-%m-%dT%H:%M:%S%z")
        iter_to = target_date + timedelta(days=1)
        
        logger.info(f"🔄 [Worker {day_str}] Started processing...")

        async def _async_wrapper():
            try:
                # 1. Fetch Data
                raw_orders, raw_invoices = await self._fetch_transactions_async(iter_from, iter_to)
                
                dfs_to_write = {} # Dictionary chứa {tên_bảng: dataframe}

                # --- XỬ LÝ ORDERS ---
                if raw_orders:
                    # Bảng Header
                    dfs_to_write["order_header"] = self._transform_and_get_df(raw_orders, "order_header")
                    
                    # Bảng Detail (Flattening)
                    raw_ord_details = []
                    for order in raw_orders:
                        # Lấy items con ra
                        items = order.get("line_items", []) or []
                        # (Optional) Nếu cần gán OrderID vào item con, xử lý ở đây
                        # Thường API CukCuk item con đã có sẵn OrderId rồi, nhưng nếu thiếu thì:
                        # parent_id = order.get("OrderId")
                        # for item in items: item["OrderId"] = parent_id
                        raw_ord_details.extend(items)
                        
                    dfs_to_write["order_detail"] = self._transform_and_get_df(raw_ord_details, "order_detail")
                else:
                    logger.info(f"   [{day_str}] ⚠️ No Orders found.")

                # --- XỬ LÝ INVOICES ---
                if raw_invoices:
                    # Bảng Header
                    dfs_to_write["invoice_header"] = self._transform_and_get_df(raw_invoices, "invoice_header")
                    
                    # Bảng Detail & Payment (Flattening)
                    raw_inv_details = []
                    raw_inv_payments = []
                    
                    for inv in raw_invoices:
                        # Tạo thông tin cha để gắn xuống con (quan trọng để Join sau này)
                        parent_info = {
                            "RefID": inv.get("RefId"), 
                            "BranchId": inv.get("BranchId"),
                            "BranchName": inv.get("BranchName"), 
                            "RefDate": inv.get("RefDate"),
                            "CustomerId": inv.get("CustomerId")
                        }
                        
                        # Tách Invoice Details
                        for item in inv.get("invoice_details", []) or []:
                            item.update(parent_info) # Kế thừa ID cha
                            raw_inv_details.append(item)
                            
                        # Tách Invoice Payments
                        for pay in inv.get("invoice_payments", []) or []:
                            pay.update(parent_info) # Kế thừa ID cha
                            raw_inv_payments.append(pay)
                    
                    dfs_to_write["invoice_detail"] = self._transform_and_get_df(raw_inv_details, "invoice_detail")
                    dfs_to_write["invoice_payment"] = self._transform_and_get_df(raw_inv_payments, "invoice_payment")
                else:
                    logger.info(f"   [{day_str}] ⚠️ No Invoices found.")

                # 3. Load to S3 (Parallel Write)
                if dfs_to_write:
                    logger.info(f"🚀 [Worker {day_str}] Writing {len(dfs_to_write)} tables to S3...")
                    
                    # Dùng ThreadPool để upload 4-5 bảng cùng lúc, tối ưu thời gian IO
                    with ThreadPoolExecutor(max_workers=5) as writer_pool:
                        futures = []
                        for tbl, df in dfs_to_write.items():
                            if not df.empty:
                                # 'report_date' là cột dùng để xóa dữ liệu cũ (Idempotency)
                                t_col = "report_date" 
                                futures.append(writer_pool.submit(self._save_dataframe, df, tbl, t_col))
                        
                        # Chờ tất cả upload xong
                        for f in futures:
                            f.result()
                    
                    logger.info(f"✅ [Worker {day_str}] Finished writing all tables.")
                
            except Exception as e:
                logger.error(f"❌ [Worker {day_str}] Failed: {e}")
                raise e

        # Chạy logic async trong event loop hiện tại
        asyncio.run(_async_wrapper())