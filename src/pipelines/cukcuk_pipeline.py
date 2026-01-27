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
        """Only perform Transform and return DataFrame (In-memory)"""
        if not raw_data:
            return pd.DataFrame()
        return self.transformer.transform(raw_data, table_name)

    def _save_dataframe(self, df: pd.DataFrame, table_name: str, time_col: str = None):
        """Only perform Load (IO Bound) - Used for ThreadPool"""
        if df.empty:
            return
        try:
            part_cols = self.partition_conf.get(table_name)
            self.loader.save_table(df, table_name, partition_cols=part_cols, time_col=time_col)
            logger.info(f"✅ Successfully wrote table: {table_name}")
        except Exception as e:
            logger.error(f"❌ Failed writing table {table_name}: {e}")
            raise e

    # ==========================================================================
    # PHASE 1: MASTER DATA
    # ==========================================================================
    async def _fetch_master_data_async(self):
        task_prod = self.extractor.extract_products()
        task_cust = self.extractor.extract_customers()
        return await asyncio.gather(task_prod, task_cust)

    async def run_master_data_sync(self):
        logger.info("\n>>> [PHASE 1] SYNC MASTER DATA...")
        try:
            raw_prods, raw_custs = await self._fetch_master_data_async()
            df_prod = self._transform_and_get_df(raw_prods, "product")
            df_cust = self._transform_and_get_df(raw_custs, "customer")

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

    # ==========================================================================
    # PHASE 2: TRANSACTIONS
    # ==========================================================================
    async def _fetch_transactions_async(self, start_str, end_dt):
        task_orders = self.extractor.extract_orders(start_str, end_dt)
        task_invoices = self.extractor.extract_invoices(start_str, end_dt)
        return await asyncio.gather(task_orders, task_invoices)

    def run_single_day_process(self, target_date: datetime):
        """
        worker_process_day: process all data for a single day
        :param target_date: datetime object representing the target date to process
        """
        day_str = target_date.strftime("%Y-%m-%d")
        iter_from = target_date.strftime("%Y-%m-%dT%H:%M:%S%z")
        iter_to = target_date + timedelta(days=1)
        
        logger.info(f"🔄 [Worker {day_str}] Started processing...")

        async def _async_wrapper():
            try:
                raw_orders, raw_invoices = await self._fetch_transactions_async(iter_from, iter_to)
                dfs_to_write = {}

                if raw_orders:
                    dfs_to_write["order_header"] = self._transform_and_get_df(raw_orders, "order_header")
                    
                    raw_ord_details = []
                    for order in raw_orders:
                        raw_ord_details.extend(order.get("line_items", []))
                    dfs_to_write["order_detail"] = self._transform_and_get_df(raw_ord_details, "order_detail")
                else:
                    logger.info(f"   [{day_str}] ⚠️ No Orders")

                if raw_invoices:
                    dfs_to_write["invoice_header"] = self._transform_and_get_df(raw_invoices, "invoice_header")
                    
                    raw_inv_details = []
                    raw_inv_payments = []
                    for inv in raw_invoices:
                        parent_info = {
                            "RefID": inv.get("RefId"), "BranchId": inv.get("BranchId"),
                            "BranchName": inv.get("BranchName"), "RefDate": inv.get("RefDate"),
                            "CustomerId": inv.get("CustomerId")
                        }
                        for item in inv.get("invoice_details", []):
                            item.update(parent_info)
                            raw_inv_details.append(item)
                        for pay in inv.get("invoice_payments", []):
                            pay.update(parent_info)
                            raw_inv_payments.append(pay)
                    
                    dfs_to_write["invoice_detail"] = self._transform_and_get_df(raw_inv_details, "invoice_detail")
                    dfs_to_write["invoice_payment"] = self._transform_and_get_df(raw_inv_payments, "invoice_payment")
                else:
                    logger.info(f"   [{day_str}] ⚠️ No Invoices")

                if dfs_to_write:
                    logger.info(f"🚀 [Worker {day_str}] Writing {len(dfs_to_write)} tables to S3 in PARALLEL...")
                    
                    with ThreadPoolExecutor(max_workers=5) as writer_pool:
                        futures = []
                        for tbl, df in dfs_to_write.items():
                            if not df.empty:
                                t_col = "report_date" 
                                futures.append(writer_pool.submit(self._save_dataframe, df, tbl, t_col))
                        
                        for f in futures:
                            f.result()
                    
                    logger.info(f"✅ [Worker {day_str}] Finished writing all tables.")
                
            except Exception as e:
                logger.error(f"❌ [Worker {day_str}] Failed: {e}")
                raise e

        asyncio.run(_async_wrapper())