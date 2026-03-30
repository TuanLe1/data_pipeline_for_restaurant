import pandas as pd
import logging
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector

logger = logging.getLogger(__name__)

class SnowflakeLoader:
    def __init__(self, config: dict):
        self.config = config
        sf_config = self.config.get("SNOWFLAKE", {})
        self.conn = snowflake.connector.connect(
            account=sf_config.get("ACCOUNT"),
            user=sf_config.get("USER"),
            password=sf_config.get("PASSWORD"),
            role=sf_config.get("ROLE", "DE_ROLE"),
            warehouse=sf_config.get("WAREHOUSE", "RESTAURANT_ETL_WH"),
            database=sf_config.get("DATABASE", "RESTAURANT_DB"),
            schema="BRONZE"
        )

    def _create_variant_table_if_not_exists(self, table_name: str):
        """Đảm bảo bảng Bronze có cấu trúc VARIANT"""
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS BRONZE.{table_name} (
            RAW_DATA VARIANT,
            INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
        self.conn.cursor().execute(create_sql)

    def save_table(self, data: list, table_name: str):
        """
        Nạp một danh sách các dict vào Snowflake dưới dạng JSON VARIANT.
        """
        if not data:
            logger.warning(f"⚠️ No data to load for {table_name}. Skipping.")
            return

        # Chuẩn hóa tên bảng: RAW_ORDER_HEADER_JSON
        table_name_upper = f"RAW_{table_name.upper()}_JSON"
        self._create_variant_table_if_not_exists(table_name_upper)

        # Chuyển list dict -> DataFrame -> 1 cột JSON
        df = pd.DataFrame(data)
        upload_df = pd.DataFrame({
            'RAW_DATA': df.apply(lambda x: x.to_dict(), axis=1)
        })

        try:
            write_pandas(
                conn=self.conn,
                df=upload_df,
                table_name=table_name_upper,
                database=self.config.get("SNOWFLAKE", {}).get("DATABASE"),
                schema="BRONZE",
                auto_create_table=False
            )
            logger.info(f"✅ Ingested {len(upload_df)} rows to {table_name_upper}")
        except Exception as e:
            logger.error(f"❌ Failed to load {table_name_upper}: {e}")
            raise e

    def load_extracted_data(self, extracted_data: dict, data_type: str):
        """
        HÀM MỚI: Tự động điều hướng dữ liệu từ Extractor vào các bảng tương ứng.
        data_type: 'orders', 'invoices', 'customers', hoặc 'products'
        """
        if data_type == 'orders':
            # Nạp 2 bảng: ORDER_HEADER và ORDER_DETAIL
            self.save_table(extracted_data.get('headers', []), 'ORDER_HEADER')
            self.save_table(extracted_data.get('details', []), 'ORDER_DETAIL')
            
        elif data_type == 'invoices':
            # Nạp 3 bảng: INVOICE_HEADER, INVOICE_DETAIL, INVOICE_PAYMENT
            self.save_table(extracted_data.get('headers', []), 'INVOICE_HEADER')
            self.save_table(extracted_data.get('details', []), 'INVOICE_DETAIL')
            self.save_table(extracted_data.get('payments', []), 'INVOICE_PAYMENT')
            
        elif data_type == 'customers':
            self.save_table(extracted_data, 'CUSTOMER')
            
        elif data_type == 'products':
            self.save_table(extracted_data, 'PRODUCT')

    def close(self):
        self.conn.close()