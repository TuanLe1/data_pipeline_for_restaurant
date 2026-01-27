import awswrangler as wr
import pandas as pd
import logging
import boto3
import uuid
import time

logger = logging.getLogger(__name__)

class S3IcebergLoader:
    def __init__(self, config: dict):
        self.config = config
        # Tự động lấy DB từ config, fallback về 'restaurant_db'
        self.database = config.get("AWS_GLUE_DB", "restaurant_db")
        self.s3_bucket = config.get("AWS_S3_BUCKET", "s3://my-restaurant-datalake")
        self.region = config.get("AWS_REGION", "ap-southeast-2")
        
        # Setup session
        self.session = boto3.Session(region_name=self.region)

    def save_table(self, df: pd.DataFrame, table_name: str, partition_cols: list = None, time_col: str = "report_date"):
        """
        Save data to Iceberg with Idempotency (DELETE existing dates -> INSERT new).
        Supports Schema Evolution (automatically adds new columns).
        """
        if df.empty:
            logger.warning(f"⚠️ Dataframe for {table_name} is empty. Skip writing.")
            return

        # Path to save the actual data
        s3_path = f"{self.s3_bucket}/{table_name}"
        
        # Path for Athena temporary files (QUAN TRỌNG: Phải unique để tránh conflict giữa các workers)
        unique_id = str(uuid.uuid4())
        temp_path = f"{self.s3_bucket}/athena_temp/{table_name}/{unique_id}/"
        
        # 1. Kiểm tra bảng có tồn tại không
        table_exists = wr.catalog.does_table_exist(database=self.database, table=table_name, boto3_session=self.session)
        
        # 2. Xử lý xóa dữ liệu cũ (Idempotency)
        # Chỉ chạy lệnh DELETE nếu bảng đã tồn tại VÀ có cột thời gian để lọc
        if table_exists and time_col in df.columns:
            dates_to_overwrite = df[time_col].unique().astype(str).tolist()
            
            if dates_to_overwrite:
                # Format: '2023-01-01', '2023-01-02'
                dates_str = "', '".join(dates_to_overwrite)
                delete_query = f"DELETE FROM {table_name} WHERE {time_col} IN ('{dates_str}')"
                
                logger.info(f"🧹 [CLEANUP] Executing Delete for dates: {dates_to_overwrite} in table '{table_name}'...")
                
                try:
                    wr.athena.start_query_execution(
                        sql=delete_query,
                        database=self.database,
                        boto3_session=self.session,
                        wait=True # Bắt buộc chờ xóa xong mới được ghi
                    )
                    logger.info("✅ Old data cleaned successfully.")
                except Exception as e:
                    # Nếu lỗi xóa (ví dụ bảng chưa có cột đó), log warning nhưng vẫn cố gắng ghi đè
                    logger.warning(f"⚠️ Failed to delete old data (Proceeding to write anyway): {e}")

        logger.info(f"🚀 Writing {len(df)} rows to Iceberg table: {self.database}.{table_name}...")

        # 3. Ghi dữ liệu mới (Append)
        try:
            wr.athena.to_iceberg(
                df=df,
                database=self.database,
                table=table_name,
                table_location=s3_path,
                partition_cols=partition_cols if partition_cols else [],
                keep_files=False, 
                mode="append", # Luôn là append vì ta đã xóa dữ liệu cũ ở bước trên
                schema_evolution=True, # <--- [NEW] Tự động thêm cột mới nếu API thay đổi
                boto3_session=self.session,
                temp_path=temp_path
            )
            logger.info(f"✅ Successfully wrote to Iceberg: {table_name}")
            
        except Exception as e:
            logger.critical(f"❌ Failed to write to S3/Iceberg: {e}")
            raise e