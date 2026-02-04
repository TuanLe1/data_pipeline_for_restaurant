import awswrangler as wr
import pandas as pd
import logging
import boto3
import uuid

logger = logging.getLogger(__name__)

class S3IcebergLoader:
    def __init__(self, config: dict):
        self.config = config
        self.database = config.get("AWS_GLUE_DB", "restaurant_db")
        self.s3_bucket = config.get("AWS_S3_BUCKET", "s3://my-restaurant-datalake")
        self.region = config.get("AWS_REGION", "ap-southeast-2")
        
        self.session = boto3.Session(region_name=self.region)

    def save_table(self, df: pd.DataFrame, table_name: str, partition_cols: list = None, time_col: str = "report_date"):
        """
        SAFE VERSION: Append Only.
        Không bao giờ xóa dữ liệu cũ. Việc lọc trùng lặp (Deduplication) sẽ do dbt đảm nhận.
        """
        if df.empty:
            logger.warning(f"⚠️ Dataframe for {table_name} is empty. Skip writing.")
            return

        # Đường dẫn lưu dữ liệu
        s3_path = f"{self.s3_bucket}/{table_name}"
        
        # Đường dẫn temp cho Athena (Bắt buộc unique)
        unique_id = str(uuid.uuid4())
        temp_path = f"{self.s3_bucket}/athena_temp/{table_name}/{unique_id}/"
        
        # ---------------------------------------------------------
        # 🚨 ĐÃ XÓA LOGIC DELETE (CLEANUP) Ở ĐÂY ĐỂ TRÁNH MẤT DATA
        # ---------------------------------------------------------

        logger.info(f"🚀 Appending {len(df)} rows to Iceberg table: {self.database}.{table_name}...")

        try:
            # Ghi thẳng dữ liệu mới vào (Append)
            wr.athena.to_iceberg(
                df=df,
                database=self.database,
                table=table_name,
                table_location=s3_path,
                partition_cols=partition_cols if partition_cols else [],
                keep_files=False, 
                
                # QUAN TRỌNG: Mode luôn là 'append'
                mode="append", 
                
                # Tự động thêm cột mới nếu API thay đổi
                schema_evolution=True, 
                
                boto3_session=self.session,
                temp_path=temp_path
            )
            logger.info(f"✅ Successfully appended to Iceberg: {table_name}")
            
        except Exception as e:
            logger.critical(f"❌ Failed to write to S3/Iceberg: {e}")
            raise e