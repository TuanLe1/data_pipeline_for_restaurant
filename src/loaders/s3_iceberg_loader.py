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
        
        # Setup session
        self.session = boto3.Session(region_name=config.get("AWS_REGION", "ap-southeast-2"))

    def save_table(self, df: pd.DataFrame, table_name: str, partition_cols: list = None, time_col: str = "report_date"):
        """
        Save data to Iceberg with DELETE (for dates in df) then INSERT mechanism.
        :param time_col: Name of the time column to identify data to delete (default is 'report_date')
        """
        if df.empty:
            logger.warning(f"⚠️ Dataframe for {table_name} is empty. Skip writing.")
            return
        # Path to save the actual data (Parquet/Iceberg data)
        s3_path = f"{self.s3_bucket}/{table_name}"
        
        # Path for Athena temporary files (Query Results)
        unique_id = str(uuid.uuid4())
        temp_path = f"{self.s3_bucket}/athena_temp/{table_name}/{unique_id}/"  
        table_exists = wr.catalog.does_table_exist(database=self.database, table=table_name, boto3_session=self.session)
        
        if table_exists and time_col in df.columns:
            # Get list of dates present in the new data
            dates_to_overwrite = df[time_col].unique().astype(str).tolist()
            
            if dates_to_overwrite:
                # Create SQL statement: DELETE FROM table WHERE report_date IN ('2025-12-29', '2025-12-30')
                dates_str = "', '".join(dates_to_overwrite)
                delete_query = f"DELETE FROM {table_name} WHERE {time_col} IN ('{dates_str}')"
                
                logger.info(f"🧹 [CLEANUP] Executing Delete for dates: {dates_to_overwrite} in table '{table_name}'...")
                
                try:
                    wr.athena.start_query_execution(
                        sql=delete_query,
                        database=self.database,
                        boto3_session=self.session,
                        wait=True
                    )
                    logger.info("✅ Old data cleaned successfully.")
                except Exception as e:
                    logger.error(f"⚠️ Failed to delete old data: {e}")

        logger.info(f"🚀 Writing {len(df)} rows to Iceberg table: {self.database}.{table_name}...")

        try:
            wr.athena.to_iceberg(
                df=df,
                database=self.database,
                table=table_name,
                table_location=s3_path,
                partition_cols=partition_cols if partition_cols else [],
                keep_files=False, 
                mode="append",
                boto3_session=self.session,
                temp_path=temp_path
            )
            logger.info(f"✅ Successfully wrote to Iceberg: {table_name}")
            
        except Exception as e:
            logger.critical(f"❌ Failed to write to S3/Iceberg: {e}")
            raise e