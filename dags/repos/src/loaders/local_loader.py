import pandas as pd
import os
from typing import List, Dict, Union
import logging

logger = logging.getLogger(__name__)

class LocalLoader:
    def __init__(self, base_path: str, config: Dict = None):
        self.base_path = base_path
        self.config = config 

    def save_parquet(self, data: Union[List[Dict], pd.DataFrame], table_name: str, partition_cols: List[str] = None):
        if isinstance(data, pd.DataFrame):
            if data.empty:
                logger.warning(f"⚠️ [LOADER] No data to save for {table_name}")
                return
            df = data.copy()
        else:
            if not data:
                logger.warning(f"⚠️ [LOADER] No data to save for {table_name}")
                return
            df = pd.DataFrame(data)

        save_path = os.path.join(self.base_path, table_name)
        
        try:
            valid_partitions = []
            if partition_cols:
                valid_partitions = [col for col in partition_cols if col in df.columns]
                
                if len(valid_partitions) != len(partition_cols):
                    missing = set(partition_cols) - set(valid_partitions)
                    logger.warning(f"⚠️ [LOADER] Missing partition cols {missing} in table {table_name}. Saving as flat file.")

            if valid_partitions:
                df.to_parquet(
                    path=save_path,
                    engine='pyarrow',
                    compression='snappy',
                    partition_cols=valid_partitions,
                    existing_data_behavior='delete_matching', 
                    index=False
                )
                logger.info(f"📂 [LOADER] Data saved to {save_path} (Partitioned by {valid_partitions})")
            
            else:
                os.makedirs(save_path, exist_ok=True)
                file_path = os.path.join(save_path, "data.parquet")
                
                df.to_parquet(
                    path=file_path,
                    engine='pyarrow',
                    compression='snappy',
                    index=False
                )
                logger.info(f"💾 [LOADER] Data saved to {file_path}")
            
        except Exception as e:
            logger.error(f"❌ [LOADER] Error saving parquet for {table_name}: {e}")
            raise e