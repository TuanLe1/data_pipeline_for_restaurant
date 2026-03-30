import pandas as pd
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

class GenericTransformer:
    def __init__(self, config: dict):
        self.config = config
        self.branch_map = config.get("Branch-Name_Mapper", {})

    def transform(self, raw_data: List[Dict], table_name: str) -> pd.DataFrame:
        if not raw_data:
            return pd.DataFrame()

        df = pd.DataFrame(raw_data)
        
        # 1. ENRICHMENT: Map tên chi nhánh
        if "BranchId" in df.columns and self.branch_map:
            clean_map = {str(k).strip().lower(): v for k, v in self.branch_map.items()}
            # Tạo cột BranchName từ BranchId
            df["BranchName"] = df["BranchId"].astype(str).str.strip().str.lower().map(clean_map)
            # Nếu không map được thì giữ nguyên ID cũ để không bị mất dữ liệu
            df["BranchName"] = df["BranchName"].fillna(df["BranchId"])

        # 2. MAPPING & FILTERING
        mapper = self.config.get("Column-Renaming", {}).get(table_name)
        if mapper:
            # Xác định những cột cần giữ lại: Cột trong Mapper + Cột BranchName vừa tạo
            cols_to_keep = [col for col in df.columns if col in mapper.keys()]
            if "BranchName" in df.columns:
                cols_to_keep.append("BranchName")
            
            # Lọc dataframe
            df = df[list(set(cols_to_keep))] # Dùng set để tránh trùng lặp cột
            
            # Đổi tên các cột API sang snake_case
            df = df.rename(columns=mapper)
        else:
            logger.warning(f"⚠️ No mapping found for {table_name}. Sending raw columns to Snowflake.")

        return df