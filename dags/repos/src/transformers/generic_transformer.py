import pandas as pd
import logging
import pytz 
from typing import List, Dict
from datetime import datetime

logger = logging.getLogger(__name__)

class GenericTransformer:
    def __init__(self, config: dict):
        self.config = config
        self.branch_map = config.get("Branch-Name_Mapper", {})

    def transform(self, raw_data: List[Dict], table_name: str) -> pd.DataFrame:
        if not raw_data:
            return pd.DataFrame()

        df = pd.DataFrame(raw_data)
        
        # Define Vietnam timezone
        vn_tz = pytz.timezone('Asia/Ho_Chi_Minh')

        # ==============================================================================
        # 1. ENRICHMENT (Bổ sung thông tin trên dữ liệu THÔ)
        # ==============================================================================
        if "BranchId" in df.columns:
            temp_id_col = df["BranchId"].astype(str).str.strip().str.lower()
            
            if self.branch_map:
                clean_map = {str(k).strip().lower(): v for k, v in self.branch_map.items()}
                
                mapped_names = temp_id_col.map(clean_map)
                
                if "BranchName" in df.columns:
                    df["BranchName"] = mapped_names.fillna(df["BranchName"]).fillna(df["BranchId"])
                else:
                    df["BranchName"] = mapped_names.fillna(df["BranchId"])
            else:
                if "BranchName" not in df.columns:
                    df["BranchName"] = df["BranchId"]
                else:
                    df["BranchName"] = df["BranchName"].fillna(df["BranchId"])

        date_col_source = None
        possible_date_cols = ["Date", "RefDate", "OrderDate", "CreatedDate", "created_at"]
        for col in possible_date_cols:
            if col in df.columns:
                date_col_source = col
                break

        if date_col_source:
            try:
                temp_series = pd.to_datetime(df[date_col_source], format='ISO8601', utc=True, errors='coerce')
            except:
                temp_series = pd.to_datetime(df[date_col_source], errors='coerce', utc=True)
            
            if pd.api.types.is_datetime64_any_dtype(temp_series):
                temp_series_vn = temp_series.dt.tz_convert(vn_tz)
                df["DateFormat"] = temp_series_vn.dt.strftime("%Y-%m-%d")

        # ==============================================================================
        # 2. MAPPING & SAFEGUARD
        # ==============================================================================
        mapper = self.config.get("Column-Renaming", {}).get(table_name)
        
        if mapper:
            for source_col in mapper.keys():
                if source_col not in df.columns:
                    df[source_col] = None
            
            system_cols = ["DateFormat", "ProcessDate"]
            cols_to_keep = [col for col in df.columns if col in mapper.keys() or col in system_cols]
            
            df = df[cols_to_keep]
            df = df.rename(columns=mapper)
        else:
            logger.warning(f"No column renaming found for {table_name}. Keeping original columns.")

        # ==============================================================================
        # 3. TYPE CASTING (Xử lý Date chính thức)
        # ==============================================================================
        schema = self.config.get("Target-Schema", {}).get(table_name, {})
        
        for col, dtype in schema.items():
            if col in df.columns:
                try:
                    if dtype in ["float", "float4", "float8", "float64"]:
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
                    elif dtype in ["int", "int4", "int8", "int64"]:
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                    
                    elif dtype == "timestamp":
                        try:
                            df[col] = pd.to_datetime(df[col], format='ISO8601', utc=True, errors='coerce')
                        except:
                            df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)
                        df[col] = df[col].dt.tz_convert(vn_tz)
                        df[col] = df[col].dt.tz_localize(None)
                    
                    elif dtype in ["string", "text"]:
                        df[col] = df[col].astype("string")
                except Exception as e:
                    logger.warning(f"Failed to cast column {col} to {dtype}: {e}")

        # ==============================================================================
        # 4. PARTITIONING
        # ==============================================================================
        master_tables = ['customer', 'product', 'branch'] 
        
        if table_name in master_tables:
            now_vn = datetime.now(vn_tz)
            current_date_str = now_vn.strftime("%Y-%m-%d")
            
            df['report_date'] = current_date_str
            df['year'] = now_vn.year
            df['month'] = now_vn.month
            df['day'] = now_vn.day
        
        else:
            target_date_series = None
            if "DateFormat" in df.columns and df["DateFormat"].notna().any():
                target_date_series = pd.to_datetime(df["DateFormat"], errors='coerce')
            else:
                date_col = None
                check_cols = ['ref_date', 'order_date', 'date', 'created_at']
                for c in check_cols:
                    if c in df.columns:
                        date_col = c
                        break
                
                if date_col:
                    temp_dt = pd.to_datetime(df[date_col], errors='coerce', utc=True)
                    target_date_series = temp_dt.dt.tz_convert(vn_tz)

            if target_date_series is not None:
                df['report_date'] = target_date_series.dt.strftime("%Y-%m-%d")
                df['year'] = target_date_series.dt.year.fillna(0).astype(int)
                df['month'] = target_date_series.dt.month.fillna(0).astype(int)
                df['day'] = target_date_series.dt.day.fillna(0).astype(int)
                df['report_date'] = df['report_date'].fillna("1970-01-01")
            else:
                now_vn = datetime.now(vn_tz)
                current_date_str = now_vn.strftime("%Y-%m-%d")
                
                df['report_date'] = current_date_str
                df['year'] = now_vn.year
                df['month'] = now_vn.month
                df['day'] = now_vn.day
                logger.warning(f"⚠️ Table {table_name} has NO date column. Defaulting partition to TODAY: {current_date_str}")

        if table_name == "invoice_header":
             if "customer_id" in df.columns:
                non_null_out = df["customer_id"].notna().sum()
                logger.info(f"😎 [TRANSFORMER OUT] Table '{table_name}': 'customer_id' ready with {non_null_out} values.")

        return df