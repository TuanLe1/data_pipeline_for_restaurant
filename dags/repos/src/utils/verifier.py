# src/utils/verifier.py
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class DataVerifier:
    def __init__(self, raw_data: list, transformed_df: pd.DataFrame, table_name: str):
        self.raw_data = raw_data
        self.df = transformed_df
        self.table_name = table_name

    def validate(self) -> bool:
        """
        Returns True if data passes all validation checks, else False.
        """
        logger.info(f"🛡️ Validating quality for table: {self.table_name}...")
        
        is_valid = True
        
        if self.table_name in ["order_header", "invoice_header"]:
            if len(self.raw_data) != len(self.df):
                logger.error(f"❌ Count Mismatch! Raw input: {len(self.raw_data)}, Transformed output: {len(self.df)}")
                is_valid = False
            else:
                logger.info(f"✅ Count Check Passed: {len(self.df)} rows")

        pk_candidates = [
            "order_id",         # Order Header
            "ref_id",           # Invoice Header
            "customer_id",      # Customer
            "product_id",       # Product
            "order_detail_id",  # Order Detail
            "ref_detail_id",    # Invoice Detail
            "invoice_payment_id", # Invoice Payment
            "Id", "id"          # Fallback
        ]
        
        pk_col = next((col for col in pk_candidates if col in self.df.columns), None)
        
        if pk_col:
            if self.df[pk_col].isnull().any() or (self.df[pk_col] == "").any():
                logger.error(f"❌ Found NULL or Empty values in Primary Key column: {pk_col}")
                is_valid = False
            else:
                logger.info(f"✅ PK Check Passed ({pk_col})")
        else:
            logger.warning(f"⚠️ Could not identify Primary Key for table {self.table_name}. Skiping PK check.")
            logger.warning(f"   Available columns: {self.df.columns.tolist()}")
        
        if "total_amount" in self.df.columns:
            if (self.df["total_amount"] < 0).any():
                logger.warning(f"⚠️ Warning: Found negative total_amount in {self.table_name} (Possible returns)")

        if is_valid:
            logger.info(f"✅ Validation Passed for {self.table_name}.")
        else:
            logger.error(f"⛔ Validation Failed for {self.table_name}.")
            
        return is_valid