import clickhouse_connect
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

class ClickHouseLoader:
    def __init__(self, config: dict):
        self.config = config
        self.target_schema = config.get("Target-Schema", {})
        self.ch_conf = config.get("ClickHouse-Config", {})
        try:
            self.client = clickhouse_connect.get_client(
                host=self.ch_conf.get("host", "clickhouse-lab"),
                port=self.ch_conf.get("port", 8123),
                username=self.ch_conf.get("user", "admin"),
                password=self.ch_conf.get("password", "password"),
                database=self.ch_conf.get("database", "restaurant_db")
            )
            logger.info("✅ ClickHouseLoader initialized successfully.")
        except Exception as e:
            logger.error(f"❌ Failed to connect to ClickHouse: {e}")
            raise e

    def _get_ch_type(self, simple_type: str) -> str:
        """Chuyển đổi type từ config sang ClickHouse syntax (luôn dùng Nullable cho an toàn)"""
        mapping = {
            "String": "Nullable(String)",
            "Int32": "Nullable(Int32)",
            "Float32": "Nullable(Float32)",
            "timestamp": "Nullable(DateTime)",
            "Date": "Nullable(Date)",
            "bool": "Nullable(Int8)"
        }
        return mapping.get(simple_type, "Nullable(String)")

    def create_table_if_not_exists(self, client, table_name: str):
        if table_name not in self.target_schema: return
        
        col_definitions = []
        for col, dtype in self.target_schema[table_name].items():
            ch_type = self._get_ch_type(dtype)
            if col.endswith('_id') or col == 'ref_id': ch_type = "String" 
            col_definitions.append(f"`{col}` {ch_type}")

        # --- FIX LỖI 122 DÒNG CÒN 1 ---
        if table_name == 'invoice_detail':
            pk = "ref_id, ref_detail_id" # Đảm bảo không nén mất món ăn
        elif table_name == 'order_detail':
            pk = "order_id, order_detail_id"
        else:
            pk = self.config.get("DB-Table-Primary-Key", {}).get(table_name, "report_date")

        create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(col_definitions)}) " \
                     f"ENGINE = ReplacingMergeTree() ORDER BY ({pk})"
        client.command(create_sql)

    def get_ingestion_sql(self, table_name, s3_full_path):
        s3_user = self.config["S3-Config"]["access_key"]
        s3_pass = self.config["S3-Config"]["secret_key"]
        
        # 🛡️ Dùng OrNull để an toàn, kết hợp với ifNull để tránh lỗi parse chuỗi rỗng
        date_func = "parseDateTimeBestEffortOrNull"

        # 1. Logic cho bảng Detail (Giữ nguyên vì đã SUCCESS, chỉ đồng bộ date_func)
        if table_name in ["invoice_detail", "order_detail"]:
            if table_name == "invoice_detail":
                return f"""
                INSERT INTO {table_name} 
                (ref_detail_id, ref_id, item_id, item_name, unit_name, unit_price, quantity, amount, ref_date)
                SELECT 
                    JSONExtractString(li, 'Id'), Id, JSONExtractString(li, 'ItemId'),
                    JSONExtractString(li, 'ItemName'), JSONExtractString(li, 'UnitName'),
                    JSONExtractFloat(li, 'Price'), JSONExtractFloat(li, 'Quantity'),
                    JSONExtractFloat(li, 'Amount'), {date_func}(Date)
                FROM s3('{s3_full_path}', '{s3_user}', '{s3_pass}', 'JSONEachRow', 'Id String, Date String, line_items Array(String)')
                ARRAY JOIN line_items AS li
                """
            else:
                return f"""
                INSERT INTO {table_name} 
                (order_detail_id, order_id, item_id, quantity, price, amount, order_date)
                SELECT 
                    JSONExtractString(li, 'Id'), Id, JSONExtractString(li, 'ItemId'),
                    JSONExtractFloat(li, 'Quantity'), JSONExtractFloat(li, 'Price'),
                    JSONExtractFloat(li, 'Amount'), {date_func}(Date)
                FROM s3('{s3_full_path}', '{s3_user}', '{s3_pass}', 'JSONEachRow', 'Id String, Date String, line_items Array(String)')
                ARRAY JOIN line_items AS li
                """

        # 2. Logic cho bảng Header (SỬA LỖI 20 VÀ 41)
        mapper = self.config.get("Column-Renaming", {}).get(table_name, {})
        target_schema = self.config.get("Target-Schema", {}).get(table_name, {})
        
        target_columns = [] # Danh sách cột đích trong ClickHouse
        select_parts = []   # Các biểu thức SELECT từ JSON
        
        for api_col, db_col in mapper.items():
            ch_type = target_schema.get(db_col, "String")
            target_columns.append(f"`{db_col}`")
            
            # Trích xuất dữ liệu thô
            if "Int" in ch_type:
                ext_expr = f"JSONExtractInt(raw, '{api_col}')"
            elif "Float" in ch_type:
                ext_expr = f"JSONExtractFloat(raw, '{api_col}')"
            else:
                ext_expr = f"JSONExtractString(raw, '{api_col}')"
            
            # Xử lý ngày tháng
            if "date" in db_col:
                # 🛡️ Nếu key rỗng, trả về NULL thay vì parse lỗi
                select_parts.append(f"if(JSONHas(raw, '{api_col}'), {date_func}(JSONExtractString(raw, '{api_col}')), NULL) AS {db_col}")
            else:
                select_parts.append(f"{ext_expr} AS {db_col}")

        # QUAN TRỌNG: Chỉ định danh sách cột trong lệnh INSERT để tránh lỗi mismatch (12 vs 17)
        cols_str = ", ".join(target_columns)
        select_str = ", ".join(select_parts)

        return f"""
        INSERT INTO {table_name} ({cols_str})
        SELECT {select_str}
        FROM s3('{s3_full_path}', '{s3_user}', '{s3_pass}', 'JSONAsString', 'raw String')
        """

    def save_table(self, df: pd.DataFrame, table_name: str):
        if df.empty: return

        client = clickhouse_connect.get_client(
            host     = self.ch_conf.get("host", "localhost"),
            port     = self.ch_conf.get("port", 8123),
            username = self.ch_conf.get("user", "default"),
            password = self.ch_conf.get("password", ""),
            database = self.ch_conf.get("database", "default")
        )

        try:
            # 1. Đảm bảo bảng tồn tại trước khi lấy metadata
            self.create_table_if_not_exists(client, table_name)
            
            # 2. Lấy Metadata thực tế từ DB
            table_info = client.query(f"DESCRIBE TABLE {table_name}").result_rows
            
            # Tra cứu Metadata: db_columns dùng để filter và sắp xếp df
            db_columns = [row[0] for row in table_info]
            is_nullable_map = {row[0]: ('Nullable' in row[1]) for row in table_info}
            type_map = {row[0]: row[1] for row in table_info}

            # 3. LÀM SẠCH VÀ CĂN CHỈNH DATAFRAME (QUAN TRỌNG)
            # - Bỏ các cột df có mà DB không có
            # - Thêm các cột DB có mà df thiếu (gán None)
            # - Sắp xếp thứ tự cột y hệt trong DB
            for col in db_columns:
                if col not in df.columns:
                    df[col] = None
            
            df = df[db_columns] # Ép df theo đúng danh sách và thứ tự cột của DB
            df = df.replace({np.nan: None, pd.NA: None, pd.NaT: None})

            final_data = []
            # Cột nạp bây giờ luôn là danh sách cột của DB
            columns = db_columns 

            for row in df.to_dict('records'):
                clean_row = []
                for col in columns:
                    val = row[col]
                    is_nullable = is_nullable_map.get(col, False)
                    ch_type = type_map.get(col, 'String')

                    # --- LOGIC XỬ LÝ NULL AN TOÀN ---
                    if val is None:
                        if is_nullable:
                            clean_row.append(None)
                        else:
                            if 'Int' in ch_type: clean_row.append(0)
                            elif 'Float' in ch_type: clean_row.append(0.0)
                            elif 'Date' in ch_type: clean_row.append(pd.Timestamp('1970-01-01').date())
                            else: clean_row.append('')
                    
                    # --- ÉP KIỂU DỮ LIỆU CHUẨN ---
                    else:
                        try:
                            if 'String' in ch_type:
                                clean_row.append(str(val))
                            elif 'Date' in ch_type and 'DateTime' not in ch_type:
                                clean_row.append(pd.to_datetime(val).date())
                            elif 'DateTime' in ch_type:
                                clean_row.append(pd.to_datetime(val).to_pydatetime())
                            elif 'Int' in ch_type:
                                clean_row.append(int(float(val)))
                            elif 'Float' in ch_type:
                                clean_row.append(float(val))
                            else:
                                clean_row.append(val)
                        except:
                            clean_row.append(None if is_nullable else "")
                            
                final_data.append(tuple(clean_row))

            logger.info(f"🚀 Ingesting {len(final_data)} rows into {table_name} (Aligned with DB Schema)")
            client.insert(table=table_name, data=final_data, column_names=columns)

        except Exception as e:
            logger.error(f"❌ ClickHouse Ingest Error [{table_name}]: {e}")
            raise e
        finally:
            client.close()

    def load_from_s3(self, s3_path: str, target_table: str):
        s3_conf = self.config.get("S3-Config", {})
        ch_conf = self.config.get("ClickHouse-Config", {})
        db_name = ch_conf.get("database", "restaurant_db")
        
        all_renaming = self.config.get("Column-Renaming", {})
        mapping = all_renaming.get(target_table, {})

        if not mapping:
            logger.warning(f"⚠️ Không tìm thấy mapping cho {target_table}")
            return # Tránh chạy tiếp gây lỗi
        
        # 👇 1. Tạo danh sách cột Destination (ClickHouse)
        column_names = list(mapping.values())
        # Chỉ append 1 lần duy nhất cho tất cả các cột meta
        column_names.extend(["report_date", "year", "month", "day"])
        
        # 👇 2. Tạo chuỗi liệt kê cột cho INSERT (Phải làm SAU KHI đã có đủ danh sách cột)
        insert_cols_str = f"({', '.join([f'`{c}`' for c in column_names])})"

        # 👇 3. Tạo chuỗi SELECT từ JSON
        select_items = [f"{src_key} AS {dst_col}" for src_key, dst_col in mapping.items()]
        # Thêm logic thời gian đồng bộ với column_names
        select_items.append("today() AS report_date")
        select_items.append("toYear(today()) AS year")
        select_items.append("toMonth(today()) AS month")
        select_items.append("toDayOfMonth(today()) AS day")
        
        select_clause = ", ".join(select_items)

        full_s3_url = f"{s3_conf['endpoint']}/{s3_conf['bucket_name']}/{s3_path}"
        
        query = f"""
            INSERT INTO {db_name}.{target_table} {insert_cols_str}
            SELECT 
                {select_clause}
            FROM s3(
                '{full_s3_url}',
                '{s3_conf['access_key']}',
                '{s3_conf['secret_key']}',
                'JSONEachRow'
            )
        """
        
        try:
            self.client.command(query)
            logger.info(f"✅ ClickHouse: Đã nạp {target_table} thành công với full metadata!")
        except Exception as e:
            logger.error(f"❌ ClickHouse: Lỗi nạp S3 cho {target_table}: {str(e)}")
            raise e