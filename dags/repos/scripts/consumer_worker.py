import redis
import json
import clickhouse_connect
import time
import logging
import requests  # Dùng để tải file từ MinIO
from src.utils.ssm_loader import load_config
from datetime import datetime, date

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("IngestionConsumer")

def flexible_get(data_dict, key_to_find):
    """
    Tìm giá trị trong dict không phân biệt hoa thường hoặc ID/Id.
    """
    if key_to_find in data_dict:
        return data_dict[key_to_find]
    
    # Chuẩn hóa toàn bộ key về lowercase để so khớp
    normalized_dict = {k.lower(): v for k, v in data_dict.items()}
    return normalized_dict.get(key_to_find.lower())

def map_and_clean_data(raw_data, table_name, config, parent_data=None):
    try:
        renaming_map = config['Column-Renaming'].get(table_name, {})
        target_schema = config['Target-Schema'].get(table_name, {})
        processed_row = {}

        # 1. Map dữ liệu thô & Di truyền Branch Name
        for raw_key, clean_key in renaming_map.items():
            val = flexible_get(raw_data, raw_key)
            if val is None and parent_data:
                # Nếu tìm OrderId/RefID không thấy, thử lấy 'Id' trực tiếp từ cha
                if clean_key in ['order_id', 'ref_id']:
                    val = flexible_get(parent_data, 'Id')
                else:
                    val = flexible_get(parent_data, raw_key)
            processed_row[clean_key] = val

        # 2. Làm giàu Branch Name nếu bị thiếu (Sửa lỗi dòng 3,4,5)
        if not processed_row.get('branch_name'):
            b_id = processed_row.get('branch_id')
            processed_row['branch_name'] = config.get('Branch-Name_Mapper', {}).get(b_id, "")

        # 3. Lấy giá trị ngày tháng "Gốc" để tính toán Year/Month/Day
        # Thử mọi key có thể chứa ngày tháng
        raw_date = None
        for k in ['order_date', 'ref_date', 'order_date', 'order_date']:
            if processed_row.get(k):
                raw_date = processed_row[k]
                break

        # 4. Ép kiểu chuẩn cho Target Schema
        final_row = {}
        for col_name, col_type in target_schema.items():
            value = processed_row.get(col_name)
            col_type_lower = col_type.lower()

            try:
                if 'timestamp' in col_type_lower or 'datetime' in col_type_lower:
                    # Parse ngày tháng linh hoạt
                    if isinstance(value, str):
                        # Cắt bỏ phần mili giây và múi giờ nếu cần để parse an toàn
                        ts_str = value.split('.')[0].replace('Z', '').split('+')[0].split('T')
                        if len(ts_str) == 2:
                             value = datetime.strptime(f"{ts_str[0]} {ts_str[1]}", '%Y-%m-%d %H:%M:%S')
                        else:
                             value = datetime.strptime(ts_str[0], '%Y-%m-%d')
                
                elif 'date' in col_type_lower:
                    if raw_date and col_name == 'report_date':
                        # Tính report_date từ raw_date
                        d_str = str(raw_date).split('T')[0]
                        value = datetime.strptime(d_str, '%Y-%m-%d').date()
                    elif isinstance(value, str):
                        value = datetime.strptime(value.split('T')[0], '%Y-%m-%d').date()
                
                elif 'int' in col_type_lower:
                    # Tính toán phái sinh cho Year/Month/Day
                    if col_name == 'year' and raw_date: value = int(str(raw_date)[:4])
                    elif col_name == 'month' and raw_date: value = int(str(raw_date)[5:7])
                    elif col_name == 'day' and raw_date: value = int(str(raw_date)[8:10])
                    else: value = int(float(value)) if value else 0
                
                elif 'float' in col_type_lower:
                    value = float(value) if value else 0.0
                else:
                    value = str(value) if value else ""
            except:
                # Gán mặc định nếu lỗi parse
                if 'int' in col_type_lower: value = 0
                elif 'float' in col_type_lower: value = 0.0
                elif 'date' in col_type_lower: value = date(1970, 1, 1)
                elif 'datetime' in col_type_lower: value = datetime(1970, 1, 1)
                else: value = ""

            final_row[col_name] = value

        return final_row
    except Exception as e:
        logger.error(f"❌ Mapping error: {repr(e)}")
        return None
        
def run_consumer():
    config = load_config()
    r = redis.Redis(host='redis-queue', port=6379, db=0)
    ch_conf = config['ClickHouse-Config']
    
    client = clickhouse_connect.get_client(
        host=ch_conf['host'], port=ch_conf['port'],
        username=ch_conf['user'], password=ch_conf['password'],
        database=ch_conf['database']
    )

    MAX_RETRIES = 3
    MAIN_QUEUE = "ingestion_queue"
    ERROR_QUEUE = "failed_ingestion_queue"

    logger.info("🚀 Consumer is active với logic Flexible Mapping & Inheritance...")

    while True:
        msg = r.brpop(MAIN_QUEUE)
        if not msg: continue
        
        job = json.loads(msg[1])
        table = job.get('table')
        path = job.get('path')
        retries = job.get('retry_count', 0)
        
        # --- QUAN TRỌNG: Khởi tạo danh sách ở đây để an toàn tuyệt đối ---
        final_data_to_insert = []
        s3_url = f"http://minio-lab:9000/restaurant-datalake/{path}"
        
        try:
            # 1. Tải file từ MinIO
            response = requests.get(s3_url, timeout=10)
            if response.status_code != 200:
                raise Exception(f"S3 Download Error: {response.status_code}")
            
            lines = response.text.strip().split('\n')
            for line in lines:
                if not line.strip(): continue
                raw_json = json.loads(line)

                # --- XỬ LÝ DETAIL (INVOICE/ORDER) ---
                if table in ['invoice_detail', 'order_detail']:
                    # Thử cả hai key, cái nào có thì lấy
                    items = flexible_get(raw_json, 'line_items') or flexible_get(raw_json, 'details') or []
                    
                    for item in items:
                        # Ép thêm ID của Header vào Item để chắc chắn có khóa ngoại
                        if 'Id' in raw_json and 'OrderId' not in item and 'RefID' not in item:
                             item['ParentId_Forced'] = raw_json['Id']
                             
                        clean_row = map_and_clean_data(item, table, config, parent_data=raw_json)
                        if clean_row: final_data_to_insert.append(clean_row)
                
                # --- XỬ LÝ HEADER/PRODUCT/CUSTOMER ---
                else:
                    clean_row = map_and_clean_data(raw_json, table, config)
                    if clean_row: final_data_to_insert.append(clean_row)

            # 2. Thực hiện Insert nếu có dữ liệu
            if final_data_to_insert:
                try:
                    # Lấy danh sách tên cột chuẩn từ Target-Schema
                    column_names = list(config['Target-Schema'][table].keys())
                    
                    # QUAN TRỌNG: Chuyển đổi dữ liệu từ Dict sang Tuple theo đúng thứ tự column_names
                    data_as_tuples = [
                        tuple(row.get(col) for col in column_names)
                        for row in final_data_to_insert
                    ]
                    
                    # Nạp dữ liệu dạng Tuple - ClickHouse sẽ không bao giờ bị KeyError nữa
                    client.insert(
                        table=table, 
                        data=data_as_tuples, 
                        column_names=column_names
                    )
                    
                    logger.info(f"✅ [SUCCESS] {table}: {len(data_as_tuples)} rows từ {path}")
                    
                except Exception as insert_err:
                    # In ra một dòng dữ liệu mẫu để debug nếu vẫn lỗi
                    sample = data_as_tuples[0] if data_as_tuples else "Empty"
                    logger.error(f"Sample data for {table}: {sample}")
                    raise Exception(f"ClickHouse Insert Error: {repr(insert_err)}")

        except Exception as e:
            logger.error(f"⚠️ [ATTEMPT {retries + 1}] Fail for {path}: {repr(e)}")
            
            # CƠ CHẾ RETRY
            if retries < MAX_RETRIES:
                job['retry_count'] = retries + 1
                r.lpush(MAIN_QUEUE, json.dumps(job))
                time.sleep(1) # Nghỉ 1s trước khi thử lại
            else:
                logger.critical(f"💀 [DEAD LETTER] Chuyển sang hàng chờ lỗi: {path}")
                job['error_log'] = repr(e)
                r.lpush(ERROR_QUEUE, json.dumps(job))

if __name__ == "__main__":
    run_consumer()