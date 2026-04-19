import os
import sys
import logging

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from src.loaders.clickhouse_loader import ClickHouseLoader
from src.utils.ssm_loader import load_config
import clickhouse_connect

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("DB_Initializer")

# ── Gold mart tables that should have TTL tiered storage ───────────────────
# Silver tables (invoice_header, invoice_detail, etc.) are queried via
# ReplacingMergeTree and need to stay hot for incremental dbt runs.
# Gold mart tables hold pre-aggregated data — older months rarely queried,
# safe to move to cold MinIO storage after 30 days.
MART_TABLES_WITH_TTL = {
    "master_sales_analytics": "report_date",   # TTL column
    "revenue_daily":          "report_date",   # TTL column
}

TTL_HOT_DAYS = 30  # days to keep on local NVMe before moving to MinIO


def create_mart_table_with_ttl(client, table_name: str, ttl_col: str):
    """
    Create a Gold mart table with:
    - ReplacingMergeTree engine
    - SETTINGS storage_policy = 'hot_to_cold'
    - TTL clause: move parts to cold disk after TTL_HOT_DAYS

    This is separate from the Silver table creation in ClickHouseLoader
    because mart tables need the TTL + storage policy, while Silver
    tables should stay fully hot for fast incremental dbt runs.
    """

    # master_sales_analytics schema
    if table_name == "master_sales_analytics":
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        (
            branch_name          Nullable(String),
            order_id             String,
            invoice_code         Nullable(String),
            customer_id          String,
            ref_detail_id        String,
            item_name            Nullable(String),
            item_id              String,
            ref_date             DateTime,
            report_date          Nullable(Date),
            report_hour          Nullable(String),
            quantity             Nullable(Int32),
            net_revenue_pre_tax  Nullable(Float64),
            tax_amount           Float32,
            net_revenue_inclusive Nullable(Float64)
        )
        ENGINE = ReplacingMergeTree(ref_date)
        ORDER BY (report_date, order_id, item_id, ref_detail_id)
        -- CHANGE: TTL moves parts older than {TTL_HOT_DAYS} days to MinIO cold disk.
        -- Data is still queryable from cold storage, just slower (network I/O).
        -- In production replace toDate({ttl_col}) with the actual date column.
        TTL toDate({ttl_col}) + INTERVAL {TTL_HOT_DAYS} DAY TO DISK 'minio_cold'
        SETTINGS
            storage_policy = 'hot_to_cold',
            allow_nullable_key = 1,
            replicated_deduplication_window = 0,
            index_granularity = 8192
        """

    # revenue_daily schema
    elif table_name == "revenue_daily":
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        (
            report_date   Date,
            branch_name   String,
            year          Int32,
            month         Int32,
            total_orders  Int64,
            total_revenue Float64,
            aov           Float64
        )
        ENGINE = MergeTree()
        ORDER BY (report_date, branch_name)
        -- CHANGE: same TTL policy — move old daily summaries to cold after 30 days
        TTL report_date + INTERVAL {TTL_HOT_DAYS} DAY TO DISK 'minio_cold'
        SETTINGS
            storage_policy = 'hot_to_cold',
            index_granularity = 8192
        """
    else:
        logger.warning(f"No DDL defined for mart table: {table_name}")
        return

    client.command(ddl)
    logger.info(
        f"✅ Mart table '{table_name}' ready "
        f"(TTL: {TTL_HOT_DAYS}d hot → MinIO cold)"
    )


def init():
    logger.info("🏗️ Starting database initialization...")

    try:
        config   = load_config()
        ch_conf  = config.get("ClickHouse-Config", {})
        target_db = ch_conf.get("database", "restaurant_db")

        # Step 1: connect to default DB to create target DB
        logger.info("🔗 Connecting to 'default' to bootstrap database...")
        client = clickhouse_connect.get_client(
            host=ch_conf.get("host"),
            port=ch_conf.get("port"),
            username=ch_conf.get("user"),
            password=ch_conf.get("password"),
            database='default'
        )
        client.command(f'CREATE DATABASE IF NOT EXISTS {target_db}')
        client.close()

        # Step 2: connect to target DB
        logger.info(f"📍 Connecting to '{target_db}'...")
        db_client = clickhouse_connect.get_client(
            host=ch_conf.get("host"),
            port=ch_conf.get("port"),
            username=ch_conf.get("user"),
            password=ch_conf.get("password"),
            database=target_db
        )

        # Step 3: verify storage policy is available
        # (requires storage_policy.xml to be mounted and ClickHouse restarted)
        try:
            result = db_client.query(
                "SELECT name FROM system.storage_policies WHERE name = 'hot_to_cold'"
            ).result_rows
            if result:
                logger.info("✅ Storage policy 'hot_to_cold' detected — TTL will be applied.")
                has_storage_policy = True
            else:
                logger.warning(
                    "⚠️ Storage policy 'hot_to_cold' not found. "
                    "Mart tables will be created WITHOUT TTL. "
                    "Mount storage_policy.xml and restart ClickHouse to enable tiered storage."
                )
                has_storage_policy = False
        except Exception:
            has_storage_policy = False
            logger.warning("⚠️ Could not check storage policies — creating tables without TTL.")

        # Step 4: create Silver (Bronze raw) tables via loader
        loader = ClickHouseLoader(config)
        silver_tables = [
            t for t in config.get("Target-Schema", {}).keys()
            if t not in MART_TABLES_WITH_TTL
        ]

        for table_name in silver_tables:
            logger.info(f"🔨 Creating Silver table: {target_db}.{table_name}")
            loader.create_table_if_not_exists(db_client, table_name)

        # Step 5: create Gold mart tables
        # CHANGE: mart tables get TTL + storage_policy if available,
        # otherwise fall back to plain table without tiered storage.
        for table_name, ttl_col in MART_TABLES_WITH_TTL.items():
            logger.info(f"🔨 Creating Gold mart table: {target_db}.{table_name}")
            if has_storage_policy:
                create_mart_table_with_ttl(db_client, table_name, ttl_col)
            else:
                # Fallback: create via standard loader without TTL
                loader.create_table_if_not_exists(db_client, table_name)
                logger.info(f"   ↳ Created without TTL (no storage policy)")

        logger.info(f"✅ Database '{target_db}' fully initialized.")
        db_client.close()

    except Exception as e:
        logger.critical(f"💥 Initialization failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    init()