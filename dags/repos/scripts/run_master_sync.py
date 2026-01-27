import sys
import os
import asyncio
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# 👇 Import chuẩn
from src.utils.ssm_loader import load_config
from src.pipelines.cukcuk_pipeline import CukCukETLPipeline

def main():
    logging.info("🚀 [Script] Starting Master Data Sync...")
    
    try:
        config = load_config()
        logging.info("✅ Config loaded successfully.")
    except Exception as e:
        logging.error(f"Failed to load config: {e}")
        sys.exit(1)

    pipeline = CukCukETLPipeline(config)
    asyncio.run(pipeline.run_master_data_sync()) 

if __name__ == "__main__":
    main()