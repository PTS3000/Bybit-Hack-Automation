#!/usr/bin/env python3
import sys
import logging
from pathlib import Path
from datetime import datetime
from automation import query_lazarus_group, upload_to_dune

# Set up logging to file
log_dir = Path('logs')
log_dir.mkdir(exist_ok=True)

log_file = log_dir / f'automation_{datetime.now().strftime("%Y%m%d")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)

def main():
    try:
        # Query Snowflake and get addresses
        dune_file = query_lazarus_group()
        
        # Upload to Dune
        logging.info("Proceeding with Dune upload...")
        upload_to_dune(dune_file)
        
        logging.info("Process completed successfully")
    except Exception as e:
        logging.error(f"Process failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 