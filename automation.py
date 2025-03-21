import snowflake.connector
import pandas as pd
import logging
from datetime import datetime
import dotenv
import os
from dune_client.client import DuneClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Snowflake connection parameters
SNOWFLAKE_CONFIG = {
    'user': 'DEFI_FROM_KAFKA',
    'password': 'en6sQ232Yv',
    'account': 'ua97033.us-east-1',
    'warehouse': 'DEFI_DEFAULT',
    'database': 'SOLIDUS_DEFI',
    'schema': 'EVM'
}

def query_lazarus_group():
    try:
        # Connect to Snowflake
        logger.info("Connecting to Snowflake...")
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        
        # Create cursor
        cur = conn.cursor()
        
        # Define query
        query = """
        SELECT *
        FROM watchlist_v3
        WHERE ARRAY_TO_STRING(CLASSIFICATIONS, ',') LIKE '%Lazarus Group%'
        """
        
        # Execute query
        logger.info("Executing query...")
        cur.execute(query)
        
        # Fetch results
        results = cur.fetchall()
        
        # Get column names
        columns = [desc[0] for desc in cur.description]
        
        # Create DataFrame
        df = pd.DataFrame(results, columns=columns)
        
        # Generate timestamp for filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f'lazarus_group_results_{timestamp}.csv'
        
        # Save full results to CSV
        logger.info(f"Saving full results to {output_file}...")
        df.to_csv(output_file, index=False)
        
        # Extract addresses and prepare for Dune
        dune_df = pd.DataFrame()
        dune_df['lazarus_addresses'] = df['ADDRESS']  # Assuming the column is named 'ADDRESS'
        dune_file = f'lazarus_addresses_{timestamp}.csv'
        dune_df.to_csv(dune_file, index=False)
        
        logger.info(f"Query completed successfully. Found {len(df)} records.")
        logger.info(f"Full results saved to {output_file}")
        logger.info(f"Dune-formatted addresses saved to {dune_file}")
        
        return dune_file
        
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        raise
        
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
            logger.info("Snowflake connection closed")

def upload_to_dune(csv_file_path):
    try:
        logger.info("Initializing Dune client...")
        # Set API key directly
        os.environ['DUNE_API_KEY'] = 'T1Zw9Dn8KVFXbNHPgg5Q6iWoSOrOTooP'
        dune = DuneClient.from_env()
        
        logger.info(f"Reading CSV file: {csv_file_path}")
        with open(csv_file_path) as open_file:
            data = open_file.read()
            
        logger.info("Uploading to Dune...")
        table = dune.upload_csv(
            data=str(data),
            description="Lazarus Group addresses extracted from Solidus watchlist",
            table_name="lazarus_v3",
            is_private=False
        )
        
        logger.info("Upload to Dune completed successfully")
        return table
        
    except Exception as e:
        logger.error(f"Error uploading to Dune: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        # Query Snowflake and get addresses
        dune_file = query_lazarus_group()
        
        # Upload to Dune
        logger.info("Proceeding with Dune upload...")
        upload_to_dune(dune_file)
        
        logger.info("Process completed successfully")
    except Exception as e:
        logger.error(f"Process failed: {str(e)}")