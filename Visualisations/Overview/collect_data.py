"""
Script to collect and analyze data from the Omeka S platform for the Islam West Africa Collection.

This script connects to the Omeka S API, fetches all items in the collection, and provides
basic statistics about the collected data, including language distribution and word counts.
It uses environment variables for authentication and configuration.

Requirements:
    - .env file in project root with OMEKA_BASE_URL, IWAC_KEY_IDENTITY, and IWAC_KEY_CREDENTIAL
    - omeka_client.py module for API interaction
"""

import os
from pathlib import Path
from collections import Counter
from omeka_client import OmekaClient, OmekaConfig, setup_logging
from dotenv import load_dotenv

# Find and load the .env file from project root
root_dir = Path(__file__).resolve().parents[2]  # Go up 2 levels to reach project root
env_path = root_dir / '.env'
load_dotenv(env_path)

# Set up logging
logger = setup_logging('collection.log')
logger.name = 'collector'  # Change logger name for this script

def main() -> None:
    """
    Main function to collect and analyze data from Omeka S.
    
    This function:
    1. Verifies required environment variables
    2. Initializes the Omeka S client
    3. Fetches all items from the collection
    4. Calculates and logs statistics about:
       - Total number of items
       - Language distribution
       - Word count statistics
    
    Raises:
        ValueError: If required environment variables are missing
        Exception: For any other errors during execution
    """
    try:
        logger.info("=== Starting IWAC Data Collection Process ===")
        
        # Verify all required environment variables are present
        required_vars: list[str] = ['OMEKA_BASE_URL', 'IWAC_KEY_IDENTITY', 'IWAC_KEY_CREDENTIAL']
        missing_vars: list[str] = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
            
        logger.info("Environment variables verified successfully")
        logger.info("Initializing Omeka S client and fetching collection data...")
        
        # Initialize client
        client = OmekaClient()
        
        # Fetch all items from the API (filtering is handled in the client)
        items: list = client.fetch_all_data()
        
        if not items:
            logger.warning("Collection is empty - no items were found!")
            return

        # Calculate and log collection statistics
        logger.info(f"Successfully retrieved {len(items)} items from the collection")
        
        # Calculate language distribution across all items using Counter
        language_counts: Counter = Counter(item.language for item in items if item.language)
        
        # Log language distribution statistics
        logger.info("\n=== Language Distribution Analysis ===")
        if language_counts:
            total_items: int = len(items)
            for lang, count in language_counts.most_common():
                percentage: float = (count / total_items) * 100
                logger.info(f"{lang}: {count} items ({percentage:.1f}%)")
        else:
            logger.warning("No language information found in any items")
        
        # Calculate and log word count statistics
        total_words: int = sum(item.word_count or 0 for item in items)
        avg_words: float = total_words / len(items) if items else 0
        logger.info("\n=== Word Count Statistics ===")
        logger.info(f"Total words across all items: {total_words:,}")
        logger.info(f"Average words per item: {avg_words:,.2f}")
        
        logger.info("\n=== Data collection and analysis completed successfully ===")
        
    except Exception as e:
        logger.error(f"Critical error during data collection: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main() 