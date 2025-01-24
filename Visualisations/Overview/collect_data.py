#!/usr/bin/env python3
import os
import json
from pathlib import Path
from omeka_client import OmekaClient, OmekaConfig, setup_logging
from dotenv import load_dotenv

# Find and load the .env file from project root
root_dir = Path(__file__).resolve().parents[2]  # Go up 2 levels to reach project root
env_path = root_dir / '.env'
load_dotenv(env_path)

# Set up logging
logger = setup_logging('collection.log')
logger.name = 'collector'  # Change logger name for this script

def main():
    """Main function to collect data from Omeka S"""
    try:
        logger.info("Starting data collection process")
        
        # Setup paths - using script directory
        script_dir = Path(__file__).resolve().parent
        logger.info(f"Script directory: {script_dir}")
        
        # Verify environment variables
        required_vars = ['OMEKA_BASE_URL', 'IWAC_KEY_IDENTITY', 'IWAC_KEY_CREDENTIAL']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
            
        logger.info("Starting data collection from Omeka S")
        
        # Initialize client with script directory as cache dir
        client = OmekaClient(OmekaConfig(cache_dir=script_dir))
        
        # Fetch all items (force cache update)
        items = client.fetch_all_data(use_cache=False)
        
        if not items:
            logger.warning("No items were collected!")
            return
            
        # Print some statistics
        logger.info(f"Collected {len(items)} items")
        
        # Print language distribution
        language_counts = {}
        for item in items:
            if item.language:
                language_counts[item.language] = language_counts.get(item.language, 0) + 1
        
        logger.info("\nLanguage distribution:")
        for lang, count in sorted(language_counts.items()):
            logger.info(f"{lang}: {count} items")
        
        # Print word count statistics
        total_words = sum(item.word_count or 0 for item in items)
        avg_words = total_words / len(items) if items else 0
        logger.info(f"\nTotal word count: {total_words}")
        logger.info(f"Average words per item: {avg_words:.2f}")
        
        logger.info("Data collection completed successfully")
        
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main() 