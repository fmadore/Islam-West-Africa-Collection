#!/usr/bin/env python3
import os
import json
from datetime import datetime, timedelta
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

def should_update(metadata_file: Path, update_interval_hours: int = 24) -> bool:
    """Check if data should be updated based on last collection time"""
    if not metadata_file.exists():
        logger.info("No metadata file found, update needed")
        return True
        
    try:
        with metadata_file.open('r') as f:
            metadata = json.load(f)
            last_update = datetime.fromisoformat(metadata['last_update'])
            next_update = last_update + timedelta(hours=update_interval_hours)
            should_update = datetime.now() > next_update
            logger.info(f"Last update: {last_update}, Next update: {next_update}")
            return should_update
    except (json.JSONDecodeError, KeyError, ValueError) as e:
        logger.warning(f"Error reading metadata file: {e}")
        return True

def save_metadata(metadata_file: Path, items_count: int):
    """Save collection metadata including timestamp"""
    metadata = {
        'last_update': datetime.now().isoformat(),
        'items_count': items_count,
        'version': '1.0'
    }
    
    try:
        with metadata_file.open('w') as f:
            json.dump(metadata, f, indent=2)
        logger.info(f"Metadata saved successfully: {metadata}")
    except IOError as e:
        logger.error(f"Failed to save metadata: {e}")
        raise

def main():
    """Main function to collect data from Omeka S"""
    try:
        logger.info("Starting data collection process")
        
        # Setup paths - using script directory
        script_dir = Path(__file__).resolve().parent
        metadata_file = script_dir / 'metadata.json'
        
        logger.info(f"Script directory: {script_dir}")
        logger.info(f"Metadata file: {metadata_file}")
        
        # Verify environment variables
        required_vars = ['OMEKA_BASE_URL', 'IWAC_KEY_IDENTITY', 'IWAC_KEY_CREDENTIAL']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        # Check if update is needed
        if not should_update(metadata_file):
            logger.info("Data is up to date, no collection needed")
            return
            
        logger.info("Starting data collection from Omeka S")
        
        # Initialize client with script directory as cache dir
        client = OmekaClient(OmekaConfig(cache_dir=script_dir))
        
        # Fetch all items (force cache update)
        items = client.fetch_all_data(use_cache=False)
        
        if not items:
            logger.warning("No items were collected!")
            return
            
        # Save metadata
        save_metadata(metadata_file, len(items))
        
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