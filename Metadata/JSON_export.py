import os
import json
import logging
import requests
from tqdm import tqdm
from dotenv import load_dotenv
from dataclasses import dataclass
from typing import List, Dict, Any, Callable

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

@dataclass
class Config:
    API_URL: str = os.getenv('OMEKA_BASE_URL')
    API_KEY_IDENTITY: str = os.getenv('OMEKA_KEY_IDENTITY')
    API_KEY_CREDENTIAL: str = os.getenv('OMEKA_KEY_CREDENTIAL')
    OUTPUT_DIR: str = os.path.join(os.path.dirname(__file__), 'JSON-LD')

class OmekaApiClient:
    def __init__(self, config: Config):
        self.config = config

    def _make_request(self, endpoint: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        if params is None:
            params = {}
        params.update({
            'key_identity': self.config.API_KEY_IDENTITY,
            'key_credential': self.config.API_KEY_CREDENTIAL
        })
        url = f"{self.config.API_URL}/{endpoint}"
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Error fetching data from API: {str(e)}")
            return []

    def fetch_items(self, resource_class_id: int) -> List[Dict[str, Any]]:
        items = []
        page = 1
        per_page = 100

        item_type = self.get_item_type_name(resource_class_id)
        logger.info(f"Starting to fetch {item_type}...")

        with tqdm(desc=f"Fetching {item_type}", unit="item") as pbar:
            while True:
                data = self._make_request('items', {
                    'resource_class_id': resource_class_id,
                    'page': page,
                    'per_page': per_page
                })
                if not data:
                    break
                items.extend(data)
                pbar.update(len(data))
                page += 1

        logger.info(f"Fetched {len(items)} {item_type}")
        return items

    def get_item_type_name(self, resource_class_id: int) -> str:
        item_type_map = {
            49: "documents",
            38: "audio/visual documents",
            58: "images",
            244: "index items (authority files)",
            54: "index items (events)",
            9: "index items (locations)",
            96: "index items (organizations)",
            94: "index items (persons)",
            60: "issues",
            36: "newspaper articles",
            35: "references (articles)",
            43: "references (chapters)",
            88: "references (theses)",
            40: "references (books)",
            82: "references (reports)",
            178: "references (reviews)",
            52: "references (books)",
            77: "references (communications)",
            305: "references (blog posts)"
        }
        return item_type_map.get(resource_class_id, f"items (class {resource_class_id})")

    def fetch_item_sets(self) -> List[Dict[str, Any]]:
        item_sets = []
        page = 1
        per_page = 100

        logger.info("Starting to fetch item sets...")

        with tqdm(desc="Fetching item sets", unit="item") as pbar:
            while True:
                data = self._make_request('item_sets', {
                    'page': page,
                    'per_page': per_page
                })
                if not data:
                    break
                item_sets.extend([item for item in data if item.get('o:is_public')])
                pbar.update(len(data))
                page += 1

        logger.info(f"Fetched {len(item_sets)} item sets")
        return item_sets

    def fetch_media(self) -> List[Dict[str, Any]]:
        media = []
        page = 1
        per_page = 100

        logger.info("Starting to fetch media...")

        with tqdm(desc="Fetching media", unit="item") as pbar:
            while True:
                data = self._make_request('media', {
                    'page': page,
                    'per_page': per_page
                })
                if not data:
                    break
                media.extend([item for item in data if item.get('o:is_public')])
                pbar.update(len(data))
                page += 1

        logger.info(f"Fetched {len(media)} media items")
        return media

    def fetch_references(self) -> List[Dict[str, Any]]:
        reference_classes = [35, 43, 88, 40, 82, 178, 52, 77, 305]
        references = []
        for resource_class_id in reference_classes:
            references.extend(self.fetch_items(resource_class_id))
        return references

    def fetch_all_items(self) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        logger.info("Starting to fetch all items...")
        
        documents = self.fetch_items(49)
        audio_visual = self.fetch_items(38)
        images = self.fetch_items(58)
        
        logger.info("Starting to fetch index items...")
        index_items = []
        for resource_class_id in [244, 54, 9, 96, 94]:
            index_items.extend(self.fetch_items(resource_class_id))
        logger.info(f"Fetched {len(index_items)} index items")
        
        issues = self.fetch_items(60)
        newspaper_articles = self.fetch_items(36)
        
        logger.info("Starting to fetch item sets...")
        item_sets = self.fetch_item_sets()
        
        logger.info("Starting to fetch media...")
        media = self.fetch_media()
        
        logger.info("Starting to fetch references...")
        references = self.fetch_references()
        
        logger.info("Finished fetching all items.")
        return documents + audio_visual + images + index_items + issues + newspaper_articles, item_sets, media, references

class JsonLdProcessor:
    def __init__(self, raw_data: List[Dict[str, Any]], item_sets: List[Dict[str, Any]], 
                 media: List[Dict[str, Any]], references: List[Dict[str, Any]]):
        self.raw_data = raw_data
        self.item_sets = item_sets
        self.media = media
        self.references = references
        self.processed_data = None

    def process(self) -> Dict[str, List[Dict[str, Any]]]:
        processed_data = {
            'audio_visual_documents': [],
            'documents': [],
            'images': [],
            'index': [],
            'issues': [],
            'item_sets': [],
            'media': [],
            'newspaper_articles': [],
            'references': []
        }

        for item in tqdm(self.raw_data, desc="Processing items"):
            item_type = self.determine_item_type(item)
            if item_type in processed_data:
                processed_data[item_type].append(item)

        processed_data['item_sets'] = self.item_sets
        processed_data['media'] = self.media
        processed_data['references'] = self.references

        logger.info("Data processing completed")
        self.processed_data = processed_data
        return processed_data

    def determine_item_type(self, item: Dict[str, Any]) -> str:
        item_types = item.get('@type', [])
        resource_class = item.get('o:resource_class', {}).get('o:id')
        if 'o:Item' in item_types:
            if 'bibo:AudioVisualDocument' in item_types:
                return 'audio_visual_documents'
            elif 'bibo:Document' in item_types:
                return 'documents'
            elif 'bibo:Image' in item_types:
                return 'images'
            elif resource_class in [244, 54, 9, 96, 94]:
                return 'index'
            elif resource_class == 60:
                return 'issues'
            elif resource_class == 36:
                return 'newspaper_articles'
        return 'other'  # Default category

class JsonLdFileGenerator:
    def __init__(self, processed_data: Dict[str, List[Dict[str, Any]]], output_dir: str):
        self.processed_data = processed_data
        self.output_dir = output_dir

    def generate_all_files(self):
        os.makedirs(self.output_dir, exist_ok=True)

        for item_type, items in self.processed_data.items():
            if items:
                self.generate_json_file(item_type, items)

    def generate_json_file(self, item_type: str, items: List[Dict[str, Any]]):
        filepath = os.path.join(self.output_dir, f"{item_type}.json")
        
        with open(filepath, 'w', encoding='utf-8') as jsonfile:
            json.dump(items, jsonfile, ensure_ascii=False, indent=2)

        logger.info(f"Generated {filepath}")

def main():
    try:
        logger.info("Starting the Omeka data export process...")
        
        config = Config()
        logger.info(f"Configuration loaded. API URL: {config.API_URL}")

        os.makedirs(config.OUTPUT_DIR, exist_ok=True)

        api_client = OmekaApiClient(config)

        raw_data, item_sets, media, references = api_client.fetch_all_items()

        if not raw_data and not item_sets and not media and not references:
            logger.warning("No data fetched from the API. Exiting.")
            return

        logger.info("Processing fetched data...")
        processor = JsonLdProcessor(raw_data, item_sets, media, references)
        processed_data = processor.process()

        logger.info("Generating JSON-LD files...")
        generator = JsonLdFileGenerator(processor.processed_data, config.OUTPUT_DIR)
        generator.generate_all_files()

        logger.info("All files generated successfully.")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()