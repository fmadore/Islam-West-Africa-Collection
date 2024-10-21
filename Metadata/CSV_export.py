import os
import csv
import logging
import requests
from tqdm import tqdm
from dotenv import load_dotenv
from dataclasses import dataclass
from typing import List, Dict, Any, Callable
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import time

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
    OUTPUT_DIR: str = os.path.join(os.path.dirname(__file__), 'CSV')

class OmekaApiClient:
    def __init__(self, config: Config):
        self.config = config
        self.session = self._create_retry_session()

    def _create_retry_session(self):
        session = requests.Session()
        retries = Retry(total=5,
                        backoff_factor=0.1,
                        status_forcelist=[500, 502, 503, 504])
        session.mount('https://', HTTPAdapter(max_retries=retries))
        return session

    def _make_request(self, endpoint: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        if params is None:
            params = {}
        params.update({
            'key_identity': self.config.API_KEY_IDENTITY,
            'key_credential': self.config.API_KEY_CREDENTIAL
        })
        url = f"{self.config.API_URL}/{endpoint}"
        max_retries = 5
        retry_delay = 5  # seconds

        for attempt in range(max_retries):
            try:
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                return response.json()
            except requests.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"Max retries reached. Unable to fetch data from API: {str(e)}")
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
                if len(data) < per_page:
                    break  # Last page reached

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
            52: "references (communications)",
            77: "references (newspapers)",
            305: "references (web sites)"
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

    def fetch_item_set_titles(self) -> Dict[int, str]:
        item_set_titles = {}
        page = 1
        per_page = 100

        logger.info("Fetching item set titles...")

        while True:
            data = self._make_request('item_sets', {
                'page': page,
                'per_page': per_page
            })
            if not data:
                break
            for item_set in data:
                item_set_id = item_set['o:id']
                item_set_titles[item_set_id] = item_set.get('o:title', '')
            page += 1

        logger.info(f"Fetched {len(item_set_titles)} item set titles")
        return item_set_titles

class DataProcessor:
    def __init__(self, raw_data: List[Dict[str, Any]], item_sets: List[Dict[str, Any]], 
                 media: List[Dict[str, Any]], references: List[Dict[str, Any]], 
                 item_set_titles: Dict[int, str]):
        self.raw_data = raw_data
        self.item_sets = item_sets
        self.media = media
        self.references = references
        self.item_set_titles = item_set_titles
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

        mapping_functions = {
            'audio_visual_documents': map_audio_visual_document,
            'documents': map_document,
            'images': map_image,
            'index': map_index,
            'issues': map_issue,
            'newspaper_articles': map_newspaper_article
        }

        for item in tqdm(self.raw_data, desc="Processing items"):
            item_type = self.determine_item_type(item)
            if item_type in processed_data:
                mapping_function = mapping_functions.get(item_type, lambda x: x)
                processed_data[item_type].append(mapping_function(item))

        processed_data['item_sets'] = [map_item_set(item_set) for item_set in tqdm(self.item_sets, desc="Processing item sets")]
        processed_data['media'] = [map_media(media_item) for media_item in tqdm(self.media, desc="Processing media")]
        processed_data['references'] = [map_reference(reference) for reference in tqdm(self.references, desc="Processing references")]

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

    def process_item_sets(self):
        for item_type, items in self.processed_data.items():
            for item in items:
                if 'o:item_set' in item:
                    item_set_urls = item['o:item_set'].split('|')
                    item_set_names = []
                    for url in item_set_urls:
                        item_set_id = url.split('/')[-1]
                        if item_set_id.isdigit():
                            item_set_names.append(self.item_set_titles.get(int(item_set_id), ''))
                    item['o:item_set'] = '|'.join(filter(None, item_set_names))

class FileGenerator:
    def __init__(self, processed_data: Dict[str, List[Dict[str, Any]]], output_dir: str):
        self.processed_data = processed_data
        self.output_dir = output_dir

    def generate_all_files(self):
        os.makedirs(self.output_dir, exist_ok=True)

        for item_type, items in self.processed_data.items():
            if items:
                self.generate_csv_file(item_type, items)

    def generate_csv_file(self, item_type: str, items: List[Dict[str, Any]]):
        filepath = os.path.join(self.output_dir, f"{item_type}.csv")
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            if items:
                fieldnames = items[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(items)

        logger.info(f"Generated {filepath}")

def get_value(item: Dict[str, Any], field: str, subfield: str = None) -> str:
    """Utility function to safely get a value from an item."""
    if field not in item or item[field] is None:
        return ''
    
    # Special cases
    if field == 'dcterms:rights':
        if item[field] and isinstance(item[field], list) and len(item[field]) > 0:
            return str(item[field][0].get('o:label', '') or item[field][0].get('@value', ''))
        return ''
    if field == 'bibo:doi':
        if item[field] and isinstance(item[field], list) and len(item[field]) > 0:
            return str(item[field][0].get('o:label', '') or item[field][0].get('@value', ''))
        return ''
    if field == 'fabio:hasURL':
        if item[field] and isinstance(item[field], list) and len(item[field]) > 0:
            return str(item[field][0].get('@id', ''))
        return ''
    
    if isinstance(item[field], list):
        display_titles = [str(v.get('display_title', '')) for v in item[field] if 'display_title' in v]
        if display_titles:
            return '|'.join(filter(None, display_titles))
        values = [str(v.get('@value', '')) for v in item[field] if '@value' in v]
        return '|'.join(filter(None, values))
    if subfield:
        if isinstance(item[field], dict):
            return str(item[field].get('display_title', '') or item[field].get('@value', ''))
        else:
            return ''
    return str(item[field])

def join_values(item: Dict[str, Any], field: str, subfield: str) -> str:
    """Utility function to join multiple values with a separator."""
    if field not in item:
        return ''
    if field == 'o:item_set':
        return '|'.join([str(val.get('@id', '')) for val in item[field]])
    display_titles = [str(val.get('display_title', '')) for val in item[field] if isinstance(val, dict) and 'display_title' in val]
    if display_titles:
        return '|'.join(filter(None, display_titles))
    values = [str(val.get('@value', '')) for val in item[field] if isinstance(val, dict) and '@value' in val]
    return '|'.join(filter(None, values))

def map_document(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'o:id': get_value(item, 'o:id'),
        'url': f"https://islam.zmo.de/s/afrique_ouest/item/{get_value(item, 'o:id')}",
        'dcterms:identifier': get_value(item, 'dcterms:identifier'),
        'o:resource_class': 'bibo:Document',
        'o:item_set': join_values(item, 'o:item_set', ''),
        'o:media/file': get_media_ids(item),
        'dcterms:title': get_value(item, 'dcterms:title'),
        'dcterms:creator': join_values(item, 'dcterms:creator', ''),
        'dcterms:date': get_value(item, 'dcterms:date'),
        'dcterms:abstract': get_value(item, 'dcterms:abstract'),
        'bibo:numPages': get_value(item, 'bibo:numPages'),
        'dcterms:subject': join_values(item, 'dcterms:subject', ''),
        'dcterms:spatial': join_values(item, 'dcterms:spatial', ''),
        'dcterms:rights': get_value(item, 'dcterms:rights'),
        'dcterms:rightsHolder': get_value(item, 'dcterms:rightsHolder'),
        'dcterms:language': get_value(item, 'dcterms:language'),
        'dcterms:source': get_value(item, 'dcterms:source'),
        'dcterms:contributor': join_values(item, 'dcterms:contributor', ''),
        'bibo:content': get_value(item, 'bibo:content'),
    }

def map_audio_visual_document(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'o:id': get_value(item, 'o:id'),
        'url': f"https://islam.zmo.de/s/afrique_ouest/item/{get_value(item, 'o:id')}",
        'dcterms:identifier': get_value(item, 'dcterms:identifier'),
        'o:resource_class': 'bibo:AudioVisualDocument',
        'o:item_set': join_values(item, 'o:item_set', ''),
        'o:media/file': get_media_ids(item),
        'dcterms:title': get_value(item, 'dcterms:title'),
        'dcterms:creator': join_values(item, 'dcterms:creator', ''),
        'dcterms:publisher': join_values(item, 'dcterms:publisher', ''),
        'dcterms:description': get_value(item, 'dcterms:description'),
        'dcterms:date': get_value(item, 'dcterms:date'),
        'bibo:volume': get_value(item, 'bibo:volume'),
        'bibo:issue': get_value(item, 'bibo:issue'),
        'dcterms:isPartOf': get_value(item, 'dcterms:isPartOf'),
        'dcterms:extent': get_value(item, 'dcterms:extent'),
        'dcterms:medium': get_value(item, 'dcterms:medium'),
        'dcterms:subject': join_values(item, 'dcterms:subject', ''),
        'dcterms:spatial': join_values(item, 'dcterms:spatial', ''),
        'dcterms:rights': get_value(item, 'dcterms:rights'),
        'dcterms:rightsHolder': get_value(item, 'dcterms:rightsHolder'),
        'dcterms:language': get_value(item, 'dcterms:language'),
        'dcterms:source': get_value(item, 'dcterms:source'),
        'dcterms:contributor': join_values(item, 'dcterms:contributor', ''),
        'bibo:content': get_value(item, 'bibo:content'),
    }

def map_image(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'o:id': get_value(item, 'o:id'),
        'url': f"https://islam.zmo.de/s/afrique_ouest/item/{get_value(item, 'o:id')}",
        'dcterms:identifier': get_value(item, 'dcterms:identifier'),
        'o:resource_class': 'bibo:Image',
        'o:item_set': join_values(item, 'o:item_set', ''),
        'o:media/file': get_media_ids(item),
        'dcterms:title': get_value(item, 'dcterms:title'),
        'dcterms:creator': join_values(item, 'dcterms:creator', ''),
        'dcterms:date': get_value(item, 'dcterms:date'),
        'dcterms:description': get_value(item, 'dcterms:description'),
        'dcterms:subject': join_values(item, 'dcterms:subject', ''),
        'dcterms:rights': get_value(item, 'dcterms:rights'),
        'dcterms:source': get_value(item, 'dcterms:source'),
        'dcterms:spatial': join_values(item, 'dcterms:spatial', ''),
        'coordinates': get_value(item, 'curation:coordinates'),
    }

def map_index(item: Dict[str, Any]) -> Dict[str, Any]:
    resource_class_id = item.get('o:resource_class', {}).get('o:id')
    resource_class_map = {
        244: 'fabio:AuthorityFile',
        54: 'bibo:Event',
        9: 'dcterms:Location',
        96: 'foaf:Organization',
        94: 'foaf:Person'
    }

    def get_fr_value(field: str) -> str:
        values = item.get(field, [])
        fr_values = [v['@value'] for v in values if v.get('@language') == 'fr']
        return '|'.join(fr_values) if fr_values else get_value(item, field)

    return {
        'o:id': get_value(item, 'o:id'),
        'url': f"https://islam.zmo.de/s/afrique_ouest/item/{get_value(item, 'o:id')}",
        'dcterms:identifier': get_value(item, 'dcterms:identifier'),
        'o:resource_class': resource_class_map.get(resource_class_id, ''),
        'o:item_set': join_values(item, 'o:item_set', ''),
        'o:media/file': get_media_ids(item),
        'dcterms:title': get_fr_value('dcterms:title'),
        'dcterms:alternative': get_fr_value('dcterms:alternative'),
        'dcterms:created': get_value(item, 'dcterms:created'),
        'dcterms:date': get_value(item, 'dcterms:date'),
        'dcterms:description': get_value(item, 'dcterms:description'),
        'dcterms:relation': join_values(item, 'dcterms:relation', ''),
        'dcterms:isReplacedBy': join_values(item, 'dcterms:isReplacedBy', ''),
        'dcterms:replaces': join_values(item, 'dcterms:replaces', ''),
        'dcterms:isPartOf': get_value(item, 'dcterms:isPartOf'),
        'dcterms:hasPart': get_value(item, 'dcterms:hasPart'),
        'dcterms:spatial': join_values(item, 'dcterms:spatial', ''),
        'foaf:firstName': get_value(item, 'foaf:firstName'),
        'foaf:lastName': get_value(item, 'foaf:lastName'),
        'foaf:gender': get_value(item, 'foaf:gender'),
        'foaf:birthday': get_value(item, 'foaf:birthday'),
        'coordinates': get_value(item, 'curation:coordinates'),
    }

def map_issue(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'o:id': get_value(item, 'o:id'),
        'url': f"https://islam.zmo.de/s/afrique_ouest/item/{get_value(item, 'o:id')}",
        'dcterms:identifier': get_value(item, 'dcterms:identifier'),
        'o:resource_class': 'bibo:Issue',
        'o:item_set': join_values(item, 'o:item_set', ''),
        'o:media/file': get_media_ids(item),
        'dcterms:title': get_value(item, 'dcterms:title'),
        'dcterms:creator': join_values(item, 'dcterms:creator', ''),
        'dcterms:publisher': join_values(item, 'dcterms:publisher', ''),
        'dcterms:date': get_value(item, 'dcterms:date'),
        'bibo:issue': get_value(item, 'bibo:issue'),
        'dcterms:abstract': get_value(item, 'dcterms:abstract'),
        'bibo:numPages': get_value(item, 'bibo:numPages'),
        'dcterms:subject': join_values(item, 'dcterms:subject', ''),
        'dcterms:spatial': join_values(item, 'dcterms:spatial', ''),
        'dcterms:rights': get_value(item, 'dcterms:rights'),
        'dcterms:rightsHolder': get_value(item, 'dcterms:rightsHolder'),
        'dcterms:language': get_value(item, 'dcterms:language'),
        'dcterms:source': get_value(item, 'dcterms:source'),
        'dcterms:contributor': join_values(item, 'dcterms:contributor', ''),
        'fabio:hasURL': get_value(item, 'fabio:hasURL'),
        'bibo:content': get_value(item, 'bibo:content'),
    }

def map_newspaper_article(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'o:id': get_value(item, 'o:id'),
        'url': f"https://islam.zmo.de/s/afrique_ouest/item/{get_value(item, 'o:id')}",
        'dcterms:identifier': get_value(item, 'dcterms:identifier'),
        'o:resource_class': 'bibo:Article',
        'o:item_set': join_values(item, 'o:item_set', ''),
        'o:media/file': get_media_ids(item),
        'dcterms:title': get_value(item, 'dcterms:title'),
        'dcterms:creator': join_values(item, 'dcterms:creator', ''),
        'dcterms:publisher': join_values(item, 'dcterms:publisher', ''),
        'dcterms:date': get_value(item, 'dcterms:date'),
        'dcterms:abstract': get_value(item, 'dcterms:abstract'),
        'bibo:pages': get_value(item, 'bibo:pages'),
        'bibo:numPages': get_value(item, 'bibo:numPages'),
        'dcterms:subject': join_values(item, 'dcterms:subject', ''),
        'dcterms:spatial': join_values(item, 'dcterms:spatial', ''),
        'dcterms:rights': get_value(item, 'dcterms:rights'),
        'dcterms:rightsHolder': get_value(item, 'dcterms:rightsHolder'),
        'dcterms:language': get_value(item, 'dcterms:language'),
        'dcterms:source': get_value(item, 'dcterms:source'),
        'dcterms:contributor': join_values(item, 'dcterms:contributor', ''),
        'fabio:hasURL': get_value(item, 'fabio:hasURL'),
        'bibo:content': get_value(item, 'bibo:content'),
    }

def map_item_set(item: Dict[str, Any]) -> Dict[str, Any]:
    def get_fr_value(field: str) -> str:
        values = item.get(field, [])
        fr_values = [v['@value'] for v in values if v.get('@language') == 'fr']
        return '|'.join(fr_values) if fr_values else get_value(item, field)

    return {
        'o:id': get_value(item, 'o:id'),
        'url': f"https://islam.zmo.de/s/afrique_ouest/item-set/{get_value(item, 'o:id')}",
        'dcterms:identifier': get_value(item, 'dcterms:identifier'),
        'o:resource_class': 'o:ItemSet',
        'o:title': get_value(item, 'o:title'),
        'dcterms:description': get_fr_value('dcterms:description'),
        'dcterms:creator': join_values(item, 'dcterms:creator', ''),
        'dcterms:date': get_value(item, 'dcterms:date'),
        'dcterms:replaces': join_values(item, 'dcterms:replaces', ''),
        'dcterms:isReplacedBy': join_values(item, 'dcterms:isReplacedBy', ''),
        'dcterms:spatial': join_values(item, 'dcterms:spatial', ''),
        'dcterms:language': get_value(item, 'dcterms:language'),
        'dcterms:rights': get_value(item, 'dcterms:rights'),
        'dcterms:rightsHolder': get_value(item, 'dcterms:rightsHolder'),
        'dcterms:source': get_value(item, 'dcterms:source'),
        'dcterms:contributor': join_values(item, 'dcterms:contributor', ''),
    }

def map_media(item: Dict[str, Any]) -> Dict[str, Any]:
    # Get the item ID, handling the case where it might be nested
    item_id = item.get('o:item', {}).get('o:id', '')
    if not item_id:
        item_id = get_value(item, 'o:item')
    
    # Construct the item URL only if we have a valid item ID
    item_url = f"https://islam.zmo.de/s/afrique_ouest/item/{item_id}" if item_id else ""

    return {
        'o:id': get_value(item, 'o:id'),
        'url': f"https://islam.zmo.de/s/afrique_ouest/media/{get_value(item, 'o:id')}",
        'o:resource_class': 'o:Media',
        'o:media_type': get_value(item, 'o:media_type'),
        'o:item': item_url,
        'o:original_url': get_value(item, 'o:original_url'),
    }

def map_reference(item: Dict[str, Any]) -> Dict[str, Any]:
    resource_class_map = {
        35: "bibo:AcademicArticle",
        43: "bibo:Chapter",
        88: "bibo:Thesis",
        40: "bibo:Book",
        82: "bibo:Report",
        178: "fabio:BookReview",
        52: "bibo:EditedBook",
        77: "bibo:PersonalCommunication",
        305: "fabio:BlogPost"
    }
    resource_class_id = item.get('o:resource_class', {}).get('o:id')
    
    return {
        'o:id': get_value(item, 'o:id'),
        'url': f"https://islam.zmo.de/s/afrique_ouest/item/{get_value(item, 'o:id')}",
        'dcterms:identifier': get_value(item, 'dcterms:identifier'),
        'o:resource_class': resource_class_map.get(resource_class_id, ''),
        'o:item_set': join_values(item, 'o:item_set', ''),
        'o:media/file': get_media_ids(item),
        'dcterms:title': get_value(item, 'dcterms:title'),
        'bibo:authorList': join_values(item, 'bibo:authorList', ''),
        'bibo:editorList': join_values(item, 'bibo:editorList', ''),
        'bibo:reviewOf': join_values(item, 'bibo:reviewOf', ''),
        'dcterms:publisher': join_values(item, 'dcterms:publisher', ''),
        'dcterms:date': get_value(item, 'dcterms:date'),
        'dcterms:type': get_value(item, 'dcterms:type'),
        'dcterms:alternative': get_value(item, 'dcterms:alternative'),
        'bibo:chapter': get_value(item, 'bibo:chapter'),
        'bibo:volume': get_value(item, 'bibo:volume'),
        'bibo:issue': get_value(item, 'bibo:issue'),
        'dcterms:abstract': get_value(item, 'dcterms:abstract'),
        'bibo:edition': get_value(item, 'bibo:edition'),
        'bibo:numPages': get_value(item, 'bibo:numPages'),
        'bibo:pageStart': get_value(item, 'bibo:pageStart'),
        'bibo:pageEnd': get_value(item, 'bibo:pageEnd'),
        'dcterms:extent': get_value(item, 'dcterms:extent'),
        'dcterms:isPartOf': get_value(item, 'dcterms:isPartOf'),
        'dcterms:provenance': get_value(item, 'dcterms:provenance'),
        'dcterms:subject': join_values(item, 'dcterms:subject', ''),
        'dcterms:spatial': join_values(item, 'dcterms:spatial', ''),
        'dcterms:language': get_value(item, 'dcterms:language'),
        'bibo:doi': get_value(item, 'bibo:doi'),
        'fabio:hasURL': get_value(item, 'fabio:hasURL'),
        'bibo:content': get_value(item, 'bibo:content'),
    }

def get_media_ids(item: Dict[str, Any]) -> str:
    if 'o:media' in item and isinstance(item['o:media'], list):
        return '|'.join([str(media.get('o:id', '')) for media in item['o:media']])
    return ''

def main():
    try:
        logger.info("Starting the Omeka data export process...")
        
        config = Config()
        logger.info(f"Configuration loaded. API URL: {config.API_URL}")

        os.makedirs(config.OUTPUT_DIR, exist_ok=True)

        api_client = OmekaApiClient(config)

        item_set_titles = api_client.fetch_item_set_titles()

        raw_data, item_sets, media, references = api_client.fetch_all_items()

        if not raw_data and not item_sets and not media and not references:
            logger.warning("No data fetched from the API. Exiting.")
            return

        logger.info("Processing fetched data...")
        processor = DataProcessor(raw_data, item_sets, media, references, item_set_titles)
        processed_data = processor.process()

        logger.info("Processing item sets...")
        processor.process_item_sets()

        logger.info("Generating CSV files...")
        generator = FileGenerator(processor.processed_data, config.OUTPUT_DIR)
        generator.generate_all_files()

        logger.info("All files generated successfully.")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()
