import os
import csv
import logging
import requests
from tqdm import tqdm
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

# Configuration
class Config:
    API_URL = os.getenv('OMEKA_BASE_URL')
    API_KEY_IDENTITY = os.getenv('OMEKA_KEY_IDENTITY')
    API_KEY_CREDENTIAL = os.getenv('OMEKA_KEY_CREDENTIAL')
    OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'CSV')

# API Client
class OmekaApiClient:
    def __init__(self, api_url, key_identity, key_credential):
        self.api_url = api_url
        self.key_identity = key_identity
        self.key_credential = key_credential

    def fetch_items(self, resource_class_id):
        items = []
        page = 1
        per_page = 100

        logger.info(f"Starting to fetch items for resource class {resource_class_id}...")

        with tqdm(desc=f"Fetching items (class {resource_class_id})", unit="item") as pbar:
            while True:
                url = f"{self.api_url}/items?resource_class_id={resource_class_id}&key_identity={self.key_identity}&key_credential={self.key_credential}&page={page}&per_page={per_page}"
                try:
                    response = requests.get(url)
                    response.raise_for_status()
                    data = response.json()

                    if not data:
                        break

                    items.extend(data)
                    pbar.update(len(data))
                    page += 1
                except requests.RequestException as e:
                    logger.error(f"Error fetching data from API: {str(e)}")
                    break

        logger.info(f"Fetched {len(items)} items for resource class {resource_class_id}")
        return items

    def fetch_item_sets(self):
        item_sets = []
        page = 1
        per_page = 100

        logger.info("Starting to fetch item sets...")

        with tqdm(desc="Fetching item sets", unit="item") as pbar:
            while True:
                url = f"{self.api_url}/item_sets?key_identity={self.key_identity}&key_credential={self.key_credential}&page={page}&per_page={per_page}"
                try:
                    response = requests.get(url)
                    response.raise_for_status()
                    data = response.json()

                    if not data:
                        break

                    item_sets.extend([item for item in data if item.get('o:is_public')])
                    pbar.update(len(data))
                    page += 1
                except requests.RequestException as e:
                    logger.error(f"Error fetching data from API: {str(e)}")
                    break

        logger.info(f"Fetched {len(item_sets)} item sets")
        return item_sets

    def fetch_media(self):
        media = []
        page = 1
        per_page = 100

        logger.info("Starting to fetch media...")

        with tqdm(desc="Fetching media", unit="item") as pbar:
            while True:
                url = f"{self.api_url}/media?key_identity={self.key_identity}&key_credential={self.key_credential}&page={page}&per_page={per_page}"
                try:
                    response = requests.get(url)
                    response.raise_for_status()
                    data = response.json()

                    if not data:
                        break

                    media.extend([item for item in data if item.get('o:is_public')])
                    pbar.update(len(data))
                    page += 1
                except requests.RequestException as e:
                    logger.error(f"Error fetching data from API: {str(e)}")
                    break

        logger.info(f"Fetched {len(media)} media items")
        return media

    def fetch_references(self):
        reference_classes = [35, 43, 88, 40, 82, 178, 52, 77, 305]
        references = []
        for resource_class_id in reference_classes:
            references.extend(self.fetch_items(resource_class_id))
        return references

    def fetch_all_items(self):
        documents = self.fetch_items(49)  # resource_class_id for documents
        audio_visual = self.fetch_items(38)  # resource_class_id for audio/visual documents
        images = self.fetch_items(58)  # resource_class_id for images
        index_items = []
        for resource_class_id in [244, 54, 9, 96, 94]:
            index_items.extend(self.fetch_items(resource_class_id))
        issues = self.fetch_items(60)  # resource_class_id for issues
        newspaper_articles = self.fetch_items(36)  # resource_class_id for newspaper articles
        item_sets = self.fetch_item_sets()  # Fetch item sets
        media = self.fetch_media()  # Fetch media
        references = self.fetch_references()  # Fetch references
        return documents + audio_visual + images + index_items + issues + newspaper_articles, item_sets, media, references

    def fetch_item_set_titles(self):
        item_set_titles = {}
        page = 1
        per_page = 100

        logger.info("Fetching item set titles...")

        while True:
            url = f"{self.api_url}/item_sets?key_identity={self.key_identity}&key_credential={self.key_credential}&page={page}&per_page={per_page}"
            try:
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()

                if not data:
                    break

                for item_set in data:
                    item_set_id = item_set['o:id']
                    item_set_titles[item_set_id] = item_set.get('o:title', '')

                page += 1
            except requests.RequestException as e:
                logger.error(f"Error fetching item set titles: {str(e)}")
                break

        logger.info(f"Fetched {len(item_set_titles)} item set titles")
        return item_set_titles

# Data Processor
class DataProcessor:
    def __init__(self, raw_data, item_sets, media, references, item_set_titles):
        self.raw_data = raw_data
        self.item_sets = item_sets
        self.media = media
        self.references = references
        self.item_set_titles = item_set_titles
        self.processed_data = None

    def process(self):
        processed_data = {
            'audio_visual_documents': [],
            'documents': [],
            'images': [],
            'index': [],
            'issues': [],
            'item_sets': [],  # Add item_sets to the processed_data dictionary
            'media': [],  # Add media to the processed_data dictionary
            'newspaper_articles': [],
            'references': []
        }

        for item in tqdm(self.raw_data, desc="Processing items"):
            item_type = self.determine_item_type(item)
            if item_type in processed_data:
                processed_data[item_type].append(self.map_item(item, item_type))

        for item_set in tqdm(self.item_sets, desc="Processing item sets"):
            processed_data['item_sets'].append(map_item_set(item_set))

        for media_item in tqdm(self.media, desc="Processing media"):
            processed_data['media'].append(map_media(media_item))

        for reference in tqdm(self.references, desc="Processing references"):
            processed_data['references'].append(map_reference(reference))

        logger.info("Data processing completed")
        self.processed_data = processed_data  # Store the processed data
        return processed_data

    def determine_item_type(self, item):
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

    def map_item(self, item, item_type):
        if item_type == 'documents':
            return map_document(item)
        elif item_type == 'audio_visual_documents':
            return map_audio_visual_document(item)
        elif item_type == 'images':
            return map_image(item)
        elif item_type == 'index':
            return map_index(item)
        elif item_type == 'issues':
            return map_issue(item)
        elif item_type == 'newspaper_articles':
            return map_newspaper_article(item)
        return item  # Default mapping

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

# File Generator
class FileGenerator:
    def __init__(self, processed_data, output_dir):
        self.processed_data = processed_data
        self.output_dir = output_dir

    def generate_all_files(self):
        os.makedirs(self.output_dir, exist_ok=True)

        for item_type, items in self.processed_data.items():
            if items:
                self.generate_csv_file(item_type, items)

    def generate_csv_file(self, item_type, items):
        filepath = os.path.join(self.output_dir, f"{item_type}.csv")
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            if items:
                fieldnames = items[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for item in items:
                    writer.writerow(item)

        logger.info(f"Generated {filepath}")

# Utility functions
def get_value(item, field, subfield=None):
    """Utility function to safely get a value from an item."""
    if field not in item or item[field] is None:
        return ''
    if isinstance(item[field], list):
        if subfield:
            return '|'.join([str(v.get(subfield, '')) for v in item[field] if subfield in v])
        else:
            return '|'.join([str(v) for v in item[field]])
    if subfield:
        if isinstance(item[field], dict):
            return item[field].get(subfield, '')
        else:
            return ''
    return item[field]

def join_values(item, field, subfield):
    """Utility function to join multiple values with a separator."""
    if field not in item:
        return ''
    if field == 'o:item_set':
        return '|'.join([val.get('@id', '') for val in item[field]])
    values = [val.get(subfield, '') for val in item[field] if isinstance(val, dict)]
    return '|'.join(filter(None, values))

# Item mapping functions
def map_document(item):
    return {
        'o:id': get_value(item, 'o:id'),
        'url': f"https://iwac.frederickmadore.com/s/afrique_ouest/item/{get_value(item, 'o:id')}",
        'dcterms:identifier': get_value(item, 'dcterms:identifier', '@value'),
        'o:resource_class': 'bibo:Document',
        'o:item_set': join_values(item, 'o:item_set', '@id'),
        'o:media/file': get_value(item, 'o:media', '@id'),
        'dcterms:title': get_value(item, 'dcterms:title', '@value'),
        'dcterms:creator': join_values(item, 'dcterms:creator', 'display_title'),
        'dcterms:date': get_value(item, 'dcterms:date', '@value'),
        'dcterms:abstract': get_value(item, 'dcterms:abstract', '@value'),
        'bibo:numPages': get_value(item, 'bibo:numPages', '@value'),
        'dcterms:subject': join_values(item, 'dcterms:subject', 'display_title'),
        'dcterms:spatial': join_values(item, 'dcterms:spatial', 'display_title'),
        'dcterms:rights': get_value(item, 'dcterms:rights', 'o:label'),
        'dcterms:rightsHolder': get_value(item, 'dcterms:rightsHolder', 'display_title'),
        'dcterms:language': get_value(item, 'dcterms:language', 'display_title'),
        'dcterms:source': get_value(item, 'dcterms:source', 'display_title'),
        'dcterms:contributor': join_values(item, 'dcterms:contributor', 'display_title'),
        'bibo:content': get_value(item, 'bibo:content', '@value'),
    }

def map_audio_visual_document(item):
    return {
        'o:id': get_value(item, 'o:id'),
        'url': f"https://iwac.frederickmadore.com/s/afrique_ouest/item/{get_value(item, 'o:id')}",
        'dcterms:identifier': get_value(item, 'dcterms:identifier', '@value'),
        'o:resource_class': 'bibo:AudioVisualDocument',
        'o:item_set': join_values(item, 'o:item_set', '@id'),
        'o:media/file': get_value(item, 'o:media', '@id'),
        'dcterms:title': get_value(item, 'dcterms:title', '@value'),
        'dcterms:creator': join_values(item, 'dcterms:creator', 'display_title'),
        'dcterms:publisher': join_values(item, 'dcterms:publisher', '@value'),
        'dcterms:description': get_value(item, 'dcterms:description', '@value'),
        'dcterms:date': get_value(item, 'dcterms:date', '@value'),
        'bibo:volume': get_value(item, 'bibo:volume', '@value'),
        'bibo:issue': get_value(item, 'bibo:issue', '@value'),
        'dcterms:isPartOf': get_value(item, 'dcterms:isPartOf', '@value'),
        'dcterms:extent': get_value(item, 'dcterms:extent', '@value'),
        'dcterms:medium': get_value(item, 'dcterms:medium', 'display_title'),
        'dcterms:subject': join_values(item, 'dcterms:subject', 'display_title'),
        'dcterms:spatial': join_values(item, 'dcterms:spatial', 'display_title'),
        'dcterms:rights': get_value(item, 'dcterms:rights', 'o:label'),
        'dcterms:rightsHolder': get_value(item, 'dcterms:rightsHolder', 'display_title'),
        'dcterms:language': get_value(item, 'dcterms:language', 'display_title'),
        'dcterms:source': get_value(item, 'dcterms:source', 'display_title'),
        'dcterms:contributor': join_values(item, 'dcterms:contributor', 'display_title'),
        'bibo:content': get_value(item, 'bibo:content', '@value'),
    }

def map_image(item):
    return {
        'o:id': get_value(item, 'o:id'),
        'url': f"https://iwac.frederickmadore.com/s/afrique_ouest/item/{get_value(item, 'o:id')}",
        'dcterms:identifier': get_value(item, 'dcterms:identifier', '@value'),
        'o:resource_class': 'bibo:Image',
        'o:item_set': join_values(item, 'o:item_set', '@id'),
        'o:media/file': get_value(item, 'o:media', '@id'),
        'dcterms:title': get_value(item, 'dcterms:title', '@value'),
        'dcterms:creator': join_values(item, 'dcterms:creator', 'display_title'),
        'dcterms:date': get_value(item, 'dcterms:date', '@value'),
        'dcterms:description': get_value(item, 'dcterms:description', '@value'),
        'dcterms:subject': join_values(item, 'dcterms:subject', 'display_title'),
        'dcterms:rights': get_value(item, 'dcterms:rights', 'o:label'),
        'dcterms:source': get_value(item, 'dcterms:source', 'display_title'),
        'dcterms:spatial': join_values(item, 'dcterms:spatial', 'display_title'),
        'coordinates': get_value(item, 'curation:coordinates', '@value'),
    }

def map_index(item):
    resource_class_id = item.get('o:resource_class', {}).get('o:id')
    resource_class_map = {
        244: 'fabio:AuthorityFile',
        54: 'bibo:Event',
        9: 'dcterms:Location',
        96: 'foaf:Organization',
        94: 'foaf:Person'
    }

    def get_fr_value(field):
        values = item.get(field, [])
        fr_values = [v['@value'] for v in values if v.get('@language') == 'fr']
        return '|'.join(fr_values) if fr_values else get_value(item, field, '@value')

    return {
        'o:id': get_value(item, 'o:id'),
        'url': f"https://iwac.frederickmadore.com/s/afrique_ouest/item/{get_value(item, 'o:id')}",
        'dcterms:identifier': get_value(item, 'dcterms:identifier', '@value'),
        'o:resource_class': resource_class_map.get(resource_class_id, ''),
        'o:item_set': join_values(item, 'o:item_set', '@id'),
        'o:media/file': get_value(item, 'o:media', '@id'),
        'dcterms:title': get_fr_value('dcterms:title'),
        'dcterms:alternative': get_fr_value('dcterms:alternative'),
        'dcterms:created': get_value(item, 'dcterms:created', '@value'),
        'dcterms:date': get_value(item, 'dcterms:date', '@value'),
        'dcterms:description': get_value(item, 'dcterms:description', '@value'),
        'dcterms:relation': join_values(item, 'dcterms:relation', 'display_title'),
        'dcterms:isReplacedBy': join_values(item, 'dcterms:isReplacedBy', 'display_title'),
        'dcterms:replaces': join_values(item, 'dcterms:replaces', 'display_title'),
        'dcterms:isPartOf': get_value(item, 'dcterms:isPartOf', 'display_title'),
        'dcterms:hasPart': get_value(item, 'dcterms:hasPart', 'display_title'),
        'dcterms:spatial': join_values(item, 'dcterms:spatial', 'display_title'),
        'foaf:firstName': get_value(item, 'foaf:firstName', '@value'),
        'foaf:lastName': get_value(item, 'foaf:lastName', '@value'),
        'foaf:gender': get_value(item, 'foaf:gender', 'display_title'),
        'foaf:birthday': get_value(item, 'foaf:birthday', '@value'),
        'coordinates': get_value(item, 'curation:coordinates', '@value'),
    }

def map_issue(item):
    return {
        'o:id': get_value(item, 'o:id'),
        'url': f"https://iwac.frederickmadore.com/s/afrique_ouest/item/{get_value(item, 'o:id')}",
        'dcterms:identifier': get_value(item, 'dcterms:identifier', '@value'),
        'o:resource_class': 'bibo:Issue',
        'o:item_set': join_values(item, 'o:item_set', '@id'),
        'o:media/file': get_value(item, 'o:media', '@id'),
        'dcterms:title': get_value(item, 'dcterms:title', '@value'),
        'dcterms:creator': join_values(item, 'dcterms:creator', 'display_title'),
        'dcterms:publisher': join_values(item, 'dcterms:publisher', 'display_title'),
        'dcterms:date': get_value(item, 'dcterms:date', '@value'),
        'bibo:issue': get_value(item, 'bibo:issue', '@value'),
        'dcterms:abstract': get_value(item, 'dcterms:abstract', '@value'),
        'bibo:numPages': get_value(item, 'bibo:numPages', '@value'),
        'dcterms:subject': join_values(item, 'dcterms:subject', 'display_title'),
        'dcterms:spatial': join_values(item, 'dcterms:spatial', 'display_title'),
        'dcterms:rights': get_value(item, 'dcterms:rights', 'o:label'),
        'dcterms:rightsHolder': get_value(item, 'dcterms:rightsHolder', 'display_title'),
        'dcterms:language': get_value(item, 'dcterms:language', 'display_title'),
        'dcterms:source': get_value(item, 'dcterms:source', 'display_title'),
        'dcterms:contributor': join_values(item, 'dcterms:contributor', 'display_title'),
        'fabio:hasURL': get_value(item, 'fabio:hasURL', '@id'),
        'bibo:content': get_value(item, 'bibo:content', '@value'),
    }

def map_newspaper_article(item):
    return {
        'o:id': get_value(item, 'o:id'),
        'url': f"https://iwac.frederickmadore.com/s/afrique_ouest/item/{get_value(item, 'o:id')}",
        'dcterms:identifier': get_value(item, 'dcterms:identifier', '@value'),
        'o:resource_class': 'bibo:Article',
        'o:item_set': join_values(item, 'o:item_set', '@id'),
        'o:media/file': get_value(item, 'o:media', '@id'),
        'dcterms:title': get_value(item, 'dcterms:title', '@value'),
        'dcterms:creator': join_values(item, 'dcterms:creator', 'display_title'),
        'dcterms:publisher': join_values(item, 'dcterms:publisher', 'display_title'),
        'dcterms:date': get_value(item, 'dcterms:date', '@value'),
        'dcterms:abstract': get_value(item, 'dcterms:abstract', '@value'),
        'bibo:pages': get_value(item, 'bibo:pages', '@value'),
        'bibo:numPages': get_value(item, 'bibo:numPages', '@value'),
        'dcterms:subject': join_values(item, 'dcterms:subject', 'display_title'),
        'dcterms:spatial': join_values(item, 'dcterms:spatial', 'display_title'),
        'dcterms:rights': get_value(item, 'dcterms:rights', 'o:label'),
        'dcterms:rightsHolder': get_value(item, 'dcterms:rightsHolder', 'display_title'),
        'dcterms:language': get_value(item, 'dcterms:language', 'display_title'),
        'dcterms:source': get_value(item, 'dcterms:source', 'display_title'),
        'dcterms:contributor': join_values(item, 'dcterms:contributor', 'display_title'),
        'fabio:hasURL': get_value(item, 'fabio:hasURL', '@id'),
        'bibo:content': get_value(item, 'bibo:content', '@value'),
    }

def map_item_set(item):
    def get_fr_value(field):
        values = item.get(field, [])
        fr_values = [v['@value'] for v in values if v.get('@language') == 'fr']
        return '|'.join(fr_values) if fr_values else get_value(item, field, '@value')

    return {
        'o:id': get_value(item, 'o:id'),
        'url': f"https://iwac.frederickmadore.com/s/afrique_ouest/item-set/{get_value(item, 'o:id')}",
        'dcterms:identifier': get_value(item, 'dcterms:identifier', '@value'),
        'o:resource_class': 'o:ItemSet',
        'o:title': get_value(item, 'o:title'),
        'dcterms:description': get_fr_value('dcterms:description'),
        'dcterms:creator': join_values(item, 'dcterms:creator', 'display_title'),
        'dcterms:date': get_value(item, 'dcterms:date', '@value'),
        'dcterms:replaces': join_values(item, 'dcterms:replaces', 'display_title'),
        'dcterms:isReplacedBy': join_values(item, 'dcterms:isReplacedBy', 'display_title'),
        'dcterms:spatial': join_values(item, 'dcterms:spatial', 'display_title'),
        'dcterms:language': get_value(item, 'dcterms:language', 'display_title'),
        'dcterms:rights': get_value(item, 'dcterms:rights', 'o:label'),
        'dcterms:rightsHolder': get_value(item, 'dcterms:rightsHolder', 'display_title'),
        'dcterms:source': get_value(item, 'dcterms:source', 'display_title'),
        'dcterms:contributor': join_values(item, 'dcterms:contributor', 'display_title'),
    }

def map_media(item):
    return {
        'o:id': get_value(item, 'o:id'),
        'url': f"https://iwac.frederickmadore.com/s/afrique_ouest/media/{get_value(item, 'o:id')}",
        'o:resource_class': 'o:Media',
        'o:media_type': get_value(item, 'o:media_type'),
        'o:item': f"https://iwac.frederickmadore.com/s/afrique_ouest/item/{get_value(item, 'o:item', 'o:id')}",
        'o:original_url': get_value(item, 'o:original_url'),
    }

def map_reference(item):
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
        'url': f"https://iwac.frederickmadore.com/s/afrique_ouest/item/{get_value(item, 'o:id')}",
        'dcterms:identifier': get_value(item, 'dcterms:identifier', '@value'),
        'o:resource_class': resource_class_map.get(resource_class_id, ''),
        'o:item_set': join_values(item, 'o:item_set', '@id'),
        'o:media/file': get_value(item, 'o:media', '@id'),
        'dcterms:title': get_value(item, 'dcterms:title', '@value'),
        'bibo:authorList': join_values(item, 'bibo:authorList', 'display_title'),
        'bibo:editorList': join_values(item, 'bibo:editorList', 'display_title'),
        'bibo:reviewOf': join_values(item, 'bibo:reviewOf', 'display_title'),
        'dcterms:publisher': join_values(item, 'dcterms:publisher', 'display_title'),
        'dcterms:date': get_value(item, 'dcterms:date', '@value'),
        'dcterms:type': get_value(item, 'dcterms:type', 'display_title'),
        'dcterms:alternative': get_value(item, 'dcterms:alternative', '@value'),
        'bibo:chapter': get_value(item, 'bibo:chapter', '@value'),
        'bibo:volume': get_value(item, 'bibo:volume', '@value'),
        'bibo:issue': get_value(item, 'bibo:issue', '@value'),
        'dcterms:abstract': get_value(item, 'dcterms:abstract', '@value'),
        'bibo:edition': get_value(item, 'bibo:edition', '@value'),
        'bibo:pages': get_value(item, 'bibo:pages', '@value'),
        'bibo:numPages': get_value(item, 'bibo:numPages', '@value'),
        'bibo:pageStart': get_value(item, 'bibo:pageStart', '@value'),
        'bibo:pageEnd': get_value(item, 'bibo:pageEnd', '@value'),
        'dcterms:extent': get_value(item, 'dcterms:extent', '@value'),
        'dcterms:isPartOf': get_value(item, 'dcterms:isPartOf', '@value'),
        'dcterms:provenance': get_value(item, 'dcterms:provenance', '@value'),
        'dcterms:subject': join_values(item, 'dcterms:subject', 'display_title'),
        'dcterms:spatial': join_values(item, 'dcterms:spatial', 'display_title'),
        'dcterms:language': get_value(item, 'dcterms:language', 'display_title'),
        'bibo:doi': get_value(item, 'bibo:doi', 'o:label'),
        'fabio:hasURL': get_value(item, 'fabio:hasURL', '@id'),
        'bibo:content': get_value(item, 'bibo:content', '@value'),
    }

def main():
    try:
        logger.info("Starting the Omeka data export process...")
        
        # Initialize configuration
        config = Config()
        logger.info(f"Configuration loaded. API URL: {config.API_URL}")

        # Ensure the output directory exists
        os.makedirs(config.OUTPUT_DIR, exist_ok=True)

        # Initialize API client
        api_client = OmekaApiClient(config.API_URL, config.API_KEY_IDENTITY, config.API_KEY_CREDENTIAL)

        # Fetch item set titles
        item_set_titles = api_client.fetch_item_set_titles()

        # Fetch data from API
        raw_data, item_sets, media, references = api_client.fetch_all_items()

        if not raw_data and not item_sets and not media and not references:
            logger.warning("No data fetched from the API. Exiting.")
            return

        # Process data
        logger.info("Processing fetched data...")
        processor = DataProcessor(raw_data, item_sets, media, references, item_set_titles)
        processed_data = processor.process()

        # Process item sets
        logger.info("Processing item sets...")
        processor.process_item_sets()

        # Generate files
        logger.info("Generating CSV files...")
        generator = FileGenerator(processor.processed_data, config.OUTPUT_DIR)
        generator.generate_all_files()

        logger.info("All files generated successfully.")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()