import os
import json
import logging
import requests
from tqdm import tqdm
from dotenv import load_dotenv
from dataclasses import dataclass
import re

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
    OUTPUT_DIR: str = os.path.join(os.path.dirname(__file__), 'Resource templates')

class OmekaApiClient:
    def __init__(self, config: Config):
        self.config = config

    def _make_request(self, endpoint: str, params: dict = None) -> list:
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

    def fetch_resource_templates(self) -> list:
        templates = []
        page = 1
        per_page = 100

        logger.info("Starting to fetch resource templates...")

        with tqdm(desc="Fetching resource templates", unit="template") as pbar:
            while True:
                data = self._make_request('resource_templates', {
                    'page': page,
                    'per_page': per_page
                })
                if not data:
                    break
                templates.extend(data)
                pbar.update(len(data))
                page += 1

        logger.info(f"Fetched {len(templates)} resource templates")
        return templates

class JsonFileGenerator:
    def __init__(self, output_dir: str):
        self.output_dir = output_dir

    def generate_json_files(self, templates: list):
        os.makedirs(self.output_dir, exist_ok=True)

        for template in tqdm(templates, desc="Generating JSON files"):
            self.generate_json_file(template)

    def generate_json_file(self, template: dict):
        template_id = template.get('o:id', 'unknown')
        template_label = template.get('o:label', f'Unknown_{template_id}')
        
        # Clean the label to create a valid filename
        clean_label = re.sub(r'[^\w\-_\. ]', '_', template_label)
        clean_label = clean_label.replace(' ', '_')
        
        filename = f"{clean_label}.json"
        filepath = os.path.join(self.output_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as jsonfile:
            json.dump(template, jsonfile, ensure_ascii=False, indent=2)

        logger.info(f"Generated {filepath}")

def main():
    try:
        logger.info("Starting the Omeka resource template export process...")
        
        config = Config()
        logger.info(f"Configuration loaded. API URL: {config.API_URL}")

        api_client = OmekaApiClient(config)
        templates = api_client.fetch_resource_templates()

        if not templates:
            logger.warning("No resource templates fetched from the API. Exiting.")
            return

        logger.info("Generating JSON files...")
        generator = JsonFileGenerator(config.OUTPUT_DIR)
        generator.generate_json_files(templates)

        logger.info("All resource template files generated successfully.")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()