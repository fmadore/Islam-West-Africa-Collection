import os
import csv
import logging
import requests
from tqdm import tqdm
from dotenv import load_dotenv
from dataclasses import dataclass
from typing import List, Dict, Any, Callable, Optional
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import time
from urllib.parse import urljoin
import json
import asyncio
import aiohttp
import inspect
import aiofiles
from datetime import datetime, timedelta
import hashlib
from tqdm.asyncio import tqdm as async_tqdm
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
import backoff
from typing import Type, Union, Callable
import sys
from contextlib import asynccontextmanager
import argparse
import gzip
import io
import concurrent.futures

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

class Cache:
    def __init__(self, cache_dir: str = None, use_cache: bool = True):
        self.cache_dir = cache_dir or os.path.join(os.path.dirname(__file__), 'cache')
        self.use_cache = use_cache
        if self.use_cache:
            os.makedirs(self.cache_dir, exist_ok=True)
        self.cache_duration = timedelta(hours=24)
        self.memory_cache = {}  # Add in-memory cache layer
        self.memory_cache_max_size = 1000  # Maximum number of items to keep in memory

    def _get_cache_path(self, key: str) -> str:
        # Create a hash of the key to use as filename
        hashed_key = hashlib.md5(key.encode()).hexdigest()
        return os.path.join(self.cache_dir, f"{hashed_key}.json.gz")  # Use compression

    async def get(self, key: str) -> Optional[Any]:
        if not self.use_cache:
            return None
        
        # First check memory cache
        if key in self.memory_cache:
            cached_item = self.memory_cache[key]
            cached_time = cached_item['timestamp']
            if isinstance(cached_time, str):
                cached_time = datetime.fromisoformat(cached_time)
            if datetime.now() - cached_time <= self.cache_duration:
                return cached_item['data']
            else:
                # Expired item, remove from memory cache
                del self.memory_cache[key]
            
        cache_path = self._get_cache_path(key)
        try:
            if not os.path.exists(cache_path):
                return None

            # Use gzip for compression
            async with aiofiles.open(cache_path, 'rb') as f:
                content = await f.read()
                try:
                    with gzip.open(io.BytesIO(content), 'rt', encoding='utf-8') as gz_f:
                        cached_data = json.loads(gz_f.read())
                except gzip.BadGzipFile:
                    # Fallback for older non-compressed files
                    cached_data = json.loads(content.decode('utf-8'))

            # Check if cache has expired
            cached_time = datetime.fromisoformat(cached_data['timestamp'])
            if datetime.now() - cached_time > self.cache_duration:
                return None

            # Add to memory cache
            if len(self.memory_cache) < self.memory_cache_max_size:
                self.memory_cache[key] = cached_data
                
            return cached_data['data']
        except Exception as e:
            logger.warning(f"Cache read error for key {key}: {str(e)}")
            return None

    async def set(self, key: str, value: Any) -> None:
        if not self.use_cache:
            return
            
        cache_path = self._get_cache_path(key)
        try:
            cache_data = {
                'timestamp': datetime.now().isoformat(),
                'data': value
            }
            
            # Add to memory cache
            if len(self.memory_cache) < self.memory_cache_max_size:
                self.memory_cache[key] = cache_data
            
            # Use gzip for compression
            compressed_data = io.BytesIO()
            with gzip.open(compressed_data, 'wt', encoding='utf-8') as f:
                f.write(json.dumps(cache_data))
                
            async with aiofiles.open(cache_path, 'wb') as f:
                await f.write(compressed_data.getvalue())
        except Exception as e:
            logger.warning(f"Cache write error for key {key}: {str(e)}")

class ConnectionManager:
    """Manages HTTP connections with proper lifecycle handling"""
    def __init__(self):
        self._clients = {}
        self._lock = asyncio.Lock()
    
    async def get_client(self, key: str = 'default') -> aiohttp.ClientSession:
        """Get a client session, creating one if it doesn't exist"""
        async with self._lock:
            if key not in self._clients or self._clients[key].closed:
                # Configure an optimal connection pool
                conn = aiohttp.TCPConnector(
                    limit=20,
                    limit_per_host=8,
                    ssl=False,
                    ttl_dns_cache=300,
                )
                timeout = aiohttp.ClientTimeout(total=30)
                self._clients[key] = aiohttp.ClientSession(timeout=timeout, connector=conn)
            return self._clients[key]
    
    async def close_all(self):
        """Close all open client sessions"""
        async with self._lock:
            for key, client in self._clients.items():
                if not client.closed:
                    try:
                        await client.close()
                    except Exception as e:
                        logger.warning(f"Error closing client {key}: {str(e)}")
            self._clients.clear()

# Global connection manager
connection_manager = ConnectionManager()

class Profiler:
    """Simple profiler to track performance metrics"""
    def __init__(self):
        self.metrics = {}
        self.start_times = {}
        self.enabled = False
    
    def enable(self):
        self.enabled = True
    
    def disable(self):
        self.enabled = False
    
    def start(self, name: str):
        """Start timing an operation"""
        if not self.enabled:
            return
        self.start_times[name] = time.time()
    
    def stop(self, name: str):
        """Stop timing an operation and record the duration"""
        if not self.enabled or name not in self.start_times:
            return
        
        duration = time.time() - self.start_times[name]
        if name not in self.metrics:
            self.metrics[name] = {
                'count': 0,
                'total_time': 0,
                'min_time': float('inf'),
                'max_time': 0
            }
        
        self.metrics[name]['count'] += 1
        self.metrics[name]['total_time'] += duration
        self.metrics[name]['min_time'] = min(self.metrics[name]['min_time'], duration)
        self.metrics[name]['max_time'] = max(self.metrics[name]['max_time'], duration)
        
        del self.start_times[name]
    
    def report(self):
        """Generate a performance report"""
        if not self.enabled or not self.metrics:
            return "Profiling disabled or no metrics collected"
        
        report = ["Performance Metrics:"]
        report.append("-" * 80)
        report.append(f"{'Operation':<40} | {'Count':>8} | {'Total (s)':>10} | {'Avg (ms)':>10} | {'Min (ms)':>10} | {'Max (ms)':>10}")
        report.append("-" * 80)
        
        # Sort operations by total time (descending)
        sorted_ops = sorted(self.metrics.items(), key=lambda x: x[1]['total_time'], reverse=True)
        
        for name, stats in sorted_ops:
            avg_ms = (stats['total_time'] / stats['count']) * 1000 if stats['count'] > 0 else 0
            min_ms = stats['min_time'] * 1000 if stats['min_time'] != float('inf') else 0
            max_ms = stats['max_time'] * 1000
            
            report.append(
                f"{name[:39]:<40} | {stats['count']:>8} | {stats['total_time']:>10.2f} | "
                f"{avg_ms:>10.2f} | {min_ms:>10.2f} | {max_ms:>10.2f}"
            )
        
        report.append("-" * 80)
        return "\n".join(report)

# Global profiler instance
profiler = Profiler()

class ProcessingError(Exception):
    """Base class for processing errors"""
    pass

class APIError(ProcessingError):
    """Errors related to API calls"""
    pass

class CacheError(ProcessingError):
    """Errors related to caching"""
    pass

class MappingError(ProcessingError):
    """Errors related to data mapping"""
    pass

@asynccontextmanager
async def error_context(context: str):
    """Context manager for error handling with proper cleanup"""
    try:
        yield
    except Exception as e:
        logger.error(f"Error in {context}: {str(e)}", exc_info=True)
        raise

def async_retry(
    max_tries: int = 3,
    exceptions: Union[Type[Exception], tuple[Type[Exception], ...]] = Exception,
    logger: logging.Logger = logger,
    delay: float = 1.0
):
    """Decorator for async retry logic with exponential backoff"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            attempt = 0
            while attempt < max_tries:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    attempt += 1
                    if attempt == max_tries:
                        logger.error(f"Failed after {max_tries} attempts: {str(e)}")
                        raise
                    wait_time = delay * (2 ** (attempt - 1))  # Exponential backoff
                    logger.warning(f"Attempt {attempt} failed, retrying in {wait_time}s: {str(e)}")
                    await asyncio.sleep(wait_time)
            return None
        return wrapper
    return decorator

class OmekaApiClient:
    def __init__(self, config: Config, use_cache: bool = True):
        self.config = config
        self.cache = Cache(use_cache=use_cache)
        self.request_semaphore = asyncio.Semaphore(10)  # Limit concurrent requests
        self.last_request_time = 0
        self.min_request_interval = 0.1  # 100ms minimum between requests
    
    async def _create_session(self):
        # Use the global connection manager
        return await connection_manager.get_client('omeka_api')

    async def _wait_for_rate_limit(self):
        """Implement simple rate limiting"""
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        if elapsed < self.min_request_interval:
            await asyncio.sleep(self.min_request_interval - elapsed)
        self.last_request_time = time.time()

    async def _close_session(self):
        # We don't need to do anything here as connection_manager will handle cleanup
        pass

    @async_retry(max_tries=5, exceptions=(aiohttp.ClientError, asyncio.TimeoutError))
    async def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        if params is None:
            params = {}
        
        cache_key = f"{endpoint}:{json.dumps(params, sort_keys=True)}"
        
        try:
            # Start profiling
            profiler.start(f"api_request_{endpoint.split('/')[0]}")
            
            # Try cache first
            cached_data = await self.cache.get(cache_key)
            if cached_data is not None:
                profiler.stop(f"api_request_{endpoint.split('/')[0]}")
                return cached_data

            # Apply rate limiting
            await self._wait_for_rate_limit()
            
            # Use semaphore to limit concurrent requests
            async with self.request_semaphore:
                # Make API request
                params.update({
                    'key_identity': self.config.API_KEY_IDENTITY,
                    'key_credential': self.config.API_KEY_CREDENTIAL
                })
                url = f"{self.config.API_URL}/{endpoint}"
                
                async with error_context(f"API request to {endpoint}"):
                    session = await self._create_session()
                    async with session.get(url, params=params) as response:
                        response.raise_for_status()
                        data = await response.json()
                        await self.cache.set(cache_key, data)
                        profiler.stop(f"api_request_{endpoint.split('/')[0]}")
                        return data

        except aiohttp.ClientError as e:
            profiler.stop(f"api_request_{endpoint.split('/')[0]}")
            raise APIError(f"API request failed: {str(e)}") from e
        except json.JSONDecodeError as e:
            profiler.stop(f"api_request_{endpoint.split('/')[0]}")
            raise APIError(f"Invalid JSON response: {str(e)}") from e
        except Exception as e:
            profiler.stop(f"api_request_{endpoint.split('/')[0]}")
            raise ProcessingError(f"Unexpected error: {str(e)}") from e

    async def fetch_items_page(self, resource_class_id: int, page: int, per_page: int) -> List[Dict[str, Any]]:
        return await self._make_request('items', {
            'resource_class_id': resource_class_id,
            'page': page,
            'per_page': per_page
        })

    async def fetch_items(self, resource_class_id: int) -> List[Dict[str, Any]]:
        items = []
        per_page = 100
        
        # First request to get initial data and potentially total count
        first_page = await self.fetch_items_page(resource_class_id, 1, per_page)
        if not first_page:
            return []
            
        items.extend(first_page)
        
        # If there might be more pages
        if len(first_page) == per_page:
            # For better concurrency, estimate the number of pages and fetch them all at once
            # We'll estimate conservatively based on the first page size
            max_concurrent_requests = 5  # Adjust based on API limitations
            
            page = 2
            while True:
                # Create a batch of concurrent requests
                batch_tasks = []
                for i in range(max_concurrent_requests):
                    current_page = page + i
                    batch_tasks.append(self.fetch_items_page(resource_class_id, current_page, per_page))
                
                # Execute the batch of requests concurrently
                batch_results = await asyncio.gather(*batch_tasks)
                
                # Process the results
                has_more_pages = False
                for i, page_items in enumerate(batch_results):
                    if page_items:
                        items.extend(page_items)
                        if len(page_items) == per_page:
                            has_more_pages = True
                
                # Update page counter
                page += max_concurrent_requests
                
                # If no more pages with data, break
                if not has_more_pages:
                    break

        item_type = self.get_item_type_name(resource_class_id)
        logger.info(f"Fetched {len(items)} {item_type}")
        return items

    async def fetch_all_items(self) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        logger.info("Starting to fetch all items...")
        
        # Fetch different types concurrently
        tasks = [
            self.fetch_items(49),  # documents
            self.fetch_items(38),  # audio_visual
            self.fetch_items(58),  # images
        ]
        
        # Add index items tasks
        for resource_class_id in [244, 54, 9, 96, 94]:
            tasks.append(self.fetch_items(resource_class_id))
            
        # Add other tasks
        tasks.extend([
            self.fetch_items(60),  # issues
            self.fetch_items(36),  # newspaper_articles
            self.fetch_item_sets(),
            self.fetch_media(),
            self.fetch_references()
        ])
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks)
        
        # Combine results appropriately
        documents = results[0]
        audio_visual = results[1]
        images = results[2]
        index_items = []
        for i in range(3, 8):  # Index items results
            index_items.extend(results[i])
        
        issues = results[8]
        newspaper_articles = results[9]
        item_sets = results[10]
        media = results[11]
        references = results[12]
        
        logger.info("Finished fetching all items.")
        return (
            documents + audio_visual + images + index_items + issues + newspaper_articles,
            item_sets,
            media,
            references
        )

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
            52: "references (edited books)",
            77: "references (communications)",
            305: "references (blog posts)"
        }
        return item_type_map.get(resource_class_id, f"items (class {resource_class_id})")

    async def fetch_item_sets(self) -> List[Dict[str, Any]]:
        item_sets = []
        page = 1
        per_page = 100

        logger.info("Starting to fetch item sets...")

        with tqdm(desc="Fetching item sets", unit="item") as pbar:
            while True:
                data = await self._make_request('item_sets', {
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

    async def fetch_media(self) -> List[Dict[str, Any]]:
        media = []
        page = 1
        per_page = 100

        logger.info("Starting to fetch media...")

        with tqdm(desc="Fetching media", unit="item") as pbar:
            while True:
                data = await self._make_request('media', {
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

    async def fetch_references(self) -> List[Dict[str, Any]]:
        reference_classes = [35, 43, 88, 40, 82, 178, 52, 77, 305]
        references = []
        # Create tasks for all reference classes
        tasks = [self.fetch_items(resource_class_id) for resource_class_id in reference_classes]
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks)
        # Combine results
        for result in results:
            references.extend(result)
        return references

    async def fetch_item_set_titles(self) -> Dict[int, str]:
        item_set_titles = {}
        page = 1
        per_page = 100

        logger.info("Fetching item set titles...")

        while True:
            data = await self._make_request('item_sets', {
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

    async def fetch_media_data(self, media_id: str) -> Dict[str, Any]:
        endpoint = f'media/{media_id}'
        cache_key = f"media_data:{media_id}"
        
        # Try to get from cache first
        cached_data = await self.cache.get(cache_key)
        if cached_data is not None:
            return cached_data
            
        # If not in cache, fetch and cache the data
        data = await self._make_request(endpoint)
        if data:
            await self.cache.set(cache_key, data)
        return data

class ProgressTracker:
    def __init__(self):
        self.start_time = None
        self.total_items = 0
        self.processed_items = 0
        self._status = "Initializing"

    def start(self, total_items: int):
        self.start_time = time.time()
        self.total_items = total_items
        self.processed_items = 0
        self._status = "Processing"

    @property
    def elapsed_time(self) -> float:
        if self.start_time is None:
            return 0
        return time.time() - self.start_time

    @property
    def progress_percentage(self) -> float:
        if self.total_items == 0:
            return 0
        return (self.processed_items / self.total_items) * 100

    @property
    def status(self) -> str:
        return self._status

    @status.setter
    def status(self, value: str):
        self._status = value
        logger.info(f"Status: {value}")

    def update(self, items_processed: int):
        self.processed_items += items_processed
        self._log_progress()

    def _log_progress(self):
        elapsed = self.elapsed_time
        progress = self.progress_percentage
        rate = self.processed_items / elapsed if elapsed > 0 else 0
        
        logger.info(
            f"Progress: {progress:.1f}% ({self.processed_items}/{self.total_items}) | "
            f"Rate: {rate:.1f} items/s | "
            f"Elapsed: {elapsed:.1f}s | "
            f"Status: {self._status}"
        )

class DataProcessor:
    def __init__(self, raw_data: List[Dict[str, Any]], item_sets: List[Dict[str, Any]], 
                 media: List[Dict[str, Any]], references: List[Dict[str, Any]], 
                 item_set_titles: Dict[int, str], api_client: OmekaApiClient, config: Config):
        self.raw_data = raw_data
        self.item_sets = item_sets
        self.media = media
        self.references = references
        self.item_set_titles = item_set_titles
        self.api_client = api_client
        self.config = config
        self.processed_data = None
        self.batch_size = 50
        # Create mapping caches
        self._media_cache = {m['o:id']: m for m in media}
        self._item_set_cache = {s['o:id']: s for s in item_sets}
        self.progress = ProgressTracker()

    def determine_item_type(self, item: Dict[str, Any]) -> str:
        """Determine the type of an item based on its metadata."""
        item_types = item.get('@type', [])
        resource_class = item.get('o:resource_class', {}).get('o:id')
        
        if 'o:Item' in item_types:
            if 'bibo:AudioVisualDocument' in item_types:
                return 'audio_visual_documents'
            elif 'bibo:Document' in item_types:
                return 'documents'
            elif 'bibo:Image' in item_types:
                return 'images'
            elif resource_class in [244, 54, 9, 96, 94]:  # Index item types
                return 'index'
            elif resource_class == 60:  # Issues
                return 'issues'
            elif resource_class == 36:  # Newspaper articles
                return 'newspaper_articles'
            
        return 'other'  # Default category for unrecognized items

    async def process(self) -> Dict[str, List[Dict[str, Any]]]:
        """Main processing method with enhanced error handling."""
        try:
            async with error_context("Data processing pipeline"):
                logger.info("Starting data processing pipeline...")
                total_items = (len(self.raw_data) + len(self.item_sets) + 
                              len(self.media) + len(self.references))
                self.progress.start(total_items)
                self.progress.status = "Sorting items into processing pools"
                
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

                processing_pools = {
                    'documents': [],
                    'issues': [],
                    'newspaper_articles': [],
                    'audio_visual_documents': [],
                    'images': [],
                    'index': []
                }

                # Sort items into processing pools with progress bar
                pbar = tqdm(total=len(self.raw_data), desc="Sorting items")
                for item in self.raw_data:
                    item_type = self.determine_item_type(item)
                    if item_type in processing_pools:
                        processing_pools[item_type].append(item)
                    pbar.update(1)
                pbar.close()

                # Process pools with detailed progress tracking
                self.progress.status = "Processing item pools"
                async with asyncio.TaskGroup() as tg:
                    tasks = []
                    for item_type, items in processing_pools.items():
                        task = tg.create_task(
                            self._process_pool(item_type, items, processed_data)
                        )
                        tasks.append(task)

                    # Process other data types
                    tasks.extend([
                        tg.create_task(self._process_item_sets(processed_data)),
                        tg.create_task(self._process_media(processed_data)),
                        tg.create_task(self._process_references(processed_data))
                    ])

                self.progress.status = "Processing completed"
                logger.info(f"Total processing time: {self.progress.elapsed_time:.2f} seconds")
                return processed_data
        except Exception as e:
            logger.critical(f"Critical error in processing pipeline: {str(e)}", exc_info=True)
            # Attempt to save any processed data before exiting
            self._save_partial_results()
            raise
        finally:
            await self._cleanup()

    async def _process_pool(self, item_type: str, items: List[Dict[str, Any]], 
                          processed_data: Dict[str, List[Dict[str, Any]]]):
        """Process a pool of items with progress tracking."""
        self.progress.status = f"Processing {item_type}"
        
        pbar = tqdm(total=len(items), desc=f"Processing {item_type}", unit="items")
        for i in range(0, len(items), self.batch_size):
            batch = items[i:i + self.batch_size]
            batch_results = await self._process_batch(item_type, batch)
            processed_data[item_type].extend(batch_results)
            
            items_processed = len(batch)
            self.progress.update(items_processed)
            pbar.update(items_processed)
        pbar.close()

    async def _process_batch(self, item_type: str, batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process a batch of items with error handling."""
        mapping_functions = {
            'documents': map_document,
            'issues': map_issue,
            'newspaper_articles': map_newspaper_article,
            'audio_visual_documents': map_audio_visual_document,
            'images': map_image,
            'index': map_index
        }

        mapper = mapping_functions[item_type]
        is_async = asyncio.iscoroutinefunction(mapper)
        results = []
        errors = []

        async with error_context(f"Processing batch of {item_type}"):
            # Optimize: Process batch items concurrently instead of sequentially
            if is_async:
                # For async mappers, use gather with tasks
                tasks = []
                for item in batch:
                    tasks.append(asyncio.create_task(mapper(item, self.api_client)))
                
                # Process all tasks with proper error handling
                for i, task in enumerate(asyncio.as_completed(tasks)):
                    try:
                        result = await task
                        results.append(result)
                    except Exception as e:
                        item = batch[i]
                        logger.error(f"Error processing {item_type} item {item.get('o:id', 'unknown')}: {str(e)}")
                        errors.append({
                            'item_type': item_type,
                            'item_id': item.get('o:id', 'unknown'),
                            'error': str(e)
                        })
                        # Add a placeholder result to maintain data integrity
                        results.append(self._create_error_placeholder(item_type, item))
            else:
                # For synchronous mappers, use thread pool
                with ThreadPoolExecutor(max_workers=min(os.cpu_count() * 2, len(batch))) as executor:
                    future_to_item = {executor.submit(mapper, item): item for item in batch}
                    for future in concurrent.futures.as_completed(future_to_item):
                        item = future_to_item[future]
                        try:
                            result = future.result()
                            results.append(result)
                        except Exception as e:
                            logger.error(f"Error processing {item_type} item {item.get('o:id', 'unknown')}: {str(e)}")
                            errors.append({
                                'item_type': item_type,
                                'item_id': item.get('o:id', 'unknown'),
                                'error': str(e)
                            })
                            # Add a placeholder result to maintain data integrity
                            results.append(self._create_error_placeholder(item_type, item))

            # Log batch processing summary
            if errors:
                logger.warning(f"Batch processing completed with {len(errors)} errors")
                self._save_errors(errors)

            return results

    def _create_error_placeholder(self, item_type: str, item: Dict[str, Any]) -> Dict[str, Any]:
        """Create a placeholder for failed items."""
        base_placeholder = {
            'o:id': item.get('o:id', 'unknown'),
            'processing_error': 'Failed to process item'
        }
        # Add minimum required fields based on item type
        return base_placeholder

    def _save_errors(self, errors: List[Dict[str, Any]]):
        """Save processing errors to a file for later analysis."""
        error_file = os.path.join(self.config.OUTPUT_DIR, 'processing_errors.json')
        try:
            existing_errors = []
            if os.path.exists(error_file):
                with open(error_file, 'r') as f:
                    existing_errors = json.load(f)
            
            existing_errors.extend(errors)
            
            with open(error_file, 'w') as f:
                json.dump(existing_errors, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save errors to file: {str(e)}")

    async def _process_item_sets(self, processed_data: Dict[str, List[Dict[str, Any]]]):
        """Process item sets with progress tracking."""
        self.progress.status = "Processing item sets"
        
        pbar = tqdm(total=len(self.item_sets), desc="Processing item sets", unit="sets")
        for i in range(0, len(self.item_sets), self.batch_size):
            batch = self.item_sets[i:i + self.batch_size]
            batch_tasks = [
                asyncio.to_thread(map_item_set, item_set)
                for item_set in batch
            ]
            batch_results = await asyncio.gather(*batch_tasks)
            processed_data['item_sets'].extend(batch_results)
            
            items_processed = len(batch)
            self.progress.update(items_processed)
            pbar.update(items_processed)
        pbar.close()

    async def _process_media(self, processed_data: Dict[str, List[Dict[str, Any]]]):
        """Process media items with progress tracking."""
        self.progress.status = "Processing media items"
        
        pbar = tqdm(total=len(self.media), desc="Processing media items", unit="items")
        for i in range(0, len(self.media), self.batch_size):
            batch = self.media[i:i + self.batch_size]
            batch_tasks = [
                asyncio.to_thread(map_media, media_item)
                for media_item in batch
            ]
            batch_results = await asyncio.gather(*batch_tasks)
            processed_data['media'].extend(batch_results)
            
            items_processed = len(batch)
            self.progress.update(items_processed)
            pbar.update(items_processed)
        pbar.close()

    async def _process_references(self, processed_data: Dict[str, List[Dict[str, Any]]]):
        """Process references with progress tracking."""
        self.progress.status = "Processing references"
        
        pbar = tqdm(total=len(self.references), desc="Processing references", unit="refs")
        for i in range(0, len(self.references), self.batch_size):
            batch = self.references[i:i + self.batch_size]
            batch_tasks = [
                asyncio.to_thread(map_reference, reference)
                for reference in batch
            ]
            batch_results = await asyncio.gather(*batch_tasks)
            processed_data['references'].extend(batch_results)
            
            items_processed = len(batch)
            self.progress.update(items_processed)
            pbar.update(items_processed)
        pbar.close()

    def get_media_data(self, media_id: str) -> Optional[Dict[str, Any]]:
        """Get media data from cache."""
        return self._media_cache.get(media_id)

    def get_item_set_data(self, item_set_id: str) -> Optional[Dict[str, Any]]:
        """Get item set data from cache."""
        return self._item_set_cache.get(item_set_id)

    async def process_item_sets(self):
        """Public method to process item sets."""
        if self.processed_data is None:
            self.processed_data = {
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
        
        await self._process_item_sets(self.processed_data)
        
        # Process item set titles
        if 'item_sets' in self.processed_data:
            for i, item in enumerate(self.processed_data['item_sets']):
                if 'o:item_set' in item:
                    item_set_urls = item['o:item_set'].split('|')
                    item_set_names = []
                    for url in item_set_urls:
                        item_set_id = url.split('/')[-1]
                        if item_set_id.isdigit():
                            item_set_names.append(self.item_set_titles.get(int(item_set_id), ''))
                    item['o:item_set'] = '|'.join(filter(None, item_set_names))

    def _save_partial_results(self):
        """Save any processed data in case of critical failure."""
        if self.processed_data:
            partial_results_file = os.path.join(self.config.OUTPUT_DIR, 'partial_results.json')
            try:
                with open(partial_results_file, 'w') as f:
                    json.dump(self.processed_data, f, indent=2)
                logger.info(f"Saved partial results to {partial_results_file}")
            except Exception as e:
                logger.error(f"Failed to save partial results: {str(e)}")

    async def _cleanup(self):
        """Cleanup resources in case of errors."""
        try:
            if hasattr(self, 'api_client'):
                await self.api_client._close_session()
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")

class FileGenerator:
    def __init__(self, processed_data: Dict[str, List[Dict[str, Any]]], output_dir: str):
        self.processed_data = processed_data
        self.output_dir = output_dir
        self.chunk_size = 1000  # Process in chunks to reduce memory pressure

    def generate_all_files(self):
        os.makedirs(self.output_dir, exist_ok=True)

        # Log what data we have
        logger.info(f"Generating files for categories: {list(self.processed_data.keys())}")
        
        for item_type, items in self.processed_data.items():
            if items:  # Only generate files for non-empty data
                filepath = os.path.join(self.output_dir, f"{item_type}.csv")
                self._write_csv_in_chunks(filepath, items)
                logger.info(f"Generated {filepath} with {len(items)} items")
            else:
                logger.warning(f"No data to generate file for {item_type}")
    
    def _write_csv_in_chunks(self, filepath: str, items: List[Dict[str, Any]]):
        """Write CSV file in chunks to reduce memory usage"""
        total_items = len(items)
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            if not items:
                # Handle empty list case
                csvfile.write("")
                return
                
            # Write header first
            fieldnames = items[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            # Process in chunks
            with tqdm(total=total_items, desc=f"Writing {os.path.basename(filepath)}", unit="rows") as pbar:
                for i in range(0, total_items, self.chunk_size):
                    chunk = items[i:i + self.chunk_size]
                    writer.writerows(chunk)
                    
                    # Force garbage collection if large dataset
                    if total_items > 10000:
                        import gc
                        gc.collect()
                    
                    pbar.update(len(chunk))

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

async def map_document(item: Dict[str, Any], api_client: OmekaApiClient) -> Dict[str, Any]:
    primary_media_url = ''
    if 'o:primary_media' in item and item['o:primary_media']:
        media_id = item['o:primary_media']['@id'].split('/')[-1]
        media_data = await api_client.fetch_media_data(media_id)
        primary_media_url = media_data.get('o:original_url', '')

    return {
        'o:id': get_value(item, 'o:id'),
        'url': f"https://islam.zmo.de/s/afrique_ouest/item/{get_value(item, 'o:id')}",
        'dcterms:identifier': get_value(item, 'dcterms:identifier'),
        'o:resource_class': 'bibo:Document',
        'o:item_set': join_values(item, 'o:item_set', ''),
        'o:media/file': get_media_ids(item),
        'o:primary_media': primary_media_url,
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

    # Get type display titles
    type_values = item.get('dcterms:type', [])
    type_display_titles = [t.get('display_title', '') for t in type_values if t.get('display_title')]
    type_string = '|'.join(filter(None, type_display_titles))

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
        'dcterms:type': type_string,
        'foaf:firstName': get_value(item, 'foaf:firstName'),
        'foaf:lastName': get_value(item, 'foaf:lastName'),
        'foaf:gender': get_value(item, 'foaf:gender'),
        'foaf:birthday': get_value(item, 'foaf:birthday'),
        'coordinates': get_value(item, 'curation:coordinates'),
    }

"""Maps an Omeka-S issue item to a standardized dictionary format.

This function takes an Omeka-S issue item and transforms it into a structured dictionary
with standardized field names and values. It handles the extraction and formatting of
various metadata fields including identifiers, titles, creators, and media references.

Args:
    item (Dict[str, Any]): The raw issue item data from Omeka-S API
    api_client (OmekaApiClient): Client instance for making additional API requests

Returns:
    Dict[str, Any]: A dictionary containing the mapped issue data with the following key fields:
        - Basic identifiers (o:id, url, dcterms:identifier)
        - Resource and set information (o:resource_class, o:item_set)
        - Media references (o:media/file, o:primary_media)
        - Bibliographic metadata (title, creator, publisher, date, type)
        - Issue-specific fields (issue number, abstract, page count)
        - Subject and spatial metadata
        - Rights and source information
        - Content and URL references
"""
async def map_issue(item: Dict[str, Any], api_client: OmekaApiClient) -> Dict[str, Any]:
    # Fetch the primary media URL if available
    primary_media_url = ''
    if 'o:primary_media' in item and item['o:primary_media']:
        media_id = item['o:primary_media']['@id'].split('/')[-1]
        media_data = await api_client.fetch_media_data(media_id)
        primary_media_url = media_data.get('o:original_url', '')

    return {
        # Basic identification fields
        'o:id': get_value(item, 'o:id'),
        'url': f"https://islam.zmo.de/s/afrique_ouest/item/{get_value(item, 'o:id')}",
        'dcterms:identifier': get_value(item, 'dcterms:identifier'),
        'o:resource_class': 'bibo:Issue',  # Fixed value for issues
        
        # Collection and media information
        'o:item_set': join_values(item, 'o:item_set', ''),  # Collection memberships
        'o:media/file': get_media_ids(item),  # Associated media files
        'o:primary_media': primary_media_url,  # URL of the primary media
        
        # Core bibliographic metadata
        'dcterms:title': get_value(item, 'dcterms:title'),  # Issue title
        'dcterms:creator': join_values(item, 'dcterms:creator', ''),  # Authors/creators
        'dcterms:publisher': join_values(item, 'dcterms:publisher', ''),  # Publishers
        'dcterms:date': get_value(item, 'dcterms:date'),  # Publication date
        'dcterms:type': get_value(item, 'dcterms:type'),  # Resource type
        
        # Issue-specific metadata
        'bibo:issue': get_value(item, 'bibo:issue'),  # Issue number
        'dcterms:abstract': get_value(item, 'dcterms:abstract'),  # Abstract/summary
        'bibo:numPages': get_value(item, 'bibo:numPages'),  # Number of pages
        
        # Subject and geographic metadata
        'dcterms:subject': join_values(item, 'dcterms:subject', ''),  # Subject terms
        'dcterms:spatial': join_values(item, 'dcterms:spatial', ''),  # Geographic coverage
        
        # Rights and attribution
        'dcterms:rights': get_value(item, 'dcterms:rights'),  # Rights statement
        'dcterms:rightsHolder': get_value(item, 'dcterms:rightsHolder'),  # Rights holder
        
        # Additional metadata
        'dcterms:language': get_value(item, 'dcterms:language'),  # Language
        'dcterms:source': get_value(item, 'dcterms:source'),  # Original source
        'dcterms:contributor': join_values(item, 'dcterms:contributor', ''),  # Contributors
        
        # External references
        'fabio:hasURL': get_value(item, 'fabio:hasURL'),  # External URL
        'bibo:content': get_value(item, 'bibo:content'),  # Full text content
    }

async def map_newspaper_article(item: Dict[str, Any], api_client: OmekaApiClient) -> Dict[str, Any]:
    primary_media_url = ''
    if 'o:primary_media' in item and item['o:primary_media']:
        media_id = item['o:primary_media']['@id'].split('/')[-1]
        media_data = await api_client.fetch_media_data(media_id)
        primary_media_url = media_data.get('o:original_url', '')

    return {
        'o:id': get_value(item, 'o:id'),
        'url': f"https://islam.zmo.de/s/afrique_ouest/item/{get_value(item, 'o:id')}",
        'dcterms:identifier': get_value(item, 'dcterms:identifier'),
        'o:resource_class': 'bibo:Article',
        'o:item_set': join_values(item, 'o:item_set', ''),
        'o:media/file': get_media_ids(item),
        'o:primary_media': primary_media_url,
        'dcterms:title': get_value(item, 'dcterms:title'),
        'dcterms:creator': join_values(item, 'dcterms:creator', ''),
        'dcterms:publisher': join_values(item, 'dcterms:publisher', ''),
        'dcterms:date': get_value(item, 'dcterms:date'),
        'dcterms:type': get_value(item, 'dcterms:type'),
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

async def async_main():
    try:
        # Parse command line arguments
        parser = argparse.ArgumentParser(description='Export data from Omeka API to CSV files')
        parser.add_argument('--cache', choices=['yes', 'no'], default=None, 
                            help='Use cached data if available (yes/no)')
        parser.add_argument('--profile', action='store_true', 
                            help='Enable performance profiling')
        parser.add_argument('--concurrent-requests', type=int, default=10,
                            help='Maximum number of concurrent API requests')
        parser.add_argument('--output-dir', type=str, default=None,
                            help='Directory to store output CSV files')
        parser.add_argument('--resource-classes', type=str, nargs='+',
                            help='Specific resource classes to fetch (space-separated IDs)')
        
        args = parser.parse_args()
        
        # Enable profiler if requested
        if args.profile:
            profiler.enable()
            logger.info("Performance profiling enabled")
        
        # Configure cache usage (command line arg or interactive)
        use_cache = None
        if args.cache:
            use_cache = (args.cache.lower() == 'yes')
        else:
            # Ask user interactively if not specified
            while True:
                response = input("Do you want to use cached data if available? (y/n): ").lower()
                if response in ['y', 'n']:
                    use_cache = (response == 'y')
                    break
                print("Please enter 'y' for yes or 'n' for no.")

        if not use_cache:
            logger.info("Cache disabled - fetching fresh data from API")
        else:
            logger.info("Cache enabled - using cached data if available")
        
        logger.info("Starting the Omeka data export process...")
        
        # Configure the export
        config = Config()
        if args.output_dir:
            config.OUTPUT_DIR = args.output_dir
        
        logger.info(f"Configuration loaded. API URL: {config.API_URL}")
        logger.info(f"Output directory: {config.OUTPUT_DIR}")

        os.makedirs(config.OUTPUT_DIR, exist_ok=True)

        # Create API client with potentially customized concurrent request limit
        api_client = OmekaApiClient(config, use_cache=use_cache)
        if args.concurrent_requests:
            api_client.request_semaphore = asyncio.Semaphore(args.concurrent_requests)
            logger.info(f"Set concurrent request limit to {args.concurrent_requests}")

        # Start profile timing for the main operations
        profiler.start("fetch_item_set_titles")
        item_set_titles = await api_client.fetch_item_set_titles()
        profiler.stop("fetch_item_set_titles")
        
        profiler.start("fetch_all_items")
        raw_data, item_sets, media, references = await api_client.fetch_all_items()
        profiler.stop("fetch_all_items")

        if not raw_data and not item_sets and not media and not references:
            logger.warning("No data fetched from the API. Exiting.")
            return

        logger.info("Processing fetched data...")
        profiler.start("process_data")
        processor = DataProcessor(raw_data, item_sets, media, references, 
                                item_set_titles, api_client, config)
        processed_data = await processor.process()
        profiler.stop("process_data")

        logger.info(f"Processed data contains categories: {list(processed_data.keys())}")
        logger.info("Generating CSV files...")
        profiler.start("generate_csv_files")
        generator = FileGenerator(processed_data, config.OUTPUT_DIR)
        generator.generate_all_files()
        profiler.stop("generate_csv_files")

        logger.info("All files generated successfully.")
        
        # Print performance report if profiling was enabled
        if args.profile:
            print("\n" + profiler.report())
            
    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}", exc_info=True)
    finally:
        # Close all connections properly
        await connection_manager.close_all()

def main():
    asyncio.run(async_main())

if __name__ == "__main__":
    main()

