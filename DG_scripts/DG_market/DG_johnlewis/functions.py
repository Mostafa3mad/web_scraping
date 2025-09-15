# functions.py
import json
import logging
import random
import asyncio
import re
import csv
import httpx
from datetime import date, datetime
from pathlib import Path
from typing import *
#from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from tqdm.asyncio import tqdm_asyncio as tqdm
from collections import defaultdict

# Constants
MIN_WAIT = 1.0
MAX_DELAY = 3.0
DEFAULT_WORKERS = 1

# Data directories
DATA_DIR = Path("data")
SITEMAPS_DIR = DATA_DIR / "sitemaps"
CATEGORIES_DIR = DATA_DIR / "categories"
PRODUCTS_DIR = DATA_DIR / "products"
OUTPUTS_DIR = DATA_DIR / "outputs"

# Configuration defaults
DEFAULT_CONFIG = {
    "save_raw_sitemaps": True,
    "save_raw_categories": True,
    "save_raw_products": True,
    "save_local": True,
    "use_scrapingbee": False,
    "scrapingbee_key": "",
    "scrape_products_only": False,
    "stream_output": True,
    "workers": 3,
    "min_delay": 1.0,
    "max_delay": 3.0,
    "max_retries": 1,
    "source_name": "generic"
}

# ScrapingBee configuration
SCRAPINGBEE_URL = "https://app.scrapingbee.com/api/v1/"

SCRAPINGBEE_PARAMS = {
    "render_js": "false",
    "premium_proxy": "false"
}
# Standard CSV schema headers
STANDARD_CSV_HEADERS = [
    "source", "date", "apiURL", "url", "sku", "name", "brand", "price",
    "previousPrice", "onSale", "saleText", "cat", "subcat1", "subcat2",
    "subcat3", "subcat4", "subcat5", "warranty", "image1", "image2",
    "image3", "image4", "image5", "desc", "reviewCount", "reviewRating"
] + [f"attributeTitle{i}" for i in range(1, 21)] + [f"attributeValue{i}" for i in range(1, 21)] + [f"attributeType{i}" for i in range(1, 21)]

# Setup logging
logger = logging.getLogger("scraper")

def setup_logger(log_file=None):
    logger = logging.getLogger("scraper")
    logger.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(formatter)

    logger.handlers = []
    
    logger.addHandler(console_handler)

    if log_file:
        try:
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except (PermissionError, OSError) as e:
            print(f"Warning: Could not create log file {log_file}: {e}")
            print("Logging to console only.")

    return logger

def ensure_data_dirs():
    """Create necessary data directories"""
    for directory in [SITEMAPS_DIR, CATEGORIES_DIR, PRODUCTS_DIR, OUTPUTS_DIR]:
        directory.mkdir(parents=True, exist_ok=True)
    
    logger.info("Data directories created")

def get_random_headers():
    """Get random headers to avoid detection"""
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.2 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:96.0) Gecko/20100101 Firefox/96.0",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36"
    ]
    
    return {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "accept-language": "en-US,en;q=0.5",
        "user-agent": random.choice(USER_AGENTS),
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "dnt": "1",
        "upgrade-insecure-requests": "1",
    }

def get_cache_path(url: str, content_type: str) -> Path:
    """Get the cache path for a URL based on content type"""
    safe_filename = re.sub(r'[^\w\-.]', '_', url)
    if len(safe_filename) > 200:
        safe_filename = safe_filename[:190]

    if content_type == "sitemap":
        return SITEMAPS_DIR / f"{safe_filename}.xml"
    elif content_type == "category":
        return CATEGORIES_DIR / f"{safe_filename}.html"
    else:  # product
        return PRODUCTS_DIR / f"{safe_filename}.html"

async def fetch_url(
    url: str,
    content_type: str = "html",
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, str]] = None,
    data: Optional[Union[Dict[str, Any], str]] = None,
    json_data: Optional[Dict[str, Any]] = None,
    method: str = "GET",
    config: Dict[str, Any] = None
) -> str:
    if config is None:
        config = DEFAULT_CONFIG

    max_retries = config.get("max_retries", 3)
    min_delay = config.get("min_delay", 1.0)
    max_delay = config.get("max_delay", 3.0)
    save_raw = False

    if content_type == "sitemap" and config.get("save_raw_sitemaps", True):
        save_raw = True
    elif content_type == "category" and config.get("save_raw_categories", True):
        save_raw = True
    elif content_type == "product" and config.get("save_raw_products", True):
        save_raw = True

    if save_raw and config.get("save_local", True):
        cache_path = get_cache_path(url, content_type)
        if cache_path.exists():
            logger.info(f"Using cached version of {url} from {cache_path}")
            with open(cache_path, "r", encoding="utf-8") as f:
                return f.read()

    if headers is None:
        headers = get_random_headers()

    for retry in range(max_retries):
        try:
            await asyncio.sleep(random.uniform(min_delay, max_delay))

            if config.get("use_scrapingbee", False) and config.get("scrapingbee_key"):
                response_text = await fetch_with_scrapingbee(url, headers, config)
            else:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    if method.upper() == "POST":
                        response = await client.post(url=url, headers=headers, params=params, data=data, json=json_data)
                    else:
                        response = await client.get(url=url, headers=headers, params=params)
                    response.raise_for_status()
                    response_text = response.text

            if save_raw and config.get("save_local", True):
                cache_path = get_cache_path(url, content_type)
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                with open(cache_path, "w", encoding="utf-8") as f:
                    f.write(response_text)
                logger.info(f"Saved raw content to {cache_path}")

            return response_text

        except Exception as e:
            logger.warning(f"Request failed. URL: {url}. Error: {repr(e)}. Attempt {retry+1}/{max_retries}")
            if retry < max_retries - 1:
                backoff_time = (2 ** retry) + random.uniform(0, 1)
                logger.info(f"Backing off for {backoff_time:.2f} seconds before retry")
                await asyncio.sleep(backoff_time)

    raise RuntimeError(f"Max retries exceeded for URL: {url}")

async def fetch_with_scrapingbee(url: str, headers: Dict[str, str], config: Dict[str, Any]) -> str:
    """Fetch URL using ScrapingBee proxy service"""
    scrapingbee_key = config.get("scrapingbee_key", "")
    if not scrapingbee_key:
        raise ValueError("ScrapingBee key is required when use_scrapingbee is True")
    
    # Prepare ScrapingBee parameters
    params = SCRAPINGBEE_PARAMS.copy()
    params["api_key"] = scrapingbee_key
    params["url"] = url
    
    # Forward headers if needed
    if headers:
        params["headers"] = json.dumps(headers)
    
    # Make the request
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.get(SCRAPINGBEE_URL, params=params)
        response.raise_for_status()
        return response.text

async def fetch_sitemap(url: str, config: Dict[str, Any] = None) -> List[str]:
    """
    Fetch and parse a sitemap XML file.
    """
    logger.info(f"Fetching sitemap from {url}")
    try:
        xml_text = await fetch_url(url, content_type="sitemap", config=config)

        # Parse XML
        root = ET.fromstring(xml_text)

        # Extract URLs from sitemap
        urls = []
        # Define namespace if present in the XML
        ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

        # Try with namespace first
        loc_elements = root.findall('.//sm:loc', ns)
        if not loc_elements:
            # Try without namespace
            loc_elements = root.findall('.//loc')

        for loc in loc_elements:
            urls.append(loc.text)

        # Save URLs to output file if configured
        if config and config.get("save_local", True):
            sitemap_output = OUTPUTS_DIR / "sitemap.txt"
            with open(sitemap_output, "w", encoding="utf-8") as f:
                for url in urls:
                    f.write(f"{url}\n")
            logger.info(f"Saved {len(urls)} sitemap URLs to {sitemap_output}")

        return urls
    except Exception as e:
        logger.error(f"Failed to fetch sitemap {url}: {repr(e)}")
        return []

def save_to_file(data, path):
    """Save data to a JSON file"""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info(f"Saved data to {path}")

def append_to_file(line: str, path: Path):
    """Append a line to a text file"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{line}\n")

# Shared counter for async operations
class SharedCounter:
    def __init__(self, initial_value=0):
        self._value = initial_value
        self._lock = asyncio.Lock()
        
    @property
    def value(self):
        return self._value
        
    async def set_value(self, v):
        async with self._lock:
            self._value = v
            
    async def increment(self):
        async with self._lock:
            self._value += 1
            return self._value

# Base data model classes
class Product:
    def __init__(self, name, url, id, price=None, brand=None, category=None, subcategory=None, 
                 sub_subcategory=None, description=None, images=None, review_count=0, review_rating=0,
                 source="generic"):
        self.name = name
        self.url = url
        self.id = id
        self.price = price
        self.brand = brand
        self.category = category
        self.subcategory = subcategory
        self.sub_subcategory = sub_subcategory
        self.description = description or ""
        self.images = images or []
        self.review_count = review_count
        self.review_rating = review_rating
        self.source = source

    def to_dict(self):
        return {
            "name": self.name,
            "url": self.url,
            "id": self.id,
            "price": self.price,
            "brand": self.brand,
            "category": self.category,
            "subcategory": self.subcategory,
            "sub_subcategory": self.sub_subcategory,
            "description": self.description,
            "images": self.images,
            "review_count": self.review_count,
            "review_rating": self.review_rating,
            "source": self.source
        }
        
    def to_csv_record(self):
        """Convert product to standard CSV schema"""
        record = {
            "source": self.source,
            "date": date.today().isoformat(),
            "apiURL": "",
            "url": self.url,
            "sku": self.id,
            "name": self.name,
            "brand": self.brand or "",
            "price": self.price or "",
            "previousPrice": "",  # Not available in current implementation
            "onSale": "",  # Not available in current implementation
            "saleText": "",  # Not available in current implementation
            "cat": self.category or "",
            "subcat1": self.subcategory or "",
            "subcat2": self.sub_subcategory or "",
            "subcat3": "",
            "subcat4": "",
            "subcat5": "",
            "warranty": "",
            "desc": self.description,
            "reviewCount": self.review_count,
            "reviewRating": self.review_rating,
        }
        
        # Add images
        for i in range(1, 6):
            if i <= len(self.images):
                record[f"image{i}"] = self.images[i-1]
            else:
                record[f"image{i}"] = ""
        
        # Add empty attribute placeholders
        for i in range(1, 21):
            record[f"attributeTitle{i}"] = ""
            record[f"attributeValue{i}"] = ""
            record[f"attributeType{i}"] = ""
        
        return record
    
    def to_pipe_delimited(self):
        """Convert product to pipe-delimited format for text files"""
        record = self.to_csv_record()
        values = [str(record.get(field, "")) for field in STANDARD_CSV_HEADERS]
        return "|".join(values)

class Category:
    def __init__(self, name: str, url: str, subcategories: Optional[List[Dict[str, Any]]] = None):
        self.name = name
        self.url = url
        self.subcategories = subcategories or []

    def to_dict(self) -> Dict[str, Any]:
        """
        Serialize the category to a dictionary.
        """
        return {
            "name": self.name,
            "url": self.url,
            "subcategories": self.subcategories,
        }

class Brand:
    def __init__(self, name: str, url: str = None):
        self.name = name
        self.url = url
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Serialize the brand to a dictionary.
        """
        return {
            "name": self.name,
            "url": self.url
        }


def save_to_csv(records: List[Dict[str, Any]], filepath: str, fieldnames: List[str] = None, id_field: str = "sku"):
    """
    Save records to CSV file using the provided schema, with deduplication.
    
    Args:
        records: List of records to save
        filepath: Path to the CSV file
        fieldnames: CSV field names (default: STANDARD_CSV_HEADERS)
        id_field: Field to use as unique identifier (default: "sku")
    """
    if fieldnames is None:
        fieldnames = STANDARD_CSV_HEADERS
        
    filepath = Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    # Deduplicate records by ID
    unique_records = {}
    for record in records:
        record_id = record.get(id_field)
        if record_id:
            unique_records[record_id] = record
    
    # Write unique records to file
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for record in unique_records.values():
            writer.writerow({k: record.get(k, "") for k in fieldnames})
    
    logger.info(f"Saved {len(unique_records)} unique records to {filepath}")

def append_to_csv(record: Dict[str, Any], filepath: str, fieldnames: List[str] = None):
    """Append a single record to CSV file"""
    if fieldnames is None:
        fieldnames = STANDARD_CSV_HEADERS
        
    filepath = Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    file_exists = filepath.exists()
    
    with open(filepath, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow({k: record.get(k, "") for k in fieldnames})

def load_product_urls_from_file() -> List[str]:
    """Load product URLs from the product_urls.txt file"""
    product_urls_file = OUTPUTS_DIR / "product_urls.txt"
    if not product_urls_file.exists():
        logger.warning(f"Product URLs file not found: {product_urls_file}")
        return []
    
    with open(product_urls_file, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

def convert_categories_to_list(categories_dict: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Convert the nested categories dictionary to a list format for saving.
    """
    result = []
    
    for category_name, category_data in categories_dict.items():
        category_entry = {
            "name": category_data["name"],
            "url": category_data["url"],
            "subcategories": []
        }
        
        # Add subcategories
        for subcategory_name, subcategory_data in category_data["subcategories"].items():
            subcategory_entry = {
                "name": subcategory_data["name"],
                "url": subcategory_data["url"],
                "sub_subcategories": []
            }
            
            # Add sub-subcategories
            for sub_subcategory_name, sub_subcategory_data in subcategory_data["sub_subcategories"].items():
                subcategory_entry["sub_subcategories"].append({
                    "name": sub_subcategory_data["name"],
                    "url": sub_subcategory_data["url"],
                    "id": sub_subcategory_data.get("id")
                })
            
            category_entry["subcategories"].append(subcategory_entry)
        
        result.append(category_entry)
    
    return result

def append_to_csv_with_deduplication(record: Dict[str, Any], filepath: str, id_field: str = "sku", fieldnames: List[str] = None):
    """
    Append a record to CSV file with deduplication based on a unique ID field.
    If a record with the same ID already exists, it will be updated.
    
    Args:
        record: The record to append
        filepath: Path to the CSV file
        id_field: Field to use as unique identifier (default: "sku")
        fieldnames: CSV field names (default: STANDARD_CSV_HEADERS)
    """
    if fieldnames is None:
        fieldnames = STANDARD_CSV_HEADERS
        
    filepath = Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    # If file doesn't exist, just write the record
    if not filepath.exists():
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow({k: record.get(k, "") for k in fieldnames})
        return
    
    # Read existing records into memory, indexed by ID
    existing_records = {}
    try:
        with open(filepath, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get(id_field):  # Only index rows with valid IDs
                    existing_records[row[id_field]] = row
    except Exception as e:
        logger.error(f"Error reading CSV file {filepath}: {e}")
        # If we can't read the file, just append
        with open(filepath, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writerow({k: record.get(k, "") for k in fieldnames})
        return
    
    # Check if record already exists
    record_id = record.get(id_field)
    if record_id and record_id in existing_records:
        # Update existing record
        existing_records[record_id] = {k: record.get(k, existing_records[record_id].get(k, "")) for k in fieldnames}
        logger.debug(f"Updated existing record with {id_field}={record_id}")
    elif record_id:
        # Add new record
        existing_records[record_id] = {k: record.get(k, "") for k in fieldnames}
        logger.debug(f"Added new record with {id_field}={record_id}")
    else:
        logger.warning(f"Record has no {id_field}, cannot deduplicate")
        # Just append the record without deduplication
        with open(filepath, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writerow({k: record.get(k, "") for k in fieldnames})
        return
    
    # Write all records back to file
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in existing_records.values():
            writer.writerow(row)

def append_to_delimited_file_with_deduplication(line: str, path: Path, id_field_index: int = 4, delimiter: str = "|"):
    """
    Append a line to a delimited text file with deduplication based on a field index.
    If a line with the same ID already exists, it will be updated.
    
    Args:
        line: The line to append
        path: Path to the text file
        id_field_index: Index of the field to use as unique identifier (default: 4 for "sku")
        delimiter: Field delimiter (default: "|")
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    
    # If file doesn't exist, just write the line
    if not path.exists():
        with open(path, "w", encoding="utf-8") as f:
            f.write(f"{line}\n")
        return
    
    # Parse the line to get the ID
    fields = line.split(delimiter)
    if len(fields) <= id_field_index:
        # Can't deduplicate, just append
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"{line}\n")
        return
    
    record_id = fields[id_field_index]
    if not record_id:
        # No ID, just append
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"{line}\n")
        return
    
    # Read existing lines into memory, indexed by ID
    existing_lines = {}
    line_order = []  # To preserve order
    
    try:
        with open(path, "r", encoding="utf-8") as f:
            for existing_line in f:
                existing_line = existing_line.strip()
                if not existing_line:
                    continue
                    
                existing_fields = existing_line.split(delimiter)
                if len(existing_fields) > id_field_index:
                    existing_id = existing_fields[id_field_index]
                    if existing_id:
                        if existing_id not in existing_lines:
                            line_order.append(existing_id)
                        existing_lines[existing_id] = existing_line
                    else:
                        # No ID, keep the line as is with a generated ID
                        dummy_id = f"_no_id_{len(line_order)}"
                        line_order.append(dummy_id)
                        existing_lines[dummy_id] = existing_line
                else:
                    # Line too short, keep as is with a generated ID
                    dummy_id = f"_short_line_{len(line_order)}"
                    line_order.append(dummy_id)
                    existing_lines[dummy_id] = existing_line
    except Exception as e:
        logger.error(f"Error reading file {path}: {e}")
        # If we can't read the file, just append
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"{line}\n")
        return
    
    # Update or add the new line
    if record_id in existing_lines:
        # Update existing line
        existing_lines[record_id] = line
        logger.debug(f"Updated existing line with ID={record_id}")
    else:
        # Add new line
        existing_lines[record_id] = line
        line_order.append(record_id)
        logger.debug(f"Added new line with ID={record_id}")
    
    # Write all lines back to file
    with open(path, "w", encoding="utf-8") as f:
        for line_id in line_order:
            f.write(f"{existing_lines[line_id]}\n")

async def generic_product_worker(
    queue, 
    process_func, 
    brands, 
    categories, 
    output_file: Path,
    processed_count,
    config: Dict[str, Any]
):
    """
    Generic worker function to process products from the queue.
    
    Args:
        queue: AsyncIO queue containing product URLs
        process_func: Function to process each product URL
        brands: Dictionary of brands
        categories: Dictionary of categories
        output_file: Path to output file for streaming results
        processed_count: SharedCounter to track progress
        config: Configuration dictionary
    """
    source_name = config.get("source_name", "generic")
    stream_output = config.get("stream_output", True)
    save_local = config.get("save_local", True)
    min_delay = config.get("min_delay", 1.0)
    max_delay = config.get("max_delay", 3.0)
    
    while True:
        try:
            product_url = await queue.get()
            if product_url is None:  # Sentinel value to stop worker
                queue.task_done()
                break
                
            # Add random delay to avoid being blocked
            await asyncio.sleep(random.uniform(min_delay, max_delay))
                
            product = await process_func(product_url, brands, categories, config)
            if product:
                # Set source name
                if hasattr(product, 'source') and not product.source:
                    product.source = source_name
                
                # Convert to dictionary and CSV record
                product_dict = product.to_dict()
                csv_record = product.to_csv_record()
                
                # Stream output to file if configured
                if stream_output and save_local:
                    # Append to CSV file with deduplication
                    append_to_csv_with_deduplication(csv_record, output_file, id_field="sku")
                    
                    # Append to pipe-delimited file with deduplication
                    pipe_file = OUTPUTS_DIR / f"output_{date.today().strftime('%Y_%m_%d')}.txt"
                    pipe_delimited = product.to_pipe_delimited()
                    append_to_delimited_file_with_deduplication(pipe_delimited, pipe_file, id_field_index=4)  # 4 is the index of "sku"
                
                # Update progress
                await processed_count.increment()
            
            queue.task_done()
        except Exception as e:
            logger.error(f"Error in worker: {repr(e)}")
            queue.task_done()

async def run_scraper(
    sitemap_urls: List[str],
    browse_sitemap_url: str,
    extract_categories_func: Callable,
    extract_brands_func: Callable,
    process_product_func: Callable,
    output_csv_path: str,
    config: Dict[str, Any] = None
):
    """
    Generic scraper workflow that can be used by any brand
    
    Args:
        sitemap_urls: List of product sitemap URLs
        browse_sitemap_url: URL for the browse sitemap
        extract_categories_func: Function to extract categories from browse URLs
        extract_brands_func: Function to extract brands from browse URLs
        process_product_func: Function to process a product URL
        output_csv_path: Path to save CSV output
        config: Configuration dictionary with options
    """
    if config is None:
        config = DEFAULT_CONFIG.copy()
    
    # Extract configuration options
    test = config.get("test", False)
    test20 = config.get("test20", False)
    workers = config.get("workers", 3)
    scrape_products_only = config.get("scrape_products_only", False)
    save_local = config.get("save_local", True)
    source_name = config.get("source_name", "generic")
    deduplicate = config.get("deduplicate", True)  # New option for deduplication
    
    logger.info(f"Starting {source_name} scraper with configuration: {config}")
    ensure_data_dirs()
    
    # If deduplicate is True, clean existing output files
    if deduplicate and save_local:
        output_file = Path(output_csv_path)
        if output_file.exists():
            # Deduplicate existing CSV file
            try:
                existing_records = []
                with open(output_file, "r", newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        existing_records.append(row)
                
                if existing_records:
                    save_to_csv(existing_records, output_file)
                    logger.info(f"Deduplicated existing CSV file: {output_file}")
            except Exception as e:
                logger.error(f"Error deduplicating existing CSV file {output_file}: {e}")
    
    # Initialize results
    categories = {}
    brands = {}
    product_urls = []
    
    # If scrape_products_only is True, load product URLs from file
    if scrape_products_only:
        logger.info("Product-only mode: Loading product URLs from file")
        product_urls = load_product_urls_from_file()
        if not product_urls:
            logger.warning("No product URLs found in file. Exiting.")
            return
    else:
        # Use a fallback approach if sitemap is blocked
        try:
            # Try to fetch browse sitemap
            browse_urls = await fetch_sitemap(browse_sitemap_url, config)
            if not browse_urls:
                raise Exception(f"Failed to fetch browse sitemap: {browse_sitemap_url}")
                
            # Extract categories and brands
            categories = extract_categories_func(browse_urls)
            brands = extract_brands_func(browse_urls)
            
            # Save categories to output file if configured
            if save_local:
                categories_output = OUTPUTS_DIR / "categories.txt"
                with open(categories_output, "w", encoding="utf-8") as f:
                    for cat in categories.values():
                        f.write(f"{cat['name']}|{cat['url']}\n")
                logger.info(f"Saved {len(categories)} categories to {categories_output}")
        except Exception as e:
            logger.warning(f"Sitemap approach failed: {e}. Using direct product URLs.")
        
        # Save categories in hierarchical format
        if categories and save_local:
            category_list = convert_categories_to_list(categories)
            save_to_file(category_list, "data/categories/categories.json")
        
        # Save brands list
        if brands and save_local:
            brand_list = [brand.to_dict() for brand in brands.values()]
            save_to_file(brand_list, "data/brands/brands.json")
        
        # Try to fetch product sitemaps
        try:
            for sitemap_url in sitemap_urls:
                urls = await fetch_sitemap(sitemap_url, config)
                product_urls.extend(urls)
                
            if not product_urls:
                raise Exception("No product URLs found in sitemaps")
                
            # Save product URLs to file if configured
            if save_local:
                product_urls_output = OUTPUTS_DIR / "product_urls.txt"
                with open(product_urls_output, "w", encoding="utf-8") as f:
                    for url in product_urls:
                        f.write(f"{url}\n")
                logger.info(f"Saved {len(product_urls)} product URLs to {product_urls_output}")
                
        except Exception as e:
            logger.warning(f"Product sitemap approach failed: {e}. Using test product URLs.")
            # Generate some test URLs if all else fails
            product_urls = []
    
    # Limit for testing
    if test:
        product_urls = product_urls[:5]
    elif test20:
        product_urls = product_urls[:20]
    
    if not product_urls:
        logger.error("No product URLs to process. Exiting.")
        return
    
    # Set up queue and workers for concurrent processing
    queue = asyncio.Queue()
    
    # Add all product URLs to the queue
    for url in product_urls:
        await queue.put(url)
    
    # Prepare output file path
    output_file = Path(output_csv_path)
    
    # Shared counter for progress tracking
    processed_count = SharedCounter(0)
    
    # Limit number of workers to avoid being blocked
    actual_workers = min(workers, 3)  # Limit to 3 workers max
    logger.info(f"Using {actual_workers} workers to avoid rate limiting")
    
    # Create worker tasks
    worker_tasks = []
    for _ in range(actual_workers):
        task = asyncio.create_task(
            generic_product_worker(
                queue, 
                process_product_func, 
                brands, 
                categories, 
                output_file,
                processed_count,
                config
            )
        )
        worker_tasks.append(task)
    
    # Set up progress bar
    total_products = len(product_urls)
    with tqdm(total=total_products) as pbar:
        # Update progress bar periodically
        last_count = 0
        while not queue.empty():
            current_count = processed_count.value
            if current_count > last_count:
                pbar.update(current_count - last_count)
                last_count = current_count
            await asyncio.sleep(0.5)
    
    # Wait for all tasks to be processed
    await queue.join()
    
    # Stop workers
    for _ in range(actual_workers):
        await queue.put(None)
    
    # Wait for all workers to finish
    await asyncio.gather(*worker_tasks)
    
    logger.info(f"Scraping completed. Processed {processed_count.value} products.")
