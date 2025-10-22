from functions import *
import os
from html import unescape
from bs4 import BeautifulSoup






if DEFAULT_CONFIG["save_local"]:

    for folder in [DATA_DIR, SITEMAPS_DIR, PRODUCTS_DIR,OUTPUTS_DIR ]:
        os.makedirs(folder, exist_ok=True)

logger = setup_logger("logs/scraper.log")




headers = {
    'referer': 'https://www.lenovo.com/gb/en/p/phones/moto/motorola-edge-series/pmipmgz38mq/pmipmgz38mq',
}

def clean_html_description(raw_html: str) -> str:
    if raw_html is None:
        raw_html = ''
    text = re.sub(r'<[^>]+>', '', raw_html)
    text = re.sub(r'\s+', ' ', text).strip()
    return unescape(text)

def clean(string):
    if string is None:
        return string
    string = str(string)
    string = re.sub(r"[\r\n\t*?#|]", "", string)
    string = string.replace('"', "'")
    string = string.replace("é", "e")
    string = re.sub(r"^[^\w\(\)]+|[^\w\(\)]+$", "", string)
    string = string.encode("ascii", "ignore").decode("ascii")
    string = string.strip()
    string = " ".join(string.split())
    return string


def get_standard_csv_headers():
    headers = [
        "source", "date", "apiURL", "url", "sku", "name", "brand", "price","stock",
        "previousPrice", "onSale", "saleText", "colour", "size", "UPC", "EAN",
        "cat", "subcat1", "subcat2", "subcat3", "subcat4", "subcat5", "warranty",
        "image1", "image2", "image3", "image4", "image5", "desc", "shortDesc",
        "reviewCount", "reviewRating", "videoURL", "isSellingFast",
        "isRestockingSoon", "isPromotion", "isOutletPrice", "lowestPriceText",
        "lowestPriceValue"
    ]
    for i in range(1, 21):
        headers += [f"attributeType{i}", f"attributeTitle{i}", f"attributeValue{i}"]
    return headers

def create_csv_file(filepath):
    if DEFAULT_CONFIG["save_local"]:

        headers = get_standard_csv_headers()
        with open(OUTPUTS_DIR/filepath, mode="w", newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()

def append_to_csv(item, filepath):

    if DEFAULT_CONFIG["save_local"]:

        headers = get_standard_csv_headers()
        with open(OUTPUTS_DIR/filepath, mode="a", newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writerow(item)



async def process_sitemaps(sitemap_url):

    products = await fetch_sitemap(sitemap_url)
    all_products = []

    for product in products:
        if "/p/" in product:
            all_products.append(product)
    return all_products
async def fetch_url(
    url: str,
    content_type: str = "html",
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, str]] = None,
    config: Dict[str, Any] = None
) -> str:

    if config is None:
        config = DEFAULT_CONFIG

    max_retries = config.get("max_retries", 3)
    min_delay = config.get("min_delay", 1.0)
    max_delay = config.get("max_delay", 3.0)
    save_raw = False

    # Determine if we should save raw content based on content type
    if content_type == "sitemap" and config.get("save_raw_sitemaps", True):
        save_raw = True
    elif content_type == "category" and config.get("save_raw_categories", True):
        save_raw = True
    elif content_type == "product" and config.get("save_raw_products", True):
        save_raw = True

    # Check if we have a cached version
    if save_raw and config.get("save_local", True):
        cache_path = get_cache_path(url, content_type)
        if cache_path.exists():
            logger.info(f"Using cached version of {url} from {cache_path}")
            with open(cache_path, "r", encoding="utf-8") as f:
                return f.read()

    # Use random headers if none provided
    if headers is None:
        headers = get_random_headers()

    for retry in range(max_retries):
        try:
            await asyncio.sleep(random.uniform(min_delay, max_delay))

            if config.get("use_scrapingbee", False) and config.get("scrapingbee_key"):
                response_text = await fetch_with_scrapingbee(url, headers, config)
            else:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    response = await client.get(url=url, headers=headers, params=params,follow_redirects=True)
                    response.raise_for_status()
                    response_text = response.text

            # Save raw content if configured
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
                # Exponential backoff with jitter
                backoff_time = (2 ** retry) + random.uniform(0, 1)
                logger.info(f"Backing off for {backoff_time:.2f} seconds before retry")
                await asyncio.sleep(backoff_time)

    raise RuntimeError(f"Max retries exceeded for URL: {url}")
async def fetch_single_product(url: str):

    id_product = url.split('/')[-1].split("?")[0]

    api_url = f'https://openapi.lenovo.com/gb/en/product/singleModelPDP/get?groupCodes=400001&productNumber={id_product}'
    datastr = await fetch_url(api_url,content_type="product", headers=headers)
    respose = await fetch_url(url, content_type="product")
    soup = BeautifulSoup(respose, 'html.parser')

    product_json = json.loads(datastr)
    if product_json:




        row ={}
        product_json = product_json.get('data', {})
        row['source'] = "lenovo"
        row["stock"] = "Y" if product_json.get("pmi",{}).get("marketingStatus","").lower() == "available" else "N"
        row["date"] = datetime.now().strftime("%Y-%m-%d")
        row['apiURL'] = api_url
        row['url'] = url
        row['sku'] = product_json.get('productNumber')
        row['name'] = product_json.get('pmi').get('summary', "")
        last_item = product_json.get("breadCrumb")
        classification = product_json.get('classification', [])
        row['brand'] = ""
        if classification:
            for item in classification[0].get('specs', []):
                brand = item.get('a', "")
                if brand == "Brand":
                    row['brand'] = item.get('b', "")
                    break


        if row['brand'] == "":
            pattern = r'"brand":\s*{\s*"name":\s*"([^"]+)"'
            match = re.search(pattern, respose)
            if match:
                brand_name = match.group(1)
                row['brand'] = brand_name.strip()
            else:
                pattern = r"<meta name='brand' content='([^']+)'>"
                matches = re.findall(pattern, respose)
                if matches:
                    row['brand'] = matches[0].strip()
                else:
                    row['brand'] = ""


        price = soup.find('meta', {'name': 'productpromotionprice'})['content']
        previousPrice = soup.find('meta', {'name': 'productprice'})['content']

        row['price'] = price if price else ""
        row['previousPrice'] = previousPrice if previousPrice and previousPrice != price else ""
        row['onSale'] = "Y" if row['previousPrice'] != "" else ""
        price_difference = float(previousPrice) - float(price) if previousPrice and price else 0.0
        row['saleText'] = f"SAVE £{round(price_difference,2)}" if row['previousPrice'] != "" else ""
        pattern = r'"a":\s*"Colour",\s*"b":\s*"([^"]+)"'
        matches = re.findall(pattern, respose)
        row['colour'] = ""

        if matches:
            for match in matches:
                row['colour'] = match.strip()
        else:
            if classification:
                for item in classification[0].get('specs', []):
                    brand = item.get('a', "")
                    if brand == "Color":
                        row['colour'] = item.get('b', "").strip()
                        break
                    else:

                        row['colour'] = ""

        row['size'] = ""
        row['UPC'] = ""
        row['EAN'] = ""
        row['cat'] = last_item[0].get("breadCrumb")
        row['subcat1'] = last_item[1].get("breadCrumb") if len(last_item) > 1 else ''
        row['subcat2'] = last_item[2].get("breadCrumb") if len(last_item) > 2 else ''
        row['subcat3'] = last_item[3].get("breadCrumb") if len(last_item) > 3 else ''
        row['subcat4'] = last_item[4].get("breadCrumb") if len(last_item) > 4 else ''
        row['subcat5'] = last_item[5].get("breadCrumb") if len(last_item) > 5 else ''
        row['warranty'] = ""
        pattern = r'"imageAddress":\s*"([^"]+\.png)"'
        matches = re.findall(pattern, respose)
        matches = list(set(matches))
        for i in range(5):
            if i < len(matches):
                row[f'image{i + 1}'] = "https:"+matches[i]
            else:
                row[f'image{i + 1}'] = ""

        row['desc'] = clean_html_description(product_json.get('pmi').get('description', ""))
        if  row['desc'] == "":
            pattern = r'"marketingLongDescription"\s*:\s*"(.*?)",'
            match = re.search(pattern, respose)
            if match:
                marketing_long_description = match.group(1)
                row['desc'] = clean_html_description(marketing_long_description)

        row['shortDesc'] = ""

        #
        row["reviewCount"] = product_json.get("productReviewData").get("totalReviewCount")
        row["reviewRating"] = product_json.get("productReviewData").get("averageOverallRating")
        video_url = product_json.get("media").get("video","")
        if video_url:
            row["videoURL"] = "https://www.youtube.com/watch?v="+video_url
        else:
            row["videoURL"] = ""
        row['isPromotion'] = ""
        row["isOutletPrice"] = ""
        row["lowestPriceText"] = ""
        row["lowestPriceValue"] = ""
        attribute_index = 1
        for spec in classification[0]['specs']:
            if attribute_index < 21:
                row[f"attributeType{attribute_index}"] = spec.get("a").upper()
                row[f"attributeTitle{attribute_index}"] = spec.get("a")
                row[f"attributeValue{attribute_index}"] = clean(spec.get("b").upper())

                attribute_index += 1
        if row["price"]:
            append_to_csv(row, "products.csv")

            return row


async def extact_data_from_product_url(all_product_urls: list[str]):
    tasks = []
    total_urls = len(all_product_urls)

    with tqdm(total=total_urls, desc="Processing product URLs", ncols=100) as pbar:
        for url in all_product_urls:
            task = asyncio.create_task(wrapped_fetch(url))
            tasks.append(task)
            pbar.update(1)
            if len(tasks) >= DEFAULT_WORKERS:
                await tasks.pop(0)

    for task in tasks:
        await task



async def wrapped_fetch(url):
    try:
        await fetch_single_product(url)
    except Exception as e:
        logger = logging.getLogger("scraper")
        logger.warning(f"Failed to fetch {url}: {e}")







async def main():
    create_csv_file("products.csv")
    url = "https://www.lenovo.com/sitemap-auto/088-intsitemap-gb-en.xml"
    products = await process_sitemaps(url)
    data = await extact_data_from_product_url(products)


if __name__ == "__main__":
    asyncio.run(main())
