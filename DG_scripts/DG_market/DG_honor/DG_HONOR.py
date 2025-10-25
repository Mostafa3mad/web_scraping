import os
from bs4 import BeautifulSoup
from functions import *
from urllib.parse import urlparse, parse_qs





if DEFAULT_CONFIG["save_local"]:

    for folder in [DATA_DIR, SITEMAPS_DIR, PRODUCTS_DIR,OUTPUTS_DIR ]:
        os.makedirs(folder, exist_ok=True)

logger = setup_logger("logs/scraper.log")



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


def extract_image_links(row,sbom: dict,image_host):
    image_links = []


    for photo in sbom.get("netAdaptWebpGroupPhotoList", []):
        if "webp" in photo["photoName"]:
            full_url = image_host + photo["photoPath"] + "800_800_90" +photo["photoName"]
            image_links.append(full_url)

    for i, url in enumerate(image_links):
        if i >= 5:
            break
        row[f"image{i+1}"] = url

    return row




async def get_id(soup,product_master_url):
    try:
        products_ids = []
        product_id = soup.find("span", {"id": "productId"}).text.strip()
        if product_id == "":
            return []
        if product_id:
            api_url = f"https://selfservice-de.hihonor.com/ccpcmd/services/dispatch/secured/CCPC/EN/eCommerce/queryPrdDisplayDetailInfo/1000?productId={product_id}&siteCode=UK"
            response = await fetch_url(api_url, content_type="product")
            response_json = json.loads(response)
            json_data = response_json["data"]


            return json_data,product_id
    except Exception as e:
        return []


async def get_json_ld(soup):

    json_ld_tag = soup.select_one('script[type="application/ld+json"]')
    if not json_ld_tag:
        return []
    json_data = json.loads(json_ld_tag.string)

    return json_data


def get_category_from_url(url, row):
    parsed = urlparse(url)
    parts = parsed.path.strip("/").split("/")
    if parts and len(parts[0]) <= 3:
        parts = parts[1:]
    fields = ["cat", "subcat1", "subcat2", "subcat3", "subcat4", "subcat5"]
    for i, field in enumerate(fields):
        row[field] = parts[i] if i < len(parts) else ""

async def fetch_single_product(product_master_url):

    response_html = await fetch_url(product_master_url, content_type="product")

    soup = BeautifulSoup(response_html, "lxml")


    response_json,product_id = await get_id(soup,product_master_url)

    data_review_json = await get_json_ld(soup)



    if response_json and product_id :
        sbomList = response_json.get("sbomList", [])
        image_host = response_json.get("imageHost", "")

        for sku_varity in sbomList:
            row = {}
            sku_id = sku_varity.get("sbomCode")
            url = f"{product_master_url}?productId={product_id}&skuCode={sku_id}"
            api_url_detail = f'https://selfservice-de.hihonor.com/ccpcmd/services/dispatch/secured/CCPC/EN/eCommerce/querySkuDetailDispInfo/1000?skuCodes={sku_id}&siteCode=uk&groupFlag=false'

            response_detail = await fetch_url(api_url_detail, content_type="product")

            response_detail = json.loads(response_detail)

            row["source"] = "HONOR"
            row["stock"] = "Y"
            row["date"] = datetime.now().strftime("%Y-%m-%d")
            row["apiURL"] = api_url_detail
            row["url"] = url
            row["sku"] = sku_id
            row["name"] = sku_varity.get("name", "")
            row["brand"] = "HONOR"
            row["price"] = ""
            skuPriceInfo = response_detail.get("data", {}).get("detailDispInfos", [])[0].get("skuPriceInfo", {})

            if skuPriceInfo:

                price = skuPriceInfo.get("handPrice", "")
                orderPrice = skuPriceInfo.get("orderPrice", "")



            row["price"] = price or ""
            if orderPrice and row["price"] and orderPrice > row["price"]:
                row["previousPrice"] = orderPrice
                row["onSale"] = "Y"
                try:
                    promo = response_detail.get("data", {}).get("detailDispInfos", [])[0].get("promoRuleList", [])[0].get(
                        "tradeInInfo", {}).get("tradeInRuleDesc", "")

                    row["saleText"] = promo
                except:
                    row["saleText"] = ""

            else:
                row["onSale"] = ""

            row["UPC"] = ""

            row["EAN"] = sku_varity.get("gbomCode","")


            get_category_from_url(product_master_url,row)




            row["warranty"] = ""
            images = extract_image_links(row,sku_varity,image_host)


            row["desc"] = sku_varity.get("seoDescription", "")
            row["shortDesc"] = sku_varity.get("seoTitle","")
            if data_review_json:
                row["reviewCount"] = data_review_json.get("aggregateRating", {}).get("reviewCount", "")
                row["reviewRating"] = data_review_json.get("aggregateRating", {}).get("ratingValue", "")

            row["videoURL"] = ""
            row["isSellingFast"] = ""
            row["isOutletPrice"] = ""
            row["lowestPriceText"] = f"£{price}"
            row["lowestPriceValue"] = price

            gbomAttrList = sku_varity.get("gbomAttrList", [])
            for attr in gbomAttrList:
                attr_name = attr.get("attrName", "").strip()

                if attr_name == "Colour":
                    row["colour"] = attr.get("attrValue", "")
                elif attr_name == "Size" or attr_name == "Storage" or attr_name == "Memory":
                    row["size"] = attr.get("attrValue", "")

            attribute_index = 1
            for attr in gbomAttrList:
                if attribute_index >= 21:
                    break

                attribute_title = attr.get("attrName", "").strip()
                attribute_value = attr.get("attrValue", "")
                attribute_type = "variant" if attr.get("label", 0) == 1 else "specification"

                row[f"attributeType{attribute_index}"] = attribute_type
                row[f"attributeTitle{attribute_index}"] = attribute_title
                row[f"attributeValue{attribute_index}"] = attribute_value

                attribute_index += 1

            if row["price"]:
                append_to_csv(row, "products.csv")






async def extract_links(sitmap_url):
    products_url = []
    products = await fetch_sitemap(sitmap_url)


    for product in products:
        if "/buy" in str(product):
            products_url.append(product)

    return products_url








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
    sitmap_url = "https://www.honor.com/sitemap-uk-EN.xml"
    products_urls = await extract_links(sitmap_url)
    data = await extact_data_from_product_url(products_urls)


if __name__ == "__main__":
    asyncio.run(main())
