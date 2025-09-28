import os
from bs4 import BeautifulSoup
from functions import *
from urllib.parse import urlparse, parse_qs





if DEFAULT_CONFIG["save_local"]:

    for folder in [DATA_DIR, SITEMAPS_DIR, PRODUCTS_DIR,OUTPUTS_DIR ]:
        os.makedirs(folder, exist_ok=True)

logger = setup_logger("logs/scraper.log")





def get_standard_csv_headers():
    headers = [
        "source", "date", "apiURL", "url", "sku", "name", "brand", "price",
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
    scrapingbee_key = config.get("scrapingbee_key", "")
    if not scrapingbee_key:
        raise ValueError("ScrapingBee key is required when use_scrapingbee is True")

    params = SCRAPINGBEE_PARAMS.copy()
    params["forward_headers"] = True
    params["api_key"] = scrapingbee_key
    params["url"] = url
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.get(SCRAPINGBEE_URL, params=params,headers=headers)
        response.raise_for_status()
        return response.text

def find_uuid_in_text(url: str):


    match = re.search(r"[0-9a-fA-F\-]{36}", url)
    return match.group(0) if match else None


async def fetch_pickers(product_id):
    api_url = f"https://www.backmarket.co.uk/product-page/products/{product_id}/pickers?premium_grade=true"

    headers = {
        "spb-accept-language": "en-GB,en-US;q=0.9,en;q=0.8,ar;q=0.7",
    }
    response = await fetch_url(api_url, content_type="product", headers=headers)
    data = json.loads(response)
    return data ,api_url


async def fetch_data_produt(product_id):
    api_url = f"https://www.backmarket.co.uk/product-page/products/{product_id}"

    headers = {
        "spb-accept-language": "en-GB,en-US;q=0.9,en;q=0.8,ar;q=0.7",
    }
    response = await fetch_url(api_url, content_type="product", headers=headers)
    data = json.loads(response)
    return data

async def fetch_review(product_id):
    api_url = f"https://www.backmarket.co.uk/reviews/products/{product_id}/reviews-summary"
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
        "origin": "https://www.backmarket.co.uk",
        "referer": "https://www.backmarket.co.uk/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
    }

    response = await fetch_url(api_url, content_type="product",config={},headers=headers)
    data = json.loads(response)
    return data

async def fetch_specifications(product_id):
    api_url = f"https://www.backmarket.co.uk/bm/product/{product_id}/technical_specifications"
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
        "origin": "https://www.backmarket.co.uk",
        "referer": "https://www.backmarket.co.uk/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
    }
    response = await fetch_url(api_url, content_type="product",config={},headers=headers)
    data = json.loads(response)
    return data

def get_categories(soup):
    #get_categories
    try:
        breadcrumb_nav = soup.select_one("nav[aria-label='Breadcrumb'] ol")
        breadcrumb_items = breadcrumb_nav.select("li [itemprop='name']")
        categories = [item.get_text(strip=True) for item in breadcrumb_items]

    except:
        categories = []

    return categories




async def fetch_single_product(url: str):
    headers = {
        "spb-accept-language": "en-GB,en-US;q=0.9,en;q=0.8,ar;q=0.7",
    }
    response = await fetch_url(url, content_type="product", headers=headers)
    soup = BeautifulSoup(response, "html.parser")
    categories = get_categories(soup)
    try:
        desc_text = soup.find("meta", {"name": "description"})
        desc_text = desc_text["content"].strip() if desc_text else None
    except:
        desc_text = None

    product_id = find_uuid_in_text(url)
    productVariants = []
    if product_id :
        productVariants.append(product_id)
    else:

        a_tag = soup.find("a", {"data-qa": "see-all-reviews"})
        href = a_tag["href"]
        product_id = href.split("/")[-1]
        date ,api_url = await fetch_pickers(product_id)
        productVariants = date.get("productVariants",[])





    for variant_id in productVariants:


        data,api_url = await fetch_pickers(variant_id)
        selected_items = []
        if not data.get("pickerGroups"):
            continue

        for item in data["pickerGroups"][0]["items"]:
            if item.get("label") == "Fair" or item.get("label") == "Good" or item.get("label") == "Excellent" or item.get("label") == "Premium" and item.get("available") :
                if variant_id == str(item.get("productId")):
                    selected_items.append({
                        "item" : item,
                        "grade" : item.get("parameters").get("grade").get("value"),
                        "grade_name" : item.get("parameters").get("grade").get("name"),
                        "price" : item.get("price").get("amount"),
                        "url" :f"https://www.backmarket.co.uk/en-gb/p/{item.get("slug")}/{item.get("productId")}?l={item.get("parameters").get("grade").get("value")}",
                    })
        for item in selected_items:
            row = {}



            sku = item.get("item","").get("productId","")
            data_product_json = await fetch_data_produt(sku)
            row["source"] = "backmarket"
            row["date"] = datetime.now().strftime("%Y-%m-%d")
            row["apiURL"] = api_url
            row["url"] =item.get("url","")
            row["sku"] = sku
            row["name"] = f"{item.get("grade_name","")} | {data_product_json.get("titles", "").get("raw", "")}"
            row["brand"] = data_product_json.get("brand","")
            price = item.get("price","")
            row["price"] = float(price)
            row["previousPrice"] = float(data_product_json.get("priceWhenNew","").get("amount",""))
            if row["previousPrice"] and row["price"] and row["previousPrice"] > row["price"]:
                row["onSale"] = "Y"
                discount_percent = round((row["previousPrice"] - row["price"]) )
                row["saleText"] = f"save £{discount_percent}"
            else:
                row["onSale"] = ""
                row["saleText"] = ""

            color = data_product_json.get("tracking",{}).get("color","")
            row["colour"] = color

            row["size"] = ""
            row["UPC"] = ""
            row["EAN"] = ""
            fields = ["cat", "subcat1", "subcat2", "subcat3", "subcat4", "subcat5"]
            for i, cat in enumerate(categories):
                if i < len(fields):
                    row[fields[i]] = cat


            images = data_product_json.get("images",[])
            for i, image in enumerate(images):
                if i >= 5:
                    break
                row[f"image{i+1}"] = image.get("url","")

            if not desc_text:
                desc_text = data.get("selectedOffer",{}).get("sellerComment","")

            if desc_text:
                desc_text =  " ".join(desc_text.split())
                row["desc"] = desc_text
            else:
                row["desc"] = ""
            row["shortDesc"] = ""


            reviews = await fetch_review(sku)
            reviewRating = reviews.get("averageRate", "")
            reviewCount = reviews.get("count", "")


            row["reviewCount"] = reviewCount
            row["reviewRating"] = reviewRating
            row["warranty"] = ""
            row["isSellingFast"] = ""
            row["isOutletPrice"] = ""
            row["isPromotion"] = "Y" if row["previousPrice"] != "" else ""
            row["lowestPriceText"] = f"£{price}"
            row["lowestPriceValue"] = price


            data_json = await fetch_specifications(sku)
            specs = data_json.get("specifications", [])
            attribute_index = 1

            for spec in specs:
                if attribute_index > 20:
                    break
                title = spec.get("display", "").strip()
                values = [v.get("label", "") for v in spec.get("values", [])]
                value = ", ".join(values).strip()
                if "storage" == title.lower():
                    row["size"] = value

                row[f"attributeType{attribute_index}"] = "specification"
                row[f"attributeTitle{attribute_index}"] = title
                row[f"attributeValue{attribute_index}"] = value

                attribute_index += 1
            append_to_csv(row, "products.csv")






async def extract_links(sitmap_url):
    all_products = []

    for site_url in sitmap_url:

        products = await fetch_sitemap(site_url,config={})

        all_products.extend(products)
    return all_products







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
    sitmap_url = ["https://www.backmarket.co.uk/sitemap_products.xml","https://www.backmarket.co.uk/sitemap_master_product_pages.xml"]
    products_urls = await extract_links(sitmap_url)
    data = await extact_data_from_product_url(products_urls)


if __name__ == "__main__":
    asyncio.run(main())
