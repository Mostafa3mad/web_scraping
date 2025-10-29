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


def extract_image_links(row,response_json: dict):
    image_links = []
    image_host = response_json["data"].get("imageHost", "")
    for sbom in response_json["data"].get("sbomList", []):
        for photo in sbom.get("groupPhotoList", []):
            full_url = image_host + photo["photoPath"] + photo["photoName"]
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
            api_url = f"https://itrinity-de.c.huawei.com/convert/queryPrdDisplayDetailInfo?productId={product_id}&siteCode=UK"
            response = await fetch_url(api_url, content_type="product")
            response_json = json.loads(response)
            json_data = response_json["data"]

            sbomList = json_data["sbomList"]
            for sbom in sbomList:
                sku_code = sbom.get("sbomCode")
                url = f"{product_master_url}?productId={product_id}&skuCode={sku_code}"
                products_ids.append({
                    "url": url,
                    "product_id" : product_id,
                    "sku_code" : sku_code,

                })
        return products_ids
    except:
        return []


async def get_json_ld(soup,product_master_url):

    json_ld_tag = soup.select_one('#pcp-mircodata script[type="application/ld+json"]')
    if not json_ld_tag:
        return []
    json_data = json.loads(json_ld_tag.string)
    name_product = product_master_url.split("/")[-3]

    urls_products = []
    hasVariant = json_data.get("hasVariant", [])
    for variant in hasVariant:
        offers = variant.get("offers", [])
        for sku in offers:
            url_varity = sku.get("url")
            if name_product in str(url_varity):
                urls_products.append(url_varity)

    products_ids = []

    for url in urls_products:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        product_id = params.get("productId", [None])[0]
        sku_code = params.get("skuCode", [None])[0]

        products_ids.append({
            "url": url,
            "product_id" : product_id,
            "sku_code" : sku_code,

        })

    return products_ids



def get_ctegory(soup,row):
    breadcrumbs = [li.get_text(strip=True) for li in soup.select(".breadcrumbs__list li")]
    fields = ["cat", "subcat1", "subcat2", "subcat3", "subcat4", "subcat5"]
    for i, field in enumerate(fields):
        row[field] = breadcrumbs[i] if i < len(breadcrumbs) else ""


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
                        response = await client.get(url=url, headers=headers, params=params,follow_redirects=True)
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

async def fetch_single_product(product_master_url):
    response_html = await fetch_url(product_master_url, content_type="product")

    soup = BeautifulSoup(response_html, "lxml")
    products_ids = await get_id(soup,product_master_url)
    if not products_ids:
        products_ids = await get_json_ld(soup,product_master_url)





    if products_ids:
        for sku_code in products_ids:
            sku_id = sku_code["sku_code"]
            productId = sku_code["product_id"]
            url_product = sku_code["url"]
            api_url_detail = f'https://itrinity-de.c.huawei.com/convert/queryProductDetailInfo?sbomCodes={sku_id}&siteCode=uk&groupFlag=false'
            response_detail = await fetch_url(api_url_detail, content_type="product")

            data_json = json.loads(response_detail)



            api_image = f"https://itrinity-de.c.huawei.com/convert/v1/product-pcp/images/query?siteCode=UK&productId={productId}&sbomCode={sku_id}"

            response_image = await fetch_url(api_image, content_type="product")
            response_image_json = json.loads(response_image)
            row = {}




            row["source"] = "huawei"
            row["date"] = datetime.now().strftime("%Y-%m-%d")
            row["apiURL"] = api_url_detail
            row["url"] = url_product
            row["sku"] = sku_id
            row["price"] = ""
            detail_infos = data_json.get("querySkuDetailDispInfoData", {}).get("data", {}).get("detailDispInfos", [])
            for detail in detail_infos:
                sku_price_info = detail.get("skuPriceInfo", {})

                sbom_name = sku_price_info.get("sbomAbbr", "")
                row["name"] = sbom_name
                row["brand"] = "HUAWEI"
                price = sku_price_info.get("orderPrice", "")
                discountRate = sku_price_info.get("unitPriceBak", "")

                row["price"] = price
                if discountRate and row["price"] and discountRate > row["price"]:
                    row["previousPrice"] = discountRate
                    row["onSale"] = "Y"
                    row["saleText"] = "On Sale"

                else:
                    row["onSale"] = ""

                row["UPC"] = ""

                row["EAN"] = ""

                get_ctegory(soup,row)

                row["warranty"] = ""

                images = extract_image_links(row,response_image_json)


                row["desc"] = sku_price_info.get("sbomName", "")
                row["shortDesc"] = ""
                row["reviewCount"] = ""
                row["reviewRating"] = ""
                row["videoURL"] = ""
                row["isSellingFast"] = ""
                row["isOutletPrice"] = ""
                row["lowestPriceText"] = f"£{price}"
                row["lowestPriceValue"] = price

            gbomAttrList = data_json.get("querySbomByCodesData", {}).get("data", {}).get("sbomList", [])[0].get("gbomAttrList", [])
            for attr in gbomAttrList:
                attr_name = attr.get("attrName", "").strip()

                if attr_name == "Colour":
                    row["colour"] = attr.get("attrValue", "")
                elif attr_name == "Size":
                    row["size"] = attr.get("attrValue", "")

            attribute_index = 1
            for attr in gbomAttrList:
                if attribute_index >= 21:
                    break

                attribute_title = attr.get("attrName", "").strip()
                attribute_value = attr.get("attrValue", "")
                attribute_type = "specification"

                row[f"attributeType{attribute_index}"] = attribute_type
                row[f"attributeTitle{attribute_index}"] = attribute_title
                row[f"attributeValue{attribute_index}"] = attribute_value

                attribute_index += 1
            if row["price"]:
                row["stock"] = "Y"
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
    sitmap_url = "https://consumer.huawei.com/uk/sitemap.xml"
    products_urls = await extract_links(sitmap_url)
    data = await extact_data_from_product_url(products_urls)


if __name__ == "__main__":
    asyncio.run(main())
