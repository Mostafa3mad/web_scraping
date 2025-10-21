import time
from functions import *
import os
from html import unescape
from bs4 import BeautifulSoup
from urllib.parse import urlparse






if DEFAULT_CONFIG["save_local"]:

    for folder in [DATA_DIR, SITEMAPS_DIR, PRODUCTS_DIR,OUTPUTS_DIR ]:
        os.makedirs(folder, exist_ok=True)

logger = setup_logger("logs/scraper.log")




headers = {
    'referer': 'https://www.lenovo.com/gb/en/p/phones/moto/motorola-edge-series/pmipmgz38mq/pmipmgz38mq',
}

def clean(text):
    if text is None:
        return ""
    text = str(text)
    soup = BeautifulSoup(text, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()
    text = soup.get_text(separator=" ")
    text = unescape(text)
    text = re.sub(r"[\r\n\t*?#|]", "", text)
    text = text.replace('"', "'")
    text = text.replace("é", "e")
    text = re.sub(r"^[^\w\(\)]+|[^\w\(\)]+$", "", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.strip()
    text = " ".join(text.split())
    return text


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



def xml_to_dict(elem):
    d = {}

    if list(elem):
        for child in elem:
            child_dict = xml_to_dict(child)

            if child.tag in d:
                if isinstance(d[child.tag], list):
                    d[child.tag].append(child_dict[child.tag])
                else:
                    d[child.tag] = [d[child.tag], child_dict[child.tag]]
            else:
                d.update(child_dict)
    else:
        d[elem.tag] = elem.text.strip() if elem.text else ""

    return {elem.tag: d[elem.tag] if elem.tag in d else d}

def extract_feature_value(attributes_raw, keyword):
    for f in attributes_raw:
        if keyword in f.get("code", "").lower():
            values = f.get("featureValues", "")
            if isinstance(values, dict):
                return values.get("value", "")
            elif isinstance(values, list):
                return ", ".join([v.get("value", "") for v in values if isinstance(v, dict)])
    return ""


async def fetch_single_product(url: str):

    response_str = await fetch_url(url, content_type="product")
    match = re.search(r'<input[^>]*id=["\']modelCode["\'][^>]*value=["\']([^"\']+)["\']', response_str, re.IGNORECASE)
    if match:
        modelCode = match.group(1)

        api_url = f"https://searchapi.samsung.com/v6/front/b2c/product/card/detail/newhybris?siteCode=uk&modelList={modelCode}&saleSkuYN=N&onlyRequestSkuYN=Y&keySummaryYN=Y&keySpecYN=Y&quicklookYN=Y&commonCodeYN=Y"

        datastr = await fetch_url(api_url, content_type="product")
        product_json = json.loads(datastr)
        if modelCode in str(product_json):
            product_json = product_json["response"]["resultData"]["productList"][0]
            row = {}
            row['source'] = "Samsung"
            row["date"] = datetime.now().strftime("%Y-%m-%d")
            row['apiURL'] = api_url
            row['url'] = url
            row['sku'] = modelCode
            model = product_json["modelList"][0]

            row['name'] = model.get('displayName', "")
            row['brand'] = "Samsung"
            current_price = model.get("promotionPrice", model.get("price", ""))
            base_price = model.get("price", "")
            row['price'] = current_price

            try:
                base_val = float(base_price)
                curr_val = float(current_price)
            except (ValueError, TypeError):
                base_val = curr_val = 0.0

            row['previousPrice'] = base_price if base_val > curr_val else ""
            row['onSale'] = "Y" if row['previousPrice'] else ""
            price_difference = base_val - curr_val if row['previousPrice'] else 0.0
            row['saleText'] = f"SAVE £{price_difference:.2f}" if row['previousPrice'] else ""

            try:
                row['colour'] = ""

                colour_options = product_json.get("chipOptions")
                for option in colour_options:
                    if option.get("fmyChipType") == "COLOR":
                        row['colour'] = option.get("optionList", [{}])[0].get("optionLocalName", "")
            except:
                row['colour'] = ""


            for option in model.get("keySpec"):
                row["size"] = ""
                if option.get("key") == "Size":
                    row["size"] = option.get("value", "")
                    break

            row['UPC'] = ""
            row['EAN'] = ""

            category_keys = ['cat', 'subcat1', 'subcat2', 'subcat3', 'subcat4', 'subcat5']
            if "buy" in str(url):
                url = url.replace("buy", "")
            parsed_url = urlparse(url)
            parts = parsed_url.path.strip("/").split("/")
            for i, key in enumerate(category_keys):
                if i + 1 < len(parts):
                    row[key] = parts[i + 1]
                else:
                    row[key] = ""
            row['warranty'] = product_json.get("warranty", "")
            image_urls = model.get("galleryImage", [])
            if image_urls:
                for i in range(5):
                    if i < len(image_urls):
                        row[f'image{i + 1}'] = "https:" + image_urls[i]
                    else:
                        row[f'image{i + 1}'] = ""

            else:
                row[f'image1'] = "https:"+model.get("thumbUrl", "")
            description = ""
            match = re.search(r'<meta\s+name=["\']description["\']\s+content=["\'](.*?)["\']\s*/?>', response_str,re.IGNORECASE)
            if match:
                description = match.group(1)

            row['desc'] = clean(description)
            row['shortDesc'] = ""
            row["reviewCount"] = model.get("reviewCount")
            row["reviewRating"] = model.get("ratings")
            row["videoURL"] = ""
            promotion_price = model.get('promotionPrice', "")
            row['isPromotion'] = "Y" if row['onSale'] == "Y" else ""
            row["isOutletPrice"] = ""
            row["lowestPriceText"] = ""
            row["lowestPriceValue"] = ""
            attribute_index = 1

            combined_attributes = model.get("keySpec", []) + model.get("fmyChipList", [])
            for option in combined_attributes:
                if attribute_index >= 21:
                    break

                if "key" in option and "value" in option:
                    attribute_type = option.get("key", "").upper()
                    attribute_title = option.get("key", "")
                    attribute_value = option.get("value", "")
                elif "fmyChipType" in option and "fmyChipName" in option:
                    attribute_type = option.get("fmyChipType", "").upper()
                    attribute_title = option.get("fmyChipName", "")
                    attribute_value = option.get("fmyChipLocalName", "")
                else:
                    continue
                row[f"attributeType{attribute_index}"] = attribute_type
                row[f"attributeTitle{attribute_index}"] = attribute_title
                row[f"attributeValue{attribute_index}"] = attribute_value
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
    products_urls = []
    sitemap_url_list = ["https://www.samsung.com/uk/da-sitemap.xml",
                        "https://www.samsung.com/uk/memory-sitemap.xml",
                        "https://www.samsung.com/uk/im-sitemap.xml",
                        "https://www.samsung.com/uk/vd-sitemap.xml",
                        "https://www.samsung.com/uk/business/b2b-sitemap.xml"]
    for sitemap_url in sitemap_url_list:
        products = await fetch_sitemap(sitemap_url)
        products_urls.extend(products)

    data = await extact_data_from_product_url(products_urls)


if __name__ == "__main__":
    asyncio.run(main())
