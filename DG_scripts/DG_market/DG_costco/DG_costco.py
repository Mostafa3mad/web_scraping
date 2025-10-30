from functions import *
import os
from html import unescape
from bs4 import BeautifulSoup
import html





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

def clean_html_one_line(raw_html):
    """
    Cleans HTML or plain text into a single readable line.
    Removes repeated dots, HTML tags, and excessive whitespace.
    """
    if not raw_html:
        return ""

    text = html.unescape(raw_html)

    text = BeautifulSoup(text, "html.parser").get_text(separator=" ", strip=True)

    text = re.sub(r"[.]{3,}", " ", text)
    text = re.sub(r"[\u2026]+", " ", text)
    text = re.sub(r"\s+", " ", text)

    return text.strip()

async def fetch_single_product(url: str):

    print(url)
    id_product = url.split('/')[-1]
    print(id_product)

    api_url = f'https://www.costco.co.uk/rest/v2/uk/products/{id_product}/?fields=FULL&lang=en_GB&curr=GBP'
    datastr = await fetch_url(api_url, content_type="product")

    root = ET.fromstring(datastr)
    data_dict = xml_to_dict(root)
    product_json = json.loads(json.dumps(data_dict))
    product_json = product_json['product']
    if product_json:
        classifications = product_json.get("classifications", {})
        attributes_raw = []

        if isinstance(classifications, dict):
            features = classifications.get("features", [])
            if isinstance(features, dict):
                attributes_raw.append(features)
            elif isinstance(features, list):
                attributes_raw.extend(features)

        elif isinstance(classifications, list):
            for item in classifications:
                if isinstance(item, dict) and "features" in item:
                    feats = item["features"]
                    if isinstance(feats, dict):
                        attributes_raw.append(feats)
                    elif isinstance(feats, list):
                        attributes_raw.extend(feats)

        # 🧩 بناء قائمة attributes بشكل آمن
        attributes = []
        for feature in attributes_raw:
            if not isinstance(feature, dict):
                continue

            attr_type = "Specification"
            title = feature.get("name", "")

            # featureValues قد تكون dict أو list أو string أو None
            values = feature.get("featureValues", "")
            if isinstance(values, dict):
                value = values.get("value", "")
            elif isinstance(values, list):
                value = ", ".join([v.get("value", "") for v in values if isinstance(v, dict)])
            elif isinstance(values, str):
                value = values
            else:
                value = ""

            attributes.append((attr_type, title, value))
        row = {}

        row['source'] = "costco"
        row["date"] = datetime.now().strftime("%Y-%m-%d")
        row['apiURL'] = api_url
        row['url'] = url
        row['sku'] = product_json.get('code',"")
        row['name'] = product_json.get('englishName',"") or product_json.get('name',"")
        stockLevelStatus = product_json.get("stock", {}).get("stockLevelStatus", "")
        if stockLevelStatus == "inStock":
            row['stock'] = "Y"
        else:
            row['stock'] = "N"
        row['brand'] = extract_feature_value(attributes_raw, "brand")
        base_price = product_json.get("basePrice", {}).get("value", "")
        current_price = product_json.get("price", {}).get("value", "")
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
        row['colour'] = extract_feature_value(attributes_raw, "colour")

        row["size"] = ""

        try:
            for option in product_json.get("baseOptions", []):
                selected = option.get("selected", {})
                qualifier = selected.get("variantOptionQualifiers", {})
                if qualifier.get("qualifier") == "size":
                    row["size"] = qualifier.get("value", "")
                    break
        except Exception as e:
            logger.error("Error extracting size:", e)

        if "," not  in row['colour']:

            row['UPC'] = ""
            row['EAN'] = ""
            categories = product_json.get("supercategories", [])
            # cat_names = [c.get("name", "") for c in categories][::-1]
            if isinstance(categories, list):
                cat_names = [c.get("name", "") for c in categories][::-1]
            else:
                cat_names = [categories] if isinstance(categories, str) else []

            category_keys = ['cat', 'subcat1', 'subcat2', 'subcat3', 'subcat4', 'subcat5']
            for i, key in enumerate(category_keys):
                row[key] = cat_names[i] if i < len(cat_names) else ""
            row['warranty'] = product_json.get("warranty", "")
            images = product_json.get("images", [])
            unique_images = {}
            for img in images:
                gallery_index = img.get("galleryIndex")
                url = img.get("url")
                if gallery_index not in unique_images and url.endswith(".jpg"):
                    unique_images[gallery_index] = url
            image_urls = ["https://www.costco.co.uk" + url for url in list(unique_images.values())[:5]]
            for i in range(5):
                row[f'image{i + 1}'] = image_urls[i] if i < len(image_urls) else ""
            row['desc'] = clean_html_one_line(product_json.get("description", ""))
            row['shortDesc'] = clean_html_one_line(product_json.get("summary", ""))
            row["reviewCount"] = product_json.get("numberOfReviews", "")
            row["reviewRating"] = product_json.get("averageRating", "")
            row["videoURL"] = ""
            row['isPromotion'] = ""
            row["isOutletPrice"] = ""
            row["lowestPriceText"] = ""
            row["lowestPriceValue"] = ""
            attribute_index = 1

            for spec in attributes:
                Specification = spec[1].strip().lower()
                if Specification in ["colour", "size", "brand"]:
                    continue

                if attribute_index < 21:
                    row[f"attributeType{attribute_index}"] = spec[0]
                    row[f"attributeTitle{attribute_index}"] = spec[1]
                    row[f"attributeValue{attribute_index}"] = spec[2]
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
    sitemap_url = "https://www.costco.co.uk/sitemap_uk_product.xml"
    products = await fetch_sitemap(sitemap_url)
    data = await extact_data_from_product_url(products)


if __name__ == "__main__":
    asyncio.run(main())
