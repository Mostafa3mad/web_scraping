import os
from html import unescape
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from functions import *
import gzip
from io import BytesIO





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


def etree_to_dict(elem):
    d = {}
    if len(elem):
        for child in elem:
            child_dict = etree_to_dict(child)
            if child.tag in d:
                if isinstance(d[child.tag], list):
                    d[child.tag].append(child_dict[child.tag])
                else:
                    d[child.tag] = [d[child.tag], child_dict[child.tag]]
            else:
                d.update(child_dict)
    else:
        d[elem.tag] = elem.text
        return d
    return {elem.tag: d}




async def fetch_api(skuCode):

    api_url = f"https://www.sony.co.uk/commerceapi/rest/v2/sony-uk/products/{skuCode}?fields=FULL&lang=en_GB&curr=GBP"
    response = await fetch_url(api_url, content_type="product")
    root = ET.fromstring(response)
    data_json = etree_to_dict(root)

    if data_json:
        return data_json,api_url

    return None
def extract_colour_and_size(data_json: dict):
    """
    Extracts colour and size qualifiers from product JSON structure.
    Returns the raw qualifier dicts (not just strings).
    """
    qualifiers = (
        data_json.get("product", {})
        .get("baseOptions", {})
        .get("selected", {})
        .get("variantOptionQualifiers", {})
    )

    colour = {"colour": "N/A", "size": "N/A"}
    size = {"colour": "N/A", "size": "N/A"}

    if isinstance(qualifiers, dict):
        if qualifiers.get("qualifier") == "rootColorEquivalent":
            colour = qualifiers
        elif qualifiers.get("qualifier") in ("otherDifferentiators", "size", "capacity"):
            size = qualifiers

    elif isinstance(qualifiers, list):
        for q in qualifiers:
            qualifier_type = q.get("qualifier")
            if qualifier_type == "rootColorEquivalent":
                colour = q
            elif qualifier_type in ("otherDifferentiators", "size", "capacity"):
                size = q


    return {"colour": colour, "size": size}
async def get_review(attr_name):
    headers = {
        'accept': '*/*',
        'bv-bfd-token': '12872_22_0,seu,en_GB',
        'origin': 'https://www.sony.co.uk',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
    }
    api_review = f"https://apps.bazaarvoice.com/bfd/v1/clients/sony-global/api-products/cv2/resources/data/statistics.json?filter=ProductId%3A{attr_name}&apiversion=5.4&stats=Reviews"

    response = await fetch_url(api_review, content_type="product",headers=headers)

    response_review = json.loads(response)

    results = response_review["response"]["Results"]
    if results:
        for item in results:
            stats = item["ProductStatistics"]["ReviewStatistics"]
            average_ratings = stats["AverageOverallRating"]
            number_of_reviews = stats["TotalReviewCount"]
            return average_ratings, number_of_reviews
    return "",""

async def fetch_single_product(id: str):
    sku = id.split('|')[0]
    url_product = id.split('|')[2]

    data_json,api_url = await fetch_api(sku)
    if data_json:
        average_ratings, number_of_reviews = await get_review(sku)
        base_product = data_json.get("product", {})


        skuCode = sku

        if data_json:
            row = {}
            row["source"] = "sony"
            row["date"] = datetime.now().strftime("%Y-%m-%d")
            row["apiURL"] = api_url
            gwtProductHierarchies = base_product.get("gwtProductHierarchies", {})

            row["url"] = f"{url_product}?sku={skuCode}" if url_product else ""
            row["sku"] = skuCode
            row["name"] = gwtProductHierarchies.get("modelName", "")
            row["brand"] = "sony"
            price =data_json.get("product", {}).get("baseOptions", {}).get("selected",{}).get("discountedPrice", {})
            row["price"] = price.get("formattedValue", "")
            previousPrice = data_json.get("product", {}).get("baseOptions", {}).get("selected",{}).get("priceData", {}).get("formattedValue", {})
            row["onSale"] = ""
            row["stock"] = "Y" if data_json.get("product", {}).get("baseOptions", {}).get("selected",{}).get("stock", {}).get("stockLevelStatus", "") == "inStock" else "N"
            if previousPrice and previousPrice != row["price"]:
                row["previousPrice"] = previousPrice
                row["onSale"] = "Y"
                row["saleText"] = ""

            result = extract_colour_and_size(data_json)
            row["colour"] = result.get("colour", "").get("name","")
            row["size"] = result.get("size", "").get("name","").strip()

            row["UPC"] = ""
            for key in ["cat", "subcat1", "subcat2", "subcat3", "subcat4", "subcat5"]:
                row[key] = ""
            breadcrumbs = data_json.get("product", {}).get("breadcrumbs", [])
            keys = ["cat", "subcat1", "subcat2", "subcat3", "subcat4", "subcat5"]
            for i, crumb in enumerate(breadcrumbs[:6]):
                row[keys[i]] = crumb.get("name", crumb.get("code", ""))


            row["warranty"] = ""


            for i in range(1, 6):
                row[f"image{i}"] = ""
            images = data_json.get("product", {}).get("images", [])

            product_images = []
            for i, img in enumerate(images):
                image_type = img.get("imageType", "").lower()
                format = img.get("format", "").lower()
                if  format == "zoom":

                    url = img.get("url", "")
                    if url.startswith("//"):
                        url = "https:" + url
                    elif url.startswith("/"):
                        url = "https://www.sony.co.uk/commerceapi/" + url
                    product_images.append(url)

            for i, url in enumerate(product_images):
                if i < 5:
                    row[f"image{i + 1}"] = url



            row["desc"] = data_json.get("product", {}).get("description", "").replace("\n", " ")  if data_json.get("product", {}).get("description", "") else ""
            row["reviewCount"] = round(number_of_reviews,2) if number_of_reviews else ""
            row["reviewRating"] = round(average_ratings,2) if average_ratings else ""
            row["videoURL"] = ""
            row["isSellingFast"] = ""
            Stock = data_json.get("product", {}).get("baseOptions", {}).get("selected",{}).get("stock",{}).get("stockLevelStatus", "")
            row["isRestockingSoon"] = "Y" if Stock == "outOfStock" else ""
            row["isPromotion"] = "Y" if row["onSale"] == "Y" else ""
            row["isOutletPrice"] = ""
            row["lowestPriceText"] = row["price"]
            row["lowestPriceValue"] = price.get("value","")

            attribute_index = 1
            max_attributes = 20

            classifications = data_json.get("product", {}).get("classifications", [])
            if isinstance(classifications, dict):
                classifications = [classifications]

            for attr in classifications:
                if not isinstance(attr, dict):
                    continue

                section_name = attr.get("name", "")
                features = attr.get("features", [])

                if isinstance(features, dict):
                    features = [features]

                for feature in features:
                    if attribute_index > max_attributes:
                        break

                    attribute_title = feature.get("name", section_name)
                    feature_values = feature.get("featureValues", {})

                    if isinstance(feature_values, dict):
                        attribute_value = feature_values.get("value", "")
                    else:
                        attribute_value = feature_values

                    row[f"attributeType{attribute_index}"] = section_name
                    row[f"attributeTitle{attribute_index}"] = attribute_title
                    row[f"attributeValue{attribute_index}"] = attribute_value.replace("\n", " ").strip()

                    attribute_index += 1
            if row["price"]:
                append_to_csv(row, "products.csv")




async def extract_links(sitmap_url):


    resource = await fetch_url(sitmap_url, content_type="sitemap")
    sitemap_json = json.loads(resource)
    with open("sony_sitemap.json", "w") as f:
        json.dump(sitemap_json, f, indent=4)


    ids = []
    if sitemap_json:
        ids_products = sitemap_json.get('searchResponse', {}).get('products', [])
        for product in ids_products:
            skus = product.get('skus',[])
            for sku in skus:
                id_data = sku.get('d2cId')
                stock = sku.get('d2c',{}).get('stock',{}).get('stockLevelStatus')
                url_product = sku.get('d2c',{}).get('url',"")
                if id_data and stock and url_product:
                    ids.append(f"{id_data}|{stock}|https://www.sony.co.uk{url_product}")

    return ids








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
    sitmap_url = "https://www.sony.co.uk/api/open/products?locale=en_GB&count=10000&"
    products_urls = await extract_links(sitmap_url)
    data = await extact_data_from_product_url(products_urls)


if __name__ == "__main__":
    asyncio.run(main())
