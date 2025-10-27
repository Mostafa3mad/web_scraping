import time
from functions import *
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


async def fetch_page(url_product_and_id) -> dict[str, Any]:
    '''

    :param url_product_and_id:
    :return:  data json  without price
    data contain  desc image
    '''

    productCode = url_product_and_id.split("|")[0]
    bindSource = url_product_and_id.split("|")[1]
    json_data = {
        'productCode': productCode,
        'storeViewCode': 'uk',
        'settleChannel': 3,
        'bindSource': bindSource,
    }
    api_data_product_sku = f"https://mallapi-eu.oneplus.com/v2/api/rest/mallapp/product/page/fetch?productCode={productCode}&bindSource={bindSource}"
    response_sku = await fetch_url(api_data_product_sku, method="POST", json_data=json_data, content_type="product")
    sku_json = json.loads(response_sku)
    return sku_json

def get_skus(sku_json) -> list:
    skus_id = []
    skus = sku_json.get("data", {}).get("mainSkuList", [])
    for sku in skus:
        sku_id = sku.get("skuCode","")
        skus_id.append(sku_id)
    return skus_id

async def  fetch_price(url_product_and_id) -> dict[str, Any]:

    productCode = url_product_and_id.split("|")[0]
    bindSource = url_product_and_id.split("|")[1]
    api_data_product = f"https://mallapi-eu.oneplus.com/v2/api/rest/mall/product/detail/fetch?productCode={productCode}&bindSource={bindSource}"
    json_data = {
        'productCode': productCode,
        'pincode': '',
        'storeViewCode': 'uk',
        'settleChannel': 3,
        'bindSource': bindSource,
    }
    response_details = await fetch_url(api_data_product, method="POST", json_data=json_data, content_type="product")
    data_json = json.loads(response_details)
    return data_json,api_data_product

async def fetch_attributes(url_product_and_id):
    productCode = url_product_and_id.split("|")[0]
    bindSource = url_product_and_id.split("|")[1]
    api_attributes_product = f"https://mallapi-eu.oneplus.com/v2/api/rest/mall/product/attachment/fetch?productCode={productCode}&bindSource={bindSource}"

    json_data = {
        'attachmentTypeList': [
            1,
            2,
            3,
            4,
            5,
            6,
        ],
        'productCode': productCode,
        'storeViewCode': 'uk',
        'settleChannel': 3,
        'bindSource': bindSource,
    }
    response_attributes = await fetch_url(api_attributes_product, method="POST", json_data=json_data, content_type="product")
    data_json_response_attributes = json.loads(response_attributes)
    return data_json_response_attributes



def get_category(response_html) -> list:

    categories = []
    soup = BeautifulSoup(response_html, "html.parser")
    scripts = soup.find_all("script")

    breadcrumbs = None
    for script in scripts:
        if "window.BreadCrumbs" in script.text:
            match = re.search(r'window\.BreadCrumbs\s*=\s*(\[.*?\])', script.text, re.S)
            if match:
                breadcrumbs_str = match.group(1)
                breadcrumbs = json.loads(breadcrumbs_str)


    return breadcrumbs


def get_images(response_html):
    soup = BeautifulSoup(response_html, "html.parser")

    script_tag = soup.find("script", {"id": "data-page"})
    json_str = script_tag.string.strip()
    data = json.loads(json_str)

    return data


def get_description(response_html):
    soup = BeautifulSoup(response_html, "html.parser")

    script_tag = soup.find("script", {"id": "data-seo"})
    json_str = script_tag.string.strip()
    description_json = json.loads(json_str)
    description = description_json.get("description","")
    shortDescription = description_json.get("shortDescription","")
    return description,shortDescription




def extract_clean_description(raw_desc: dict) -> str:

    cleaned = re.sub(r"<.*?>", " ", str(raw_desc))
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    parts = [part.strip() for part in cleaned.split(" . ") if part.strip()]
    return " | ".join(parts)


async def fetch_single_product(url_product_and_id: str):
    bindSource = url_product_and_id.split("|")[1]
    response_html = await fetch_url(bindSource, content_type="product")
    try:
        breadcrumbs = get_category(response_html)
    except:
        logger.info("Out of stock or page not found")

    sku_json = await fetch_page(url_product_and_id)

    skus_id = get_skus(sku_json)

    data_json ,api_data_product = await fetch_price(url_product_and_id)

    fetch_attributes_json = await fetch_attributes(url_product_and_id)
    #
    #

    if data_json:

            products = data_json.get("data",{}).get("products", [])
            for product in products:
                row = {}
                skuCode = product.get("skuCode", "")
                for sku in skus_id:
                    if sku == skuCode:
                        row["source"] = "oneplus.com"
                        row["date"] = datetime.now().strftime("%Y-%m-%d")
                        row["apiURL"] = api_data_product
                        row["url"] = bindSource
                        row["sku"] = sku
                        row["name"] = product.get("name", "")
                        row["brand"] = "OnePlus"
                        prices = data_json.get("data", {}).get("prices", {})

                        price_info = prices.get(sku, {})
                        sale_price = price_info.get("salePrice")
                        price = price_info.get("originalPrice")
                        discountRate = price_info.get("discountRate")

                        row["price"] = sale_price
                        row["onSale"] = ""
                        row["stock"] = "N"
                        if row["price"] :
                            row["stock"] = "Y"

                        if discountRate != 0:
                            row["previousPrice"] = price
                            row["onSale"] = "Y"
                            row["saleText"] = f"{discountRate}% off"
                        sku_varitys = sku_json.get("data", {}).get("mainSkuList", {})
                        for sku_varity in sku_varitys:
                            sku_data = sku_varity.get("skuCode")
                            if sku_data == sku:
                                virtualOptions = sku_varity.get("virtualOptions", [])
                                for virtualOption in virtualOptions:
                                    if virtualOption.get("attrTitle") == "Color":
                                        row["colour"] = virtualOption.get("optLabel", "")
                                    if virtualOption.get("attrTitle") == "Storage":
                                        row["size"] = virtualOption.get("optLabel", "")
                                row["UPC"] = ""
                                row["EAN"] = ""
                                try:
                                    fields = ["cat", "subcat1", "subcat2", "subcat3", "subcat4", "subcat5"]
                                    for i, crumb in enumerate(breadcrumbs):
                                        if i < len(fields):
                                            row[fields[i]] = crumb["text"]
                                except:
                                    row["isRestockingSoon"] = "Y"

                                row["warranty"] = ""
                                try:
                                    images_json = get_images(response_html)
                                    imageLibrary = images_json.get("imageLibrary",[])
                                    for image in imageLibrary:
                                        sku_image = image.get("skuCode","")
                                        if sku_image == sku:
                                            images_list = image.get("images",[])
                                            for i, url in enumerate(images_list):
                                                if i > 5:
                                                    break
                                                row[f"image{i + 1}"] = url
                                except:
                                    pass
                                try:
                                    description,shortDescription = get_description(response_html)
                                except:
                                    description = ""
                                    shortDescription = ""
                                row["desc"] = extract_clean_description(description)
                                row["shortDesc"] = extract_clean_description(shortDescription)
                                row["reviewCount"] = ""
                                row["reviewRating"] = ""
                                row["videoURL"] = ""
                                row["isSellingFast"] = ""
                                if discountRate != 0:
                                    row["isPromotion"] = ""
                                row["isOutletPrice"] = ""
                                row["lowestPriceText"] = f"£{row['price']}"
                                row["lowestPriceValue"] = row['price']
                                attribute_index = 1
                                try:
                                    basicAttributeList = fetch_attributes_json.get("data", {}).get("productSpecsInfo",
                                                                                                   {}).get(
                                        "basicAttributeList", [])
                                    for attr in basicAttributeList:
                                        section_name = attr.get("name", "")

                                        for detail in attr.get("nameDetailList", []):
                                            if attribute_index >= 21:
                                                break

                                            attribute_title = detail.get("name", section_name)
                                            values = [v.get("content", "") for v in detail.get("valueList", [])]
                                            attribute_value = ", ".join([v for v in values if v])

                                            row[f"attributeType{attribute_index}"] = "specification"
                                            row[f"attributeTitle{attribute_index}"] = attribute_title
                                            row[f"attributeValue{attribute_index}"] = attribute_value

                                            attribute_index += 1
                                except:
                                    pass
                                append_to_csv(row, "products.csv")





async def extract_links(sitmap_url):
    urls_product = []
    json_data = {
        'settleChannel': 3,
        'storeViewCode': 'uk',
    }
    response = await fetch_url(sitmap_url, method="POST", json_data=json_data, content_type="sitemap")

    category_json = json.loads(response)


    categoryList = category_json.get("data", {}).get("categoryList", [])

    for category in categoryList:
        categoryName = category.get("categoryName", "")
        if categoryName == "Store":
            categoryCode = category.get("categoryCode", "")
            break

    json_data_category = {
        'settleChannel': 3,
        'storeViewCode': 'uk',
        'filterGroupQueryList': [],
        'categoryCode': categoryCode,
        'sortOption': 'LATEST',
        'sceneType': 1,
        'pageSize': 1000,
        'currentPage': 1,
    }

    api_goods = "https://mallapi-eu.oneplus.com/v2/api/rest/mallweb/category/goods/fetch"
    response_data = await fetch_url(api_goods, method="POST", json_data=json_data_category, content_type="sitemap")


    category_json = json.loads(response_data)

    productList = category_json.get("data", {}).get("productList", [])
    for product in productList:
        productDetailUrl = product.get("productDetailUrl", "")
        productCode = product.get("productCode", "")

        urls_product.append(f"{productCode}|{productDetailUrl}")


    return urls_product









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
    sitmap_url = "https://mallapi-eu.oneplus.com/v2/api/rest/mallweb/category/config/fetch"
    products_urls = await extract_links(sitmap_url)
    data = await extact_data_from_product_url(products_urls)


if __name__ == "__main__":
    asyncio.run(main())
