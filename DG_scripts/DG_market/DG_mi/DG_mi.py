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





async def fetch_single_product(id_url):

    api_url = f"https://ams-go.buy.mi.com/uk/v2/item/productdetail?from=pc&tag={id_url}"

    response = await fetch_url(api_url,content_type="product")
    response_json = json.loads(response)

    # print(response_json)

    if response_json:

        specs_url = f"https://www.mi.com/uk/product/{id_url}/specs/"
        response_html  = await fetch_url(specs_url,content_type="product")
        soup = BeautifulSoup(response_html, "html.parser")

        row = {}
        script_tag = soup.find("script", string=re.compile("__PRELOADED_STATE__"))
        script_text = script_tag.string
        json_text = script_text.split("=", 1)[1].strip().rstrip(";")
        specs_json = json.loads(json_text)
        spu_list = response_json.get("data",{}).get("item_detail", {}).get("spu_list",[])

        for spu in spu_list:
            items = spu.get("item_list", [])
            for item in items:
                is_stock = item.get("is_out_of_stock", False)
                # print(is_stock)
                if is_stock:
                    row["stock"] = "N"

                else:
                    row["stock"] = "Y"
                sku = item.get("item_id", "")
                row["source"] = "xiaomi"
                row["date"] = datetime.now().strftime("%Y-%m-%d")
                row["apiURL"] = api_url
                row["url"] = f"https://www.mi.com/uk/product/{id_url}/buy/?gid={sku}"
                row["sku"] = sku
                row["name"] = item.get("item_name", "")
                row["brand"] = "xiaomi"
                row["price"] = item.get("price", "")

                previousPrice = item.get("market_price", "")
                if previousPrice and row["price"] and previousPrice > row["price"]:
                    row["previousPrice"] = previousPrice
                    row["onSale"] = "Y"
                    discount_percent = round((previousPrice - row["price"]))
                    row["saleText"] = f"save £{discount_percent}"
                else:
                    row["previousPrice"] = ""
                    row["onSale"] = ""
                    row["saleText"] = ""

                try:
                    specs_list = response_json.get("data", {}).get("item_detail", []).get("specs_list", {}).get(
                        "sku_list", [])
                    for specs in specs_list:
                        item_id = specs.get("item_id", "")
                        if sku == item_id:
                            spec_values = specs.get("specs_item", [])
                            if len(spec_values) > 0:
                                row["colour"] = spec_values[0]
                            if len(spec_values) > 1:
                                row["size"] = spec_values[1]
                except:
                    row["colour"] = ""
                    row["size"] = ""

                row["UPC"] = ""
                row["EAN"] = ""

                categories = item.get("categories", [])
                fields = ["cat", "subcat1", "subcat2", "subcat3", "subcat4", "subcat5"]
                for i, cat in enumerate(categories):
                    if i < len(fields):
                        row[fields[i]] = cat.get("title", "")

                seo_desc = specs_json.get("pagedata", {}).get("seo", {}).get("description", "")

                row["desc"] = " ".join(seo_desc.split())
                row["shortDesc"] = ""
                row["reviewCount"] = response_json.get("data", {}).get("review", "").get("comments_total", "")
                row["reviewRating"] = response_json.get("data", {}).get("review", "").get("comments_star", "")

                row["warranty"] = ""
                images = item.get("resource_list", [])
                for i, image in enumerate(images[:5]):
                    row[f"image{i + 1}"] = image.get("src", "")

                data_raw = specs_json.get("pagedata", {}).get("data", "{}")
                data_json = json.loads(data_raw)
                attribute_index = 1
                capture = False
                last_title = None

                for key, val in data_json.items():
                    if not isinstance(val, dict):
                        continue

                    text = val.get("trans") or val.get("alt")
                    if not text:
                        continue

                    text = text.strip()
                    if (("specifications" in text.lower() or
                         "Processor" in text.lower()) or
                            "RAM" in text.lower() or
                            "Storage" in text.lower()):
                        capture = True
                        continue
                    if capture and text.lower() in ["package contents", "specification", "features"]:
                        break

                    if capture:
                        if attribute_index >= 20:
                            break
                        if last_title is None:
                            last_title = text
                        else:
                            clean_text = " ".join(text.split()).replace("*","")
                            clean_last_title = " ".join(last_title.split())

                            row[f"attributeType{attribute_index}"] = "specification"
                            row[f"attributeTitle{attribute_index}"] = clean_last_title
                            row[f"attributeValue{attribute_index}"] = clean_text

                            attribute_index += 1
                            last_title = None

                # print(row)
                append_to_csv(row, "products.csv")






async def extract_links():
    url = "https://ams-api.buy.mi.com/uk/search/v1/api/index//0/0/0/1/0/0?version=v4&cacheable=2&productIds=&from=pc&pagesize=100"
    response = await fetch_url(url, content_type="sitemap")
    response_json = json.loads(response)
    total_pages = response_json["data"]["dataProvider"]["total_pages"]
    all_list_product = []
    for page in range(0, total_pages):
        url = f"https://ams-api.buy.mi.com/uk/search/v1/api/index//0/0/0/{page}/0/0?version=v4&cacheable=2&productIds=&from=pc&pagesize=100"
        response = await fetch_url(url, content_type="sitemap")
        response_json = json.loads(response)
        products = response_json["data"]["dataProvider"]["data"]

        for product in products:
            product_id = product["product"]["production_station"]
            if product_id:
                all_list_product.append(product_id)


    all_list_product = list(dict.fromkeys(all_list_product))

    # print(all_list_product)
    return all_list_product








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
    products_urls = await extract_links()
    data = await extact_data_from_product_url(products_urls)


if __name__ == "__main__":
    asyncio.run(main())
