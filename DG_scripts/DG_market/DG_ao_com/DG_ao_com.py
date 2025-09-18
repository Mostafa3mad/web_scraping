import time
from functions import *
import os
from html import unescape
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from functions import *
import gzip
from io import BytesIO
import html





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
        with open(OUTPUTS_DIR/filepath, mode="w", newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()

def append_to_csv(item, filepath):

    if DEFAULT_CONFIG["save_local"]:

        headers = get_standard_csv_headers()
        with open(OUTPUTS_DIR/filepath, mode="a", newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writerow(item)



def js_object_to_json_str(s: str) -> str:
    s = re.sub(r'([{\[,]\s*)([A-Za-z_]\w*)\s*:', r'\1"\2":', s)
    s = re.sub(r',\s*([}\]])', r'\1', s)
    return s

def extract_swatches_data(html: str):
    m = re.search(r"window\.swatchesData\s*=\s*({[\s\S]*?})\s*(?:;</script>|</script>|;)", html, re.I)
    if not m:
        raise ValueError("swatchesData not found")
    raw_obj = m.group(1).strip()

    try:
        return json.loads(raw_obj)
    except Exception:
        fixed = js_object_to_json_str(raw_obj)
        return json.loads(fixed)

def get_colors(html: str, colourid: str | int):
    data = extract_swatches_data(html)
    variants = data.get("variants", [])
    for v in variants:
        if str(v.get("ProductColourId")) == str(colourid):
            return  v.get("TrueColour") or v.get("BaseColour")
    return ""
def get_size(html: str, colourid: str | int):
    data = extract_swatches_data(html)
    variants = data.get("variants", [])
    for v in variants:
        if str(v.get("ProductColourId")) == str(colourid):
            return  v.get("SizeLocalised")
    return ""

def get_cat(response):
    data_cat = re.search(r'<script[^>]+id=["\']product-page-json-ld["\'][^>]*>(.*?)</script>',response,re.DOTALL | re.IGNORECASE)
    raw_json_cat = data_cat.group(1).strip()
    raw_json_cat = raw_json_cat.replace("\r", "").replace("\n", "")
    data = json.loads(raw_json_cat)
    return data
def get_images(response):
    data_images = re.search(r'<script[^>]+id=["\']product-json["\'][^>]*>(.*?)</script>',response,re.DOTALL | re.IGNORECASE)
    raw_json_images = data_images.group(1).strip()
    raw_json_images = raw_json_images.replace("\r", "").replace("\n", "")
    data = json.loads(raw_json_images)
    images = data.get("Gallery", {}).get("images", [])

    return images
def extract_desc(html: str):
    m = re.search(r"window\.productOverviewData\s*=\s*({[\s\S]*?})\s*(?:;</script>|</script>|;)", html, re.I)
    if not m:
        raise ValueError("swatchesData not found")
    raw_obj = m.group(1).strip()

    try:
        return json.loads(raw_obj)
    except Exception:
        fixed = js_object_to_json_str(raw_obj)
        return json.loads(fixed)


def clean_and_format(data):
    def clean_html(text):
        clean_text = re.sub(r'<.*?>', '', text)
        clean_text = re.sub(r'&\w+;', '', clean_text)
        return clean_text.strip()

    paragraphs = re.findall(r'<p.*?>(.*?)</p>', data['summary'], re.DOTALL)
    cleaned_paragraphs = [clean_html(p) for p in paragraphs if p.strip()]

    # cleaned_features = [clean_html(feature) for feature in data['keyFeatures']]

    # cleaned_things = []
    # for item in data['thingsToKnow']:
    #     cleaned_item = clean_html(item['Description'])
    #     cleaned_things.append(f"{item['TabTitle']}: {cleaned_item}")

    result = " | ".join(cleaned_paragraphs)

    return result
def clean_attribute_value(val: str) -> str:
    val = re.sub(r'\s+', ' ', val)  # remove extra spaces/newlines
    return val.strip()


def extract_specifications(html):
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select('div[data-accordion-content] > div.flex.items-center.p-3')

    attributes_list = []
    for row in rows:
        title_div = row.find("div", {"data-tag-type": "accordion"})
        attribute_title = title_div.get_text(strip=True) if title_div else ""
        value_span = row.find("span", class_=re.compile(r"text-right"))
        attribute_value = value_span.get_text(strip=True) if value_span else ""
        attributes_list.append({
            "displayName": attribute_title,
            "values": [attribute_value]
        })

    return attributes_list
async def fetch_single_product(url):


    for i in range(10):

        try:
            response = await fetch_url(url,content_type="product",headers={})

            break
        except Exception as e:
            pass

    if response:
        response = await fetch_url(url, content_type="product", headers={})
        data_product = re.search(r'<script id=" ?digital-data-product ?">(.*?)</script>', response, re.DOTALL)
        if data_product:
            data_product = data_product.group(1).replace('\n','').replace("window.digitalData.page.product = Object.assign(window.digitalData.page.product, ","")
            json_str = data_product
            json_str_cleaned = re.sub(r'\);$', '', json_str)
            product_data = json.loads(json_str_cleaned)

            row = {}
            row["source"] = "ao.com"
            row["date"] = datetime.now().strftime("%Y-%m-%d")
            row['apiURL'] = ""
            row['url'] = url
            row['sku'] = product_data.get("sku","")
            row['name'] = product_data.get("title","")
            row['brand'] = product_data.get("brand","")
            row['price'] = product_data.get("price","")
            match_previousPrice = re.search(r'data-was-price="([^"]+)"', response)
            if match_previousPrice:
                price = match_previousPrice.group(1)
                row['previousPrice'] = "" if html.unescape(price) == "£0" else html.unescape(price)
            else:
                row['previousPrice'] = ""

            if row['previousPrice']:
                row["onSale"] = "Y"
                row["saleText"] = f"Was {row['previousPrice']}, now {row['price']}"
            else:
                row["onSale"] = ""
                row["saleText"] = ""
            colourid  =  product_data.get("colourId","")
            color = get_colors(response, colourid)

            row['colour'] = color
            size = get_size(response, colourid)

            row['size'] = size
            row['UPC'] = ""
            row['EAN'] = ""
            cat = get_cat(response)[0]
            if cat:
                row['cat'] = cat.get("itemListElement",[])[0].get("name","") if len(cat.get("itemListElement",[])) > 0 else ""
                row['subcat1'] = cat.get("itemListElement",[])[1].get("name","") if len(cat.get("itemListElement",[])) > 1 else ""
                row['subcat2'] = cat.get("itemListElement",[])[2].get("name","") if len(cat.get("itemListElement",[])) > 2 else ""
                row['subcat3'] = cat.get("itemListElement",[])[3].get("name","") if len(cat.get("itemListElement",[])) > 3 else ""
                row['subcat4'] = cat.get("itemListElement",[])[4].get("name","") if len(cat.get("itemListElement",[]))>4 else ""
                row['subcat5'] = cat.get("itemListElement",[])[5].get("name","") if len(cat.get("itemListElement",[]))>5 else ""
            row["warranty"] = ""


            images = get_images(response)
            for i, img in enumerate(images):
                if i >=5:
                    break
                row[f"image{i+1}"] = img.get("large","")


            try:
                row["desc"] = clean_and_format(extract_desc(response)) or ""
            except:

                row["desc"] = ""


            row["shortDesc"] = get_cat(response)[0].get("description", "").replace("\n", " ").strip()

            reviewCount = product_data.get("reviewCount",0)
            row["reviewCount"] = reviewCount
            if reviewCount:
                reviewRating = product_data.get("reviewsInfo",0).split("|")[0]
                row["reviewRating"] = float(reviewRating)
            row["videoURL"] = ""
            row["isSellingFast"] = ""
            row["isRestockingSoon"] = "Y" if product_data.get("isInStock",False) == False else ""
            row["isPromotion"] = "Y" if row["onSale"] == "Y" else ""
            row["isOutletPrice"] = ""
            row["lowestPriceText"] = f"£{row['price']}"
            row["lowestPriceValue"] = row["price"]
            specs = extract_specifications(response)

            attribute_index = 1
            for attr in specs:
                if attribute_index >= 21:
                    break

                attribute_title = attr.get("displayName", "")
                attribute_values = attr.get("values", [])
                attribute_value = ", ".join(attribute_values) if attribute_values else ""

                row[f"attributeType{attribute_index}"] = "specification"
                row[f"attributeTitle{attribute_index}"] = attribute_title
                row[f"attributeValue{attribute_index}"] = clean_attribute_value(attribute_value)

                attribute_index += 1
            append_to_csv(row, "products.csv")








async def extract_links(sitmap_url):
    urls = []
    urls_sitemaps = []
    for i in range(10):
        try:
            response = await fetch_url(sitmap_url,content_type="sitemap",headers={})
            break
        except Exception as e:
            pass
    if response:
        root = ET.fromstring(response)
        ns = {'sm': 'http://www.google.com/schemas/sitemap/0.9'}
        loc_elements = root.findall('.//sm:loc', ns)
        if not loc_elements:
            loc_elements = root.findall('.//loc')
        for loc in loc_elements:
            urls_sitemaps.append(loc.text)
    logger.info(urls_sitemaps)
    for url in urls_sitemaps:
        for i in range(10):
            try:
                response = await fetch_url(url,content_type="sitemap",headers={})
                break
            except Exception as e:
                pass
        if response:
            root = ET.fromstring(response)
            ns = {'sm': 'http://www.google.com/schemas/sitemap/0.9'}
            loc_elements = root.findall('.//sm:loc', ns)
            if not loc_elements:
                loc_elements = root.findall('.//loc')
            for loc in loc_elements:

                urls.append(loc.text)

    return urls









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
    sitmap_url = "https://ao.com/sitemaps/product/toc.xml"
    products_urls = await extract_links(sitmap_url)
    data = await extact_data_from_product_url(products_urls[0:10])


if __name__ == "__main__":
    asyncio.run(main())
