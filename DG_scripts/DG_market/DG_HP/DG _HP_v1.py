from functions import *
from bs4 import BeautifulSoup
from html import unescape
import os




if DEFAULT_CONFIG["save_local"]:

    for folder in [DATA_DIR, SITEMAPS_DIR, PRODUCTS_DIR,OUTPUTS_DIR ]:
        os.makedirs(folder, exist_ok=True)

logger = setup_logger("logs/scraper.log")







def find_between(s, first, last):
    try:
        start = s.index(first) + len(first)
        end = s.index(last, start)
        return s[start:end]
    except ValueError:
        return ""


def clean_html_description(raw_html: str) -> str:
    if raw_html is None:
        raw_html = ''
    text = re.sub(r'<[^>]+>', '', raw_html)
    text = re.sub(r'\s+', ' ', text).strip()
    return unescape(text)

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











async def fetch_review(product_id):
    row = {}
    reviews_url = f'https://api.bazaarvoice.com/data/batch.json?passkey=ca4yId3QMwbLS4120AdQjlPblC1UBGX8rE1uNyvnk9q7Q&apiversion=5.5&&resource.q0=products&filter.q0=id:eq:{product_id}&filteredstats.q0=reviews'
    response = await fetch_url(reviews_url, content_type='product')
    try:
        response = response.replace("BV._internal.dataHandler0(", "").rstrip(");")
        # print(response)
    except Exception:
        print("Error processing response")
        return {}
    if response:
        data_review = json.loads(response)
        try:
            row["reviewCount"] = data_review["BatchedResults"]["q0"]["Results"][0]["FilteredReviewStatistics"]["TotalReviewCount"]
            row["reviewRating"] = round(data_review["BatchedResults"]["q0"]["Results"][0]["FilteredReviewStatistics"]["AverageOverallRating"],1)

            return row
        except:
            row["reviewCount"] = ""
            row["reviewRating"] = ""
            return row


async def fetch_single_product(url):
    response = await fetch_url(url,content_type='product')

    if response:
        pattern = r'window\.__PRELOADED_PRODUCT_DETAILS__\s*=\s*(\{[\s\S]*?\})\s*;\s*</script>'
        match = re.search(pattern, response)
        if match:
            soup = BeautifulSoup(response, 'html.parser')
            meganav = soup.find('div', class_='meganav new-design')
            breadcrumb_links = meganav.find_all('a')
            current_item = soup.find('div', class_='breadcrumbs__current').text.strip()
            breadcrumb_texts = [link.text for link in breadcrumb_links] + [current_item]
            breadcrumb_texts = [text for text in breadcrumb_texts if text != "Back"]
            row = {}
            json_text = match.group(1)
            json_text_fixed = json_text.replace("\\'", "\\\\'")

            product_json = json.loads(json_text_fixed)
            availability = product_json.get("availability",{}).get("level","")
            row["stock"] = "Y"
            if availability == "OUT_OF_STOCK":
                row["stock"] = "N"
            row['source'] = 'HP'
            row["date"] = datetime.now().strftime("%Y-%m-%d")
            row['apiURL'] = "api_url"
            row['url'] = url
            row['sku'] = product_json.get('sku', "")
            row['name'] = product_json.get('name', "")
            row['brand'] = "HP"
            regular_price = product_json.get('price', {}).get('regularPrice', 0)
            price_difference = product_json.get('price', {}).get('priceDifference',0) or 0
            row['price'] = round(regular_price - price_difference, 2)
            row['previousPrice'] = "" if price_difference == 0 else round(regular_price, 2)
            row['onSale'] = "Y" if row['previousPrice'] != "" else ""
            row['saleText'] = f"SAVE £{price_difference}" if row['previousPrice'] != "" else ""
            for item in product_json.get('techspecs', {}).get('technical_specifications', []):
                for spec in item.get('technical_specifications', []):
                    if spec['name'] == 'Product color':
                        row['colour'] = spec['value']
                        break

            if 'colour' not in row:
                row['colour'] = ""
            row['size'] = ""
            row['UPC'] = ""
            row['EAN'] = ""
            row['cat'] = breadcrumb_texts[0]
            row['subcat1'] = breadcrumb_texts[1] if len(breadcrumb_texts) > 1 else ''
            row['subcat2'] = breadcrumb_texts[2] if len(breadcrumb_texts) > 2 else ''
            row['subcat3'] = breadcrumb_texts[3] if len(breadcrumb_texts) > 3 else ''
            row['subcat4'] = breadcrumb_texts[4] if len(breadcrumb_texts) > 4 else ''
            row['subcat5'] = breadcrumb_texts[5] if len(breadcrumb_texts) > 5 else ''
            for item in product_json.get('techspecs', {}).get('technical_specifications', []):
                for spec in item.get('technical_specifications', []):
                    if spec['name'] == 'Manufacturer Warranty':
                        row['warranty'] = spec['value']
                        break

            if 'warranty' not in row:
                row['warranty'] = ""

            media_gallery = product_json.get('media_gallery', None)
            if media_gallery and isinstance(media_gallery, list):
                for i in range(min(5, len(media_gallery))):
                    row[f'image{i + 1}'] = media_gallery[i].get('sizes', {}).get('large', {}).get('url', '')
            else:
                for i in range(5):
                    row[f'image{i + 1}'] = ""


            row['desc'] = clean_html_description(product_json.get('product_overview', {}).get('description','') if product_json and product_json.get('product_overview') else '')
            row['shortDesc'] = ""
            product_id = row['sku'].split("#")[0]
            review_data = await fetch_review(product_id)
            row["reviewCount"] = review_data['reviewCount']
            row["reviewRating"] = review_data['reviewRating']
            video_url = None
            if media_gallery and isinstance(media_gallery, list):
                for item in media_gallery:
                    if item.get('video_content'):
                        video_url = item['video_content']
                        break
            row["videoURL"] = video_url if video_url else ""


            row['isPromotion'] = product_json.get('promotions', [{}])[0].get('type', "") if product_json.get('promotions', [{}]) else ""
            row["isOutletPrice"] = ""
            row["lowestPriceText"] = ""
            row["lowestPriceValue"] = ""

            for i, displayAttributes in enumerate(product_json.get('techspecs', {}).get('highlights', []), start=1):
                attribute_name = displayAttributes.get('name', 'N/A')
                attribute_value = displayAttributes.get('value', 'N/A')
                description = displayAttributes.get('description', 'N/A')

                row[f"attributeType{i}"] = attribute_value
                row[f"attributeTitle{i}"] = attribute_value
                row[f"attributeValue{i}"] = attribute_name + (f" {description}" if description else "")

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
    url = "https://www.hp.com/sitemap-gb-en-isc.xml"
    products = await fetch_sitemap(url)
    all_products = []
    for product in products:
        if "product.aspx" in product:
            all_products.append(product)


    data = await extact_data_from_product_url(all_products)


if __name__ == "__main__":
    asyncio.run(main())
