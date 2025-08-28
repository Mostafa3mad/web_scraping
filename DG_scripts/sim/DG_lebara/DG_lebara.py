import os
from functions import *
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import html





if DEFAULT_CONFIG["save_local"]:

    for folder in [DATA_DIR, SITEMAPS_DIR, PRODUCTS_DIR,OUTPUTS_DIR ]:
        os.makedirs(folder, exist_ok=True)

logger = setup_logger("logs/scraper.log")




def get_standard_csv_headers():
    headers = [
        "source", "date", "apiURL", "url", "sku", "name", "brand","stock",
        "advance","paymentAmount","phoneContractDuration","sim_price","simContractname","simContractDuration","phoneContractPrice","isPhoneContractAvailableWOsim"
        ,"phoneContractSimPackage","handsetOnlyCostCash","handsetOnlyContract",
        "previousPrice", "onSale", "saleText",
        "plan_type","sim_data","simOfferData", "sim1YearIncrease", "sim2YearIncrease", "sim3YearIncrease","simDesc",
        "colour", "size", "UPC", "EAN",
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


def extract_images(html: str, base_url: str = "https://phones.lebara.co.uk"):
    """
    Extract all unique handset images (full paths).
    Args:
        html (str): page HTML
        base_url (str): base domain for relative paths
    Returns:
        list: list of image URLs
    """
    soup = BeautifulSoup(html, "html.parser")
    images = []

    for a in soup.select("#handset-gallery a[href]"):
        full = urljoin(base_url, a.get("href").strip())
        images.append(full)

    chosen = soup.select_one(".handset-chosen img[src]")
    if chosen:
        full = urljoin(base_url, chosen.get("src").strip())
        if full not in images:
            images.insert(0, full)

    seen = set()
    unique_images = []
    for img in images:
        if img not in seen:
            unique_images.append(img)
            seen.add(img)

    return unique_images

def get_size(soup, fallback_name="", fallback_url=""):
    size_tag = soup.select_one("div.configure-container.default p")
    if size_tag:
        return size_tag.get_text(strip=True)

    if fallback_name:
        match = re.search(r"\b\d+\s*(GB|TB)\b", fallback_name, re.IGNORECASE)
        if match:
            return match.group(0).upper()

    if fallback_url:
        match = re.search(r"(\d+)(gb|tb)", fallback_url, re.IGNORECASE)
        if match:
            return match.group(0).upper()

    return ""


def extract_specs(raw_data, max_attributes=20):
    if isinstance(raw_data, dict):
        raw_html = raw_data.get("html", "")
    else:
        raw_html = raw_data

    clean_html = html.unescape(raw_html)
    clean_html = clean_html.replace('\\"', '"').replace("\\/", "/")

    soup = BeautifulSoup(clean_html, "html.parser")

    specs = []
    rows = soup.select("table.spec-section tr")
    for tr in rows[:max_attributes]:
        th = tr.select_one("th.spec-title")
        td = tr.select_one("td.spec-copy")
        if not th or not td:
            continue

        title = th.get_text(strip=True).rstrip(":")
        value = " ".join(td.stripped_strings)

        specs.append({
            "attributeType": "SPECIFICATION",
            "attributeTitle": title,
            "attributeValue": value.upper()
        })
    return specs



async def fetch_single_product(url: str):


    response = await fetch_url(url, content_type="product")

    soup = BeautifulSoup(response, "html.parser")
    items = [li.get_text(strip=True) for li in soup.select(".summary li")]

    desc = " | ".join(items)
    pattern = r"dataLayer\.push\((\{[\s\S]*?\})\);"
    matches = re.findall(pattern, response)

    for block in matches:
        if "'ecommerce'" in block or '"ecommerce"' in block:
            fixed = block.replace("'", '"')
            data_product = json.loads(fixed)
            if data_product :
                row = {}
                row["source"] = "lebara"
                row["date"] = datetime.now().strftime("%Y-%m-%d")
                row["apiURL"] = ""
                row["url"] = url
                row["sku"] = data_product["ecommerce"]["impressions"][0]["id"]


                row["name"] = url.split("/")[-1].split("?")[0].replace("-", " ").title()
                row["brand"] = data_product["ecommerce"]["impressions"][0]["brand"]
                row["stock"] = ""
                row["desc"] = desc
                row["shortDesc"] = ""
                data = {
                    'handset_id': row["sku"],
                }
                api_video = f"https://phones.lebara.co.uk/functions_handset/get_video?{row["sku"]}"

                response_video = await fetch_url(api_video,method="POST", content_type="product", data=data)
                response_video_json = json.loads(response_video)
                clean_html = html.unescape(response_video_json["html"])

                soupvideo = BeautifulSoup(clean_html, "html.parser")
                iframe = soupvideo.find("iframe")
                video_url = iframe["src"] if iframe else ""

                if video_url.startswith("//"):
                    video_url = "https:" + video_url

                row["videoURL"] = video_url if video_url else ""

                row["lowestPriceValue"] = ""
                row["reviewRating"] = ""
                row["reviewCount"] = ""
                row["handsetOnlyCostCash"] = data_product["ecommerce"]["impressions"][0]["price"]
                promo = soup.select_one(".promo-pill")
                if promo:
                    text = promo.get_text(strip=True)
                    match = re.search(r"\d+", text)
                    if match:
                        row["previousPrice"] = float(match.group(0))+float(row["handsetOnlyCostCash"])
                else:
                    row["previousPrice"] = ""

                row["onSale"] = "Y" if row["previousPrice"] else ""
                row["saleText"] = promo.get_text(strip=True) if promo else ""

                row["colour"] = data_product["ecommerce"]["impressions"][0]["variant"]
                name = data_product["ecommerce"]["impressions"][0]["name"]
                match = re.search(r"(\d+\s*[A-Za-z]+)", name)

                row["size"] = get_size(soup)
                row["UPC"] = ""
                row["EAN"] = ""
                cat_str  = data_product["ecommerce"]["impressions"][0]["category"]
                parts = [c.strip() for c in cat_str.split(">")]
                row["cat"] = parts[0] if len(parts) > 0 else ""
                row["subcat1"] = parts[1] if len(parts) > 1 else ""
                row["subcat2"] = parts[2] if len(parts) > 2 else ""
                row["subcat3"] = parts[3] if len(parts) > 3 else ""
                row["subcat4"] = parts[4] if len(parts) > 4 else ""
                row["subcat5"] = parts[5] if len(parts) > 5 else ""
                for li in soup.find_all("li"):
                    text = li.get_text(strip=True)
                    if "warranty" in text.lower():
                        match = re.search(r"\d+\s*[-]?\s*\w+", text)
                        if match:
                            row["warranty"] = match.group(0)
                        break
                    else:
                        row["warranty"] = ""
                row["isSellingFast"] = ""
                row["isRestockingSoon"] = ""
                row["isPromotion"] = "Y" if promo else ""
                row["isOutletPrice"] = ""
                row["lowestPriceText"] = ""
                imgs = extract_images(response)

                for idx in range(1, 5 + 1):
                    if idx <= len(imgs):
                        row[f"image{idx}"] = imgs[idx - 1]
                    else:
                        row[f"image{idx}"] = ""


                api_spec = f"https://phones.lebara.co.uk/functions_handset/get_spec?{row["sku"]}"

                response_spec = await fetch_url(api_spec,method="POST", content_type="product", data=data)
                specs = extract_specs(response_spec)
                for idx, spec in enumerate(specs, start=1):
                    row[f"attributeType{idx}"] = spec["attributeType"]
                    row[f"attributeTitle{idx}"] = spec["attributeTitle"]
                    row[f"attributeValue{idx}"] = spec["attributeValue"]

                append_to_csv(row, "products.csv")










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





def get_product_links(urls):
    bad_keywords = [
        "deal", "deals", "offer", "offers", "sale", "gift", "clearance",
        "help", "about", "upgrade", "sitemap", "submit", "trackorder",
        "legals", "trade-in", "winter-sale", "summer-sale", "black-friday"
    ]
    product_links = []
    for url in urls:
        parts = url.strip("/").split("/")
        if len(parts) > 4:
            slug = parts[-1].lower()

            if not any(bad in slug for bad in bad_keywords):
                product_links.append(url+"?simfree=1")

    return list(set(product_links))


async def main():
    create_csv_file("products.csv")
    url = "https://phones.lebara.co.uk/sitemap.xml"
    site_maps = await fetch_sitemap(url)
    products = get_product_links(site_maps)


    data = await extact_data_from_product_url(products)


if __name__ == "__main__":
    asyncio.run(main())
