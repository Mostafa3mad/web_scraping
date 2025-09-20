import time

from functions import *
import os
from html import unescape
from bs4 import BeautifulSoup
import urllib.parse






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
        with open(OUTPUTS_DIR/filepath, mode="w", newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()

def append_to_csv(item, filepath):

    if DEFAULT_CONFIG["save_local"]:

        headers = get_standard_csv_headers()
        with open(OUTPUTS_DIR/filepath, mode="a", newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writerow(item)





def clean(raw_desc):

    if raw_desc:
        soup = BeautifulSoup(raw_desc, "html.parser")
        parts = [item.get_text(strip=True) for item in soup.find_all(["li", "p", "span"]) if item.get_text(strip=True)]
        clean_desc = " | ".join(parts)
    else:
        clean_desc = ""

    return clean_desc

def extract_sim_contract_duration(token):
    token = token.lower()
    if "12 months" in token or "12month" in token or "12m" in token:
        return 12
    if "24 months" in token or "24month" in token or "24m" in token:
        return 24
    if "30 days" in token or "30day" in token:
        return 1
    if "month" in token or "months" in token:
        words = token.split()
        for word in words:
            if word.isdigit():
                return int(word)

    return "1"
async def fetch_single_product(url: str):
    response = await fetch_url(url, content_type="product")
    if response:
        row = {}
        soup = BeautifulSoup(response, "html.parser")


        script_tag = soup.find("script", {"id": "__NEXT_DATA__"})
        data = json.loads(script_tag.string)
        pdp_data = data.get("props", {}).get("pageProps", {}).get("pdpSeoData", {})
        row["source"] = "lycamobile.co.uk"
        row["date"] = datetime.now().strftime("%Y-%m-%d")
        row["apiURL"] = ""
        row["url"] = url
        row["sku"] = pdp_data["urlKey"]
        row["name"] = pdp_data["productDisplayName"]
        row["brand"] = ""
        row["stock"] = ""
        longDescription = pdp_data.get("longDescription", "")
        shortDescription = pdp_data.get("shortDescription", "")

        row["desc"] = clean(longDescription)
        row["shortDesc"] = clean(shortDescription)
        bundleGroupDetails = pdp_data.get("bundleGroupDetails", [])[0]
        base_price = float(bundleGroupDetails.get("basePrice") or 0.0)
        promo_price = float(bundleGroupDetails.get("promotionalPricing") or 0.0)
        row["onSale"] = "Y" if promo_price and base_price and promo_price < base_price else ""
        if row["onSale"]:
            row["saleText"] = bundleGroupDetails.get("promotionalText","")
            row["previousPrice"] = base_price

        row["plan_type"] =  data.get("props", {}).get("pageProps", {}).get("url", "").get("baseUrl", "") or pdp_data.get("planType",[])[0].get("planType", "")

        row["sim_data"] =  pdp_data.get("promotionalDataAllowance", 0) if pdp_data.get("promotionalDataAllowance")  else pdp_data.get("dataAllowance", 0)

        if row["onSale"]  == "Y":
            row["sim_price"] = promo_price
        else:
            row["sim_price"] = base_price

        if row["onSale"]  == "Y":
            offers = [
                pdp_data.get("headerPromoOfferLine1", ""),
                pdp_data.get("headerPromoOfferLine2", ""),
                pdp_data.get("promoOfferLine1", ""),
                pdp_data.get("promoOfferLine2", "")
            ]
            row["simOfferData"] = " | ".join([o for o in offers if o])

        row["simContractname"] = pdp_data.get("title", "")
        months = extract_sim_contract_duration(row["name"])

        row["simContractDuration"] = months
        row["simDesc"] = pdp_data.get("description", "") if pdp_data.get("description") else row["desc"]
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





async def extract_site_map(site_map_url):
    url_products = []
    products = await fetch_sitemap(site_map_url)
    for product in products:
        if "https://www.lycamobile.co.uk/en/bundle/" in product or "https://www.lycamobile.co.uk/paymonthly/en/bundle/" in product:
            url_products.append(product)

    return url_products


async def main():
    create_csv_file("products.csv")
    site_map_url = "https://www.lycamobile.co.uk/sitemap.xml"
    products  = await extract_site_map(site_map_url)
    data = await extact_data_from_product_url(products)


if __name__ == "__main__":
    asyncio.run(main())
