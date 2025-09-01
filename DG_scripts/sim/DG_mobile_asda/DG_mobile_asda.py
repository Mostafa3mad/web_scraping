from functions import *
import os
from html import unescape
from bs4 import BeautifulSoup
import html






if DEFAULT_CONFIG["save_local"]:

    for folder in [DATA_DIR, SITEMAPS_DIR, PRODUCTS_DIR,OUTPUTS_DIR ]:
        os.makedirs(folder, exist_ok=True)

logger = setup_logger("logs/scraper.log")





def clean_html_description(raw_html: str) -> str:
    if raw_html is None:
        raw_html = ''
    text = re.sub(r'<[^>]+>', '', raw_html)
    text = re.sub(r'\s+', ' ', text).strip()
    return unescape(text)

def clean(string):
    if string is None:
        return string
    string = str(string)
    string = re.sub(r"[\r\n\t*?#|]", "", string)
    string = string.replace('"', "'")
    string = string.replace("é", "e")
    string = re.sub(r"^[^\w\(\)]+|[^\w\(\)]+$", "", string)
    string = string.encode("ascii", "ignore").decode("ascii")
    string = string.strip()
    string = " ".join(string.split())
    return string


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
        with open(OUTPUTS_DIR / filepath, mode="a", newline='', encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writerow(item)








async def fetch_single_product(url: str):


    response = await fetch_url(url, content_type="product")
    url_sim_data = "https://mobile.asda.com/view-sim-plans"
    response_sim_data = await fetch_url(url_sim_data, content_type="product")

    soup_sim_data = BeautifulSoup(response_sim_data, 'html.parser')
    data_attr_sim = soup_sim_data.find("div", id="app")["data-page"]
    decoded_sim = html.unescape(data_attr_sim)
    data_json_sim = json.loads(decoded_sim)
    data_plans = data_json_sim.get("props", {}).get("consumables", [])

    soup = BeautifulSoup(response, 'html.parser')
    data_attr = soup.find("div", id="app")["data-page"]
    decoded = html.unescape(data_attr)
    data_json = json.loads(decoded)
    if data_json:
        row = {}
        handset = data_json["props"]["handset"]
        variations = handset["handset_variations"]
        for variation in variations:

            promotional_price = variation["price"] if variation["price"] != variation["promotional_price"] else ""
            row["source"] = "ASDA Mobile"
            row["date"] = datetime.now().strftime("%Y-%m-%d")
            row["apiURL"] = url
            row["url"] = url
            row["sku"] = variation["sku"]
            row["name"] = variation["title"]+ " " + variation["subtitle"]
            row["brand"] = handset["brand"]["name"]
            row["stock"] = "Y" if variation["stock_status"] == "in-stock" else ""

            if row["stock"] == "Y":

                row["previousPrice"] = promotional_price
                row["onSale"] = "Y" if row["previousPrice"] else ""
                row["saleText"] = variation["promotion"]["title"] if variation["promotion"] else ""
                row["colour"] = variation["colour"]["name"]
                row["size"] = variation["formatted_storage_size"]
                row["UPC"] = ""
                row["EAN"] = variation["ean"]
                row["cat"] = url.split("/")[3]
                row["subcat1"] = url.split("/")[4]
                row["subcat2"] = url.split("/")[5]
                row["subcat3"] = url.split("/")[6] if len(url.split("/")) > 6 else ""
                row["subcat4"] = url.split("/")[7]if len(url.split("/")) > 7 else ""
                row["subcat5"] = url.split("/")[8]if len(url.split("/")) > 8 else ""
                row["warranty"] = ""
                row["isSellingFast"] = ""
                row["isRestockingSoon"] = ""
                row["isPromotion"] = row["onSale"]
                row["isOutletPrice"] = ""
                row["lowestPriceText"] = ""
                images = variation.get("images", [])
                i = 1

                for image in images:
                    if i > 5:
                        break
                    row[f"image{i}"] = image
                    i += 1


                row["desc"] = handset["brand"]["description"]
                row["shortDesc"] = ""



                row["advance"] = 0
                is_eligible_for_finance = variation.get("is_eligible_for_finance", False)
                cash_plan = {
                    "cash": {
                        "duration": 1,
                        "cost": variation["price"],
                        "principal_cost": variation["price"],
                        "interest": "FREE",
                        "interest_rate": 0,
                        "total_cost": variation["price"],
                        "apr": 0,
                        "instalment_breakdown": []
                    }
                }
                if is_eligible_for_finance:
                    new_finance = cash_plan.copy()
                    new_finance.update(variation["finance_options"])
                else:
                    new_finance = cash_plan

                variation["finance_options"] = new_finance

                for plan_name, plan_list in variation["finance_options"].items():
                    if isinstance(plan_list, dict):
                        advance = plan_list.get("advance", 0)
                        phoneContractPrice = plan_list.get("total_cost")
                        total_cost = plan_list.get("total_cost")
                        total_cost = total_cost.split("£")[1]
                        phoneContractDuration = plan_list.get("duration", 0)
                        paymentAmount = (float(total_cost)/float(phoneContractDuration)) if plan_list.get("principal_cost", 0) == "FREE" else float(plan_list.get("principal_cost").split("£")[1])
                        paymentAmount = round(paymentAmount, 2)
                        row["advance"] = advance
                        row["handsetOnlyCostCash"] = phoneContractPrice
                        row["phoneContractPrice"] = 0 if phoneContractDuration == 1 else phoneContractPrice
                        row["paymentAmount"] = paymentAmount
                        row["phoneContractDuration"] = phoneContractDuration
                        row["isPhoneContractAvailableWOsim"] = "N"
                        row["handsetOnlyContract"] = ""

                        plan_type_map = {
                            "bundle": "Pay As You Go",
                            "contract_bundle": "Pay Monthly"
                        }

                        for data in data_plans:
                            row["plan_type"] = plan_type_map.get(data.get("type"))
                            row["sim_data"] = data.get("data")
                            row["sim_price"] = data.get("cost")
                            sim_price = data.get("cost").split("£")[1]
                            row["simOfferData"] = data.get("name_with_promotion")
                            row["sim1YearIncrease"] = ""
                            row["sim2YearIncrease"] = ""
                            row["sim3YearIncrease"] = ""
                            row["simDesc"] = data.get("minutes_and_texts_description")
                            row["simContractname"] = data.get("name")
                            row["simContractDuration"] = 1

                            if row["plan_type"] == "Pay Monthly":
                                row["simContractDuration"] = data.get("name").split("(")[1].split(" months")[0]



                            sim_price_val = float(sim_price.replace("£", "").replace(",", "").strip()) if isinstance(
                                sim_price, str) else float(sim_price)
                            phone_price_val = float(
                                phoneContractPrice.replace("£", "").replace(",", "").strip()) if isinstance(
                                phoneContractPrice, str) else float(phoneContractPrice)
                            duration_val = float(row["simContractDuration"])

                            row["phoneContractSimPackage"] = round((duration_val * sim_price_val) + phone_price_val, 2)

                            Specifications = handset["features"]["specifications"]
                            attribute_index = 1
                            for category, details in Specifications.items():
                                if isinstance(details, dict):
                                    combined_value = " | ".join(
                                        f"{k}={v}" for k, v in details.items()
                                    )
                                    row[f"attributeType{attribute_index}"] = "SPECIFICATION"
                                    row[f"attributeTitle{attribute_index}"] = category
                                    row[f"attributeValue{attribute_index}"] = combined_value.upper()
                                    attribute_index += 1

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







async def main():
    create_csv_file("products.csv")
    url = "https://mobile.asda.com/sitemap.xml"
    products = []
    sitemap = await fetch_sitemap(url)
    for sitemap in sitemap:
        if "shop/" in sitemap and "accessories" not in sitemap and "sim-only" not in sitemap and "sim-plans" not in sitemap:

            parts = sitemap.split("/")
            if len(parts) > 5 and parts[4] != "" and parts[4] != "shop":
                products.append(sitemap)


    data = await extact_data_from_product_url(products)


if __name__ == "__main__":
    asyncio.run(main())
