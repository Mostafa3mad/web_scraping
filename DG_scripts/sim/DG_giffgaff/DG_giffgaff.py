import time

from functions import *
import os
from html import unescape
from bs4 import BeautifulSoup






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
        with open(OUTPUTS_DIR/filepath, mode="w", newline='',encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()

def append_to_csv(item, filepath):

    if DEFAULT_CONFIG["save_local"]:

        headers = get_standard_csv_headers()
        price_fields = [
            "advance", "paymentAmount","sim_price", "phoneContractPrice",
            "phoneContractSimPackage", "handsetOnlyCostCash",
            "handsetOnlyContract", "previousPrice"
        ]
        for key in price_fields:
            if key in item and isinstance(item[key], (int, float, str)):
                try:
                    item[key] = '="{:.2f}"'.format(float(item[key]))
                except ValueError:
                    pass

        with open(OUTPUTS_DIR/filepath, mode="a", newline='',encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writerow(item)





async def extract_plan(response_data_paln):
    soup = BeautifulSoup(response_data_paln, 'html.parser')
    plans = []
    plan_info_sections = soup.find_all('div', class_='sc-gtJxfw byUKQu')
    for plan_info in plan_info_sections:
        price = plan_info.find_previous('span', class_='sc-cKXybt hAYJhu')
        if price:
            price = re.findall(r'\d+', price.text.strip())
            if price:
                price = price[0]
            else:
                price = "0"
        else:
            price = "0"

        data_type = plan_info.find('h2', class_='sc-cTTdyq sc-lmUcrn hyywXO jSZYRQ')
        if data_type:
            data_type = data_type.text.strip()
        else:
            data_type = ""

        duration = plan_info.find('h2', class_='sc-cTTdyq sc-fscmHZ hyywXO hGlodx')
        if duration:
            duration = re.findall(r'\d+', duration.text.strip())
            if duration:
                duration = duration[0]
            else:
                duration = "1"
        else:
            duration = "1"

        contract = plan_info.find('small', class_='sc-bWJUgm fkkceO')
        if contract:
            contract = contract.text.strip()
        else:
            contract = ""

        sim_type = plan_info.find('small', class_='sc-iowXnY fzoeoU')
        if sim_type:
            sim_type = sim_type.text.strip()
        else:
            sim_type = ""

        plans.append({
            'data_type': data_type,
            'price': price,
            'sim': sim_type,
            'sim_desc': "Unlimited UK calls and texts | EU roaming included up to 5 GB | Keep your number | 5G speeds at no extra cost",
            'duration': duration,
            'contract': contract
        })
    return plans

async def fetch_single_product(url: str):

    response = await fetch_url(url, content_type="product")
    plans = await extract_plan(response)
    soup = BeautifulSoup(response, 'html.parser')
    script_tag = soup.find('script', {'type': 'application/json', 'data-hypernova-key': 'PhoneDetailsPage'})

    if script_tag:
        row = {}
        json_data = script_tag.string
        start_index = json_data.find("<!--") + 4
        end_index = json_data.find("-->", start_index)
        json_str = json_data[start_index:end_index].strip()

        product_data = json.loads(json_str)

        variants_sku = product_data["phoneDetails"]["variants"].keys()
        data = product_data["phoneDetails"]["variants"]
        Brand = product_data["phoneDetails"]["manufacturer"]["key"]
        name = product_data["phoneDetails"]["phoneDisplayName"]
        warrantyPeriod = product_data["phoneDetails"]["warrantyPeriod"]
        Specifications = product_data["phoneSpecifications"]["features"]
        for variant in variants_sku:
            row["source"] = "giffgaff"
            row["date"] = datetime.now().strftime("%Y-%m-%d")
            row["apiURL"] = url
            row["url"] = url
            row["sku"] = variant
            row["name"] = name
            row["brand"] = Brand
            row["desc"] = ""
            row["shortDesc"] = ""
            row["videoURL"] = ""
            row["lowestPriceValue"] = ""
            row["reviewRating"] = ""
            row["reviewCount"] = ""
            row["colour"] = data.get(variant, {}).get("colour", "")
            row["size"] = data.get(variant, {}).get("memory", "")
            stock_value = data.get(variant, {}).get("stock", "")
            row["stock"] = "N" if stock_value == 0 else "Y" if stock_value and stock_value > 0 else ""
            row["UPC"] = ""
            row["EAN"] = ""
            row["cat"] = url.split("/")[3]
            row["subcat1"] = url.split("/")[4]
            row["subcat2"] = url.split("/")[5]
            row["subcat3"] = url.split("/")[6] if len(url.split("/")) > 6 else ""
            row["subcat4"] = url.split("/")[7]if len(url.split("/")) > 7 else ""
            row["subcat5"] = url.split("/")[8]if len(url.split("/")) > 8 else ""
            row["warranty"] = warrantyPeriod if warrantyPeriod else ""
            row["isSellingFast"] = ""
            row["isRestockingSoon"] = ""
            row["isPromotion"] = ""
            row["isOutletPrice"] = ""
            row["lowestPriceText"] = ""
            row["image1"] = f"https://static.giffgaff.com/images/phones/products/{row['subcat2']}/{row["colour"]}/slider_190x375/1.png"
            row["image2"] = ""
            row["image3"] = ""
            row["image4"] = ""
            row["image5"] = ""
            handsetOnlyCostCash = round(float(data.get(variant, {}).get("price")/100),2)
            discount = round(float(data.get(variant, {}).get("discount")/100),2)
            previousPrice = float(handsetOnlyCostCash + discount ) if discount != 0.0 else ""
            row["saleText"] = f"£{int(discount)} off" if discount > 0 else ""
            row["onSale"] = "Y" if discount > 0 else ""



            estimatedLoanDetails = data.get(variant, {}).get("estimatedLoanDetails", []).get("estimatedLoanOptions", [])
            for Upfront in estimatedLoanDetails:
                for option_contract in estimatedLoanDetails.get(Upfront, []):
                    Upfront = float(Upfront)

                    phoneContractPrice = round(option_contract.get("estimatedTotal", "")/100 + Upfront/100,2)
                    paymentAmount = round(option_contract.get("estimatedMonthlyCost", "")/100,2)
                    phoneContractDuration = option_contract.get("duration", "")

                    row["handsetOnlyCostCash"] = round(handsetOnlyCostCash, 2)
                    row["previousPrice"] = round(previousPrice, 2) if previousPrice else ""
                    row["advance"] = round(Upfront / 100, 2)
                    row["phoneContractPrice"] = round(phoneContractPrice, 2)
                    row["paymentAmount"] = round(paymentAmount, 2)
                    row["phoneContractDuration"] = phoneContractDuration
                    row["isPhoneContractAvailableWOsim"] = "N"


                    for plan in plans:
                        sim_price = float(plan['price'])
                        sim_desc = plan['sim_desc']
                        simContractname = plan['contract']
                        plan_type = plan['sim'] +" " + plan['contract']
                        simDesc = plan['sim_desc']
                        sim_data = plan['data_type']
                        simContractDuration_raw = plan['duration']
                        simContractDuration = float(re.findall(r'\d+', simContractDuration_raw)[0]) if re.findall(r'\d+', simContractDuration_raw) else 1.0
                        phoneContractSimPackage = phoneContractPrice + (simContractDuration * sim_price)
                        isPhoneContractAvailableWOsim = "N"
                        row["plan_type"] = plan_type
                        row["sim_data"] = sim_data
                        row["sim_price"] = round(sim_price,2)
                        row["simOfferData"] = ""
                        row["simContractname"] = simContractname
                        row["simContractDuration"] = round(simContractDuration, 2)
                        row["isPhoneContractAvailableWOsim"] = isPhoneContractAvailableWOsim
                        row["phoneContractSimPackage"] = round(phoneContractSimPackage,2)
                        row["handsetOnlyContract"] = ""
                        row["sim1YearIncrease"] = ""
                        row["sim2YearIncrease"] = ""
                        row["sim3YearIncrease"] = ""
                        row["simDesc"] = simDesc
                        attribute_index = 1
                        for category in Specifications:
                            for detail in category.get("details", []):
                                title = detail.get("title", "").strip()
                                description = detail.get("description", "").strip()
                                row[f"attributeType{attribute_index}"] = "SPECIFICATION"
                                row[f"attributeTitle{attribute_index}"] = title
                                row[f"attributeValue{attribute_index}"] = description.upper()
                                attribute_index += 1




                        append_to_csv(row, "products.csv")

                        return row

def extract_sim(prod, category):
    offers = prod.get("offers", {})
    name = prod.get("name", "")
    desc = prod.get("description", "")
    if desc:
        desc = f"{desc} | EU roaming included up to 5 GB | Keep your number | 5G speeds at no extra cost".replace(",", " |")
    url = prod.get("url", "")
    img = prod.get("image", "")

    try:
        price = float(offers.get("price", 0))
    except:
        price = 0.0

    data_type = ""
    unlimited = False
    m = re.search(r"(\d+)\s*GB", name, re.IGNORECASE)
    if m:
        data_type = f"{m.group(1)} GB"
    if "Unlimited" in name:
        unlimited = True
        data_type = "Unlimited"

    duration = None
    if "18 Month" in name or "18 Month" in desc:
        duration = 18
    elif "Monthly" in name or "Monthly" in desc:
        duration = 1
    elif "PAYG" in name:
        duration = 0

    return {
        "category": category,
        "simContractname": name,
        "simDesc": desc,
        "sim_data": data_type,
        "plan_type": "sim-only-deals",
        "unlimited_data": unlimited,
        "sim_price": price,
        "simContractDuration": duration,
        "calls_texts": "unlimited",
        "url": url,
        "image": img
    }



async def extract_sim_only(url: str):
    response = await fetch_url(url, content_type="product")
    soup = BeautifulSoup(response, 'html.parser')

    script = soup.find("script", {"type": "application/ld+json"})
    data = json.loads(script.string)

    plans = []

    for prod in data.get("isRelatedTo", []):
        plans.append(extract_sim(prod, "contract_18m"))

    for prod in data.get("isSimilarTo", []):
        name = prod.get("name", "")
        if "Monthly Rolling" in name:
            plans.append(extract_sim(prod, "monthly_rolling"))
        elif "PAYG" in name:
            plans.append(extract_sim(prod, "payg"))

    for plan in plans:
        plan_sim = {}
        plan_sim["source"] = "giffgaff"
        plan_sim["date"] = datetime.now().strftime("%Y-%m-%d")
        plan_sim["apiURL"] = ""
        plan_sim["url"] = plan["url"]
        plan_sim["plan_type"] = plan["plan_type"]
        plan_sim["sim_data"] = plan["sim_data"]
        plan_sim["sim_price"] = plan["sim_price"]
        plan_sim["simOfferData"] = ""
        plan_sim["simContractname"] = plan["simContractname"]
        plan_sim["simContractDuration"] = plan["simContractDuration"]
        plan_sim["isPhoneContractAvailableWOsim"] = "N"
        plan_sim["phoneContractSimPackage"] = ""
        plan_sim["handsetOnlyContract"] = ""
        plan_sim["sim1YearIncrease"] = ""
        plan_sim["sim2YearIncrease"] = ""
        plan_sim["sim3YearIncrease"] = ""
        plan_sim["simDesc"] = plan["simDesc"]
        # print(plan_sim)
        append_to_csv(plan_sim, "products.csv")

    return plans


async def get_products_from_sitemap(urls: list[str]) -> list:
    products = []

    for url in urls:
        response = await fetch_url(url, content_type="sitemap")

        soup = BeautifulSoup(response, 'html.parser')

        products += [a['href'] for a in soup.find_all('a', class_='PhoneCard__PhoneCardContainer-sc-1xsk9mq-0 icBhdU phone-card with-promo', href=True)]
        products += [a['href'] for a in soup.find_all('a', class_='PhoneCard__PhoneCardContainer-sc-1xsk9mq-0 icBhdU phone-card', href=True)]
    products = [f"https://www.giffgaff.com{link}" for link in products]
    return products



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
    url_sim = "https://www.giffgaff.com/sim-only-deals"
    plans = await extract_sim_only(url_sim)
    urls = ["https://www.giffgaff.com/mobile-phones","https://www.giffgaff.com/mobile-phones/refurbished"]
    products = await get_products_from_sitemap(urls)
    data = await extact_data_from_product_url(products)


if __name__ == "__main__":
    asyncio.run(main())
