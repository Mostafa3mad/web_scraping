import time

from functions import *
import os
from html import unescape
from bs4 import BeautifulSoup
import urllib.parse
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


async def extract_json_ld(soup):
    scripts = soup.find_all("script", {"type": "application/ld+json"})
    product_data = None
    for script in scripts:
        try:
            data = json.loads(script.string)
            if isinstance(data, dict) and data.get("@type") == "Product":
                product_data = data
                return product_data
        except:
            continue


async def get_images(soup):
    images = []
    for slide in soup.find_all('div', class_='swiper-slide'):
        style = slide.get('style', '')
        if 'background-image' in style:
            start_idx = style.find("url('") + len("url('")
            end_idx = style.find("')", start_idx)
            image_url = style[start_idx:end_idx]
            images.append(image_url)
    return images
async def get_video(soup):
    video_url = ""
    video_tag = soup.find('video')
    if video_tag:
        source_tag = video_tag.find('source')
        if source_tag and source_tag.get('src'):
            video_url = source_tag['src']
    return video_url

async def get_colour(soup):
    color = ""
    color_paragraph = soup.find('p', class_='hidden-xs')
    if color_paragraph:
        color = color_paragraph.get_text(strip=True).split(":")[1].strip()
    return color
async def get_size_value(soup):
    size_value = ""
    capacity_p_tag = soup.select_one('.col-sm-5.deal-details .hidden-xs p')
    if capacity_p_tag:
        size_value = capacity_p_tag.contents[1].strip()
    return size_value

async def get_plans(soup):
    plans = []

    plan_cards = soup.find_all('article', class_='col-xs-12 col-sm-6 col-md-12')
    if plan_cards == []:
        return [{"monthly_cost":0, "upfront_cost": 0, "data_allowance": "", "contract_duration": 0, "sim1YearIncrease": "", "sim2YearIncrease": "", "desc": ""}]


    for plan_card in plan_cards:
        plan_details = {}

        plan_details['plan_id'] = plan_card.get('data-deal-id')
        monthly_cost_text = plan_card.find('div', class_='plan-monthly-cost').get_text(strip=True)
        monthly_cost = re.search(r'\d+(\.\d{1,2})?', monthly_cost_text)
        plan_details['monthly_cost'] = monthly_cost.group(0) if monthly_cost else None
        upfront_cost_text = plan_card.find('div', class_='plan-upfront-cost').get_text(strip=True)
        upfront_cost = re.search(r'\d+(\.\d{1,2})?', upfront_cost_text)
        plan_details['upfront_cost'] = upfront_cost.group(0) if upfront_cost else None
        sim_contract_name = plan_card.find('div', class_='plan-monthly-cost').get_text(strip=True)
        sim_contract_match = re.search(r'\d+-month plan', sim_contract_name)
        plan_details['sim_contract'] = sim_contract_match.group(0) if sim_contract_match else "N/A"
        plan_details['data_allowance'] = plan_card.find('div', class_='data-allowance').get_text(strip=True)
        mins_allowance_text = plan_card.find('div', class_='mins-allowance').get_text(strip=True)
        plan_details['mins_allowance'] = mins_allowance_text if mins_allowance_text else "Unlimited"

        texts_allowance_text = plan_card.find('div', class_='texts-allowance').get_text(strip=True)
        plan_details['texts_allowance'] = texts_allowance_text if texts_allowance_text else "Unlimited"

        contract_duration_text = plan_card.find('div', class_='contract-duration').get_text(strip=True)
        contract_duration = re.search(r'\d+', contract_duration_text)
        plan_details['contract_duration'] = contract_duration.group(0) if contract_duration else None
        sim1_year_increase_text = plan_card.find('div', class_='plan-mrc1').get_text(strip=True)
        sim2_year_increase_text = plan_card.find('div', class_='plan-mrc2').get_text(strip=True)
        sim1_year_increase = re.search(r'\d+(\.\d{1,2})?', sim1_year_increase_text)
        sim2_year_increase = re.search(r'\d+(\.\d{1,2})?', sim2_year_increase_text)
        plan_details['sim1YearIncrease'] = sim1_year_increase.group(0) if sim1_year_increase else None
        plan_details['sim2YearIncrease'] = sim2_year_increase.group(0) if sim2_year_increase else None

        plan_details['desc'] = f"{plan_details['data_allowance']} | {plan_details['contract_duration']} | {plan_details['sim_contract']} | {plan_details['mins_allowance']} | {plan_details['texts_allowance']}"

        plans.append(plan_details)
    return plans

async def fetch_single_product(url: str):
    prodact_name = url.split("/")[-1]
    config = DEFAULT_CONFIG.copy()
    config["use_scrapingbee"] = True
    response = await fetch_url(url, config=config,headers={},content_type="product")
    soup = BeautifulSoup(response, "html.parser")
    product_data = await extract_json_ld(soup)
    variant_links = []
    if "isSimilarTo" in product_data:
        for item in product_data["isSimilarTo"]:
            if "URL" in item:
                variant_links.append(item["URL"])

    variant_links = [link for link in variant_links if not link.endswith(prodact_name)]

    if variant_links == []:
        variant_links.append(url)


    #
    for link in variant_links:
        response_single = await fetch_url(link, config=config,headers={},content_type="product")
        soup = BeautifulSoup(response_single, "html.parser")
        data_product_json = await extract_json_ld(soup)
        for key in ["isSimilarTo", "isRelatedTo"]:
            if key in data_product_json:
                del data_product_json[key]

        if data_product_json:
            row = {}
            images = []
            row["source"] = "idmobile"
            row["date"] = datetime.now().strftime("%Y-%m-%d")
            row["apiURL"] = ""
            row["url"] = data_product_json["url"]
            row["sku"] = data_product_json["sku"]
            row["name"] = data_product_json["name"]
            row["brand"] = data_product_json["brand"]
            row["stock"] = ""
            overview_section = soup.find('div', class_='col-sm-offset-1 col-sm-10 col-lg-offset-2 col-lg-8')
            if overview_section:
                header = overview_section.find('h2', class_='font-size-20')
                if header and 'Overview' in header.get_text(strip=True):
                    paragraphs = overview_section.find_all('p')
                    combined_text = " ".join([p.get_text(strip=True) for p in paragraphs])
                    row["desc"] = combined_text
            row["shortDesc"] = ""
            images = await get_images(soup)
            for i in range(5):
                row[f"image{i + 1}"] = images[i] if i < len(images) else ""
            video_url = await get_video(soup)
            row["videoURL"] = video_url if video_url else ""
            row["lowestPriceValue"] = ""
            row["reviewRating"] = ""
            row["reviewCount"] = ""
            row["onSale"] = ""
            colour = await get_colour(soup)
            row["colour"] = colour
            size = await get_size_value(soup)
            row["size"] = size
            row["UPC"] = ""
            row["EAN"] = data_product_json["gtin13"] if "gtin13" in data_product_json else ""
            row["cat"] = url.split("/")[3]
            row["subcat1"] = url.split("/")[4] if len(url.split("/")) > 4 else ""
            row["subcat2"] = url.split("/")[5] if len(url.split("/")) > 5 else ""
            row["subcat3"] = url.split("/")[6] if len(url.split("/")) > 6 else ""
            row["subcat4"] = url.split("/")[7] if len(url.split("/")) > 7 else ""
            row["subcat5"] = url.split("/")[8] if len(url.split("/")) > 8 else ""
            row["warranty"] = ""
            row["isSellingFast"] = ""
            row["isRestockingSoon"] = ""
            row["isPromotion"] = ""
            row["isOutletPrice"] = ""
            row["lowestPriceText"] = ""
            row["saleText"] = ""
            row["handsetOnlyCostCash"] = data_product_json.get("offers").get("price","")
            row["previousPrice"] = ""
            plans = await get_plans(soup)
            for plan in plans:
                row["advance"] = plan["upfront_cost"]
                row["phoneContractDuration"] = int(plan["contract_duration"])
                row["phoneContractPrice"] = float(plan["monthly_cost"]) * int(plan["contract_duration"]) + float(plan["upfront_cost"])
                row["paymentAmount"] = plan["monthly_cost"]
                row["plan_type"] = "sim-free" if row["handsetOnlyCostCash"] != "" else "Contract"
                row["sim_data"] = plan["data_allowance"]
                row["sim_price"] = float(plan["monthly_cost"])
                row["simOfferData"] = ""
                row["simContractname"] = f"plan of  {plan["data_allowance"]}" if  plan["data_allowance"] != "" else ""
                row["simContractDuration"] = int(plan["contract_duration"])
                row["isPhoneContractAvailableWOsim"] = "N"
                row["phoneContractSimPackage"] = row["phoneContractPrice"]
                row["handsetOnlyContract"] = ""
                row["sim1YearIncrease"] = plan["sim1YearIncrease"]
                row["sim2YearIncrease"] = plan["sim2YearIncrease"]
                row["sim3YearIncrease"] = ""
                row["simDesc"] = plan["desc"]

                attribute_index = 1
                features_list = soup.find('ul', class_='margin-left-plus-20px')
                if features_list:
                    features = features_list.find_all('li')
                    for feature in features:
                        title = feature.get_text(strip=True)
                        description = title

                        row[f"attributeType{attribute_index}"] = "SPECIFICATION"
                        row[f"attributeTitle{attribute_index}"] = title
                        row[f"attributeValue{attribute_index}"] = description.upper()

                        attribute_index += 1

                pattern = r'data-mappedDeal="([^"]+)"'
                matches = re.findall(pattern, response)
                if matches:
                    decoded_json_str = html.unescape(matches[0]).replace("&quot;", '"').replace("'", '"')
                    match = re.search(r'"product_features_json":\s*"({.*?})"', decoded_json_str)
                    if match:
                        cleaned_string = match.group(1).replace(r'\"', '"').replace(r'\\', '\\')
                        matches_json = json.loads(cleaned_string)

                        for group_name, group_data in matches_json.items():
                            if attribute_index > 20:
                                break

                            values = []

                            for attr in group_data:
                                name = attr.get("name", "").strip()
                                val = "Y" if attr.get("value", "").strip() == "1" else attr.get("value", "").strip()
                                if name and val:
                                    values.append(f"{name}: {val}")

                            if values:
                                row[f"attributeType{attribute_index}"] = "SPECIFICATION"
                                row[f"attributeTitle{attribute_index}"] = group_name
                                row[f"attributeValue{attribute_index}"] = " | ".join(values)
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


async def get_links_site_map(url):
    products = []
    config = DEFAULT_CONFIG.copy()
    xml_text = await fetch_url(url , content_type="sitemap" , headers={},config=config)
    root = ET.fromstring(xml_text)
    urls = []
    ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    loc_elements = root.findall('.//sm:loc', ns)
    if not loc_elements:
        loc_elements = root.findall('.//loc')

    for loc in loc_elements:
        urls.append(loc.text)

    for product in urls:
        model_match = re.search(r'\/(?:shop\/pay-monthly|sim-free-phones)\/([a-z0-9\-]+)', product)
        if model_match:
            model = model_match.group(1)
            products.append(product)

    return products






async def main():
    create_csv_file("products.csv")
    siteurl = "https://www.idmobile.co.uk/sitemap.xml"
    products  = await get_links_site_map(siteurl)
    data = await extact_data_from_product_url(products)


if __name__ == "__main__":
    asyncio.run(main())
