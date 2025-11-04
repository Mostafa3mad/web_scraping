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
    def normalize_value(val):
        if val is None or val == "-1":
            return "Unlimited"
        try:
            mb = int(val)
            if mb >= 1000:
                return f"{mb // 1000}GB"
            else:
                return f"{mb}MB"
        except:
            return val

    plans = []
    plan_cards = soup.find_all('input', {
        'name': 'plan-choice',
        'type': 'radio'
    })

    if plan_cards == []:
        return [
            {"monthly_cost": 0, "upfront_cost": 0, "data_allowance": "", "contract_duration": 0, "sim1YearIncrease": "",
             "sim2YearIncrease": "", "desc": ""}]

    for plan_card in plan_cards:
        label = soup.find('label', {'for': plan_card.get('id')})


        plan_details = {}

        plan_details['plan_id'] = plan_card.get('data-deal-id')
        monthly_cost_text = plan_card.get("data-monthly-cost")
        monthly_cost = re.search(r'\d+(\.\d{1,2})?', monthly_cost_text)
        plan_details['monthly_cost'] = monthly_cost.group(0) if monthly_cost else None
        upfront_cost_text = plan_card.get("data-upfront-cost")
        upfront_cost = re.search(r'\d+(\.\d{1,2})?', upfront_cost_text)
        plan_details['upfront_cost'] = upfront_cost.group(0) if upfront_cost else None
        sim_contract = plan_card.get("data-contract-length")
        plan_details['sim_contract'] = sim_contract if sim_contract else "N/A"

        data_allowance = normalize_value(plan_card.get("data-data"))
        plan_details['data_allowance'] = data_allowance

        mins_allowance_text = normalize_value(plan_card.get("data-mins"))
        plan_details['mins_allowance'] = mins_allowance_text

        texts_allowance_text = normalize_value(plan_card.get("data-mins"))
        plan_details['texts_allowance'] = texts_allowance_text

        contract_duration = plan_card.get("data-contract-length")
        plan_details['contract_duration'] = contract_duration if contract_duration else None

        mrc1 = label.find("span", class_="plan-mrc1")
        mrc2 = label.find("span", class_="plan-mrc2")
        sim1_year_increase = mrc1.find("strong", class_="plan-figure").get_text(strip=True) if mrc1 else None
        sim2_year_increase = mrc2.find("strong", class_="plan-figure").get_text(strip=True) if mrc2 else None
        plan_details['sim1YearIncrease'] = sim1_year_increase if sim1_year_increase else None
        plan_details['sim2YearIncrease'] = sim2_year_increase if sim2_year_increase else None

        plan_details[
            'desc'] = f"data_allowance: {plan_details['data_allowance']} | contract_duration: {plan_details['contract_duration']} | sim_contract: {plan_details['sim_contract']} | mins_allowance: {plan_details['mins_allowance']} | texts_allowance: {plan_details['texts_allowance']}"

        plans.append(plan_details)

    return plans

async def get_sim_deals():

    url_plan_sim = "https://www.idmobile.co.uk/sim-only-deals"

    response = await fetch_url(url_plan_sim,config=DEFAULT_CONFIG,headers={},content_type="product")
    soup = BeautifulSoup(response, "html.parser")
    plan_inputs = soup.find_all("article", {"data-plan-card-wrapper": True})
    plans = []
    for inp in plan_inputs:
        label = soup.find("label", {"for": inp.get("id")})

        def extract_price(inp):
            price_tags = inp.select(".plan-monthly-cost strong.plan-figure span[itemprop='price']")

            if not price_tags:
                return None

            if len(price_tags) > 1:
                return float(price_tags[0].get_text(strip=True))

            return float(price_tags[0].get_text(strip=True))

        sim_price = extract_price(inp)

        if inp.select_one("a[data-original-data]") :
            sim_data_tag = inp.select_one("a[data-original-data]").get("data-original-data")
        else:
            strong_tag = inp.select_one(".data-allowance strong")
            sim_data_tag = strong_tag.get_text(strip=True) if strong_tag else None

        if inp.select_one("a[data-deal-tag]"):
            simOfferData = inp.select_one("a[data-deal-tag]").get("data-deal-tag")
        else :
            tag_block = inp.select_one(".plan-deal-tag span[data-countdown-container]")
            simOfferData = tag_block.get_text(strip=True) if tag_block else None

        mins_tag = inp.select_one(".mins-allowance strong")
        mins = mins_tag.get_text(strip=True) if mins_tag else inp.get("data-mins")
        texts_tag = inp.select_one(".texts-allowance strong")
        texts = mins_tag.get_text(strip=True) if mins_tag else inp.get("data-texts")

        simContractDuration = inp.get("data-contract-length")

        simDesc = f"data_allowance: {sim_data_tag} | contract_duration: {simContractDuration} | mins_allowance: {mins} | texts_allowance: {texts}"

        cta = inp.find("a", class_="cta-btn")
        plan_url = cta.get("href") if cta else None

        plan = {

                    "source": "idmobile",
                    "date": datetime.now().strftime("%Y-%m-%d"),
                    "apiURL": "",
                    "url": f"https://www.idmobile.co.uk{plan_url}",
                    "brand": "idmobile",
                    "plan_type": "sim-only-deals",
                    "simContractDuration": simContractDuration,
                    "sim_price": sim_price,
                    "simOfferData": simOfferData,
                    "simContractname": f"plan of  {sim_data_tag}" if  sim_data_tag != "" else "",
                    "sim_data": sim_data_tag,
                    "isPhoneContractAvailableWOsim": "N",
                    "simDesc": simDesc,
        }


        append_to_csv(plan, "products.csv")
async def get_sim_deals_pay_as_go():
    def normalize_value(val):
        if val is None or val == "-1":
            return "Unlimited"
        try:
            mb = int(val)
            if mb >= 1000:
                return f"{mb // 1000}GB"
            else:
                return f"{mb}MB"
        except:
            return val




    url_sim_pay_as_go = "https://www.idmobile.co.uk/sim-only-deals/pay-as-you-go"
    response = await fetch_url(url_sim_pay_as_go,config=DEFAULT_CONFIG,headers={},content_type="product")
    soup = BeautifulSoup(response, "html.parser")
    cards = soup.find_all("article", {"data-handset": "PAYIDMULTISIM"})

    BASE_URL = "https://www.idmobile.co.uk"

    for card in cards:
        link = card.find("a")
        url_sim = BASE_URL + link["href"] if link else None
        sim_data = normalize_value(card.get("data-data"))
        mins = normalize_value(card.get("data-mins"))
        texts = normalize_value(card.get("data-texts"))
        simContractDuration = card.get("data-contract-length")

        simDesc = f"data_allowance: {sim_data} | contract_duration: {simContractDuration} | mins_allowance: {mins} | texts_allowance: {texts}"

        price_block = card.select_one("p > span:nth-of-type(4) strong")
        price = price_block.get_text(strip=True).replace("£", "") if price_block else None

        plan = {
            "source": "idmobile",
            "date": datetime.now().strftime("%Y-%m-%d"),
            "apiURL": "",
            "url": url_sim,
            "brand": "idmobile",
            "plan_type": "pay-as-you-go",
            "simContractDuration": simContractDuration,
            "sim_price": price,
            "simOfferData": "",
            "simContractname": f"plan of  {sim_data}" if sim_data != "" else "",
            "sim_data": sim_data,
            "isPhoneContractAvailableWOsim": "N",
            "simDesc": simDesc,
        }


        append_to_csv(plan, "products.csv")

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
            availability = data_product_json.get("offers", {}).get("availability", "")
            if availability == "OutOfStock" :
                row["stock"] = "N"
            if availability == "InStock" :
                row["stock"] = "Y"

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
            price_product = data_product_json.get("offers").get("price","")
            if price_product:
                row["handsetOnlyCostCash"] = price_product
            try:
                if price_product == "":
                    price_block = soup.select_one(".plan-costs .plan-monthly-cost .plan-figure")
                    price = price_block.get_text(strip=True).replace("£", "") if price_block else None
                    row["handsetOnlyCostCash"] = price

            except:
                pass
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
    await get_sim_deals()
    await get_sim_deals_pay_as_go()

    siteurl = "https://www.idmobile.co.uk/sitemap.xml"
    products  = await get_links_site_map(siteurl)
    data = await extact_data_from_product_url(products)


if __name__ == "__main__":
    asyncio.run(main())
