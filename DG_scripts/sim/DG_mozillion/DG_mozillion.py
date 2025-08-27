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
        with open(OUTPUTS_DIR/filepath, mode="w", newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()

def append_to_csv(item, filepath):

    if DEFAULT_CONFIG["save_local"]:

        headers = get_standard_csv_headers()
        with open(OUTPUTS_DIR/filepath, mode="a", newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writerow(item)






async def fetch_single_product(url: str):

    # print(url)

    with httpx.Client() as client:
        response = client.get(url)

        mozillion_session = response.cookies.get("mozillion_session")
    #####################################################

    match = re.search(r'_token:\s*"([^"]+)"', response.text)
    _token =  match.group(1)
    # print("_token",_token)
    # print("mozillion_session:", mozillion_session)

    #####################################################

    response = await fetch_url(url, content_type="product")

    # print(response)
    pattern_product_model_id = r'product_model_id: "(\d+)"'
    product_model_id = re.findall(pattern_product_model_id, response)[0]
    soup = BeautifulSoup(response, 'html.parser')



    #####################################################
    # sim_detals
    try:
        sim_items = [li.get_text(strip=True) for li in soup.select("#tab-sim ul li")]
        simDesc = " | ".join(sim_items)
    except:
        simDesc = ""

    #####################################################
    try:
        spec_items = soup.select("#accordion-flush-body-1 ul li")
    except:
        spec_items = []
    #####################################################
    try:
        less_text = soup.find('span', class_='less-text').get_text(strip=True)
        more_text = soup.find('span', class_='more-text').get_text(strip=True)
        combined_text = less_text + " " + more_text
    except:
        less_text = soup.find('span', class_='less-text').get_text(strip=True)
        more_text= ""
        combined_text = less_text + " " + more_text


    #####################################################
    pattern_colors = r'data-color-id="color_(\d+)"\s+title="([^"]+)"'
    matches_colors = re.findall(pattern_colors, response)
    result_colors = [f"{num}_{title}" for num, title in matches_colors]

    pattern_capacity = r'data-capacity-id="(\d+)"\s*href="[^"]*"\s*class="[^"]*">([^<]+)<'
    matches_capacity = re.findall(pattern_capacity, response)
    result_capacity = [f"{num}_{capacity}" for num, capacity in matches_capacity]
    try:
        slider = soup.find("input", {"id": "price-slider"})

        max_value = slider.get("max")
        # print("Max upfront:", max_value)
    except:
        max_value = 0
    # print(result_colors)
    # print(result_capacity)
    # print(product_model_id)

    for color in result_colors:
        for capacity in result_capacity:
            row = {}
            capacity_code = capacity.split('_')[0]
            capacitys = capacity.split('_')[1]


            color_code = color.split("_")[0]
            colors = color.split("_")[1]

            # print(capacity_code)
            # print(capacitys)
            # print(color_code)
            # print(colors)

            headers = {

                'cookie': f'mozillion_session={mozillion_session}',
            }

            data = {
                'product_model_id': product_model_id,
                'color_id': color_code,
                'capacity': capacity_code,
                'condition': '',
                '_token': _token,
                'colorchangeflag': 'true',
                'storageIdOld': '',
                'conditionIdOld': '',

            }
            # print(url)
            # print(url.split("/")[-1])
            if "bundle" in url:
                url_api = f"https://www.mozillion.com/get-available-bundle-variants?{url.split("/")[-1]}_color={colors.replace(" ", "_")}_capacity={capacitys}"
            else:
                url_api = f"https://www.mozillion.com/get-available-variants?{url.split("/")[-1]}_color={colors.replace(" ", "_")}_capacity={capacitys}"

            response_data = await fetch_url(url_api, method="POST", json_data=data, headers=headers, content_type="product")
            data_product = json.loads(response_data)
            row["source"] = "mozillion"
            row["date"] = datetime.now().strftime("%Y-%m-%d")
            row["apiURL"] = url_api
            row["url"] = url
            available_sims = data_product.get("available_sims", [{}])

            for simplan in available_sims:
                if isinstance(simplan, dict):
                    phone = simplan.get("phone", {})
                    row["sku"] = phone.get("id", "")
                else:
                    row["sku"] = ""
                row["name"] = f"{url.split("/")[-1].replace("-","_")}_{colors.replace(" ","_")}_{capacitys}"
                row["brand"] = url.split("/")[-2]
                row["stock"] = ""
                row["desc"] = combined_text
                row["shortDesc"] = ""
                row["videoURL"] = ""
                row["lowestPriceValue"] = ""
                row["reviewRating"] = ""
                row["reviewCount"] = ""
                row["onSale"] = ""
                row["colour"] = colors
                row["size"] = capacitys
                row["UPC"] = ""
                row["EAN"] = ""
                row["cat"] = url.split("/")[3]
                row["subcat1"] = url.split("/")[4]
                row["subcat2"] = url.split("/")[5]
                row["subcat3"] = url.split("/")[6] if len(url.split("/")) > 6 else ""
                row["subcat4"] = url.split("/")[7] if len(url.split("/")) > 7 else ""
                row["subcat5"] = url.split("/")[8] if len(url.split("/")) > 8 else ""
                row["warranty"] = ""
                row["isSellingFast"] = ""
                row["isRestockingSoon"] = ""
                row["isPromotion"] = ""
                row["isOutletPrice"] = ""
                row["lowestPriceText"] = ""
                images = data_product["variant_images"]
                row[f"image1"] = images[0] ["full_path"] if len(images) > 0 else ""
                row[f"image2"] = images[1]["full_path"] if len(images) > 1 else ""
                row[f"image3"] = images[2]["full_path"] if len(images) > 2 else ""
                row[f"image4"] = images[3]["full_path"] if len(images) > 3 else ""
                row[f"image5"] = images[4]["full_path"] if len(images) > 4 else ""
                row["saleText"] = ""
                row["handsetOnlyCostCash"] = float(data_product["min_price"])
                if row["handsetOnlyCostCash"]:
                    row["previousPrice"] = ""

                    plans = data_product.get("available_sims") or [{}]

                    for bundle in plans:
                        row["phoneContractDuration"] = bundle.get("durationInt", 0)
                        for Upfront in range(0, int(max_value) + 1, 10):
                            row["advance"] = Upfront
                            paymentAmount = round(((row["handsetOnlyCostCash"]-row["advance"])/24),2)
                            row["paymentAmount"] = 0 if row["phoneContractDuration"] == 0 else paymentAmount
                            row["plan_type"] = "contract"
                            row["sim_data"] = simplan.get("data-tariff", "")
                            row["sim_price"] = 0 if row["phoneContractDuration"] == 0 else paymentAmount
                            row["simOfferData"] = ""
                            attributes = bundle.get("attributes", {})

                            texts_minutes = attributes.get("texts-minutes") or ""
                            data_tariff = attributes.get("data-tariff") or ""
                            duration = attributes.get("duration") or ""

                            if texts_minutes or data_tariff or duration:
                                sim_contract_name = (
                                    f'Texts Minutes - {texts_minutes}, '
                                    f'data-tariff - {data_tariff}, '
                                    f'Duration - {duration}'
                                )
                            else:
                                sim_contract_name = ""
                            row["simContractname"] = sim_contract_name
                            row["simContractDuration"] = 0 if row["phoneContractDuration"] == 0 else row["phoneContractDuration"]
                            row["isPhoneContractAvailableWOsim"] = "N"
                            row["phoneContractSimPackage"] = 0 if row["phoneContractDuration"] == 0 else row["handsetOnlyCostCash"]
                            row["handsetOnlyContract"] = ""
                            row["sim1YearIncrease"] = ""
                            row["sim2YearIncrease"] = ""
                            row["sim3YearIncrease"] = ""
                            row["simDesc"] = simDesc
                            attribute_index = 1

                            for li in spec_items:
                                if attribute_index > 20:
                                    break
                                strong = li.find("strong")
                                if strong:
                                    title = strong.get_text(strip=True).replace(":", "")
                                    description = strong.next_sibling.strip() if strong.next_sibling else ""
                                    row[f"attributeType{attribute_index}"] = "SPECIFICATION"
                                    row[f"attributeTitle{attribute_index}"] = title
                                    row[f"attributeValue{attribute_index}"] = description.upper()
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





async def get_products_list(urlsite_map: str):
    products_all = await fetch_sitemap(urlsite_map)
    products = [url for url in products_all if "/phone/" in url or "/bundle/" in url]
    products = ["/".join(u.rstrip("/").split("/")[:-1])  for u in products]
    filtered_products = []
    for url in products:
        parts = url.strip("/").split("/")
        if "phone" in parts or "bundle" in parts:
            idx = parts.index("phone") if "phone" in parts else parts.index("bundle")
            if len(parts) > idx + 2:
                filtered_products.append(url)
    return list(set(filtered_products))


async def main():
    create_csv_file("products.csv")
    urlsite_map = "https://www.mozillion.com/sitemapxml"
    products = await get_products_list(urlsite_map)


    data = await extact_data_from_product_url(products)


if __name__ == "__main__":
    asyncio.run(main())
