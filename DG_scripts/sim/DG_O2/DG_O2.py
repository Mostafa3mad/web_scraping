from functions import *
import os
import sys






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

def sanitize_filename(url):
    return re.sub(r'[<>:"/\\|?*]', '_', url)


async def clean_text(text):
    cleaned_text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
    cleaned_text = re.sub(r'&nbsp;', ' ', cleaned_text)
    cleaned_text = re.sub(r'<.*?>', '', cleaned_text)
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text)
    cleaned_text = cleaned_text.strip()
    cleaned_text_with_pipe = cleaned_text.replace(". ", " | ").replace("\n", "|")

    return cleaned_text_with_pipe



async def fetch_single_product(url: str):
    print(url)

    config = DEFAULT_CONFIG.copy()
    config['use_scrapingbee'] = True
    if SCRAPINGBEE_PARAMS["render_js"].strip().lower() == 'false':
        print("Please set render_js to true")
        sys.exit()
    response = await fetch_url(url,headers={},config=config,content_type="product")
    pattern = r"window\.ncPortlets\['DeviceDetailsPortlet'\] = ({.*?});</script>"
    match = re.search(pattern, response, re.DOTALL)

    if match:
        device_details_json = match.group(1)
        device_details = json.loads(device_details_json)
        device_codes = []
        for option in device_details.get('initData', {}).get('deviceDetails', {}).get('deviceOptions', []):
            color_name = option['color']['name']
            for capacity in option.get('capacityValues', []):
                data_product = f"{capacity.get("offeringCode")}_{color_name}_{capacity.get("name")}"
                device_codes.append(data_product)



        for device_code in device_codes:
            row = {}
            sku = device_code.split("_")[0]
            color = device_code.split("_")[1]
            size = device_code.split("_")[2]
            if size == "None":
                size = ""

            api_url = f"{url}?code={sku}"
            safe_filename = sanitize_filename(api_url) + ".json"

            api_response = await fetch_url(api_url, headers={}, config=config, content_type="product")
            pattern = r"window\.ncPortlets\['DeviceDetailsPortlet'\] = ({.*?});</script>"
            match = re.search(pattern, api_response, re.DOTALL)

            if match:
                device_details_json = match.group(1)
                device_details = json.loads(device_details_json)
                deviceDetails = device_details.get('initData', {}).get('deviceDetails', {})
                row["source"] = "O2"
                row["date"] = datetime.now().strftime("%Y-%m-%d")
                row["apiURL"] = ""
                row["url"] = api_url
                row["sku"] = sku
                row["name"] = f"{deviceDetails.get('name', '')} {size} {color}"
                row["brand"] = deviceDetails.get('brand', '')
                row["stock"] = "Y" if deviceDetails.get('stockAvailability', '').get('availabilityStatus', '') == 'in-stock' else ""
                if row["stock"] == "Y":
                    row["desc"] = await clean_text(deviceDetails.get('productDescription', ''))
                    row["shortDesc"] = ""
                    device_gallerys = device_details.get("initData", {}).get("deviceGallery", {})
                    for deviceGallery in device_gallerys:
                        if deviceGallery["color"]["name"] == color:
                            gallery_items = deviceGallery.get("galleryItems", [])
                            for i in range(5):
                                if i < len(gallery_items):
                                    row[f"image{i + 1}"] = gallery_items[i]["url"]
                                else:
                                    row[f"image{i + 1}"] = ""
                            break
                    row["lowestPriceValue"] = ""
                    row["reviewRating"] = ""
                    row["reviewCount"] = ""
                    row["onSale"] = ""
                    row["colour"] = color
                    row["size"] = size
                    row["UPC"] = ""
                    row["EAN"] = deviceDetails.get("id","")
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

                    cost_device = deviceDetails.get("repTableData", {})
                    handsetOnlyCostCash = cost_device.get(("totalDeviceCost")).split("£")[-1].strip()
                    row["handsetOnlyCostCash"] = float(handsetOnlyCostCash)
                    row["previousPrice"] = ""
                    try:
                        row["advance"] = float(cost_device["totalUpfront"].split("£")[-1].strip())
                    except:
                        row["advance"] = 0
                    try:
                        row["phoneContractDuration"] =int(cost_device.get("selectedMonths",0).split(" ")[0])
                    except:
                        row["phoneContractDuration"] = 0
                    try:
                        row["paymentAmount"] = float(cost_device["totalMonthly"].split("£")[-1].strip())
                    except:
                        row["paymentAmount"] = row["handsetOnlyCostCash"]


                    row["phoneContractPrice"] = row["paymentAmount"] * row["phoneContractDuration"] + row["advance"]
                    tariffCardDetails = deviceDetails.get("tariffCardDetails").get("tariffCards", [])

                    if not tariffCardDetails:
                        tariffCardDetails = ["0"]

                    for plan in tariffCardDetails:
                        try:
                            row["plan_type"] = "contract"

                            allowances = plan.get("allowances")
                            row["sim_data"] = allowances["data"]["name"]
                            price = plan["mrc"]["originalPrice"]["amount"]
                            row["sim_price"] = float(price)

                            row["simOfferData"] = ""

                            row["simContractname"] = allowances["data"]["name"]
                            row["simContractDuration"] = int(plan["eeccDurationIfApplicable"])
                            row["isPhoneContractAvailableWOsim"] = "N"
                            row["phoneContractSimPackage"] = row["phoneContractPrice"]
                            row["handsetOnlyContract"] = ""
                            index = 1
                            for increase in plan["apiPriceDetails"]["apiPrices"]:
                                row[f"sim{index}YearIncrease"] = increase["price"]
                                index += 1
                            desc = plan["offers"][0]["cmsDescription"]

                            desc_cleac = await clean_text(desc)
                            row["simDesc"] = desc_cleac
                        except:

                            row["phoneContractPrice"] = ""
                            row["plan_type"] = "sim_free"

                            pass


                        try:
                            attribute_index = 1
                            specification = deviceDetails['specificationGroups'][0]['specifications']
                            for feature in specification:
                                if attribute_index > 20:
                                    break

                                title_clean = re.sub(r'\s+', ' ', feature.get("name", "").strip())

                                value_raw = feature.get("value", "")
                                value_no_html = re.sub(r'<.*?>', '', value_raw)
                                value_clean = re.sub(r'\s+', ' ', value_no_html).strip()
                                row[f"attributeType{attribute_index}"] = "SPECIFICATION"
                                row[f"attributeTitle{attribute_index}"] = title_clean
                                row[f"attributeValue{attribute_index}"] = value_clean
                                attribute_index += 1
                        except:
                            pass
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
    url_site = "https://www.o2.co.uk/shop/sitemap.xml"
    products = await fetch_sitemap(url_site)
    product_urls = []
    for url in products:
        if url.count('/') >= 5:
            product_urls.append(url)
    product_urls = ["https://www.o2.co.uk/shop/samsung/galaxy-s25-ultra-5g"]
    data = await extact_data_from_product_url(product_urls)


if __name__ == "__main__":
    asyncio.run(main())
