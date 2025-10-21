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


async def fetch_url(
    url: str,
    content_type: str = "html",
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, str]] = None,
    data: Optional[Union[Dict[str, Any], str]] = None,
    json_data: Optional[Dict[str, Any]] = None,
    method: str = "GET",
    config: Dict[str, Any] = None,
    client: Optional[httpx.AsyncClient] = None,
) -> str:
    if config is None:
        config = DEFAULT_CONFIG

    max_retries = config.get("max_retries", 3)
    min_delay = config.get("min_delay", 1.0)
    max_delay = config.get("max_delay", 3.0)
    save_raw = False

    if content_type == "sitemap" and config.get("save_raw_sitemaps", True):
        save_raw = True
    elif content_type == "category" and config.get("save_raw_categories", True):
        save_raw = True
    elif content_type == "product" and config.get("save_raw_products", True):
        save_raw = True

    if save_raw and config.get("save_local", True):
        cache_path = get_cache_path(url, content_type)
        if cache_path.exists():
            logger.info(f"Using cached version of {url} from {cache_path}")
            with open(cache_path, "r", encoding="utf-8") as f:
                return f.read()

    if headers is None:
        headers = get_random_headers()

    for retry in range(max_retries):
        try:
            await asyncio.sleep(random.uniform(min_delay, max_delay))

            if config.get("use_scrapingbee", False) and config.get("scrapingbee_key"):
                response_text = await fetch_with_scrapingbee(url, headers, config)
            else:
                if client is not None:
                    if method.upper() == "POST":
                        response = await client.post(url, headers=headers, params=params, data=data, json=json_data)
                    else:
                        response = await client.get(url, headers=headers, params=params)
                else:
                    async with httpx.AsyncClient(timeout=30.0) as temp_client:
                        if method.upper() == "POST":
                            response = await temp_client.post(url, headers=headers, params=params, data=data, json=json_data)
                        else:
                            response = await temp_client.get(url, headers=headers, params=params)
                response.raise_for_status()
                response_text = response.text

            if save_raw and config.get("save_local", True):
                cache_path = get_cache_path(url, content_type)
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                with open(cache_path, "w", encoding="utf-8") as f:
                    f.write(response_text)
                logger.info(f"Saved raw content to {cache_path}")

            return response_text

        except Exception as e:
            logger.warning(f"Request failed. URL: {url}. Error: {repr(e)}. Attempt {retry+1}/{max_retries}")
            if retry < max_retries - 1:
                backoff_time = (2 ** retry) + random.uniform(0, 1)
                logger.info(f"Backing off for {backoff_time:.2f} seconds before retry")
                await asyncio.sleep(backoff_time)

    raise RuntimeError(f"Max retries exceeded for URL: {url}")

async def get_platform_session_id(session):
    cookies_obj = session.cookies
    cookies_dict = {cookie.name: cookie.value for cookie in cookies_obj.jar}
    token_enc_prod1_p_id_token = cookies_dict.get("eShop-auth-prod1_p_id_token")
    if token_enc_prod1_p_id_token:
        token_decoded_prod1_p_id_token = urllib.parse.unquote(token_enc_prod1_p_id_token)

        decoded = token_decoded_prod1_p_id_token
        if decoded.startswith("j:"):
            decoded = decoded[2:]

        data = json.loads(decoded)
        platform_session_id = data.get("platformSessionId")
        return platform_session_id

async def get_specification(session,api_specification,row):
    specification_response = await fetch_url(api_specification, content_type="", client=session,headers={},config={})
    specification_json = json.loads(specification_response)
    spec_groups = specification_json["specification"]["specificationGroups"]

    attribute_index = 1

    for group in specification_json.get("specification", {}).get("specificationGroups", []):
        if attribute_index > 20:
            break
        group_name = group.get("name", "").strip()

        values = []
        for attr in group.get("specificationAttributes", []):
            val = attr.get("value", "").strip()
            if val:
                values.append(val.upper())

        if values:
            row[f"attributeType{attribute_index}"] = "SPECIFICATION"
            row[f"attributeTitle{attribute_index}"] = group_name
            row[f"attributeValue{attribute_index}"] = " | ".join(values)

            attribute_index += 1


async def fetch_plan(url, pay_method, platform_session_id, brand, model, journey_id, sku,session,size_variant):
    all_plans = []
    data_row_plan = {}

    if pay_method == "pay-monthly-contracts":
        max_upfront = float(size_variant.get("upfrontPriceConfigurator", {}).get("maximumUpfrontPrice", {}).get("gross").get("value"))
        min_upfront = float(size_variant.get("upfrontPriceConfigurator", {}).get("minimumUpfrontPrice", {}).get("gross").get("value"))
        max_month = float(size_variant.get("tenureConfigurator", {}).get("maximumTenure", {}).get("value")) or 0
        min_month = float(size_variant.get("tenureConfigurator", {}).get("minimumTenure", {}).get("value")) or 0
        list_upfront = list(range(int(min_upfront), int(max_upfront)+1,100))
        if list_upfront[-1] != int(max_upfront):
            list_upfront.append(int(max_upfront))
        for month in [int(max_month), int(min_month)]:

            for upfront in list_upfront:
                # print("-------------------")
                # print(month)
                # print(upfront)

                # print("max_month",max_month)
                # print("min_month",min_month)
                # print("max_upfront",max_upfront)
                # print("min_upfront",min_upfront)

                total_device = float(size_variant.get("upfrontPriceConfigurator", {}).get("totalHandsetPrice", {}).get("gross", {}).get("value"))
                # print("total_device",total_device)
                deviceMonthlyPrice = float(round((total_device - upfront) / month, 2))
                # print("deviceMonthlyPrice",deviceMonthlyPrice)
                api_package_contract = f"https://www.vodafone.co.uk/mobile/pay-monthly-contracts/api/digital/v2/device-purchase/paym/v3/{platform_session_id}/{brand}/{model}/device-group-journeys/{journey_id}/package?{sku}"

                json_data_contract = {
                    'tenure': month,
                    'upfrontPrice': upfront,
                    'deviceMonthlyPrice': deviceMonthlyPrice,
                    'confirmConfigurator': True,
                }
                headers = {
                    'X-HTTP-Method-Override': 'PATCH',
                }

                package_response = await fetch_url(api_package_contract, content_type="", client=session, method="POST",
                                                   json_data=json_data_contract, headers=headers, config={})
                package_response_json = json.loads(package_response)
                # print(package_response_json)
                plan_api = f"https://www.vodafone.co.uk/mobile/pay-monthly-contracts/api/digital/v2/device-purchase/paym/v3/{platform_session_id}/{brand}/{model}/device-group-journeys/{journey_id}/device-variants/{sku}/plans"
                plan_response = await fetch_url(plan_api, content_type="", client=session, headers={}, config={})
                plans_response = json.loads(plan_response)
                # print(plans_response)
                Upfront = plans_response.get("packageBuildSummary").get("deviceUpfrontCost").get("gross").get("value")
                device_monthly = plans_response.get("_links").get("select-device-variant-tenure").get("parameters").get(
                    "deviceMonthlyPrice")
                handsetOnlyCostCash = plans_response.get("packageBuildSummary").get("deviceTotalCost").get("gross").get("value")
                Duration_contract_device = plans_response.get("_links", {}).get("select-device-variant-tenure", {}).get(
                    "parameters", {}).get("tenure") or 0
                Duration_contract_data_plan = plans_response.get("filters").get("duration")[0].get("value")

                if Duration_contract_device:
                    Duration_contract_data_plan = float(Duration_contract_data_plan.split(" ")[0])
                    # print(Duration_contract_data_plan)
                    phoneContractPrice = float(device_monthly) * float(Duration_contract_device) + float(Upfront)

                for plan in plans_response.get("plans", []):
                    simDesc_list = []
                    name = plan.get("name")
                    plan_monthly = plan.get("planPrices", {}).get("monthlyPrices", {}).get("currentPrice", {}).get("gross")
                    total_monthly = plan.get("prices", {}).get("monthlyPrices", {}).get("currentPrice", {}).get("gross")
                    data_row_plan["phoneContractDuration"] = Duration_contract_device
                    data_row_plan["handsetOnlyCostCash"] = handsetOnlyCostCash
                    data_row_plan["advance"] = Upfront
                    data_row_plan["phoneContractPrice"] = phoneContractPrice
                    data_row_plan["paymentAmount"] = device_monthly
                    data_row_plan["plan_type"] = "contract"
                    data_row_plan["sim_data"] = name.split(" ")[0]
                    data_row_plan["sim_price"] = plan_monthly
                    data_row_plan["simOfferData"] = ""
                    data_row_plan["simContractname"] = name
                    data_row_plan["simContractDuration"] = Duration_contract_data_plan
                    data_row_plan["isPhoneContractAvailableWOsim"] = "N"
                    data_row_plan["phoneContractSimPackage"] = total_monthly
                    inclusiveProducts = plan.get("inclusiveProducts", [])
                    for inclusiveProduct in inclusiveProducts:

                        simDesc_list.append(inclusiveProduct.get("name", ""))

                    data_row_plan["simDesc"] = " | ".join(simDesc_list)
                    data_row_plan["handsetOnlyContract"] = ""
                    index = 1
                    for rise in plan.get("bundlePriceRise", []):
                        price = rise.get("monthlyPrice", {}).get("gross")
                        text = rise.get("text", "")
                        if price and text != "":
                            # print(f"increase in {index} year £{price} {text}")
                            data_row_plan[f"sim{index}YearIncrease"] = price
                            index += 1
                    all_plans.append(data_row_plan)
                    # print(data_row_plan["simDesc"])
                    data_row_plan={}


        return all_plans

    if pay_method == "smart-watches-and-wearables":
        max_upfront = float(size_variant.get("upfrontPriceConfigurator", {}).get("maximumUpfrontPrice", {}).get("gross").get("value"))
        min_upfront = float(size_variant.get("upfrontPriceConfigurator", {}).get("minimumUpfrontPrice", {}).get("gross").get("value"))
        max_month = float(size_variant.get("tenureConfigurator", {}).get("maximumTenure", {}).get("value")) or 0
        min_month = float(size_variant.get("tenureConfigurator", {}).get("minimumTenure", {}).get("value")) or 0
        list_upfront = list(range(int(min_upfront), int(max_upfront)+1,100))
        if list_upfront[-1] != int(max_upfront):
            list_upfront.append(int(max_upfront))
        for month in [int(max_month), int(min_month)]:

            for upfront in list_upfront:
                # print("-------------------")
                # print(month)
                # print(upfront)

                # print("max_month",max_month)
                # print("min_month",min_month)
                # print("max_upfront",max_upfront)
                # print("min_upfront",min_upfront)

                total_device = float(size_variant.get("upfrontPriceConfigurator", {}).get("totalHandsetPrice", {}).get("gross", {}).get("value"))
                # print("total_device",total_device)
                deviceMonthlyPrice = float(round((total_device - upfront) / month, 2))
                # print("deviceMonthlyPrice",deviceMonthlyPrice)
                api_package_contract = f"https://www.vodafone.co.uk/smart-watches-and-wearables/api/smart-device-purchase/v3/{platform_session_id}/consumer/watch/paym/{brand}/{model}/smart-device-groups-journey/{journey_id}/package?{sku}"

                json_data_contract = {
                    'tenure': month,
                    'upfrontPrice': upfront,
                    'deviceMonthlyPrice': deviceMonthlyPrice,
                    'confirmConfigurator': True,
                }
                headers = {
                    'X-HTTP-Method-Override': 'PATCH',
                }

                package_response = await fetch_url(api_package_contract, content_type="", client=session, method="POST",
                                                   json_data=json_data_contract, headers=headers, config={})
                package_response_json = json.loads(package_response)
                # print(package_response_json)
                plan_href  = package_response_json["_links"]["get-plans"]["href"]

                plan_api = f"https://www.vodafone.co.uk/smart-watches-and-wearables/api"+plan_href
                plan_response = await fetch_url(plan_api, content_type="", client=session, headers={}, config={})
                plans_response = json.loads(plan_response)
                # print(plans_response)
                Upfront = plans_response.get("packageBuildSummary").get("deviceUpfrontCost").get("gross").get("value")
                device_monthly = plans_response.get("packageBuildSummary").get("deviceMonthlyCost").get("gross").get("value")
                handsetOnlyCostCash = float(deviceMonthlyPrice)
                Duration_contract_device = plans_response.get("packageBuildSummary", {}).get("deviceTenure", {}).get("value") or 0



                for plan in plans_response.get("plans", []):
                    if Duration_contract_device:
                        Duration_contract_data_plan = float(plan.get("commitmentPeriod").split(" ")[0])
                        # print(Duration_contract_data_plan)
                        phoneContractPrice = float(device_monthly) * float(Duration_contract_device) + float(Upfront)

                    name = plan.get("name")
                    plan_monthly = plan.get("monthlyPrice", {}).get("gross", {}).get("value", 0)
                    # total_monthly = plan.get("prices", {}).get("monthlyPrices", {}).get("currentPrice", {}).get("gross")
                    data_row_plan["phoneContractDuration"] = Duration_contract_device
                    data_row_plan["handsetOnlyCostCash"] = handsetOnlyCostCash
                    data_row_plan["advance"] = Upfront
                    data_row_plan["phoneContractPrice"] = round(phoneContractPrice, 2)
                    data_row_plan["paymentAmount"] = device_monthly
                    data_row_plan["plan_type"] = "contract"
                    data_row_plan["sim_data"] = ""
                    data_row_plan["sim_price"] = plan_monthly
                    data_row_plan["simOfferData"] = ""
                    data_row_plan["simContractname"] = name
                    data_row_plan["simContractDuration"] = Duration_contract_data_plan
                    data_row_plan["isPhoneContractAvailableWOsim"] = "N"
                    data_row_plan["phoneContractSimPackage"] = round(float(device_monthly) + float(plan_monthly),2)
                    data_row_plan["handsetOnlyContract"] = ""
                    index = 1
                    for rise in plan.get("bundlePriceRise", []):
                        price = rise.get("monthlyPrice", {}).get("gross")
                        text = rise.get("text", "")
                        if price and text != "":
                            # print(f"increase in {index} year £{price} {text}")
                            data_row_plan[f"sim{index}YearIncrease"] = price
                            index += 1
                    all_plans.append(data_row_plan)
                    data_row_plan={}


        return all_plans

    elif "pay-as-you-go" in str(url):
        plan_device_api = f"https://www.vodafone.co.uk/mobile/pay-as-you-go/api/payg-device-purchase/{platform_session_id}/consumer/voice/{brand}/{model}/journeys/{journey_id}/device-variants/{sku}/plans"
        plan_response = await fetch_url(plan_device_api, content_type="", client=session, headers={},
                                        config={})
        plan_json = json.loads(plan_response)
        # print(plan_json)
        plans = plan_json.get("bundles", [])

        for plan in plans:
            name = plan.get("name")
            Duration_contract_data_plan = 1
            plan_monthly = float(plan.get("price", {}).get("gross", {}).get("value", ""))
            plan_id = plan.get("id", "")
            price_phone = float(size_variant.get("originalOneOffPrice", {}).get("gross", "").get("value", ""))
            total_monthly = price_phone + plan_monthly
            data_bundels = plan.get("allowances", [])

            # print(plan_id)
            ORIGDATA = ""
            DATA = ""
            simOfferData = ""
            # print(data_bundels)
            for data_bundel in data_bundels:
                if data_bundel.get("type") == "ORIGDATA":
                    ORIGDATA = data_bundel.get("value", "")
                elif data_bundel.get("type") == "DATA":
                    DATA = data_bundel.get("value", "")

            if ORIGDATA == DATA:
                DATA = ""

            data_row_plan["phoneContractDuration"] = 0
            data_row_plan["handsetOnlyCostCash"] = price_phone
            data_row_plan["advance"] = 0
            data_row_plan["phoneContractPrice"] = 0
            data_row_plan["paymentAmount"] = 0
            data_row_plan["plan_type"] = "pay_as_yo_go"
            data_row_plan["sim_data"] = ORIGDATA
            data_row_plan["sim_price"] = plan_monthly
            data_row_plan["simOfferData"] = DATA
            data_row_plan["simContractname"] = name
            data_row_plan["simContractDuration"] = 1
            data_row_plan["isPhoneContractAvailableWOsim"] = "N"
            data_row_plan["phoneContractSimPackage"] = total_monthly
            data_row_plan["handsetOnlyContract"] = ""


            all_plans.append(data_row_plan)
            data_row_plan={}
        return all_plans



async def fetch_single_product(url: str):
    pay_method = ""
    if "pay-monthly-contracts" in str(url):
        pay_method = "pay-monthly-contracts"
    elif "pay-as-you-go" in str(url):
        pay_method = "pay-as-you-go"
    elif "smart-watches-and-wearables" in str(url):
        pay_method = "smart-watches-and-wearables"

    async with httpx.AsyncClient(timeout=30.0) as session:
        auth_html = await fetch_url("https://www.vodafone.co.uk/web-shop/login/auth/session", client=session,headers={},config={})
        platform_session_id = await get_platform_session_id(session)
        # print(platform_session_id)
        slogn_url = url.strip("/").split("/")[-2:]
        brand, model = slogn_url
        if pay_method == "pay-monthly-contracts":
            api_url = f"https://www.vodafone.co.uk/mobile/pay-monthly-contracts/api/digital/v2/device-purchase/paym/v3/{platform_session_id}/{brand}/{model}/device-group-journeys/latest?segment=Consumer"
        elif pay_method == "pay-as-you-go":
            api_url = f"https://www.vodafone.co.uk/mobile/pay-as-you-go/api/payg-device-purchase/{platform_session_id}/consumer/voice/{brand}/{model}/journeys/latest"

        elif pay_method == "smart-watches-and-wearables":
            api_url = f"https://www.vodafone.co.uk/smart-watches-and-wearables/api/smart-device-purchase/v3/{platform_session_id}/consumer/watch/paym/{brand}/{model}/smart-device-groups-journey/latest"


        response_Consumer = await fetch_url(api_url, content_type="", client=session,headers={},config={})
        # print(response_Consumer)
        payload = json.loads(response_Consumer)

        if pay_method == "pay-monthly-contracts":
            variants_href = payload["_links"]["get-device-variants"]["href"]

            variants_href = f"https://www.vodafone.co.uk/mobile/pay-monthly-contracts/api/digital/v2" + variants_href
        elif pay_method == "pay-as-you-go":
            variants_href = payload["_links"]["get-device-variants"]["href"]

            variants_href = f"https://www.vodafone.co.uk/mobile/pay-as-you-go/api" + variants_href
        elif pay_method == "smart-watches-and-wearables":
            variants_href = payload["_links"]["get-smart-device-variants"]["href"]
            variants_href = f"https://www.vodafone.co.uk/smart-watches-and-wearables/api/"+ variants_href

        vatites_response = await fetch_url(variants_href, content_type="", client=session,headers={},config={})
        data = json.loads(vatites_response)
        # print(data)
        for variant in data.get("variants", []):
            row = {}
            colour = variant.get("colourName")

            for size_variant in variant.get("sizeVariants", []):
                # print(size_variant)
                availability = size_variant.get("availability", "Unknown")
                if availability.lower() != "in stock":
                    continue
                sku = size_variant.get("id", "N/A")
                availability = size_variant.get("availability", "Unknown")
                Name = size_variant.get("name", "N/A")
                Brand = data.get('groupName').split()[0]
                size = f"{size_variant.get('size', {}).get('value')} {size_variant.get('size', {}).get('uom')}"
                row["source"] = "Vodafone"
                row["date"] = datetime.now().strftime("%Y-%m-%d")
                row["apiURL"] = ""
                row["url"] = url
                row["sku"] = sku
                row["name"] = Name
                row["brand"] = Brand
                row["stock"] = "Y" if availability.lower() == "in stock" else ""
                clean_desc = data["deviceSummary"].replace("\xa0", " ").strip()
                row["desc"] = clean_desc
                row["shortDesc"] = ""
                row["videoURL"] = ""
                row["lowestPriceValue"] = ""
                row["reviewRating"] = ""
                row["reviewCount"] = ""
                row["onSale"] = ""
                row["colour"] = colour
                row["size"] = size
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
                if "imageSet" in variant:
                    for i, img in enumerate(variant["imageSet"][:5], start=1):
                        src = img.get("src")
                        row[f"image{i}"] = f"http:{src}" if src else ""
                row["saleText"] = ""
                row["previousPrice"] = ""
                try:
                    package_href = size_variant.get("_links").get("select-device-variant-tenure").get("href")
                except:
                    package_href = size_variant.get("_links").get("select-device-variant").get("href")
                if pay_method == "pay-monthly-contracts":
                    package_api = f"https://www.vodafone.co.uk/mobile/{pay_method}/api/digital/v2/{package_href}?{sku}"
                elif pay_method == "pay-as-you-go":
                    package_api = f"https://www.vodafone.co.uk/mobile/pay-as-you-go/api{package_href}?{sku}"

                elif pay_method == "smart-watches-and-wearables":
                    package_api = f"https://www.vodafone.co.uk/smart-watches-and-wearables/api{package_href}?{sku}"

                json_data = {
                    'deviceId': sku,
                }
                headers = {
                    'X-HTTP-Method-Override': 'PATCH',
                }

                package_response = await fetch_url(package_api, content_type="", client=session, method="POST", json_data=json_data, headers=headers,config={})
                package_response_json = json.loads(package_response)
                if pay_method == "pay-monthly-contracts":
                    api_url = f"https://www.vodafone.co.uk/mobile/{pay_method}/api/digital/v2/device-purchase/paym/v3/{platform_session_id}/{brand}/{model}/device-group-journeys/latest?segment=Consumer"
                    response_to_get_response = await fetch_url(api_url, content_type="", client=session,headers={},config={})

                    payload = json.loads(response_to_get_response)
                    # print(payload)
                    variants_href = payload["_links"]["get-device-variants"]["href"]

                #####################################################variants_href###########################################################
                # print(variants_href)
                if pay_method == "pay-monthly-contracts":
                    variants_href = f"https://www.vodafone.co.uk/mobile/pay-monthly-contracts/api/digital/v2" + variants_href

                    parts = variants_href.split("/")
                    journey_id = parts[parts.index("device-group-journeys") + 1]
                    # print(journey_id)
                    vatites_response = await fetch_url(variants_href, content_type="", client=session, headers={},config={})
                    vatites_response_json = json.loads(vatites_response)


                if pay_method == "pay-as-you-go":
                    variants_href = f"https://www.vodafone.co.uk/mobile/pay-as-you-go/api" + variants_href
                    parts = variants_href.split("/")
                    journey_id = parts[parts.index("journeys") + 1]
                    # print(journey_id)
                if pay_method == "smart-watches-and-wearables":
                    variants_href = f"https://www.vodafone.co.uk/smart-watches-and-wearables/api/" + variants_href
                    parts = variants_href.split("/")
                    journey_id = parts[parts.index("smart-device-groups-journey") + 1]
                    # print(journey_id)


                #####################################################################api_specification#####################################################################
                if pay_method == "pay-monthly-contracts":
                    api_specification = f"https://www.vodafone.co.uk/mobile/pay-monthly-contracts/api/digital/v2/device-purchase/paym/v3/{platform_session_id}/{brand}/{model}/device-group-journeys/{journey_id}/device-variants/{sku}/specification"
                    await get_specification(session,api_specification,row)

                elif pay_method == "pay-as-you-go":
                    api_specification = f"https://www.vodafone.co.uk/mobile/pay-as-you-go/api/payg-device-purchase/{platform_session_id}/consumer/voice/{brand}/{model}/journeys/{journey_id}/device-variants/{sku}/specification"
                    await get_specification(session,api_specification,row)
                elif pay_method == "smart-watches-and-wearables":
                    api_specification = f"https://www.vodafone.co.uk/mobile/pay-as-you-go/api/smart-device-purchase/v3/{platform_session_id}/consumer/watch/paym/{brand}/{model}/smart-device-groups-journey/{journey_id}/device-variants/{sku}/specification"
                    await get_specification(session,api_specification,row)

                #####################################################################api_plans#####################################################################
                all_plans = await fetch_plan(url, pay_method, platform_session_id, brand, model, journey_id, sku, session,size_variant)
                #
                for plans in all_plans:
                    row.update(plans)
                    # print(row)


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
    responses = await fetch_sitemap(url,config={})
    for product in responses:
        if "/mobile/pay-monthly-contracts/" in str(product) or "pay-as-you-go" in str(product) or "smart-watches-and-wearables" in str(product):
            parts = product.strip("/").split("/")
            if len(parts) > 6:
                products.append(product)

    return products






async def main():
    create_csv_file("products.csv")
    siteurl = "https://www.vodafone.co.uk/https_sitemap.xml"
    products  = await get_links_site_map(siteurl)
    data = await extact_data_from_product_url(products)


if __name__ == "__main__":
    asyncio.run(main())
