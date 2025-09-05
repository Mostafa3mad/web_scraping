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


async def fetch_single_product(url: str):

    async with httpx.AsyncClient(timeout=30.0) as session:
        auth_html = await fetch_url("https://www.vodafone.co.uk/web-shop/login/auth/session", client=session)
        platform_session_id = await get_platform_session_id(session)
        slogn_url = url.strip("/").split("/")[-2:]
        brand, model = slogn_url
        api_url = f"https://www.vodafone.co.uk/mobile/pay-monthly-contracts/api/digital/v2/device-purchase/paym/v3/{platform_session_id}/{brand}/{model}/device-group-journeys/latest?segment=Consumer"

        response_Consumer = await fetch_url(api_url, content_type="", client=session)
        payload = json.loads(response_Consumer)
        variants_href = payload["_links"]["get-device-variants"]["href"]
        variants_href = "https://www.vodafone.co.uk/mobile/pay-monthly-contracts/api/digital/v2" + variants_href
        vatites_response = await fetch_url(variants_href, content_type="", client=session)
        data = json.loads(vatites_response)
        for variant in data.get("variants", []):
            row = {}
            colour = variant.get("colourName")

            for size_variant in variant.get("sizeVariants", []):
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
                row["apiURL"] = "api_url_varites"
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
                package_href = size_variant.get("_links").get("select-device-variant").get("href")
                package_api = f"https://www.vodafone.co.uk/mobile/pay-monthly-contracts/api/digital/v2/{package_href}?{sku}"
                json_data = {
                    'deviceId': sku,
                }
                headers = {
                    'X-HTTP-Method-Override': 'PATCH',
                }

                package_response = await fetch_url(package_api, content_type="", client=session, method="POST", json_data=json_data, headers=headers)
                package_response_json = json.loads(package_response)
                api_url = f"https://www.vodafone.co.uk/mobile/pay-monthly-contracts/api/digital/v2/device-purchase/paym/v3/{platform_session_id}/{brand}/{model}/device-group-journeys/latest?segment=Consumer"
                response_to_get_response = await fetch_url(api_url, content_type="", client=session)

                payload = json.loads(response_to_get_response)
                variants_href = payload["_links"]["get-device-variants"]["href"]
                variants_href = "https://www.vodafone.co.uk/mobile/pay-monthly-contracts/api/digital/v2" + variants_href
                parts = variants_href.split("/")
                journey_id = parts[parts.index("device-group-journeys") + 1]
                vatites_response = await fetch_url(variants_href, content_type="", client=session)
                vatites_response_json = json.loads(vatites_response)

                plan_api = f"https://www.vodafone.co.uk/mobile/pay-monthly-contracts/api/digital/v2/device-purchase/paym/v3/{platform_session_id}/{brand}/{model}/device-group-journeys/{journey_id}/device-variants/{sku}/plans?preBuilt=true"
                plan_response = await fetch_url(plan_api, content_type="", client=session)
                plan_json = json.loads(plan_response)
                plans = plan_json.get("plans", [])

                for plan in plans:
                    handsetOnlyCostCash = plan_json.get("packageBuildSummary").get("deviceTotalCost").get("gross").get("value")
                    name = plan.get("name")
                    Duration_contract_data_plan = plan.get("commitmentPeriod")
                    device_monthly = plan.get("deviceMonthlyPrice", {}).get("gross", {}).get("value")
                    upfront = plan.get("oneOffPrice", {}).get("gross", {}).get("value")
                    plan_monthly = plan.get("planPrices", {}).get("monthlyPrices", {}).get("currentPrice", {}).get("gross")
                    total_monthly = plan.get("prices", {}).get("monthlyPrices", {}).get("currentPrice", {}).get("gross")
                    plan_id = plan.get("_links", {}).get("select-plan", {}).get("parameters", {}).get("planId")
                    Duration_contract_device = plan.get("_links", {}).get("select-plan", {}).get("parameters", {}).get("tenure")
                    row["phoneContractDuration"] = Duration_contract_device
                    row["handsetOnlyCostCash"] = handsetOnlyCostCash
                    row["advance"] = upfront
                    row["phoneContractPrice"] = float(device_monthly) * float(Duration_contract_device) + float(upfront)
                    row["paymentAmount"] = device_monthly
                    row["plan_type"] = "contract"
                    row["sim_data"] = name.split(" ")[0]
                    row["sim_price"] = plan_monthly
                    row["simOfferData"] = ""
                    row["simContractname"] = name
                    row["simContractDuration"] = Duration_contract_data_plan
                    row["isPhoneContractAvailableWOsim"] = "N"
                    row["phoneContractSimPackage"] = total_monthly
                    row["handsetOnlyContract"] = ""

                    index = 1
                    for rise in plan.get("bundlePriceRise", []):
                        price = rise.get("monthlyPrice", {}).get("gross")
                        text = rise.get("text", "")
                        if price and text != "":
                            row[f"sim{index}YearIncrease"] = price
                            index += 1
                    api_specification = f"https://www.vodafone.co.uk/mobile/pay-monthly-contracts/api/digital/v2/device-purchase/paym/v3/{platform_session_id}/{brand}/{model}/device-group-journeys/{journey_id}/device-variants/{sku}/specification"
                    specification_response = await fetch_url(api_specification, content_type="", client=session)
                    specification_json = json.loads(specification_response)
                    spec_groups = specification_json["specification"]["specificationGroups"]


                    attribute_index = 1
                    last_group = None

                    for group in spec_groups:
                        group_name = group.get("name", "").strip()

                        if group_name != last_group:
                            last_group = group_name

                        for attr in group.get("specificationAttributes", []):
                            title = group_name
                            description = attr.get("value", "").strip()

                            row[f"attributeType{attribute_index}"] = "SPECIFICATION"
                            row[f"attributeTitle{attribute_index}"] = title
                            if f"attributeValue{attribute_index}" in row:
                                row[f"attributeValue{attribute_index}"] += " | " + description.upper()
                            else:
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


async def get_links_site_map(url):
    products = []
    responses = await fetch_sitemap(url)
    for product in responses:
        if "/mobile/pay-monthly-contracts/" in str(product):
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
