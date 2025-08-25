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





def generate_steps(min_val, max_val):
    values = []

    if max_val <= 0 or min_val <= 0 or max_val <= min_val:
        return values

    current = min_val
    while current < max_val:
        values.append(round(current, 2))

        if current < 150:
            current += 10
        elif current < 300:
            current += 25
        else:
            current += 50

    if values and values[-1] != round(max_val, 2):
        values.append(round(max_val, 2))

    return values

async def fetch_single_product(url: str):

    response = await fetch_url(url, content_type="product")

    soup = BeautifulSoup(response, "html.parser")
    row = {}
    script_tag = soup.find("script", {"id": "__NEXT_DATA__"})

    if script_tag:
        data_str = script_tag.text
        data_str = data_str.replace("undefined", "null")
        data = json.loads(data_str)
        bundle_key = next(k for k in data["props"]["apolloState"]["ROOT_QUERY"] if k.startswith("deviceBundle("))
        data_json = data["props"]["apolloState"]["ROOT_QUERY"][bundle_key]
        variants = data_json["deviceBundleVariants"]
        baseDeviceSeoId = data_json["product"]["baseDeviceSeoId"]
        for v in variants:
            row = {}
            product = v["product"]
            dimensions = product["dimensions"]

            dims = {d["key"]: d["value"] for d in dimensions}

            color = dims.get("color", "")
            capacity = dims.get("capacity", "")
            size = dims.get("watchScreenSize", "")


            if capacity and not "tablet" in url:
                params = {
                    'operationName': 'FlexPayProductDetailsQuery',
                    'variables': f'{{"deviceBundleBySeoInput":{{"bundleSeoId":"pay-monthly-phones","baseProductSeoId":"{baseDeviceSeoId}","dimensions":[{{"key":"capacity","value":"{capacity}"}},{{"key":"color","value":"{color}"}}]}}}}',
                    'extensions': '{"persistedQuery":{"version":1,"sha256Hash":"74a0a54b03ccd740c5a3e7f5767146b2a10a7f45e5874074328b1b63fb61f59e"}}',
                }
            elif "broadband" in url:

                params = {
                    'operationName': 'FlexPayProductDetailsQuery',
                    'variables': f'{{"deviceBundleBySeoInput":{{"bundleSeoId":"pay-monthly-mobile-broadband","baseProductSeoId":"{baseDeviceSeoId}","dimensions":[{{"key":"color","value":"{color}"}}]}}}}',
                    'extensions': '{"persistedQuery":{"version":1,"sha256Hash":"74a0a54b03ccd740c5a3e7f5767146b2a10a7f45e5874074328b1b63fb61f59e"}}',
                }
            elif "wearables" in url:
                params = {
                    'operationName': 'ProductDetailsQuery',
                    'variables': f'{{"deviceBundleBySeoInput":{{"bundleSeoId":"pay-monthly-{url.split("pay-monthly-")[1].split("-gallery")[0]}","baseProductSeoId":"{baseDeviceSeoId}","dimensions":[{{"key":"watchScreenSize","value":"{size}"}},{{"key":"color","value":"{color}"}}]}}}}',
                    'extensions': '{"persistedQuery":{"version":1,"sha256Hash":"4d049d3a7d5913eac7aa4467610512496e76cad121b46e896f143f50594c2846"}}',
                }
            elif "computing-tablets" in str(url):

                params = {
                    'operationName': 'FlexPayProductDetailsQuery',
                    'variables': f'{{"deviceBundleBySeoInput":{{"bundleSeoId":"add-pay-monthly-tablets","baseProductSeoId":"{baseDeviceSeoId}","dimensions":[{{"key":"capacity","value":"{capacity}"}},{{"key":"color","value":"{color}"}}]}}}}',
                    'extensions': '{"persistedQuery":{"version":1,"sha256Hash":"4d049d3a7d5913eac7aa4467610512496e76cad121b46e896f143f50594c2846"}}',
                }
            headers = {

                'cookie': 'XSRF-TOKEN=50e73457-1da8-4f86-b033-9b76de36ef06; isAcquisitionFlexpay=true; incap_ses_452_2335605=kXO/M/6l6xatdzwwwNNFBhfZpWgAAAAAvM/+wY9WImdfYbXrzb6qsQ==; visid_incap_2407824=Z+Wb5oOmT4GEV9+y5Z3PBMbppWgAAAAAQUIPAAAAAAAMCZFrJMtCOAxjz9FJ3c5S; nlbi_2407824=XzbZeaklhm0XdNpyrQnCxwAAAACYeiaM1SQbpv4m0i9JVqlI; incap_ses_452_2407824=ni90OVP8ngAbBkcwwNNFBsfppWgAAAAA1QnDsHbImiY1cIG0UsLzGQ==; visid_incap_2407823=ifpxnS1UTGW0RrP0eVewLsrppWgAAAAAQUIPAAAAAADY0q/eKAGg5teGWZwFuiHp; nlbi_2407823=zOvBeuePuwJKdckf9N3RlQAAAAAWDJZbMn/aKnQ1v4fyqJjp; incap_ses_452_2407823=RsNbI+Dy+245CUcwwNNFBsrppWgAAAAAa6W09QBNEGgAmBwT8uHZow==; incap_ses_1572_2335605=Q4IJV+WG0j/gg2uiDd7QFc3ppWgAAAAA1OgXLERqc3OarEdRUrzCzA==; visid_incap_2407832=LQ6FZY9RQbulv8Nv+Z5Owzikp2gAAAAAQUIPAAAAAAAoV9DLY9kUriRmyQ9Qk5RF; nlbi_2407832=zT0+XZaF1wiVxNrBKTvRZgAAAADJZpq8ciuroLHLZlw/P8e0; incap_ses_416_2407832=bBNqewO/1Cl7F0ev7u3FBTikp2gAAAAAvPI/aGDkJXJ8/J3kEaoJOg==; OptanonAlertBoxClosed=2025-08-21T22:56:11.560Z; at_check=true; abFeatureTesting.addlineCheckout=true; abFeatureTesting.acquisitionSubsidySemiHeadless=true; dtCookie=v_4_srv_2_sn_1D918CBBC2EBA0171D2BD4BA87D10FEF_perc_100000_ol_0_mul_1_app-3A7556ecaf88963360_1_app-3A7b6134e75ed78205_1_app-3Ae7f5606eaba145d0_1_rcs-3Acss_0; visid_incap_2407808=x7FBHwyiTPOM9K6iTtRHGZukp2gAAAAAQUIPAAAAAACUx10kqmHEmx7goXoKw08J; nlbi_2407808=kPmYTRn9bmxiq9BD8OLD1wAAAABShJaq4ha/3+Wz8+4AY2jA; incap_ses_416_2407808=6T/RCj3kJF1VYUev7u3FBZukp2gAAAAAvtz5/E0D46P0KFCv5Ee2rg==; visid_incap_2407836=UMcukpfDSdufhAREQObFapukp2gAAAAAQUIPAAAAAADWG7izLWqeKUtEwxwzWtKQ; nlbi_2407836=azpfY2O31mhwUcPlOV2ikAAAAAD9Y+oBE9RETXrGgQ0o38/z; incap_ses_416_2407836=bU5vXoyBDgReYUev7u3FBZukp2gAAAAAxsLEj0ApPg7fGrFh/ErTbg==; incap_ses_416_2407823=87mfaVB0gXJmYUev7u3FBZukp2gAAAAAAiKIMhX68BCQYvQJIXGzsw==; ee-minicart-details=eyJiYXNrZXRTdGVwIjoiL2V4cC9iYXNrZXQiLCJudW1CYXNrZXRJdGVtcyI6MH0=; JSESSIONID=Y6-f1afecde-76d9-41d0-a70d-f02c2161affa; incap_ses_9125_2335605=94T8FOWJJAob1prccYOifnhlqGgAAAAA5JKBbCnBLVVdDuY1c9vcnA==; incap_ses_416_2335605=qp+JabfhljaCC+6v7u3FBT5rqGgAAAAAikaG/J2y5MtL9RL0y5RyBg==; incap_ses_1378_2335605=vJAOJ59miHWcKyiABKQfE0RrqGgAAAAAyH8B4nZ4ZNuaS1k6qwomTQ==; AWSELB=2BDB77B51852D32BE6379457F72939339CB3D6001A9FFBC722D0D176EAF7F7A5727E67A8F1246A6890D70DC61D637C8C61DFB7E52731DC0113A5813D6C144933E1EE2BE4A6; incap_ses_455_2335605=M4rFczNNXDfDEfMKZnxQBkRrqGgAAAAAVpzJmy1Fzt/KYW9j7PKiCQ==; incap_ses_1364_2335605=JEXraOaFnVxS/bp6EOftEklrqGgAAAAA2AhsNRa1i1QzVx7FGCtnnQ==; incap_ses_1376_2335605=IzpXaZYb5Vdu+8yPCYkYE0prqGgAAAAA2746R8Pw3zNjjXEnl4Lq8w==; affinity="08a2a2b772930702"; nlbi_2335605=0HlEQiri63eFvHjw4NzfzwAAAABVFy2BuDyxBIcNNisZluEZ; incap_ses_1371_2335605=o0VwRyLFGxDEH7r3hsUGE3VrqGgAAAAAE1JbO+lpSJTJHAQFMC0Nhw==; visid_incap_2335605=i731d5CHTRSHu7nre7tmLHZrqGgAAAAAQUIPAAAAAAC17sRNgVHWtP3KBdG3ZVbJ; incap_ses_1380_2335605=xq6AdN+41CCeT8XBAb8mE3ZrqGgAAAAASOAhLFkuFZ456Za/J7a3uA==; OptanonConsent=isGpcEnabled=0&datestamp=Fri+Aug+22+2025+16%3A06%3A11+GMT%2B0300+(Eastern+European+Summer+Time)&version=202501.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=a8532de6-965b-41a8-95b2-5d48d5d29939&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=0%3A1%2C1%3A1%2C2%3A1%2C3%3A1%2C5%3A1%2C6%3A1%2C7%3A1&intType=1&geolocation=EG%3BALX&AwaitingReconsent=false; is-one-page-config=1; mbox=PC#130117c0263e4913adc3fd854dbdea6d.37_0#1819112774|session#4c9e692a388943d78a3a4f35efe6b223#1755869834',
            }
            query_string = urllib.parse.urlencode(params)
            api_url_varites = f"https://ee.co.uk/graphql?{query_string}"

            url_varites = f"https://ee.co.uk/graphql?{baseDeviceSeoId}_{color}_{capacity}'"
            response_varites = await fetch_url(url_varites, content_type="product", params=params, headers=headers)

            if response_varites:

                data_varites_json = json.loads(response_varites)

                data_product = data_varites_json["data"]["deviceBundle"]["product"]
                try:
                    first_combo = data_varites_json["data"]["deviceBundle"]["productPlanCombinations"][0]
                    handsetOnlyCostCash = first_combo["productPrice"]["payTodayPrice"]
                    maxLoanUpfrontCostPercentage = data_varites_json["data"]["guidedSellingConfig"]["maxLoanUpfrontCostPercentage"]
                    maxUpfront = int(handsetOnlyCostCash * (maxLoanUpfrontCostPercentage / 100))
                    minLoanUpfrontCostPercentage = first_combo["productPrice"].get("minimumPayTodayPrice", 0.0)

                    phoneContractDurations = first_combo["productPrice"].get("availableSubscriptionTermsInMonths", [1])
                except:
                    handsetOnlyCostCash = 0
                    phoneContractDurations = [1]
                    minLoanUpfrontCostPercentage = 0.0
                    maxUpfront = 0.0

                row["source"] = "EE"
                row["date"] = datetime.now().strftime("%Y-%m-%d")
                row["apiURL"] = api_url_varites
                row["url"] = url
                row["sku"] = data_product["code"]
                row["name"] = data_product["name"]
                row["brand"] = data_product["manufacturer"]
                row["stock"] = "Y" if data_product["stock"]["message"] == "In stock" else "N"
                if row["stock"] == "Y":
                    row["desc"] = data_product["baseDeviceShortDescription"]
                    row["shortDesc"] = data_product["seoTitle"]
                    row["videoURL"] = ""
                    row["lowestPriceValue"] = ""
                    row["reviewRating"] = ""
                    row["reviewCount"] = ""
                    row["onSale"] = ""
                    row["colour"] = color
                    row["size"] = capacity
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
                    images = []
                    for img in data_product["images"]:
                        for fmt in img["formats"]:
                            if fmt["mimeType"] == "image/webp":
                                images.append(fmt["url"])
                                if len(images) == 5:
                                    break
                        if len(images) == 5:
                            break
                    for i in range(5):
                        row[f"image{i + 1}"] = images[i] if i < len(images) else ""
                    row["saleText"] = ""
                    row["handsetOnlyCostCash"] = handsetOnlyCostCash if handsetOnlyCostCash else ""
                    row["previousPrice"] = ""

                    for term in phoneContractDurations:
                        if data_varites_json["data"]["deviceBundle"].get("productPlanCombinations", []):
                            upfronts = generate_steps(minLoanUpfrontCostPercentage, maxUpfront)
                            if not upfronts:
                                upfronts = [0]

                            for advance in upfronts:

                                for combo in data_varites_json["data"]["deviceBundle"].get("productPlanCombinations", []):
                                    plan = combo.get("plan")
                                    row["phoneContractDuration"] = "" if term == 1 else term
                                    row["handsetOnlyCostCash"] = handsetOnlyCostCash
                                    row["advance"] = advance
                                    paymentAmount = round((handsetOnlyCostCash-advance) / term, 2)
                                    row["phoneContractPrice"] = handsetOnlyCostCash
                                    row["paymentAmount"] = paymentAmount

                                    plan_type = "contract"
                                    sim_data = ""
                                    for ent in plan.get("entitlements", []):
                                        if ent.get("code") == "ME_DATA_MB":
                                            qty = ent.get("quantity", 0)
                                            if qty == -1:
                                                sim_data = "Unlimited"
                                            else:
                                                sim_data = f"{int(qty / 1000)}GB"
                                    sim_price = plan.get("price", {}).get("payMonthlyPrice")
                                    simOfferData = "" if  plan.get("price", {}).get("wasPayMonthlyPrice") == 0.0 else plan.get("price", {}).get("wasPayMonthlyPrice")
                                    simContractname = plan.get("name")
                                    simContractDuration = plan.get("price", {}).get("subscriptionTermInMonths", 0)
                                    isPhoneContractAvailableWOsim = "N"
                                    phoneContractPrice = 0
                                    phoneContractSimPackage =  phoneContractPrice +  (simContractDuration * sim_price)
                                    handsetOnlyContract =  ""
                                    future_prices = plan.get("price", {}).get("futurePrices", [])

                                    sim1YearIncrease = ""
                                    sim2YearIncrease = ""
                                    sim3YearIncrease = ""

                                    if len(future_prices) > 0:
                                        sim1YearIncrease = future_prices[0].get("price", "")
                                    if len(future_prices) > 1:
                                        sim2YearIncrease = future_prices[1].get("price", "")
                                    if len(future_prices) > 2:
                                        sim3YearIncrease = future_prices[2].get("price", "")


                                    plan_family = plan.get("planFamily", {})
                                    features = [f.get("name", "") for f in plan_family.get("features", [])]
                                    special = [s.get("name", "") for s in plan_family.get("specialFeatures", [])]

                                    simDesc = plan.get("summary", "")
                                    if features:
                                        simDesc += " | Features: " + "".join(features)
                                    if special:
                                        simDesc += " | Special: " + "".join(special)


                                    row["plan_type"] = plan_type
                                    row["sim_data"] = sim_data
                                    row["sim_price"] = sim_price
                                    row["simOfferData"] = simOfferData
                                    row["simContractname"] = simContractname
                                    row["simContractDuration"] = simContractDuration
                                    row["isPhoneContractAvailableWOsim"] = isPhoneContractAvailableWOsim
                                    row["phoneContractSimPackage"] = phoneContractSimPackage
                                    row["handsetOnlyContract"] = handsetOnlyContract
                                    row["sim1YearIncrease"] = sim1YearIncrease
                                    row["sim2YearIncrease"] = sim2YearIncrease
                                    row["sim3YearIncrease"] = sim3YearIncrease
                                    row["simDesc"] = simDesc

                                    attribute_index = 1

                                    for feature in data_product.get("features", []):
                                        title = feature.get("assistiveText", "").strip()
                                        description = feature.get("name", "").strip()

                                        row[f"attributeType{attribute_index}"] = "SPECIFICATION"
                                        row[f"attributeTitle{attribute_index}"] = title
                                        row[f"attributeValue{attribute_index}"] = description.upper()
                                        attribute_index += 1
                                    append_to_csv(row, "products.csv")

                        else:

                            row["phoneContractDuration"] = ""
                            row["handsetOnlyCostCash"] = handsetOnlyCostCash if handsetOnlyCostCash else ""
                            row["advance"] = 0.0
                            row["phoneContractPrice"] = 0.0
                            row["paymentAmount"] = 0.0
                            row["plan_type"] = ""
                            row["sim_data"] = ""
                            row["sim_price"] = ""
                            row["simOfferData"] = ""
                            row["simContractname"] = ""
                            row["simContractDuration"] = 0
                            row["isPhoneContractAvailableWOsim"] = "N"
                            row["phoneContractSimPackage"] = 0
                            row["handsetOnlyContract"] = ""
                            row["sim1YearIncrease"] = ""
                            row["sim2YearIncrease"] = ""
                            row["sim3YearIncrease"] = ""
                            row["simDesc"] = ""
                            attribute_index = 1
                            for feature in data_product.get("features", []):
                                title = feature.get("assistiveText", "").strip()
                                description = feature.get("name", "").strip()

                                row[f"attributeType{attribute_index}"] = "SPECIFICATION"
                                row[f"attributeTitle{attribute_index}"] = title
                                row[f"attributeValue{attribute_index}"] = description.upper()
                                attribute_index += 1


                            append_to_csv(row, "products.csv")






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
    siteurl = "https://ee.co.uk/sitemap-shop-hybris.xml"
    products  = await fetch_sitemap(siteurl)
    data = await extact_data_from_product_url(products)


if __name__ == "__main__":
    asyncio.run(main())
