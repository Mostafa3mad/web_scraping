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


async def fetch_contract_data(url_product):
    baseProductSeoId = url_product.split("/")[-1].replace("-details", "")
    bundleSeoId = url_product.split("/")[-2].replace("-gallery", "")
    # print(bundleSeoId)
    # print(baseProductSeoId)

    url = f"https://ee.co.uk/graphql"

    payload = {
        "operationName": "FlexPayProductDetailsQuery",
        "variables": {
            "deviceBundleBySeoInput": {
                "bundleSeoId": bundleSeoId,
                "baseProductSeoId": baseProductSeoId,

            }
        },
        "extensions": {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "74a0a54b03ccd740c5a3e7f5767146b2a10a7f45e5874074328b1b63fb61f59e"
            }
        }
    }


    response = await fetch_url(f"{url}?{bundleSeoId}_{baseProductSeoId}", json_data=payload,method="POST",content_type="product")

    data_json = json.loads(response)

    if "errors" in data_json:
        logger.error(f"Error in response:{url_product}")
        # print("Error in response:", data_json["errors"])
        return

    # print(data_json)
    try:
        deviceBundleVariants = data_json["data"]["deviceBundle"]["deviceBundleVariants"]
    except:
        deviceBundleVariants = []

    if not deviceBundleVariants :
        deviceBundleVariants = [0]

    for variant in deviceBundleVariants:


        if deviceBundleVariants == [0]:
            variant = data_json["data"]["deviceBundle"]


        row = {}

        dimensions = variant["product"]["dimensions"]
        dims = [{"key": d["key"], "value": d["value"]} for d in dimensions]
        # print("Variant Dimensions:", dims)
        variant_name = "_".join(d["value"] for d in dims)
        # print(variant_name)
        params = {
            "operationName": "FlexPayProductDetailsQuery",
            "variables": json.dumps({
                "deviceBundleBySeoInput": {
                    "bundleSeoId": bundleSeoId,
                    "baseProductSeoId": baseProductSeoId,
                    "dimensions": dims
                }
            }),
            "extensions": json.dumps({
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": "74a0a54b03ccd740c5a3e7f5767146b2a10a7f45e5874074328b1b63fb61f59e"
                }
            })
        }

        headers = {

            'cookie': 'isAcquisitionFlexpay=true ; abFeatureTesting.acquisitionSubsidySemiHeadless=true;OptanonConsent=isGpcEnabled=0&datestamp=Fri+Aug+22+2025+14%3A52%3A54+GMT%2B0300+(Eastern+European+Summer+Time)&version=202501.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=4da3c9d0-fd7a-42d0-8d78-4bf731ad8a6f&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=0%3A1%2C1%3A0%2C2%3A0%2C3%3A0%2C5%3A0%2C6%3A0%2C7%3A0&intType=2&geolocation=EG%3BALX&AwaitingReconsent=false',
        }


        try:
            if deviceBundleVariants == [0]:
                raise Exception("No variants")

            response = await fetch_url(f"{url}?{bundleSeoId}_{baseProductSeoId}_{variant_name}", params=params,headers=headers,content_type="product")
            data_varites_json = json.loads(response)
            # print("data_varites_json", data_varites_json)
            data_product = data_varites_json["data"]["deviceBundle"]["product"]



        except:
            data_product = variant["product"]
            data_varites_json = data_json



        query_string = urllib.parse.urlencode(params)
        api_url_varites = f"https://ee.co.uk/graphql?{query_string}"

        #################################################################
        row["source"] = "EE"
        row["date"] = datetime.now().strftime("%Y-%m-%d")
        row["apiURL"] = api_url_varites
        row["url"] = url_product
        row["sku"] = data_product["code"]
        row["name"] = data_product["name"]
        row["brand"] = data_product["manufacturer"]
        row["stock"] = "Y" if data_product["stock"]["message"] == "In stock" else "N"


        row["desc"] = data_product["baseDeviceShortDescription"]
        row["shortDesc"] = data_product["seoTitle"]
        row["videoURL"] = ""
        row["lowestPriceValue"] = ""
        row["reviewRating"] = ""
        row["reviewCount"] = ""
        row["onSale"] = ""
        for dimension in dims:
            key = dimension["key"].lower()
            value = dimension["value"]
            if key == "color":
                row["colour"] = value
            elif any(word in key for word in ("capacity", "storage", "size")):
                row["size"] = value
        row["UPC"] = ""
        row["EAN"] = ""
        row["cat"] = url_product.split("/")[3]
        row["subcat1"] = url_product.split("/")[4]
        row["subcat2"] = url_product.split("/")[5]
        row["subcat3"] = url_product.split("/")[6] if len(url_product.split("/")) > 6 else ""
        row["subcat4"] = url_product.split("/")[7] if len(url_product.split("/")) > 7 else ""
        row["subcat5"] = url_product.split("/")[8] if len(url_product.split("/")) > 8 else ""
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

        try:
            first_combo = data_varites_json["data"]["deviceBundle"]["productPlanCombinations"][0]
            if first_combo:
                handsetOnlyCostCash = first_combo["productPrice"]["payTodayPrice"]
                maxLoanUpfrontCostPercentage = data_varites_json["data"]["guidedSellingConfig"][
                    "maxLoanUpfrontCostPercentage"]
                max_upfront = int(handsetOnlyCostCash * (maxLoanUpfrontCostPercentage / 100))
                min_upfront = int(first_combo["productPrice"].get("minimumPayTodayPrice", 0.0))
                phoneContractDurations = first_combo["productPrice"].get("availableSubscriptionTermsInMonths", [1])
                step = 100
        except:
            logger.error(f"NO plan and old product {url_product}")
            continue


        # print("handsetOnlyCostCash",handsetOnlyCostCash)
        # print("maxLoanUpfrontCostPercentage",maxLoanUpfrontCostPercentage)
        # print("max_upfront",max_upfront)
        # print("min_upfront",min_upfront)
        # print("phoneContractDurations",phoneContractDurations)

        values = list(range(min_upfront, max_upfront, step))
        values.append(max_upfront)
        # print(values)

        row["handsetOnlyCostCash"] = handsetOnlyCostCash if handsetOnlyCostCash else ""
        row["previousPrice"] = ""

        for term in phoneContractDurations:
            if data_varites_json["data"]["deviceBundle"].get("productPlanCombinations", []):
                upfronts = generate_steps(min_upfront, max_upfront)
                if not upfronts:
                    upfronts = [0]

                for advance in upfronts:

                    for combo in data_varites_json["data"]["deviceBundle"].get("productPlanCombinations", []):
                        plan = combo.get("plan")
                        row["phoneContractDuration"] = "" if term == 1 else term
                        row["handsetOnlyCostCash"] = handsetOnlyCostCash
                        row["advance"] = advance
                        paymentAmount = round((handsetOnlyCostCash - advance) / term, 2)
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
                        simOfferData = "" if plan.get("price", {}).get("wasPayMonthlyPrice") == 0.0 else plan.get(
                            "price", {}).get("wasPayMonthlyPrice")
                        simContractname = plan.get("name")
                        simContractDuration = plan.get("price", {}).get("subscriptionTermInMonths", 0)
                        isPhoneContractAvailableWOsim = "N"
                        phoneContractPrice = 0
                        phoneContractSimPackage = phoneContractPrice + (simContractDuration * sim_price)
                        handsetOnlyContract = ""
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


                        # print(row)
                        append_to_csv(row, "products.csv")

async def fetch_product_only(url_product: str):
    productSeoId = url_product.split("/")[-1]

    URL_api = "https://api.ee.co.uk/v1/ee-sales-services/graphql"
    json_data = {
        'operationName': 'MarketplaceDeviceQuery',
        'variables': {
            'includeFinanceConfiguration': True,
            'productBySeoInput': {
                'productSeoId': productSeoId,

            },
        },
        'query': 'query MarketplaceDeviceQuery($productBySeoInput: ProductBySeoInput!, $includeFinanceConfiguration: Boolean = true) {\n  getSimpleProductOffering(productBySeoInput: $productBySeoInput) {\n    __typename\n    product {\n      __typename\n      code\n      name\n      seoData {\n        __typename\n        metaTitle\n        metaDescription\n        canonicalUrl\n        includedInIndex\n        includedInSitemap\n      }\n      ... on SubscriptionProduct {\n        baseSubscriptionName\n        baseSubscriptionSeoId\n        baseSubscriptionType\n        description\n        provider\n        images {\n          __typename\n          formats {\n            url\n            format\n            __typename\n          }\n          altText\n        }\n        features {\n          __typename\n          name\n        }\n        specifications {\n          __typename\n          name\n          icon\n          values {\n            __typename\n            name\n            unit\n            values\n          }\n        }\n        __typename\n      }\n      ... on MarketplaceDevice {\n        promotedPaymentOption\n        sapArticleCode\n        typeOfProduct\n        eligibleForFinance\n        getDeliveryOptions {\n          deliveryType\n          excludedDateIncludingBankHolidays\n          excludedDays\n          courierName\n          noExtraCost\n          cutOffDateAndTime\n          payToday\n          __typename\n        }\n        managedByHybris\n        eligibleForATP\n        excessFee {\n          __typename\n          name\n          value\n        }\n        images {\n          __typename\n          formats {\n            url\n            format\n            __typename\n          }\n          altText\n        }\n        dimensions {\n          __typename\n          key\n          value\n          text\n        }\n        baseDeviceSeoId\n        keywords\n        baseDeviceName\n        baseDeviceShortDescription\n        descriptions\n        manufacturer\n        categoryPath {\n          __typename\n          name\n          alias\n        }\n        features {\n          __typename\n          name\n        }\n        specifications {\n          __typename\n          name\n          icon\n          values {\n            __typename\n            name\n            unit\n            values\n          }\n        }\n        ean\n        financeConfiguration @include(if: $includeFinanceConfiguration) {\n          financeType\n          promoAprCode\n          financeOption {\n            overrideMinBasketValue\n            promoCode\n            apr {\n              term\n              value\n              __typename\n            }\n            __typename\n          }\n          __typename\n        }\n        stockStatus\n        stock {\n          __typename\n          message\n          description\n          status\n          availableQuantity\n          preOrderLaunchDate\n          preOrderStartDate\n        }\n        manufactureName\n        manufacturePartNumber\n        productOrigin\n        __typename\n      }\n    }\n    prices {\n      __typename\n      priceId\n      payTodayPrice\n      wasPayTodayPrice\n      payMonthlyPrice\n      wasPayMonthlyPrice\n      totalPriceForSubscriptionPeriod\n      subscriptionTermInMonths\n      billingPolicy {\n        __typename\n        subscriptionTerm\n        recurringPrice\n        wasRecurringPrice\n        billingFrequency\n        trial {\n          duration\n          term\n          __typename\n        }\n      }\n      discount {\n        __typename\n        message\n        discountType\n      }\n      discounts {\n        __typename\n        message\n        payToday\n        discountType\n        validUntil\n        title\n        notificationType\n        proposalCodes\n      }\n      totalDiscountAmount\n      ... on LoanPrice {\n        __typename\n        interestRate\n        fullCost\n        requiredBasketTotal\n      }\n    }\n    bundledProducts {\n      product {\n        __typename\n        ... on SubscriptionProduct {\n          name\n          code\n          images {\n            __typename\n            formats {\n              url\n              format\n              __typename\n            }\n            altText\n          }\n          specifications {\n            __typename\n            name\n            icon\n            values {\n              __typename\n              name\n              unit\n              values\n            }\n          }\n          __typename\n        }\n        ... on MarketplaceDevice {\n          name\n          code\n          images {\n            __typename\n            formats {\n              url\n              format\n              __typename\n            }\n            altText\n          }\n          specifications {\n            __typename\n            name\n            icon\n            values {\n              __typename\n              name\n              unit\n              values\n            }\n          }\n          financeConfiguration @include(if: $includeFinanceConfiguration) {\n            financeType\n            promoAprCode\n            financeOption {\n              overrideMinBasketValue\n              promoCode\n              apr {\n                term\n                value\n                __typename\n              }\n              __typename\n            }\n            __typename\n          }\n          __typename\n        }\n      }\n      prices {\n        __typename\n        priceId\n        payTodayPrice\n        wasPayTodayPrice\n        payMonthlyPrice\n        wasPayMonthlyPrice\n        totalPriceForSubscriptionPeriod\n        subscriptionTermInMonths\n        billingPolicy {\n          __typename\n          subscriptionTerm\n          recurringPrice\n          wasRecurringPrice\n          billingFrequency\n          trial {\n            duration\n            term\n            __typename\n          }\n        }\n        discount {\n          __typename\n          message\n          discountType\n        }\n        discounts {\n          __typename\n          message\n          payToday\n          discountType\n          validUntil\n          title\n          notificationType\n          proposalCodes\n          durationMonths\n        }\n        totalDiscountAmount\n        ... on LoanPrice {\n          __typename\n          interestRate\n          fullCost\n          requiredBasketTotal\n        }\n      }\n      __typename\n    }\n    productOfferingVariants {\n      product {\n        __typename\n        ... on SubscriptionProduct {\n          name\n          code\n          baseSubscriptionType\n          labels {\n            type\n            text\n            __typename\n          }\n          features {\n            name\n            __typename\n          }\n          __typename\n        }\n        ... on MarketplaceDevice {\n          name\n          code\n          baseDeviceSeoId\n          getDeliveryOptions {\n            deliveryType\n            excludedDateIncludingBankHolidays\n            excludedDays\n            courierName\n            noExtraCost\n            cutOffDateAndTime\n            payToday\n            __typename\n          }\n          stock {\n            __typename\n            message\n            description\n            status\n            availableQuantity\n            preOrderLaunchDate\n            preOrderStartDate\n          }\n          ean\n          excessFee {\n            __typename\n            name\n            value\n          }\n          images {\n            __typename\n            formats {\n              url\n              format\n              __typename\n            }\n            altText\n          }\n          dimensions {\n            __typename\n            key\n            value\n            text\n          }\n          __typename\n        }\n      }\n      prices {\n        __typename\n        priceId\n        payTodayPrice\n        payMonthlyPrice\n        wasPayTodayPrice\n        wasPayMonthlyPrice\n        billingPolicy {\n          recurringPrice\n          wasRecurringPrice\n          billingFrequency\n          trial {\n            duration\n            term\n            __typename\n          }\n          __typename\n        }\n        discounts {\n          __typename\n          message\n          payToday\n          discountType\n          validUntil\n          title\n          notificationType\n        }\n      }\n      __typename\n    }\n  }\n}\n',
    }
    headers = {
        'apigw-client-id': 'ef341323-a723-4203-bdfa-2a9d45cda8b3',
        'apigw-tracking-header': '6d886c39-e484-493f-a58f-4e1df87eb67f',
    }

    response = await fetch_url(f"{URL_api}?{productSeoId}", json_data=json_data, method="POST", headers=headers,content_type="product")

    data_json = json.loads(response)
    # print(data_json)

    params = {
        "parent_product_sku": f"{productSeoId.upper()}",
        "origin": "ee.co.uk",
        "merchant_identifier": "ee-marketplace",
        "since_period": "ALL",
        "sort": "-updated_date",
        "feefo_parameters": "include",
        "media": "include",
        "translate_attributes": "exclude",
        "reviews_with_content_count": "include"
    }
    url_reviews = "https://widgets.reevoo.com/api-feefo/api/10/reviews/summary/product"

    response = await fetch_url(f"{url_reviews}?{productSeoId}_reviews", params=params,headers=headers, content_type="product")
    data_review = json.loads(response)
    reviewCount = data_review.get("rating", {}).get("product",{}).get("count", 0)
    reviewRating = data_review.get("rating", {}).get("rating",0.0)
    # print(reviewCount, reviewRating)


    deviceBundleVariants = data_json["data"]["getSimpleProductOffering"]["productOfferingVariants"]
    if not deviceBundleVariants :
        deviceBundleVariants = [0]

    for variant in deviceBundleVariants:

        if deviceBundleVariants == [0]:
            variant = data_json["data"]["getSimpleProductOffering"]

        dimensions = variant["product"]["dimensions"]
        dims = [{"key": d["key"], "value": d["value"]} for d in dimensions]
        dims_texts = [{"key": d["key"], "value": d["value"],"text" : d["text"]} for d in dimensions]

        # print("Variant Dimensions:", dims)
        variant_name = "_".join(d["value"] for d in dims)
        # print(variant_name)
        json_data = {
            'operationName': 'MarketplaceDeviceQuery',
            'variables': {
                'includeFinanceConfiguration': True,
                'productBySeoInput': {
                    'productSeoId': productSeoId,
                    "dimensions": dims

                },
            },
            'query': 'query MarketplaceDeviceQuery($productBySeoInput: ProductBySeoInput!, $includeFinanceConfiguration: Boolean = true) {\n  getSimpleProductOffering(productBySeoInput: $productBySeoInput) {\n    __typename\n    product {\n      __typename\n      code\n      name\n      seoData {\n        __typename\n        metaTitle\n        metaDescription\n        canonicalUrl\n        includedInIndex\n        includedInSitemap\n      }\n      ... on SubscriptionProduct {\n        baseSubscriptionName\n        baseSubscriptionSeoId\n        baseSubscriptionType\n        description\n        provider\n        images {\n          __typename\n          formats {\n            url\n            format\n            __typename\n          }\n          altText\n        }\n        features {\n          __typename\n          name\n        }\n        specifications {\n          __typename\n          name\n          icon\n          values {\n            __typename\n            name\n            unit\n            values\n          }\n        }\n        __typename\n      }\n      ... on MarketplaceDevice {\n        promotedPaymentOption\n        sapArticleCode\n        typeOfProduct\n        eligibleForFinance\n        getDeliveryOptions {\n          deliveryType\n          excludedDateIncludingBankHolidays\n          excludedDays\n          courierName\n          noExtraCost\n          cutOffDateAndTime\n          payToday\n          __typename\n        }\n        managedByHybris\n        eligibleForATP\n        excessFee {\n          __typename\n          name\n          value\n        }\n        images {\n          __typename\n          formats {\n            url\n            format\n            __typename\n          }\n          altText\n        }\n        dimensions {\n          __typename\n          key\n          value\n          text\n        }\n        baseDeviceSeoId\n        keywords\n        baseDeviceName\n        baseDeviceShortDescription\n        descriptions\n        manufacturer\n        categoryPath {\n          __typename\n          name\n          alias\n        }\n        features {\n          __typename\n          name\n        }\n        specifications {\n          __typename\n          name\n          icon\n          values {\n            __typename\n            name\n            unit\n            values\n          }\n        }\n        ean\n        financeConfiguration @include(if: $includeFinanceConfiguration) {\n          financeType\n          promoAprCode\n          financeOption {\n            overrideMinBasketValue\n            promoCode\n            apr {\n              term\n              value\n              __typename\n            }\n            __typename\n          }\n          __typename\n        }\n        stockStatus\n        stock {\n          __typename\n          message\n          description\n          status\n          availableQuantity\n          preOrderLaunchDate\n          preOrderStartDate\n        }\n        manufactureName\n        manufacturePartNumber\n        productOrigin\n        __typename\n      }\n    }\n    prices {\n      __typename\n      priceId\n      payTodayPrice\n      wasPayTodayPrice\n      payMonthlyPrice\n      wasPayMonthlyPrice\n      totalPriceForSubscriptionPeriod\n      subscriptionTermInMonths\n      billingPolicy {\n        __typename\n        subscriptionTerm\n        recurringPrice\n        wasRecurringPrice\n        billingFrequency\n        trial {\n          duration\n          term\n          __typename\n        }\n      }\n      discount {\n        __typename\n        message\n        discountType\n      }\n      discounts {\n        __typename\n        message\n        payToday\n        discountType\n        validUntil\n        title\n        notificationType\n        proposalCodes\n      }\n      totalDiscountAmount\n      ... on LoanPrice {\n        __typename\n        interestRate\n        fullCost\n        requiredBasketTotal\n      }\n    }\n    bundledProducts {\n      product {\n        __typename\n        ... on SubscriptionProduct {\n          name\n          code\n          images {\n            __typename\n            formats {\n              url\n              format\n              __typename\n            }\n            altText\n          }\n          specifications {\n            __typename\n            name\n            icon\n            values {\n              __typename\n              name\n              unit\n              values\n            }\n          }\n          __typename\n        }\n        ... on MarketplaceDevice {\n          name\n          code\n          images {\n            __typename\n            formats {\n              url\n              format\n              __typename\n            }\n            altText\n          }\n          specifications {\n            __typename\n            name\n            icon\n            values {\n              __typename\n              name\n              unit\n              values\n            }\n          }\n          financeConfiguration @include(if: $includeFinanceConfiguration) {\n            financeType\n            promoAprCode\n            financeOption {\n              overrideMinBasketValue\n              promoCode\n              apr {\n                term\n                value\n                __typename\n              }\n              __typename\n            }\n            __typename\n          }\n          __typename\n        }\n      }\n      prices {\n        __typename\n        priceId\n        payTodayPrice\n        wasPayTodayPrice\n        payMonthlyPrice\n        wasPayMonthlyPrice\n        totalPriceForSubscriptionPeriod\n        subscriptionTermInMonths\n        billingPolicy {\n          __typename\n          subscriptionTerm\n          recurringPrice\n          wasRecurringPrice\n          billingFrequency\n          trial {\n            duration\n            term\n            __typename\n          }\n        }\n        discount {\n          __typename\n          message\n          discountType\n        }\n        discounts {\n          __typename\n          message\n          payToday\n          discountType\n          validUntil\n          title\n          notificationType\n          proposalCodes\n          durationMonths\n        }\n        totalDiscountAmount\n        ... on LoanPrice {\n          __typename\n          interestRate\n          fullCost\n          requiredBasketTotal\n        }\n      }\n      __typename\n    }\n    productOfferingVariants {\n      product {\n        __typename\n        ... on SubscriptionProduct {\n          name\n          code\n          baseSubscriptionType\n          labels {\n            type\n            text\n            __typename\n          }\n          features {\n            name\n            __typename\n          }\n          __typename\n        }\n        ... on MarketplaceDevice {\n          name\n          code\n          baseDeviceSeoId\n          getDeliveryOptions {\n            deliveryType\n            excludedDateIncludingBankHolidays\n            excludedDays\n            courierName\n            noExtraCost\n            cutOffDateAndTime\n            payToday\n            __typename\n          }\n          stock {\n            __typename\n            message\n            description\n            status\n            availableQuantity\n            preOrderLaunchDate\n            preOrderStartDate\n          }\n          ean\n          excessFee {\n            __typename\n            name\n            value\n          }\n          images {\n            __typename\n            formats {\n              url\n              format\n              __typename\n            }\n            altText\n          }\n          dimensions {\n            __typename\n            key\n            value\n            text\n          }\n          __typename\n        }\n      }\n      prices {\n        __typename\n        priceId\n        payTodayPrice\n        payMonthlyPrice\n        wasPayTodayPrice\n        wasPayMonthlyPrice\n        billingPolicy {\n          recurringPrice\n          wasRecurringPrice\n          billingFrequency\n          trial {\n            duration\n            term\n            __typename\n          }\n          __typename\n        }\n        discounts {\n          __typename\n          message\n          payToday\n          discountType\n          validUntil\n          title\n          notificationType\n        }\n      }\n      __typename\n    }\n  }\n}\n',
        }
        try:
            if deviceBundleVariants == [0]:
                raise Exception("No variants")
            response = await fetch_url(f"{URL_api}?{productSeoId}_{variant_name}", json_data=json_data, method="POST", headers=headers,content_type="product")
            data_varites_json = json.loads(response)
            data_product = data_varites_json["data"]["getSimpleProductOffering"]["product"]

        except:
            data_product = variant["product"]
            data_varites_json = data_json
            # print("data_varites_json" , data_varites_json)

        # print("data_varites_json",data_varites_json)
        row = {}
        row["source"] = "EE"
        row["date"] = datetime.now().strftime("%Y-%m-%d")
        row["apiURL"] = f"{URL_api}?{productSeoId}_{variant_name}"
        row["url"] = url_product
        row["sku"] = data_product["code"]
        row["name"] = data_product["name"]
        row["brand"] = data_product["manufacturer"]
        row["stock"] = "Y" if data_product["stock"]["message"] == "In stock" else "N"
        descriptions = data_product.get("descriptions", [])
        desc_list = []
        for description in descriptions:
            desc_list.append(description.strip())

        row["desc"] = " ".join(desc_list)
        row["shortDesc"] = data_product["baseDeviceShortDescription"]
        row["videoURL"] = ""

        prices = data_varites_json.get("data", {}).get("getSimpleProductOffering", {}).get("prices", [])

        discounts = ""
        typename = prices[0].get("__typename","")
        # print(typename)
        if prices and prices[0].get("discounts"):
            discounts = prices[0]["discounts"][0].get("payToday")

        if discounts:
            if typename == "AtpPrice":
                previousPrice = prices[0].get("totalPriceForSubscriptionPeriod", 0.0)
                payTodayPrice = round(previousPrice - discounts,2)
                billingPolicy = prices[0].get("billingPolicy", {})
                subscriptionTerm = billingPolicy.get("subscriptionTerm", 1)
                advance = prices[0].get("payTodayPrice", 0)
                paymentAmount = billingPolicy.get("recurringPrice", 0)
                row["advance"] = advance
                row["phoneContractDuration"] = subscriptionTerm
                row["handsetOnlyCostCash"] = ""
                row["phoneContractPrice"] = payTodayPrice
                row["paymentAmount"] = paymentAmount

            if typename == "SubsidyPrice":
                previousPrice = prices[0].get("payTodayPrice", 0.0)
                payTodayPrice = round(previousPrice - discounts,2)

            row["onSale"] = "Y"
            row["saleText"] = f"Saving £{discounts}"
            row["isPromotion"] = "Y"
            row["lowestPriceValue"] = payTodayPrice
            row["lowestPriceText"] = f"£{payTodayPrice}"
            row["previousPrice"] = previousPrice
        else:
            if typename == "AtpPrice":

                if typename == "AtpPrice":
                    payTodayPrice = prices[0].get("totalPriceForSubscriptionPeriod", 0.0)
                    billingPolicy = prices[0].get("billingPolicy", {})
                    subscriptionTerm = billingPolicy.get("subscriptionTerm", 1)
                    advance = prices[0].get("payTodayPrice", 0)
                    paymentAmount = billingPolicy.get("recurringPrice", 0)
                    row["advance"] = advance
                    row["phoneContractDuration"] = subscriptionTerm
                    row["handsetOnlyCostCash"] = ""
                    row["phoneContractPrice"] = payTodayPrice
                    row["paymentAmount"] = paymentAmount
                    row["isPhoneContractAvailableWOsim"] = "N"


            if typename == "SubsidyPrice":
                payTodayPrice = prices[0].get("payTodayPrice", 0.0)

            row["onSale"] = ""
            row["saleText"] = ""
            row["isPromotion"] = ""
            row["lowestPriceValue"] = payTodayPrice
            row["lowestPriceText"] = f"£{payTodayPrice}"
            row["previousPrice"] = ""


        for dimension in dims_texts:
            key = dimension["key"].lower()
            value = dimension["text"]
            if key == "color":
                row["colour"] = value
            elif any(word in key for word in ("capacity", "storage", "size")):
                row["size"] = value
        row["UPC"] = ""
        row["EAN"] = ""
        categories = data_product.get("categoryPath", [])

        row["cat"] = categories[0]["alias"] if len(categories) > 0 else ""
        row["subcat1"] = categories[1]["alias"] if len(categories) > 1 else ""
        row["subcat2"] = categories[2]["alias"] if len(categories) > 2 else ""
        row["subcat3"] = categories[3]["alias"] if len(categories) > 3 else ""
        row["subcat4"] = categories[4]["alias"] if len(categories) > 4 else ""
        row["subcat5"] = categories[5]["alias"] if len(categories) > 5 else ""
        row["warranty"] = ""
        row["isSellingFast"] = ""
        row["isRestockingSoon"] = ""
        row["isOutletPrice"] = ""
        row["reviewRating"] = reviewRating
        row["reviewCount"] = reviewRating
        images = []
        for img in data_product["images"]:
            for fmt in img["formats"]:
                if fmt["__typename"] == "ImageFormat":
                    images.append(fmt["url"])
                    if len(images) == 5:
                        break
            if len(images) == 5:
                break
        for i in range(5):
            row[f"image{i + 1}"] = images[i] if i < len(images) else ""


        # print(row)
        append_to_csv(row, "products.csv")

        is_finace = data_product.get("eligibleForFinance", False)

        # print(is_finace)
        if typename == "SubsidyPrice" and is_finace:
            overrideMinBasketValue = data_product.get("financeConfiguration", {}).get("financeOption", [])[0].get("overrideMinBasketValue", 0)
            # print(overrideMinBasketValue)
            min_upfront = float(payTodayPrice * 10 / 100)
            max_upfront = int(payTodayPrice * 50 / 100)

            # print(min_upfront)
            # print(max_upfront)

            short_term_default = [
                {"term": 3, "apr": 0.0},
                {"term": 6, "apr": 0.0},
                {"term": 9, "apr": 0.0}
            ]
            terms = {}

            finance_config = data_product.get("financeConfiguration", {})
            options = finance_config.get("financeOption", [])
            promoAprCode = finance_config.get("promoAprCode", "")
            # print(options[0])
            for option in options:
                overrideMinBasketValue = option.get("overrideMinBasketValue", 0)
                promoCode = option.get("promoCode", "")
                if (promoAprCode or "") in (promoCode or ""):
                    if payTodayPrice >= overrideMinBasketValue:
                        for apr_item in option.get("apr", []):
                            term = apr_item.get("term")
                            apr = apr_item.get("value")
                            terms[term] = apr
            long_term_list = [{"term": t, "apr": a} for t, a in sorted(terms.items())]
            final_terms = short_term_default.copy()

            existing_terms = {item["term"] for item in final_terms}

            for item in long_term_list:
                if item["term"] not in existing_terms:
                    final_terms.append(item)

            # print(final_terms)
            for term_plan in final_terms:
                if int(term_plan["term"]) > 9:
                    # print(term_plan["term"])




                    upfronts = generate_steps(min_upfront, max_upfront)

                else:
                    upfronts = [0]




                for advance in upfronts:
                    subscriptionTerm = term_plan["term"]
                    apr = term_plan["apr"]
                    monthly_rate = (1 + apr / 100) ** (1 / 12) - 1
                    if apr == 0.0:
                        paymentAmount = round((payTodayPrice - advance) / subscriptionTerm, 2)
                    else:
                        loan = payTodayPrice - advance

                        paymentAmount = round(loan * (monthly_rate * (1 + monthly_rate) ** subscriptionTerm) / ((1 + monthly_rate) ** subscriptionTerm - 1), 2)

                    phoneContractPrice = round(advance + paymentAmount * subscriptionTerm,2)
                    row["advance"] = advance
                    row["paymentAmount"] = paymentAmount
                    row["phoneContractDuration"] = subscriptionTerm
                    row["phoneContractPrice"] = phoneContractPrice
                    row["isPhoneContractAvailableWOsim"] = "N"
                    row["handsetOnlyContract"] = payTodayPrice
                    attribute_index = 1

                    for spec in data_product.get("specifications", []):

                        spec_name = spec.get("name", "").strip()
                        values = spec.get("values", [])

                        for value in values:
                            if attribute_index >= 20:
                                break
                            name = value.get("name", "").strip()
                            value_list = value.get("values", [])

                            joined_values = ", ".join(value_list)

                            row[f"attributeType{attribute_index}"] = "SPECIFICATION"
                            row[f"attributeTitle{attribute_index}"] = f"{name}".strip(" -")
                            row[f"attributeValue{attribute_index}"] = joined_values
                            attribute_index += 1


                    # print(row)
                    append_to_csv(row, "products.csv")




        else:
            # print(row)
            append_to_csv(row, "products.csv")

async def get_plan_sim():


    url_site_plans = ["https://ee.co.uk/mobile/pay-as-you-go-sim-only","https://ee.co.uk/mobile/sim-only-deals"]
    for url_site_plan in url_site_plans:
        bundleSeoId = url_site_plan.split("/")[-1]

        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept": "*/*",
        }
        if bundleSeoId == "sim-only-deals":

            # payload = {"operationName":"SimBundleQuery","query":"query SimBundleQuery($bundleSeoId: String!, $simSeoId: String!) { simBundle(simBundleInput: { bundleSeoId: $bundleSeoId, simSeoId: $simSeoId }) { productPlanCombinations { plan { code name price { payMonthlyPrice futurePayMonthlyPrice subscriptionTermInMonths futurePrices { price appliedDate isTloApplied } } planFamily { title type description features { name richName } } entitlements { name quantity unit } labels { type text ... on PromotionLabel { title subTitle isWide position endDateTime } } } } } }","variables":{"bundleSeoId":"sim-only-deals","simSeoId":"multi-sim-paym"}}
            params = {"operationName": "SimBundleQuery",
                       "query": "query SimBundleQuery($bundleSeoId: String!, $simSeoId: String!) { simBundle(simBundleInput: { bundleSeoId: $bundleSeoId, simSeoId: $simSeoId }) { productPlanCombinations { plan { code name price { payMonthlyPrice futurePayMonthlyPrice subscriptionTermInMonths futurePrices { price appliedDate isTloApplied } } planFamily { title type description features { name richName } } entitlements { name quantity unit } labels { type text ... on PromotionLabel { title subTitle isWide position endDateTime } } } } } }",
                       "variables": json.dumps({"bundleSeoId": "sim-only-deals", "simSeoId": "multi-sim-paym"})}
        elif bundleSeoId == "pay-as-you-go-sim-only":
            # payload = {"operationName":"SimBundleQuery","query":"query SimBundleQuery($bundleSeoId: String!, $simSeoId: String!) { simBundle(simBundleInput: { bundleSeoId: $bundleSeoId, simSeoId: $simSeoId }) { productPlanCombinations { plan { code name price { wasPayTodayPrice payTodayPrice futurePayMonthlyPrice subscriptionTermInMonths futurePrices { price appliedDate isTloApplied } } planFamily { title description features { name richName } } entitlements { name quantity unit } labels { type text ... on PromotionLabel { title subTitle isWide position endDateTime } } } } } }","variables":{"bundleSeoId":"pay-as-you-go-sim-only","simSeoId":"mulit-sim-payg"}}
            params = {"operationName": "SimBundleQuery",
                      "variables": json.dumps({"bundleSeoId": "pay-as-you-go-sim-only", "simSeoId": "mulit-sim-payg"}),
                      "query": "query SimBundleQuery($bundleSeoId: String!, $simSeoId: String!) { simBundle(simBundleInput: { bundleSeoId: $bundleSeoId, simSeoId: $simSeoId }) { productPlanCombinations { plan { code name price { wasPayTodayPrice payTodayPrice futurePayMonthlyPrice subscriptionTermInMonths futurePrices { price appliedDate isTloApplied } } planFamily { title description features { name richName } } entitlements { name quantity unit } labels { type text ... on PromotionLabel { title subTitle isWide position endDateTime } } } } } }"}

        query_string = urllib.parse.urlencode(params)
        base_url = "https://ee.co.uk/graphql"

        api_sim_url = f"{base_url}?{query_string}"
        response = await fetch_url(f"{base_url}?{bundleSeoId}_SimBundleQuery", params=params, headers=headers,content_type="product")
        data_sim_json = json.loads(response)
        if data_sim_json:
            productPlanCombinations = data_sim_json.get("data", {}).get("simBundle", {}).get("productPlanCombinations", [])
            for productPlanCombination in productPlanCombinations:
                data_plan = productPlanCombination.get("plan",{})
                row ={}
                row["source"] = "EE"
                row["date"] = datetime.now().strftime("%Y-%m-%d")
                row["apiURL"] = api_sim_url
                row["url"] = url_site_plan
                row["sku"] = data_plan["code"]
                row["name"] = data_plan["name"]
                row["brand"] = "EE"
                sim_price = data_plan.get("price",{}).get("payMonthlyPrice",0) or data_plan.get("price",{}).get("payTodayPrice",0)
                row["sim_price"] = sim_price

                previousPrice = data_plan.get("price",{}).get("wasPayTodayPrice","")
                if previousPrice:
                    row["previousPrice"] = previousPrice
                    row["onSale"] = "Y"
                    row["saleText"] = f"Save £{previousPrice - sim_price}"

                row["simContractname"] = row["name"]
                simContractDuration = data_plan.get("price",{}).get("subscriptionTermInMonths",1) or 1
                row["simContractDuration"] = simContractDuration
                plan_type  = bundleSeoId
                row["plan_type"] = plan_type
                entitlements = data_plan.get("entitlements",[])

                for entitlement in entitlements:
                    unit = entitlement.get("unit","").lower()
                    if "mb" in unit:
                        sim_data = int(entitlement.get("quantity",0))

                        if isinstance(sim_data, (int, float)):
                            if sim_data < 1:
                                sim_data = "Unlimited"
                            elif sim_data > 1:
                                sim_data = f"{int(sim_data / 1000)} GB"
                        else:
                            sim_data = "Unlimited"

                row["sim_data"] = sim_data
                simOfferData = " | ".join([label.get("title","") for label in data_plan.get("labels",[])])
                row["simOfferData"] = simOfferData
                futurePrices = data_plan.get("price",{}).get("futurePrices", [])
                for i,futurePrice in enumerate(futurePrices):
                    i = i+1
                    row[f"sim{i}YearIncrease"] = futurePrice.get("price",0)
                simDesc = data_plan.get("planFamily",{}).get("features",{})
                simDescText = " | ".join([feature.get("richName") or feature.get("name", "") for feature in simDesc if(feature.get("richName") or feature.get("name"))])
                row["simDesc"] = simDescText


                # print(row)
                append_to_csv(row, "products.csv")

async def fetch_single_product(url_product: str):


    if "pay-monthly" in url_product:
        await fetch_contract_data(url_product)
    if "/products/" in url_product:
        await fetch_product_only(url_product)



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



async def extract_site_maps(siteurls):
    products = []

    for siteurl in siteurls:
        product  = await fetch_sitemap(siteurl)

        products.extend(product)

    return products

async def main():
    create_csv_file("products.csv")

    #get_palns_only_and_save_csv
    await get_plan_sim()



    siteurls = [
        "https://ee.co.uk/sitemap-shop-hybris.xml",
        "https://ee.co.uk/sitemap-marketplace.xml"
    ]
    products = await extract_site_maps(siteurls)

    data = await extact_data_from_product_url(products)


if __name__ == "__main__":
    asyncio.run(main())
