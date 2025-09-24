import time
from functions import *
import os
from html import unescape
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from functions import *
import gzip
from io import BytesIO





if DEFAULT_CONFIG["save_local"]:

    for folder in [DATA_DIR, SITEMAPS_DIR, PRODUCTS_DIR,OUTPUTS_DIR ]:
        os.makedirs(folder, exist_ok=True)

logger = setup_logger("logs/scraper.log")







def get_standard_csv_headers():
    headers = [
        "source", "date", "apiURL", "url", "sku", "name", "brand", "price",
        "previousPrice", "onSale", "saleText", "colour", "size", "UPC", "EAN",
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


def clean_attribute_value(value: str) -> str:
    """
    Clean attributeValue:
    - Remove common bullet-like symbols (•, *, -, etc.)
    - Replace newlines with |
    - Collapse multiple spaces
    - Return everything on one line
    """
    if not value:
        return ""

    cleaned = re.sub(r"^[\s\-\*\•●▪‣◦·]+", "", value, flags=re.MULTILINE)

    parts = re.split(r"[\r\n]+", cleaned)

    parts = [re.sub(r"\s+", " ", p).strip() for p in parts if p.strip()]

    return " | ".join(parts)


def extract_price_info(variant: dict) -> dict:
    """
    Extract price information from variant.
    - price: current price
    - previousPrice: first reductionHistory value if > current price
    - onSale: "Y" if previousPrice > price, else ""
    """
    result = {"price": "", "previousPrice": "", "onSale": ""}

    price_data = variant.get("price", {})
    if not price_data:
        return result

    # Current price
    result["price"] = price_data.get("value", "")

    # Previous price (from reductionHistory)
    reduction_history = price_data.get("reductionHistory", [])
    prev_price = reduction_history[0].get("value") if reduction_history else price_data.get("value", "")

    try:
        if prev_price and float(prev_price) > float(result["price"]):
            result["previousPrice"] = prev_price
            result["onSale"] = "Y"
    except (ValueError, TypeError):
        pass

    return result

def extract_barcodes(variant: dict) -> dict:
    """
    Extract UPC and EAN from variant['consumerBarcodes'].
    - UPC: takes first value from 'upc12' if available.
    - EAN: prefers first value from 'ean13', otherwise 'ean8'.
    Missing values return as "".
    """
    result = {"UPC": "", "EAN": ""}

    barcodes = variant.get("consumerBarcodes") or {}

    upc_list = barcodes.get("upc12", [])
    result["UPC"] = upc_list[0] if upc_list else ""

    ean13_list = barcodes.get("ean13", [])
    ean8_list = barcodes.get("ean8", [])
    if ean13_list:
        result["EAN"] = ean13_list[0]
    elif ean8_list:
        result["EAN"] = ean8_list[0]

    return result


def extract_categories(response_json: dict) -> dict:
    """
    Extract category hierarchy from response_json.
    Returns a dict with keys: cat, subcat1, subcat2, subcat3, subcat4, subcat5.
    Missing levels will be set to "".
    """
    category_keys = ['cat', 'subcat1', 'subcat2', 'subcat3', 'subcat4', 'subcat5']
    categories = {key: "" for key in category_keys}

    hierarchy = (
        response_json.get("data", {})
        .get("activeAndRetiredProducts", {})
        .get("activeProducts", [{}])[0]
        .get("browsePageHierarchy", [])
    )

    for idx, key in enumerate(category_keys):
        if idx < len(hierarchy):
            categories[key] = hierarchy[idx].get("name", "")

    return categories
def extract_warranty(variant: dict) -> str:

    for service in variant.get("services", []):
        title = service.get("title", "")
        if "guarantee" in title.lower():
            return title
    return ""
def extract_images(variant: dict, max_images: int = 5) -> dict:

    row_images = {}
    images = variant.get("images", {})

    row_images["image1"] = (
        "https:" + images.get("primary", "") if images.get("primary") else ""
    )

    alternatives = images.get("alternatives", [])
    for idx in range(2, max_images + 1):
        if idx - 2 < len(alternatives):
            row_images[f"image{idx}"] = "https:" + alternatives[idx - 2].replace("\\/", "/")
        else:
            row_images[f"image{idx}"] = ""

    return row_images

def extract_clean_description(response_json: dict) -> str:
    raw_desc = (
        response_json.get("data", {})
        .get("activeAndRetiredProducts", {})
        .get("activeProducts", [{}])[0]
        .get("description", "")
    )

    if not raw_desc:
        return ""

    cleaned = re.sub(r"<.*?>", " ", raw_desc)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    parts = [part.strip() for part in cleaned.split(" . ") if part.strip()]
    return " | ".join(parts)
def extract_video_url(response_json: dict) -> str:
    """
    Extract the video stream URL from response_json.
    Looks inside: data.activeAndRetiredProducts.activeProducts[*].media.videos[*].sources
    Returns the https URL if found, else "".
    """
    active_products = response_json.get("data", {}).get("activeAndRetiredProducts", {}).get("activeProducts", [])
    for product in active_products:
        videos = product.get("media", {}).get("videos", [])
        for video in videos:
            for source in video.get("sources", []):
                if source.get("key") == "stream" and "url" in source:
                    return "https:" + source["url"].replace("\\/", "/")

    return ""
async def fetch_single_product(id: str):
    url = "https://api.johnlewis.com/catalogue/graphql"
    headers = {
        "Content-Type": "application/json",
        "x-api-key": "AIzaSyDtVsqXz2-LpOo5-RFYiAa2InbnSyfNZAM"
    }

    payload = {
        "operationName": "getByProductIds",
        "variables": {
            "id": id
        },
        "query": """
        query getByProductIds($id: [String!]!) {\n  activeAndRetiredProducts(where: { pimProductIds: $id }) {\n    activeProducts {\n      __typename\n      title\n      eligibleForTradeIn\n      aliases {\n        pimProductId\n      }\n      availability {\n        futureAvailability {\n          availableToOrderFromMessage\n        }\n        preOrder {\n          streetDateMessage\n        }\n      }\n      defaultVariant {\n        id\n      }\n      browsePageHierarchy {\n        categoryId\n        name\n        url\n      }\n      reviewSummary {\n        averageRating\n        maxRating\n        averageRatingAsPercentage\n        numberOfReviews\n      }\n      sets {\n        title\n        url\n        productListQuery {\n          baseId\n          modifiers {\n            name\n            value\n          }\n        }\n      }\n      canonicalURL\n      pdpURL {\n        url\n        path\n      }\n      variantPriceRange {\n        display {\n          max\n          min\n        }\n        for\n        reductionHistory {\n          chronology\n          display {\n            max\n            min\n          }\n        }\n        value {\n          max\n          min\n        }\n        offerTypeSummary\n      }\n      seo {\n        index\n        pageHeading\n        browserTitle\n      }\n      unitOfMeasure\n      variants {\n        __typename\n        id\n        title\n        canonicalURL\n        pdpURL {\n          url\n          path\n          query\n        }\n        displayCode\n        messaging {\n          title\n          type\n          description\n          ... on PromotionalMessage {\n            promotionGroup\n            promotionCategory\n            promotionType\n          }\n        }\n        price {\n          for\n          value\n          display\n          offerType\n          reductionHistory {\n            chronology\n            value\n            display\n          }\n        }\n        availability {\n          availableToOrder\n          unavailableReason\n          shipFromStore\n          futureAvailability {\n            availableToOrderFrom\n            availableToOrderFromMessage\n          }\n          preOrder {\n            streetDateMessage\n            streetDate\n          }\n        }\n        images {\n          primary\n          alternatives\n        }\n        customerNotifiableEvents {\n          backInStock\n          onRelease\n          onPreorderLaunch\n          newlyInStock\n        }\n        ... on CompositeBundleVariant {\n          aliases {\n            skuId\n          }\n          creditEligibility\n          creditOfferingIds\n          components {\n            __typename\n            variant {\n                aliases {\n                  skuId\n                }\n              __typename\n              services {\n                __typename\n                title\n                automaticallyIncluded\n                id\n                ... on ExtendedWarranty {\n                  aliases {\n                    skuId\n                  }\n                  confirmation\n                  policyDetail\n                  price {\n                    display\n                    value\n                  }\n                  duration\n                  purchaseDescription\n                }\n                ...on IncludedGuarantee {\n                  aliases {\n                    skuId\n                  }\n                }\n              }\n            }\n          }\n        }\n        ... on StaticBundleVariant {\n          aliases {\n            skuId\n          }\n          creditEligibility\n          creditOfferingIds\n          components {\n            variant {\n              aliases {\n                skuId\n              }\n            }\n          }\n        }\n        ... on StockVariant {\n          aliases {\n            skuId\n          }\n          colourSwatch\n          pdpURL {\n            url\n            path\n            query\n          }\n          differentiators {\n            colour\n            size\n            sizeHeadline\n          }\n          colour {\n            colour\n            trueColour\n          }\n          attributes {\n            key\n            values\n            displayName\n            helpText\n          }\n          creditEligibility\n          creditOfferingIds\n          returnsOutlet\n          services {\n            id\n            __typename\n            title\n            ... on DeliveryService {\n              aliases {\n                dmsId\n                dbsId\n              }\n              description\n              price {\n                value\n                display\n              }\n            }\n            ... on IncludedGuarantee {\n              aliases {\n                skuId\n              }\n            }\n            ... on ExtendedWarranty {\n              aliases {\n                skuId\n              }\n              price {\n                value\n                display\n              }\n              duration\n              policyDetail\n              tooltip\n              purchaseDescription\n              confirmation\n            }\n          }\n          swatch {\n            availableToOrder\n          }\n          sellingRestrictions {\n            age {\n              minimumAge\n              bladedArticle\n            }\n          }\n          pricePerUnit {\n            quantity\n            unitOfMeasure\n            display\n          }\n          consumerBarcodes {\n            ean13\n            upc12\n            ean8\n          }\n          energy {\n            rating {\n              legacy\n              value\n              title\n              description\n            }\n            label\n            informationSheet\n          }\n        }\n      }\n      ... on CompositeBundle {\n        description\n        attributes {\n          key\n          values\n          displayName\n          helpText\n        }\n        brand {\n          name\n          logo\n          story\n        }\n      }\n      ... on SimpleProduct {\n        description\n        howToUse\n        sellingProductType\n        brand {\n          name\n          logo\n          story\n        }\n        sizeGuide\n        attributes {\n          key\n          values\n          displayName\n          helpText\n        }\n        icons {\n          iconUrl\n          linkUrl\n          text\n        }\n        links {\n          linkUrl\n          text\n        }\n        termsAndConditions {\n          detail\n          customerConfirmationRequired\n      	}\n        media {\n          videos {\n            sources {\n              key\n              url\n              contentType\n            }\n          }\n        }\n        termsAndConditions {\n          detail\n          customerConfirmationRequired\n        }\n        variantGroups(groupBy: { differentiator: colour }) {\n          value\n          variants {\n            id\n          }\n          pdpURL {\n            url\n            path\n          }\n          messaging {\n            title\n            type\n            description\n            ... on PromotionalMessage {\n              promotionGroup\n              promotionCategory\n              promotionType\n            }\n          }\n          variantPriceRange {\n            display {\n              min\n              max\n            }\n            reductionHistory {\n              display {\n                min\n                max\n              }\n              chronology\n            }\n            offerTypeSummary\n          }\n          availability {\n            futureAvailability {\n              availableToOrderFromMessage\n            }\n            preOrder {\n              streetDateMessage\n            }\n          }\n        }\n        configurationRequirements {\n          type\n        }\n      }\n      messaging {\n        description\n        title\n        type\n        ... on PromotionalMessage {\n          promotionGroup\n          promotionCategory\n          promotionType\n        }\n      }\n      fulfilmentOptions {\n        available {\n          aliases {\n            dmsId\n          }\n          headline\n          description\n          shortDescription\n          deliveryGroupId\n          price {\n            value\n            display\n          }\n          earliestFulfilment {\n            orderByLocalTimeToday\n            collectFromLocalTimeTomorrow\n          }\n        }\n        unavailableCollection {\n          headline\n          description\n        }\n      }\n    }\n    retiredProducts {\n      __typename\n      title\n      aliases {\n        pimProductId\n      }\n      defaultVariant {\n        id\n      }\n      sets {\n        title\n        url\n      }\n      browsePageHierarchy {\n        categoryId\n        name\n      }\n      seo {\n        index\n        browserTitle\n        pageHeading\n      }\n      canonicalURL\n      pdpURL {\n        url\n        path\n      }\n      variants {\n        __typename\n        id\n        title\n        aliases {\n          skuId\n        }\n        canonicalURL\n        pdpURL {\n          url\n          path\n          query\n        }\n        displayCode\n        images {\n          primary\n          alternatives\n        }\n        ... on RetiredStockVariant {\n          colourSwatch\n          differentiators {\n            size\n            colour\n            sizeHeadline\n          }\n          attributes {\n            key\n            values\n            displayName\n            helpText\n          }\n        }\n        ... on RetiredStaticBundleVariant {\n          components {\n            variant {\n              title\n              aliases {\n                skuId\n              }\n              displayCode\n              images {\n                primary\n                alternatives\n              }\n              colourSwatch\n              differentiators {\n                size\n                colour\n                sizeHeadline\n              }\n              attributes {\n                key\n                values\n                displayName\n                helpText\n              }\n              parentProduct {\n                __typename\n                id\n                title\n                description\n                aliases {\n                  pimProductId\n                }\n                defaultVariant {\n                  displayCode\n                }\n                icons {\n                  iconUrl\n                  linkUrl\n                  text\n                }\n                links {\n                  linkUrl\n                  text\n                }\n                canonicalURL\n                pdpURL {\n                  url\n                  path\n                }\n                attributes {\n                  key\n                  values\n                  displayName\n                  helpText\n                }\n              }\n            }\n            isActiveComponent\n          }\n        }\n      }\n      ... on RetiredSimpleProduct {\n        description\n        attributes {\n          key\n          values\n          displayName\n          helpText\n        }\n        brand {\n          name\n          logo\n        }\n        icons {\n          iconUrl\n          linkUrl\n          text\n        }\n        links {\n          linkUrl\n          text\n        }\n        media {\n          videos {\n            sources {\n              key\n              url\n              contentType\n            }\n          }\n        }\n        variantGroups(groupBy: { differentiator: colour }) {\n          value\n          variants {\n            id\n          }\n          canonicalURL\n          pdpURL {\n            url\n            path\n          }\n        }\n      }\n      ... on RetiredCompositeBundle {\n        description\n        attributes {\n          key\n          values\n          displayName\n          helpText\n        }\n        brand {\n          name\n          logo\n        }\n        icons {\n          iconUrl\n          linkUrl\n          text\n        }\n        links {\n          linkUrl\n          text\n        }\n      }\n    }\n  }\n}\n
        """
    }
    api_url = f"{url}?{id}"
    response = await fetch_url(api_url, method="POST", headers=headers, json_data=payload, content_type="product")
    response_json = json.loads(response)
    if response_json:
        retiredProducts = response_json.get("data", {}).get("activeAndRetiredProducts", {}).get("retiredProducts", [])
        activeProducts = response_json.get("data", {}).get("activeAndRetiredProducts", {}).get("activeProducts", [])

        if retiredProducts not in (None, []):
            return

        if activeProducts in (None, []):
            return

        row = {}
        row["source"] = "JohnLewis"
        row["date"] = datetime.now().strftime("%Y-%m-%d")
        row['apiURL'] = api_url
        row['brand'] = response_json.get("data", {}).get("activeAndRetiredProducts", {}).get("activeProducts", [])[0].get("brand", {}).get("name", "")


        variants = response_json.get("data", {}).get("activeAndRetiredProducts", {}).get("activeProducts", [])[0].get("variants", [])
        for variant in variants:

            row['url'] = f"https://www.johnlewis.com{variant.get("pdpURL").get("url")}" if variant.get("pdpURL") else ""
            row['sku'] = variant.get("aliases").get("skuId") if variant.get("aliases") else ""
            row['name'] = variant.get("title") if variant.get("title") else ""
            row['price'] = variant.get("price", {}).get("value") if variant.get("price") else ""
            row['previousPrice'] = variant.get("price", {}).get("reductionHistory")[0].get("value") if variant.get("price", {}).get("reductionHistory") else variant.get("price", {}).get("value") if variant.get("price") else ""
            if float(row['previousPrice']) > float(row['price']):
                row["onSale"] = "Y"
            else:
                row['previousPrice'] = ""
                row["onSale"] = ""

            row['saleText'] = variant.get("messaging")[0].get("title") if variant.get("messaging") else ""
            differentiators = variant.get("differentiators") or {}
            # row["colour"] = differentiators.get("colour", "")
            row["colour"] = variant.get("colour", {}).get("trueColour", "") if variant.get("colour") else ""


            row["size"] = differentiators.get("size", "")
            row.update(extract_price_info(variant))

            row.update(extract_barcodes(variant))

            row.update(extract_categories(response_json))

            row["warranty"] = extract_warranty(variant)

            row.update(extract_images(variant, max_images=5))

            row["desc"] = extract_clean_description(response_json)

            row["shortDesc"] = ""

            reviewsummary = response_json.get("data", {}).get("activeAndRetiredProducts", {}).get("activeProducts", [])[0].get("reviewSummary", {})
            if reviewsummary:
                row["reviewCount"] = reviewsummary.get("numberOfReviews", "")
                row["reviewRating"] = reviewsummary.get("averageRating", "")

            row["videoURL"] = extract_video_url(response_json)
            row["isSellingFast"] = ""
            row["isPromotion"] = "Y" if variant.get("messaging") and variant.get("messaging")[0].get("type") == "promotional" else ""
            row["isOutletPrice"] = "Y" if variant.get("returnsOutlet") else ""
            row["lowestPriceText"] = variant.get("price", {}).get("display", "")
            row["lowestPriceValue"] = variant.get("price", {}).get("value", "")

            attributes_list = (
                response_json.get("data", {})
                .get("activeAndRetiredProducts", {})
                .get("activeProducts", [{}])[0]
                .get("attributes", [])
            )

            attribute_index = 1
            for attr in attributes_list:
                if attribute_index >= 21:
                    break

                attribute_title = attr.get("displayName", "")
                attribute_values = attr.get("values", [])
                attribute_value = ", ".join(attribute_values) if attribute_values else ""

                row[f"attributeType{attribute_index}"] = "specification"
                row[f"attributeTitle{attribute_index}"] = attribute_title
                row[f"attributeValue{attribute_index}"] = clean_attribute_value(attribute_value)

                attribute_index += 1


            append_to_csv(row, "products.csv")


async def fetch_url_content(
    url: str,
    content_type: str,
    headers: Optional[Dict[str, str]] = None,
    config: Dict[str, Any] = None
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

            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url=url, headers=headers)
                response.raise_for_status()

                with gzip.GzipFile(fileobj=BytesIO(response.content)) as f:
                    response_bytes = f.read()

                try:
                    response_text = response_bytes.decode("utf-8")
                except UnicodeDecodeError:
                    response_text = response_bytes.decode("latin1")

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


async def extract_links_products(urls):
    headers = {
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',

    }

    products = []
    for url in urls:

        xml_content = await fetch_url_content(url,headers=headers,content_type="sitemap")
        ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        root = ET.fromstring(xml_content)

        links = [loc.text for loc in root.findall(".//ns:loc", ns)]

        product_ids = [re.search(r"/p(\d+)", url).group(1) for url in links if re.search(r"/p(\d+)", url)]
        product_ids_set = set(product_ids)
        products.extend(product_ids_set)


    return products


async def extract_links(sitmap_url):
    headers = {
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    links = await fetch_url(sitmap_url,headers=headers,content_type="sitemap")
    root = ET.fromstring(links)
    urls = []
    ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    loc_elements = root.findall('.//sm:loc', ns)
    if not loc_elements:
        loc_elements = root.findall('.//loc')
    for loc in loc_elements:
        urls.append(loc.text)

    links_products_ids = await extract_links_products(urls)
    return links_products_ids









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
    sitmap_url = "https://www.johnlewis.com/sitemap/products/products.xml"
    products_urls = await extract_links(sitmap_url)
    data = await extact_data_from_product_url(products_urls)


if __name__ == "__main__":
    asyncio.run(main())
