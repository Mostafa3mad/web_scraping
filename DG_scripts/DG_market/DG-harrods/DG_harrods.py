import time
from functions import *
import os
from html import unescape
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import math






if DEFAULT_CONFIG["save_local"]:

    for folder in [DATA_DIR, SITEMAPS_DIR, PRODUCTS_DIR,OUTPUTS_DIR ]:
        os.makedirs(folder, exist_ok=True)

logger = setup_logger("logs/scraper.log")



def clean(text):
    if text is None:
        return ""
    text = str(text)
    soup = BeautifulSoup(text, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()
    text = soup.get_text(separator=" ")
    text = unescape(text)
    text = re.sub(r"[\r\n\t*?#|]", "", text)
    text = text.replace('"', "'")
    text = text.replace("é", "e")
    text = re.sub(r"^[^\w\(\)]+|[^\w\(\)]+$", "", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.strip()
    text = " ".join(text.split())
    return text


def get_standard_csv_headers():
    headers = [
        "source", "date", "apiURL", "url", "sku", "name", "brand", "price","stock",
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








async def fetch_single_product(url: str):

    id = url.split("-")[-1]
    json_data = {
        'payload': {
            'referenceKeys': [
                f'{id}',
            ],
                    'with': {
                        'attributes': {
                            'withKey': [
                                'learnMoreType',
                                'sizeGuideActivated',
                                'sizeGuideType',
                                'sizeGuideBrand',
                                'harrodsColour',
                                'brand',
                                'sAPBrand',
                                'name',
                                'promotion',
                                'fastenerType',
                                'design',
                                'extras',
                                'material',
                                'print',
                                'careInstructions',
                                'fitting',
                                'upperLength',
                                'sleeveLength',
                                'shirtCut',
                                'shortsLength',
                                'trousersLength',
                                'skirtLength',
                                'neckline',
                                'trousersCut',
                                'vendorColour',
                                'promotionTag',
                                'exclusive',
                                'contentTags',
                                'reductionType',
                                'nonReturnable',
                                'gender',
                                'grapeVariety',
                                'wineRegion',
                                'luggageCaseType',
                                'pattern',
                                'numberOfSeats',
                                'necklineType',
                                'collarType',
                                'sleeveType',
                                'denimWash',
                                'rise',
                                'claspDetail',
                                'strapDetail',
                                'hardwareDetail',
                                'eyewearLensTint',
                                'uvProtection',
                                'burnTime',
                                'fitType',
                                'heelType',
                                'towelType',
                                'towelSize',
                                'beddingType',
                                'beddingSize',
                                'fillingType',
                                'duvetTog',
                                'dishwasherSafe',
                                'microwaveSafe',
                                'compatibleWith',
                                'suitableFrom',
                                'suitablefrom',
                                'modelSize',
                                'leadTime',
                                'fasteningType',
                                'claspDetail',
                                'alcoholContent',
                                'dietaryInformation',
                                'wineVintageYear',
                                'bottleClosure',
                                'numberOfRecliningPositions',
                                'coverage',
                                'finishType',
                                'skinConcern',
                                'skinType',
                                'sPF',
                                'hairConcernType',
                                'hairType',
                                'fragranceType',
                                'containsAlcohol',
                                'watchDialShape',
                                'watchCaseComposition',
                                'watchMovementType',
                                'strapMovementFeatures',
                                'hallmarking',
                                'subBrand',
                                'priceOnApplication',
                                'inStoreOnly',
                                'sellableWithoutStock',
                                'preOrder',
                                'category',
                                'gender',
                                'soldOut',
                                'groupingType',
                                'volumeDescription',
                                'threadCount',
                                'rolexNonTransactional',
                                'rolexReference',
                                'rolexAge',
                                'rolexMaterial',
                                'rolexDiameter',
                                'rolexStockNumber',
                                'rolexModel',
                                'rolexOriginalBox',
                                'rolexOriginalPapers',
                                'rolexBezel',
                                'rolexDial',
                                'rolexCrystal',
                                'rolexMovement',
                                'rolexCalibre',
                                'rolexPowerReserve',
                                'rolexBracelet',
                                'rolexWaterproof',
                                'rolexCollection',
                            ],
                        },
                        'advancedAttributes': {
                            'withKey': [
                                'promotionMessage',
                                'materialCompositionTextile',
                                'productDescription',
                                'combineWith',
                                'additionalService',
                                'quote',
                                'materialComposition',
                                'buggyContents',
                                'beautyProductBenefit',
                                'ingredients',
                                'nutritionalInformation',
                                'fabric',
                                'exteriorPockets',
                                'interiorPockets',
                                'fragranceNotes',
                                'functionalFeatures',
                                'productDimensions',
                                'productWeight',
                                'capacity',
                                'productContents',
                                'power',
                                'madeInCountry',
                                'specialPackagingDetail',
                                'warranty',
                                'disclaimer',
                                'productModelNumber',
                                'interiorPockets',
                                'fragranceNotes',
                                'functionalFeatures',
                                'storageConditions',
                                'fragranceTopNotes',
                                'fragranceMiddleNotes',
                                'fragranceBaseNotes',
                                'dietaryInformation',
                                'mayContain',
                                'watchCaseMeasurement',
                                'gemstoneSetting',
                                'watchCaseWaterResistance',
                                'watchDialClarity',
                                'availableAt',
                                'hamperContentInformation',
                                'alcoholContent',
                                'containsAlcohol',
                                'vendorReference',
                            ],
                        },
                        'variants': {
                            'attributes': {
                                'withKey': [
                                    'price',
                                    'size',
                                    'subscriptionEligibility',
                                    'maximumQuantityPerOrder',
                                    'volumeDescription',
                                ],
                            },
                            'lowestPriorPrice': True,
                        },
                        'images': {
                            'attributes': {
                                'withKey': [
                                    'imageType',
                                    'imageView',
                                    'imageBackground',
                                    'digitalAssetType',
                                    'imagePosition',
                                ],
                            },
                        },
                        'categories': 'all',
                        'priceRange': True,
                        'lowestPriorPrice': True,
                    },
                    'includeSoldOut': True,
                },
            }

    api_url = f"https://www.harrods.com/api/rpc/getProductsByReferenceKeys?{id}"
    response = await fetch_url(api_url, method="POST", json_data=json_data,content_type="product")
    if response:
        product_json = json.loads(response)[0]
        row = {}
        row['source'] = 'harrods'
        row["date"] = datetime.now().strftime("%Y-%m-%d")
        row['apiURL'] = api_url
        row['url'] = url
        row['sku'] = product_json.get("referenceKey")
        row['name'] = product_json.get("attributes",{}).get("name",{}).get("values",{}).get("label")
        row['brand'] = product_json.get("attributes",{}).get("brand",{}).get("values",{}).get("label")
        variants = product_json.get("variants",[])
        for variant in variants:
            availability = variant.get("stock",{}).get("quantity",0)
            if availability <=0:
                continue

            price = float(variant.get("price",{}).get("withTax","")/100)
            row['price'] = price
            stock = product_json.get("isSoldOut", False)
            if stock:
                row['stock'] = "N"
            else:
                row['stock'] = "Y"

            appliedReductions = variant.get("price",{}).get("appliedReductions",[])
            row["onSale"] = ""
            if appliedReductions:
                row["onSale"] = "Y"
                discount_percent = appliedReductions[0]["amount"]["relative"]
                previousPrice = price / (1 - discount_percent)
                row["previousPrice"] = float(math.ceil(previousPrice))
                saleText = f"{int(discount_percent * 100)}% OFF"
                row["saleText"] = saleText

            vendorColour = product_json.get("attributes",{}).get("vendorColour",{}).get("values").get("label")
            row["colour"] = vendorColour
            size = variant.get("attributes",{}).get("size",{}).get("values",{}).get("label")
            row["size"] = size
            row['UPC'] = ""
            row['EAN'] = ""
            fields = ["cat", "subcat1", "subcat2", "subcat3", "subcat4", "subcat5"]
            categories = product_json["attributes"]["category"]["values"]
            full_path = categories[-1]["label"]
            parts = full_path.split("|")
            for i, field in enumerate(fields):
                row[field] = parts[i] if i < len(parts) else ""
            row['warranty'] = ""
            images = product_json.get("images",[])
            for i,image in enumerate(images):
                if i>=5:
                    break
                url_image = image["hash"]
                if url_image:
                    row[f'image{i+1}'] = f"https://hrd-live.cdn.scayle.cloud/{url_image}"

            description = product_json.get("advancedAttributes",{}).get("productDescription",{}).get("values",[])[0].get("fieldSet",[])[0][0].get("value")
            shortDesc = product_json.get("advancedAttributes",{}).get("quote",{}).get("values",[])[0].get("fieldSet",[])[0][0].get("value")
            row["desc"] = description
            row["shortDesc"] = shortDesc
            row["reviewCount"] = ""
            row["reviewRating"] = ""
            row["videoURL"] = ""
            row['isPromotion'] = "Y" if row['onSale'] == "Y" else ""
            row["isOutletPrice"] = ""
            row["lowestPriceText"] = f"£{price}"
            row["lowestPriceValue"] = price
            row["isRestockingSoon"] = ""
            row["isOutletPrice"] = ""

            attributes = product_json.get("attributes", {})
            attribute_index = 1

            for key, attr in attributes.items():
                if attribute_index > 20:
                    break

                attribute_title = key
                values = attr.get("values")
                attribute_value = ""

                # Extract actual value(s)
                if isinstance(values, list):  # multi select
                    attribute_value = ", ".join(v.get("label", "") for v in values if v.get("label"))
                elif isinstance(values, dict):  # single value
                    attribute_value = values.get("label", "")
                else:
                    continue  # skip if no values

                normalized_value = str(attribute_value).strip().lower()

                if not normalized_value or normalized_value == "false":
                    continue

                attribute_type = "specification"
                row[f"attributeType{attribute_index}"] = attribute_type
                row[f"attributeTitle{attribute_index}"] = attribute_title
                row[f"attributeValue{attribute_index}"] = attribute_value

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





async def extract_links(sitemap_xml):
    sitemap_url_list = []
    links = await fetch_sitemap(sitemap_xml)


    for link in links:
        if "en-gb/products"  in link:
            sitemap_url_list.append(link)



    return sitemap_url_list

async def main():
    create_csv_file("products.csv")
    products_urls = []

    sitemap_xml = "https://www.harrods.com/en-gb/sitemap.xml"
    sitemap_url_list = await extract_links(sitemap_xml)

    for sitemap_url in sitemap_url_list:
        products = await fetch_sitemap(sitemap_url)
        products_urls.extend(products)


    data = await extact_data_from_product_url(products_urls)


if __name__ == "__main__":
    asyncio.run(main())
