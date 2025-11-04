from bs4 import BeautifulSoup
from functions import *
import os




semaphore = asyncio.Semaphore(DEFAULT_WORKERS)
current_date = datetime.now().strftime("%Y-%m-%d")
sitemap_url = "https://www.maplin.co.uk/sitemap.xml"




if DEFAULT_CONFIG["save_local"]:

    for folder in [DATA_DIR, SITEMAPS_DIR, PRODUCTS_DIR,OUTPUTS_DIR ]:
        os.makedirs(folder, exist_ok=True)

    # Configure logging
    logging.basicConfig(
        filename=f'data/{current_date}_log.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger()


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












GET_PRODUCT_BY_ID_QUERY = '''
query getAllProductData($handle: String!, $countryCode: CountryCode!, $languageCode: LanguageCode!) 
@inContext(country: $countryCode, language: $languageCode) {
  productByHandle(handle: $handle) {
    id
    availableForSale
    title
    handle
    createdAt
    updatedAt
    description
    descriptionHtml
    productType
    onlineStoreUrl

    options {
      id
      name
      values
    }

    featuredImage {
      id
      originalSrc
    }

    tags
    totalInventory
    vendor
    requiresSellingPlan

    compareAtPriceRange {
      maxVariantPrice {
        amount
        currencyCode
      }
      minVariantPrice {
        amount
        currencyCode
      }
    }

    priceRange {
      maxVariantPrice {
        amount
        currencyCode
      }
      minVariantPrice {
        amount
        currencyCode
      }
    }

    media(first: 100) {
      edges {
        node {
          id
          alt
          previewImage {
            url
            id
          }
        }
      }
    }

    images(first: 100) {
      edges {
        node {
          id
          originalSrc
        }
      }
    }

    seo {
      title
      description
    }

    metafields(identifiers: [
      { namespace: "custom", key: "warranty" },
      { namespace: "custom", key: "energy_class" }
    ]) {
      namespace
      key
      value
      type
    }

    collections(first: 100) {
      edges {
        node {
          id
          title
          handle
        }
      }
    }

    variants(first: 100) {
      edges {
        node {
          id
          sku
          title
          price {
            amount
            currencyCode
          }
          weight
          weightUnit
          requiresShipping
          currentlyNotInStock
          compareAtPrice {
            amount
            currencyCode
          }
          quantityAvailable
          selectedOptions {
            name
            value
          }
          image {
            id
            originalSrc
          }
        }
      }
    }
  }
}

'''



async def extract_key_value_tags(product_data):
    product = product_data['data']['productByHandle']
    tags = product.get('tags', [])

    key_value_tags = {}

    for tag in tags:
        if ':' in tag:
            key, value = tag.split(':', 1)
            key_value_tags[key.strip()] = value.strip()

    return key_value_tags


headers = {
    'accept': '*/*',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,ar;q=0.7',
    'content-type': 'application/json',
    'origin': 'https://www.maplin.co.uk',
    'priority': 'u=1, i',
    'referer': 'https://www.maplin.co.uk/',
    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'cross-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
    'x-shopify-storefront-access-token': '079b2c74feab259f4e8566b7ba8ef018',
}

params = {
    'skipListener': 'true',
}



async def process_sitemaps(sitemap_urls):
    all_product_urls = []

    for sitemap_url in tqdm(sitemap_urls, desc="Processing product sitemap URLs"):
        logger.info(f"Fetching URLs from: {sitemap_url}")
        product_urls = await fetch_sitemap(sitemap_url,config={})
        all_product_urls.extend(product_urls)

    return all_product_urls





async def data_json(handels: str) -> Dict[str, Any]:
    return {
        "query": GET_PRODUCT_BY_ID_QUERY,
        "variables": {
            'handle': handels,
            "countryCode": "GB",
            "languageCode": "EN"
        }
    }




async def fetch_post_api(url: str, json_data: Dict[str, Any], max_retries: int = 3, delay: float = 2.0) :
    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(url, headers=headers, params=params, json=json_data)
                response.raise_for_status()
                data = response.json()
                if not isinstance(data, dict):
                    raise ValueError("Response is not a JSON object")
                return data
        except (httpx.HTTPError, ValueError) as e:
            logger.warning(f"[Retry {attempt + 1}/{max_retries}] Request failed: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(delay)
            else:
                raise RuntimeError(f"Failed after {max_retries} retries") from e

async def get_all_product_data():
    product_data = []
    has_next_page = True
    after_cursor = None

    while has_next_page:
        query = """
        query {
          products(first: 250%s) {
            edges {
              node {
                id
                handle
                title
              }
            }
            pageInfo {
              hasNextPage
              endCursor
            }
          }
        }
        """ % (f', after: "{after_cursor}"' if after_cursor else "")

        url = "https://maplin-uk.myshopify.com/api/2025-07/graphql.json"
        json_data = {"query": query}

        data = await fetch_post_api(url, json_data)

        edges = data["data"]["products"]["edges"]
        for edge in edges:
            product_data.append(f"{edge["node"]["handle"]}|{edge["node"]["id"]}")

        page_info = data["data"]["products"]["pageInfo"]
        has_next_page = page_info["hasNextPage"]
        after_cursor = page_info["endCursor"]

    return product_data


async def extract_single_data(product_id: str):
    async with semaphore:

        id_product = product_id.split('/')[-1]
        product_file = PRODUCTS_DIR / f"{id_product}.json"

        handels= product_id.split('|')[0]

        json_data = await data_json(handels)
        url = "https://maplin-uk.myshopify.com/api/2025-07/graphql.json"

        if product_file.exists():
            try:
                with open(product_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    product = data.get('data', {}).get('productByHandle')

            except json.JSONDecodeError as e:
                logger.error(f"Error reading data from {product_file}. Error: {e}")
                return


        else:
            data = await fetch_post_api(url, json_data)

            product = data.get('data', {}).get('productByHandle')
            if product is None:
                logger.error(f"No product data found for product_id: {id_product}")
                return

            if DEFAULT_CONFIG["save_local"]:
                try:
                    with open(f"{PRODUCTS_DIR}/{id_product}.json", "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                        logger.info(f"Saved product data to {PRODUCTS_DIR / f'{id_product}.json'}")
                except Exception as e:
                    logger.error(f"Error saving data for {id_product}. Error: {e}")












        row = {}
        handel = product.get('handle', "")
        apiURL =f"https://maplin-uk.myshopify.com/api/2025-07/graphql.json?skipListener=true&query=query%20getAllProductData($handle:%20String!,%20$countryCode:%20CountryCode!,%20$languageCode:%20LanguageCode!)%20@inContext(country:%20$countryCode,%20language:%20$languageCode)%20%7B%20productByHandle(handle:%20$handle)%20%7B%20id%20availableForSale%20title%20handle%20createdAt%20description%20descriptionHtml%20productType%20onlineStoreUrl%20options%20%7B%20id%20name%20values%20%7D%20featuredImage%20%7B%20id%20originalSrc%20transformedSrc(maxWidth:%20800,%20maxHeight:%20800,%20crop:%20CENTER)%20%7D%20updatedAt%20tags%20vendor%20compareAtPriceRange%20%7B%20maxVariantPrice%20%7B%20amount%20currencyCode%20%7D%20minVariantPrice%20%7B%20amount%20currencyCode%20%7D%20%7D%20priceRange%20%7B%20maxVariantPrice%20%7B%20amount%20currencyCode%20%7D%20minVariantPrice%20%7B%20amount%20currencyCode%20%7D%20%7D%20media(first:%20100)%20%7B%20edges%20%7B%20node%20%7B%20id%20alt%20previewImage%20%7B%20url%20id%20%7D%20%7D%20%7D%20%7D%20images(first:%20100)%20%7B%20edges%20%7B%20node%20%7B%20id%20originalSrc%20transformedSrc(maxWidth:%20800,%20maxHeight:%20800,%20crop:%20CENTER)%20%7D%20%7D%20%7D%20seo%20%7B%20title%20description%20%7D%20collections(first:%205)%20%7B%20edges%20%7B%20node%20%7B%20id%20title%20handle%20%7D%20%7D%20%7D%20variants(first:%20100)%20%7B%20edges%20%7B%20node%20%7B%20id%20sku%20title%20price%20%7B%20amount%20currencyCode%20%7D%20weight%20weightUnit%20requiresShipping%20currentlyNotInStock%20compareAtPrice%20%7B%20amount%20currencyCode%20%7D%20quantityAvailable%20selectedOptions%20%7B%20name%20value%20%7D%20availableForSale%20image%20%7B%20id%20originalSrc%20transformedSrc(maxWidth:%20800,%20maxHeight:%20800,%20crop:%20CENTER)%20%7D%20%7D%20%7D%20%7D%20%7D%20%7D&variables=%7B%22handle%22%3A%22{handel}%22%2C%22countryCode%22%3A%22GB%22%2C%22languageCode%22%3A%22EN%22%7D"
        review_data = await get_review_summary(id_product)
        collections = product['collections']['edges']
        category_titles = [c['node']['title'] for c in collections if c['node'].get('title')]
        compare_price = product.get('compareAtPriceRange', {}).get('minVariantPrice', {}).get('amount', '0.0')
        image_urls = []
        featured = product.get('featuredImage')
        if featured and featured.get('originalSrc'):
            image_urls.append(featured['originalSrc'])

        images = product.get('images', {}).get('edges', [])
        for img in images:
            node = img.get('node')
            if node and node.get('originalSrc'):
                image_urls.append(node['originalSrc'])

        variants = product.get('variants', {}).get('edges', [])
        for variant in variants:
            node = variant.get('node')
            image = node.get('image') if node else ""
            if image and image.get('originalSrc'):
                image_urls.append(image['originalSrc'])
        image_urls = list(dict.fromkeys(image_urls))[:5]

        for edge in product["variants"]["edges"]:
            variant = edge["node"]
            compare_price = variant['compareAtPrice']['amount'] if variant.get('compareAtPrice') else "0.0"
            row = {}

            row['source'] = 'maplin'
            row["date"] = datetime.now().strftime("%Y-%m-%d")
            row['apiURL'] = ""
            id_product = variant.get('id', "").split('/')[-1]
            currentlyNotInStock = variant.get('currentlyNotInStock', False)

            if currentlyNotInStock:
                row['stock'] = 'N'
            else:
                row['stock'] = 'Y'
            row['url'] = product.get('onlineStoreUrl', "") + f"?variant={id_product}"
            row['sku'] = variant['sku']
            row['name'] = product.get("title", "")
            row['brand'] = product['vendor']
            row['price'] = variant['price']['amount']
            row['previousPrice'] = variant['compareAtPrice']['amount'] if variant.get('compareAtPrice') else ""
            row['onSale'] = "Y" if float(compare_price) > 0 else ""
            row['saleText'] = ""
            tags = product.get("tags", [])
            colour = ""
            for tag in tags:
                if tag.startswith("Colour:"):
                    colour = tag.split("Colour:")[1].strip()
                    break
            row["colour"] = colour
            row['size'] = variant.get('title', "")
            row['UPC'] = ""
            row['EAN'] = ""
            row['cat'] = category_titles[0] if category_titles else ""
            row['subcat1'] = category_titles[1] if len(category_titles) > 1 else ""
            row['subcat2'] = category_titles[2] if len(category_titles) > 2 else ""
            row['subcat3'] = category_titles[3] if len(category_titles) > 3 else ""
            row['subcat4'] = category_titles[4] if len(category_titles) > 4 else ""
            row['subcat5'] = category_titles[5] if len(category_titles) > 5 else ""
            warranty = next((m['value'] for m in product.get('metafields', []) if m and m.get('key') == 'warranty'), "")
            row['warranty'] = warranty

            for i in range(5):
                row[f'image{i + 1}'] = image_urls[i] if i < len(image_urls) else ""
            row['desc'] = product['description'].replace('. ', ' - ')
            row['shortDesc'] = ""
            row['reviewCount'] = review_data['reviewCount']
            row['reviewRating'] = review_data['reviewRating']
            row['videoURL'] = ""
            row['isPromotion'] = ""
            row['isOutletPrice'] = ""
            row['lowestPriceText'] = ""
            row['lowestPriceValue'] = ""

            attribute_index = 1
            key_value_tags = await extract_key_value_tags(data)
            for key, value in key_value_tags.items():
                if attribute_index > 20:
                    break
                row[f"attributeType{attribute_index}"] = "filter"
                row[f"attributeTitle{attribute_index}"] = key
                row[f"attributeValue{attribute_index}"] = value
                attribute_index += 1


            append_to_csv(row, "products.csv")


async def get_review_summary(product_id: str) -> dict:
    url = "https://api.judge.me/reviews/reviews_for_widget"
    params = {
        "url": "maplin-uk.myshopify.com",
        "shop_domain": "maplin-uk.myshopify.com",
        "platform": "shopify",
        "product_id": product_id
    }
    try:

        config = {}
        response = await fetch_url(url, params=params,config=config)
        response = json.loads(response)



        html_content = response.get('html', '')
        soup = BeautifulSoup(html_content, 'html.parser')

        reviews = soup.find_all("div", class_="jdgm-rev")

        total_reviews = 0
        total_rating = 0

        for review in reviews:
            rating_tag = review.find("span", class_="jdgm-rev__rating")
            rating = rating_tag.get("data-score", "") if rating_tag else ""

            if rating.isdigit():
                total_reviews += 1
                total_rating += int(rating)

        average_rating = round(total_rating / total_reviews, 2) if total_reviews > 0 else 0.0

        return {
            'reviewCount': total_reviews,
            'reviewRating': average_rating
        }

    except Exception as e:
        logger.error("Error parsing reviews:", e)
        return {
            'reviewCount': 0,
            'reviewRating': 0.0
        }

async def extact_data_from_product_url(all_product_urls:list[str]):

    tasks = []
    total_urls = len(all_product_urls)
    with tqdm(total=total_urls, desc="Processing product URLs", ncols=100) as pbar:

        for product_id in all_product_urls:
            task = asyncio.create_task(extract_single_data(product_id=product_id))
            tasks.append(task)
            pbar.update(1)
            if len(tasks) >= DEFAULT_WORKERS:
                await tasks.pop(0)


    for task in tasks:
        await task


async def main():
    create_csv_file("products.csv")
    config = {}
    urls_product = await fetch_sitemap(sitemap_url, config=config)
    all_product_urls = await process_sitemaps(urls_product)


    products = await get_all_product_data()
    # print("Number of products:", len(products))
    data = await extact_data_from_product_url(products)



asyncio.run(main())