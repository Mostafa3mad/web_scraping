import requests
# email = "tahirajamil900@gmail.com"
# password = "d^4mEj7W%8Z@6@"


# ==================== 1 login ===============================================
email = "tazjammy@gmail.com"
password = "RPbhqq6gr#@k7Q"

session = requests.Session()
response = session.get('https://auth.waitrose.com/v1/csrf')
_csrf = response.json().get('token')
cookies = session.cookies
jsession_id = cookies.get("JSESSIONID_AS")
#_____________________________________________________________________________________________
url = "https://auth.waitrose.com/v1/login"
headers = {
    "Host": "auth.waitrose.com",
    "X-Csrf-Token": _csrf,
}
cookies = {
    "JSESSIONID_AS": jsession_id,
}
data = {
    "_csrf": _csrf,
    "email": email,
    "password": password
}
session = requests.Session()
response = session.post(url, headers=headers, cookies=cookies, data=data, allow_redirects=False)
override_session_url = response.headers['Location']
#_____________________________________________________________________________________________
response = session.get(override_session_url, allow_redirects=False)
authorization_url = response.headers['Location']
#_____________________________________________________________________________________________
response = session.get(authorization_url, allow_redirects=False)
oauth2_url = response.headers['Location']
#_____________________________________________________________________________________________
response = session.get(oauth2_url, allow_redirects=False)
code_url = response.headers['Location']
#_____________________________________________________________________________________________
response = session.get(code_url, allow_redirects=False)
override_session2_url = response.headers['Location']
JSESSIONID_TC = session.cookies.get("JSESSIONID_TC")
#_____________________________________________________________________________________________
response = session.get(override_session2_url, allow_redirects=False)
waitrose_url = response.headers['Location']
#_____________________________________________________________________________________________
response = session.get(waitrose_url)
#_____________________________________________________________________________________________
headers = {
    'Authorization': 'Bearer unauthenticated',
}
csrf_url = "https://www.waitrose.com/api/token-client-prod/v1/csrf"
response = session.get(csrf_url,headers=headers)
_csrf_header = response.json().get('token')
#_____________________________________________________________________________________________
headers["X-Csrf-Token"] = _csrf_header
api_clint = "https://www.waitrose.com/api/token-client-prod/v1/token"
response = session.post(api_clint, headers=headers)

Authorization = f"Bearer {response.json().get('accessToken')}"
print(Authorization)
# ============================================================================
# ==================== 2 get data customer ===============================================

headers = {
    'User-Agent': 'MobileApp/3.3.0.12910.12910 (Google; sdk_gphone64_x86_64; Android 14)',
    'Authorization': Authorization,
}



json_data = {
    "query": """
    query {
      shoppingContext {
        customerId
        customerOrderId
        customerOrderState
        defaultBranchId
      }
      addresses {
        id
        line1
        line2
        town
        region
        postalCode
        country
        addressee {
          title
          firstName
          lastName
          contactNumber
        }
      }
    }
    """
}

response = requests.post(
    'https://www.waitrose.com/api/graphql-prod/graph/live',
    headers=headers,
    json=json_data,
)

data_customer = response.json()['data']['shoppingContext']
data_address = response.json()['data']['addresses'][0]
trolleyId = data_customer['customerOrderId']
customerId = data_customer['customerId']
branchId = data_customer['defaultBranchId']
addressId = data_address["id"]
postcode = data_address["postalCode"]
print("trolleyId", trolleyId)
print("customerId",customerId)
print("branchId", branchId)
# ==================== 3 get slot avalible ===============================================
json_data = {
    'query': 'query slotDays($slotDaysInput: SlotDaysInput) {\n  slotDays(slotDaysInput: $slotDaysInput) {\n    content {\n      id\n      branchId\n      slotType\n      date\n      slots {\n        slotId: id\n        startDateTime\n        endDateTime\n        shopByDateTime\n        slotGridType\n        deliveryPassSlot\n        greenSlot\n        slotStatus: status\n        charge {\n          amount\n          currencyCode\n        }\n      }\n    }\n    variant\n    links {\n      rel\n      title\n      href\n    }\n    failures {\n      message\n      type\n    }\n  }\n}\n',
    'variables': {
        'slotDaysInput': {
            'branchId': branchId,
            'slotType': 'DELIVERY',
            'customerOrderId': trolleyId,
            'postcode': postcode,
            'addressId': addressId,
            'fromDate': '',
            'size': 1,
        },
    },
}

response = requests.post(
    'https://www.waitrose.com/api/graphql-prod/graph/live',
    headers=headers,
    json=json_data,
)

dataslot = response.json()
slots = dataslot['data']['slotDays']['content'][0]['slots']
available_slots = [slot for slot in slots if slot['slotStatus'] == 'AVAILABLE']

if available_slots:
    first_available = available_slots[0]
    startDateTime = first_available['startDateTime']
    endDateTime = first_available['endDateTime']
    print("Start:", startDateTime)
    print("End:", endDateTime)
else:
    print("No available slots found.")
# ==================== 4 set slot book ===============================================

slot_payload = {
    "branchId": branchId,
    "slotType": "DELIVERY",
    "customerOrderId": trolleyId,
    "customerId": customerId,
    'postcode': postcode,
    'addressId': addressId,
    "startDateTime": startDateTime,
    "endDateTime": endDateTime,
    "slotGridType": "DEFAULT_GRID",
    "greenSlot": False
}
r = requests.post(
    "https://www.waitrose.com/api/slot-orchestration-prod/v1/slot-reservations",
    headers=headers,
    json=slot_payload
)

print("Slot booked:", r.status_code, r.text)
# ==================== 5 get all master catogry ===============================================


response = requests.get(
    'https://www.waitrose.com/api/taxonomy-entity-prod/v1/taxonomy/waitrose-ecomm-groceries',
    headers=headers,
)

data = response.json()

all_ids = []

for lvl1 in data["childCategories"]:
    for lvl2 in lvl1.get("childCategories", []):
        for lvl3 in lvl2.get("childCategories", []):
            if "id" in lvl3:
                all_ids.append(f"{lvl3["id"]}|{lvl3["shortName"]}")

print(all_ids)
print(len(all_ids))
# ==================== 6 get all products from category  ===============================================

category_id = all_ids[0].split("|")[0]

print(category_id)

GRAPHQL_URL = "https://www.waitrose.com/api/graphql-prod/graph/live?clientType=WEB_APP"

id_products = []
start = 0
page_size = 100

while True:
    json_data = {
        'query': 'fragment ProductFragment on Product {\n  availableDays\n  barCodes\n  conflicts {\n    lineNumber\n    messages\n    nextSlotDate\n    outOfStock\n    priority\n    productId\n    prohibitedActions\n    resolutionActions\n    slotOptionDates {\n      type\n      date\n    }\n  }\n  containsAlcohol\n  lineNumber\n  images {\n    extraLarge\n    large\n    medium\n    small\n  }\n  id\n  productType\n  size\n  brand\n  thumbnail\n  name\n  leadTime\n  reviews {\n    averageRating\n    total\n  }\n  customerProductDetails {\n    customerFavourite\n    customerPyo\n  }\n  currentSaleUnitPrice {\n    quantity {\n      amount\n      uom\n    }\n    price {\n      amount\n      currencyCode\n    }\n  }\n  defaultQuantity {\n    amount\n    uom\n  }\n  depositCharge {\n    amount\n    currencyCode\n  }\n  pricing {\n    displayPrice\n    displayUOMPrice\n    displayPriceQualifier\n    displayPriceEstimated\n    formattedPriceRange\n    currentSaleUnitRetailPrice {\n      price {\n        amount\n        currencyCode\n      }\n      quantity {\n        amount\n        uom\n      }\n    }\n    promotions {\n      discount {\n        type\n      }\n      groups {\n        threshold\n        name\n        lineNumbers\n      }\n      hidden\n      myWaitrosePromotion\n      promotionDescription\n      promotionExpiryDate\n      promotionId\n      promotionType\n      pyoPromotion\n      wasDisplayPrice\n    }\n  }\n  persistDefault\n  markedForDelete\n  substitutionsProhibited\n  displayPrice\n  displayPriceEstimated\n  displayPriceQualifier\n  leadTime\n  productShelfLife\n  maxPersonalisedMessageLength\n  summary\n  supplierOrder\n  restriction {\n    availableDates {\n      restrictionId\n      startDate\n      endDate\n      cutOffDate\n    }\n  }\n  weights {\n    pricePerUomQualifier\n    defaultQuantity {\n      amount\n      uom\n    }\n    servings {\n      min\n      max\n    }\n    sizeDescription\n    uoms\n    formattedWeightRange\n  }\n  categories {\n    id\n    name\n  }\n  productTags {\n    name\n    tooltip\n  }\n  marketingBadges {\n    name\n  }\n}\nfragment ProductPod on Product {\n              adTechSponsoredPosition,\n              brand,\n              categories {\n                  name,\n                  id\n              },\n              cqResponsive {\n                deviceBreakpoints {\n                  name\n                  visible\n                  width\n                }\n              },\n              crealytics {\n                beaconUrls\n                clickUrls\n              }\n              currentSaleUnitPrice {\n                price {\n                  amount\n                  currencyCode\n                }\n                quantity {\n                  amount\n                  uom\n                }\n              },\n              defaultQuantity {\n                  uom\n              },\n              depositCharge {\n                amount,\n                currencyCode\n              },\n              displayPrice,\n              displayPriceEstimated,\n              displayPriceQualifier,\n              formattedWeightRange,\n              formattedPriceRange,\n              id,\n              leadTime,\n              lineNumber\n              maxPersonalisedMessageLength,\n              name,\n              markedForDelete,\n              persistDefault,\n              productImageUrls {\n                  extraLarge,\n                  large,\n                  medium,\n                  small\n              }\n              productType,\n              promotion {\n                discount {\n                  type\n                }\n                groups {\n                  threshold\n                  name\n                  lineNumbers\n                }\n                hidden\n                myWaitrosePromotion\n                promotionDescription\n                promotionId\n                promotionTypeCode\n                wasDisplayPrice\n              },\n              promotions {\n                discount {\n                  type\n                }\n                groups {\n                  threshold\n                  name\n                  lineNumbers\n                }\n                hidden\n                myWaitrosePromotion\n                promotionDescription\n                promotionId\n                promotionTypeCode\n                wasDisplayPrice\n              },\n              restriction {\n                  availableDates {\n                      restrictionId,\n                      startDate,\n                      endDate,\n                      cutOffDate\n                  },\n              },\n              resultType,\n              reviews {\n                averageRating\n                reviewCount\n              },\n              size,\n              sponsored,\n              sponsorshipId,\n              substitutionsProhibited,\n              thumbnail\n              typicalWeight {\n                amount\n                uom\n              }\n              servings {\n                min\n                max\n              }\n              weights {\n                  uoms ,\n                  pricePerUomQualifier,\n                  perUomQualifier,\n                  defaultQuantity {\n                      amount,\n                      uom\n                  },\n                  servings {\n                      max,\n                      min\n                  },\n                  sizeDescription\n              },\n              productTags {\n                name\n                tooltip\n              },\n              marketingBadges {\n                name\n              },\n            }query(\n  $customerId: String!\n  $withRecommendations: Boolean!\n  $size: Int\n  $start: Int\n  $category: String\n  $filterTags: [filterTag]\n  $recommendationsSize: Int\n  $recommendationsStart: Int\n  $sortBy: String\n  $trolleyId: String\n  $withFallback: Boolean\n) {\n  getProductListPage(\n    category: $category\n    customerId: $customerId\n    filterTags: $filterTags\n    recommendationsSize: $recommendationsSize\n    recommendationsStart: $recommendationsStart\n    size: $size\n    start: $start\n    sortBy: $sortBy\n    trolleyId: $trolleyId\n    withFallback: $withFallback\n  ) {\n  productGridData {\n      failures{\n          field\n          message\n          type\n      }\n      componentsAndProducts {\n        __typename\n        ... on GridProduct {\n          searchProduct {\n            ...ProductPod\n          }\n        }\n        ... on GridCmsComponent {\n          aemComponent\n        }\n        ... on GridSponsoredBannerComponent {\n          sponsoredBanner\n        }\n      }\n      conflicts {\n        messages\n        outOfStock\n        priority\n        productId\n        prohibitedActions\n        resolutionActions\n        nextSlotDate\n    }\n      criteria {\n        alternative\n        sortBy\n        filters {\n          group\n          filters {\n            applied\n            filterTag {\n              count\n              group\n              id\n              text\n              value\n            }\n          }\n        }\n        searchTags {\n          group\n          text\n          value\n        }\n        suggestedSearchTags {\n          group\n          text\n          value\n        }\n      }\n      locations {\n        header\n        masthead\n        seoContent\n      }\n      metaData {\n        description\n        title\n        keywords\n        turnOffIndexing\n        pageTitle\n        canonicalTag\n      }\n      productsInResultset\n      relevancyWeightings\n      searchTime\n      showPageTitle\n      subCategories {\n        name\n        categoryId\n        expectedResults\n        hiddenInNav\n      }\n      totalMatches\n      totalTime\n    }\n    recommendedProducts @include(if: $withRecommendations) {\n      failures{\n        field\n        message\n        type\n      }\n      fallbackRecommendations\n      products {\n        ...ProductFragment\n        metadata {\n          recToken\n          monetateId\n        }\n      }\n      totalResults\n    }\n  }\n}\n',
        'variables': {
            'start': start,
            'size': page_size,
            'sortBy': 'MOST_POPULAR',
            'trolleyId': trolleyId,
            'recommendationsSize': 0,
            'withRecommendations': False,
            'withFallback': False,
            'category': category_id,
            'customerId': customerId,
            'filterTags': [],
        },
    }

    response = requests.post(GRAPHQL_URL, headers=headers, json=json_data)
    data = response.json()
    try:
        status_code = data.get("data",{}).get("getProductListPage",{}).get("productGridData",{}).get("failures",[])[0].get("type")
    except:
        status_code = 200

    if response.status_code != 200 or status_code == "401":

        print(f"Error: {response.status_code}")
        print(response.text[:300])
        break

    grid_data = data.get('data', {}).get('getProductListPage', {}).get('productGridData', {})
    products = grid_data.get('componentsAndProducts', [])
    total = grid_data.get('totalMatches', 0)

    count = 0
    for item in products:
        product = item.get('searchProduct', {})
        if product:
            pid = product.get('lineNumber')
            if pid:
                id_products.append(pid)
                count += 1

    print(f"Fetched {count} products (start={start})")

    if count < page_size:
        break

    start += page_size

if status_code != "401":
    unique_products = list(dict.fromkeys(id_products))

    print(f"Done! Total products fetched: {len(unique_products)}")
    print(unique_products)
# ==================== 7 fetch data product  ===============================================

for product_id in unique_products:

    response = requests.get(
        f'https://www.waitrose.com/api/products-prod/v1/products/{product_id}?view=EXTENDED&filterByCustomerSlot=false&trolleyId=-1',
        headers=headers,
    )
    print(response.json())