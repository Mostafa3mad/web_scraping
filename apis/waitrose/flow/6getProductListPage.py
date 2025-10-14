import requests

headers = {
    'User-Agent': 'MobileApp/3.3.0.12910.12910 (Google; sdk_gphone64_x86_64; Android 14)',
    'authorization': 'Bearer eyJraWQiOiJTc2J6a2JEVFJXa19zTTk1SXc2d2hhaGZfQmt4Q2FPZmxHX3RabzNzYlZVIiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiI3MDE4MjI1MDEiLCJodHRwOi8vd2FpdHJvc2UuY29tL3ByaW5jaXBhbF9pZCI6IjcwMTgyMjUwMSIsImh0dHA6Ly93YWl0cm9zZS5jb20vdG91Y2hwb2ludCI6IldFQl9BUFAiLCJodHRwOi8vd2FpdHJvc2UuY29tL3Blcm1pc3Npb25zIjpbXSwiaXNzIjoid2FpdHJvc2UuY29tIiwiaHR0cDovL3dhaXRyb3NlLmNvbS9yb2xlcyI6W10sImh0dHA6Ly93YWl0cm9zZS5jb20vY2xpZW50X2lkIjoiek1acUcxbWQyWnRTRXFWMzRDWkxDRnRaMHkzRV9WTTVnaUM5NlhTYk5nWSIsImh0dHA6Ly93YWl0cm9zZS5jb20vY3VzdG9tZXJfaWQiOiI3MDE4MjI1MDEiLCJhdWQiOiJ3YWl0cm9zZS5jb20iLCJuYmYiOjE3NjAzNjU2MTUsImh0dHA6Ly93YWl0cm9zZS5jb20vY3VzdG9tZXJfZW1haWwiOiJ0YXpqYW1teUBnbWFpbC5jb20iLCJzY29wZSI6WyJvcGVuaWQiXSwiZXhwIjoxNzYwMzY2NTE1LCJpYXQiOjE3NjAzNjU2MTUsImp0aSI6ImY5MDk3YWEwLWMzMzYtNGJiYy05YmI5LTg4NGYwYjdmYWQ2OSJ9.ZKwe6qWVCKg3pch6L3FC7Ay5S7qvxImr356UdIhQxp4bbA-mKGAsdZN4CBhZEWUgh9NeJfaExFQ24fOeL-bB3s1XBbc7ZSRf72nMr_HaMeyp1_ejgYctxVEFyDW6EBo-R4EQ11RpLYIzIyQOmFDExaoOkB3PtsbvmgESfhxPnwLKgoyYAU1vWcq3Okq38No0SQ_bOkYtykfIAsH0ZtnmG1R6Lir_0XowM_Q12euS8ixM706CsXo1bRdJzqAvt6N5pnAS04iQHzNPXcqsy_mPdE2rfgRtTyNdSQ0WNRk-4ZjoEE3-_8xxBGa211NMhCDYETzmy05PxzGcQImJzBc82Q',
}

category_id = '041cf895-b845-4cad-bce2-94f7b4b7e8bf'
trolleyId = '1029093420'
customerId = '701822501'

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

        print(f"❌ Error: {response.status_code}")
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

