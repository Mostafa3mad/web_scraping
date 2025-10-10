import requests




headers = {
    'User-Agent': 'MobileApp/3.3.0.12910.12910 (Google; sdk_gphone64_x86_64; Android 14)',
    'Authorization': 'Bearer unauthenticated',
}


json_data = {
    'customerSearchRequest': {
        'queryParams': {
            'category': '335082',
            'filterTags': [],
            'orderId': -1,
            'sortBy': 'MOST_POPULAR',
            'start': 0,
        },
    },
}

response = requests.post(
    'https://www.waitrose.com/api/content-prod/v2/cms/publish/productcontent/browse/-1?clientType=WEB_APP',
    headers=headers,
    json=json_data,
)

data = response.json()
componentsAndProducts = data.get('componentsAndProducts', [])


id_products = []
for componentsAndProduct in componentsAndProducts:

    componentsAndProductId = componentsAndProduct.get('searchProduct', {}).get('lineNumber', [])
    if componentsAndProductId:
        id_products.append(componentsAndProductId)


print(id_products)
print(len(id_products))
