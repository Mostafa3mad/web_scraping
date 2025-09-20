import requests



products = []
headers = {
    'Authorization': 'Bearer 1|SFS9MMnn5deflq0BMcUTSijwSMBB4mc7NSG2rOhqb2765466',
    'User-Agent': 'Mozilla/5.0 (Linux; Android 14; sdk_gphone64_x86_64 Build/UE1A.230829.050; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/113.0.5672.136 Mobile Safari/537.36',
}


venu_id = 7923
salesArea_id = 1110
menu_id = 28369
all_products_from_menu = f"https://ca.jdw-apps.net/api/v0.1/jdw/venues/{venu_id}/sales-areas/{salesArea_id}/menus/{menu_id}"

response = requests.get(all_products_from_menu, headers=headers)
data = response.json()


categories = data.get('data',{}).get('categories',[])
for category in categories:
    categoryName = category.get('name',"")
    itemGroups = category.get('itemGroups',[])
    for itemGroup in itemGroups:
        items = itemGroup.get('items',[])
        itemGroup_name = itemGroup.get('name',"")
        for item in items:
            productName = item.get('name',"")
            productId = item.get('id',"")

            prices = item.get('options',{}).get('portion',{}).get('options',[])
            for price in prices:
                priceValue = price.get('value',{}).get('price',{}).get('value',"")
                port_size = price.get('label',"")

                products.append({
                    'categoryName': categoryName,
                    'itemGroup_name': itemGroup_name,
                    'productName': productName,
                    'priceValue': priceValue,
                    'port_size': port_size,
                    'productId': productId,

                })


for product in products:
    print("------------------")
    print("Category Name: ",product['categoryName'])
    print("Item Group Name: ",product['itemGroup_name'])
    print("Product Name: ",product['productName'])
    print("Product port_size: ",product['port_size'])
    print("Price Value: ",product['priceValue'])
    print("Product ID: ",product['productId'])