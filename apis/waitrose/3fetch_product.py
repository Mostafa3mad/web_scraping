import requests


headers = {
    'User-Agent': 'MobileApp/3.3.0.12910.12910 (Google; sdk_gphone64_x86_64; Android 14)',
    'Authorization': 'Bearer unauthenticated',
}



response = requests.get(
    'https://www.waitrose.com/api/products-prod/v1/products/947413?view=EXTENDED&filterByCustomerSlot=false&trolleyId=-1',
    headers=headers,
)
print(response.json())