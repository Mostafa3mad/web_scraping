import requests

# Configure Burp Suite proxy
proxies = {
    "http": "http://192.168.0.102:8888",  # Proxy for HTTP traffic
    "https": "http://192.168.0.102:8888",  # Proxy for HTTPS traffic
}

# Define headers for the request
headers = {
    'accept-language': 'en-US,en;q=0.9,ar;q=0.8',
}

# Define parameters for the request
params = {
    'delivery_type': 'hungerstation',
    'lat': '1',
    'long': '1',
}

# Vendor ID
vender_id = '105126'

# Send the request through the Burp Suite proxy
response = requests.get(f'https://hungerstation.com/menuxp/v2/menu/{vender_id}', params=params, headers=headers, proxies=proxies, verify=False)

if response.status_code == 200:
    print(response.json())  # Print or process the response data
else:
    print(f"Failed to fetch the data. Response status: {response.status_code}")
