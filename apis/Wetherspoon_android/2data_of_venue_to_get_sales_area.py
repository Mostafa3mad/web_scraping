import requests



salesAreas_ids = []
headers = {
    'Authorization': 'Bearer 1|SFS9MMnn5deflq0BMcUTSijwSMBB4mc7NSG2rOhqb2765466',
    'User-Agent': 'Mozilla/5.0 (Linux; Android 14; sdk_gphone64_x86_64 Build/UE1A.230829.050; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/113.0.5672.136 Mobile Safari/537.36',
}

venu_id = 7923

url_get_data_of_venue = f'https://ca.jdw-apps.net/api/v0.1/venues/{venu_id}'
response = requests.get(url_get_data_of_venue, headers=headers)
data = response.json()

salesAreas = data.get('data',{}).get('salesAreas',[])

for salesArea in salesAreas:

    id = salesArea.get('id',"")

    salesAreas_ids.append({
        'salesArea_id': id
    })


print(salesAreas_ids)