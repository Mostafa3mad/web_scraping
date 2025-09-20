import requests



all_menus = []
headers = {
    'Authorization': 'Bearer 1|SFS9MMnn5deflq0BMcUTSijwSMBB4mc7NSG2rOhqb2765466',
    'User-Agent': 'Mozilla/5.0 (Linux; Android 14; sdk_gphone64_x86_64 Build/UE1A.230829.050; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/113.0.5672.136 Mobile Safari/537.36',
}


venu_id = 7923
salesArea_id = 1110

all_menus_from_venue = f"https://ca.jdw-apps.net/api/v0.1/jdw/venues/{venu_id}/sales-areas/{salesArea_id}/menus?type=available"

response = requests.get(all_menus_from_venue, headers=headers)
data = response.json()
menus = data.get('data',[])
for menu in menus:
    menuName = menu.get('name')
    id = menu.get('id')
    all_menus.append({
        'menuName': menuName,
        'id': id
    })
print(all_menus)