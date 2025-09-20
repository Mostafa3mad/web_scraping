import requests

headers = {
    'Authorization': 'Bearer 1|SFS9MMnn5deflq0BMcUTSijwSMBB4mc7NSG2rOhqb2765466',
    'User-Agent': 'Mozilla/5.0 (Linux; Android 14; sdk_gphone64_x86_64 Build/UE1A.230829.050; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/113.0.5672.136 Mobile Safari/537.36',

}

url_all_venues = "https://ca.jdw-apps.net/api/v0.1/venues"


venu_list = []
response = requests.get(url_all_venues, headers=headers)
data = response.json()
venueRefs =data.get('data',[])
for venue in venueRefs:

    venueName = venue.get('name')
    venueRef = venue.get('venueRef')

    venu_list.append({
        'venueName': venueName,
        'venueRef': venueRef
    })

print(venu_list)
