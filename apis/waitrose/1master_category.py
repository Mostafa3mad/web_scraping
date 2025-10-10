import requests


headers = {
    'User-Agent': 'MobileApp/3.3.0.12910.12910 (Google; sdk_gphone64_x86_64; Android 14)',
    'Authorization': 'Bearer unauthenticated',
}



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
                all_ids.append(lvl3["id"])

print(all_ids)
print(len(all_ids))
