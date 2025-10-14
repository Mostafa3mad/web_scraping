import requests


headers = {
    'User-Agent': 'MobileApp/3.3.0.12910.12910 (Google; sdk_gphone64_x86_64; Android 14)',
    'authorization': 'Bearer eyJraWQiOiJTc2J6a2JEVFJXa19zTTk1SXc2d2hhaGZfQmt4Q2FPZmxHX3RabzNzYlZVIiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiI3MDE4MjI1MDEiLCJodHRwOi8vd2FpdHJvc2UuY29tL3ByaW5jaXBhbF9pZCI6IjcwMTgyMjUwMSIsImh0dHA6Ly93YWl0cm9zZS5jb20vdG91Y2hwb2ludCI6IldFQl9BUFAiLCJodHRwOi8vd2FpdHJvc2UuY29tL3Blcm1pc3Npb25zIjpbXSwiaXNzIjoid2FpdHJvc2UuY29tIiwiaHR0cDovL3dhaXRyb3NlLmNvbS9yb2xlcyI6W10sImh0dHA6Ly93YWl0cm9zZS5jb20vY2xpZW50X2lkIjoiek1acUcxbWQyWnRTRXFWMzRDWkxDRnRaMHkzRV9WTTVnaUM5NlhTYk5nWSIsImh0dHA6Ly93YWl0cm9zZS5jb20vY3VzdG9tZXJfaWQiOiI3MDE4MjI1MDEiLCJhdWQiOiJ3YWl0cm9zZS5jb20iLCJuYmYiOjE3NjAzNTgzOTgsImh0dHA6Ly93YWl0cm9zZS5jb20vY3VzdG9tZXJfZW1haWwiOiJ0YXpqYW1teUBnbWFpbC5jb20iLCJzY29wZSI6WyJvcGVuaWQiXSwiZXhwIjoxNzYwMzU5Mjk4LCJpYXQiOjE3NjAzNTgzOTgsImp0aSI6IjA4NTg0MDA4LWYzNjQtNDg1MS1iODlkLWMyOTVkZmI0YWMwYiJ9.iJCFztFq3h804INFzinrs2Lp2jr8Wl_KAKYd6HRykz06_UKHgToWOj0xsGvZVdRGI9KoXawO8BTyB_5t6w6Nrs2mE0enHtzuduyywL-ND15Tr1NpkefuZIAm87iFRlhX5BHbCvsW0WGWXB_7Vgpsbh8yr1Ohs-wj6PDsUnnuMMksYprlYFIyVxobjn1bUzMQ4dGI-Y2JcfMi49KlD06npyaLYKG_lfcBgZ3Z3LjcUpM8UfNtOfkH9AYILKW5NjeK_GxLXwb9k4JBnCvute33qlNxgBK2QX4npjZQEv55ZgQgjiAfj5hAZRuSfMYGne6YOV3FwB6rD-RNNES22G2Zsg',
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
                all_ids.append(f"{lvl3["id"]}|{lvl3["shortName"]}")

print(all_ids)
print(len(all_ids))
