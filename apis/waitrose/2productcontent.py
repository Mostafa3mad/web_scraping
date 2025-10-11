import requests




headers = {
    'User-Agent': 'MobileApp/3.3.0.12910.12910 (Google; sdk_gphone64_x86_64; Android 14)',
    'Authorization': 'Bearer eyJraWQiOiJTc2J6a2JEVFJXa19zTTk1SXc2d2hhaGZfQmt4Q2FPZmxHX3RabzNzYlZVIiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiI3MDE4MjI1MDEiLCJodHRwOi8vd2FpdHJvc2UuY29tL3ByaW5jaXBhbF9pZCI6IjcwMTgyMjUwMSIsImh0dHA6Ly93YWl0cm9zZS5jb20vdG91Y2hwb2ludCI6IldFQl9BUFAiLCJodHRwOi8vd2FpdHJvc2UuY29tL3Blcm1pc3Npb25zIjpbXSwiaXNzIjoid2FpdHJvc2UuY29tIiwiaHR0cDovL3dhaXRyb3NlLmNvbS9yb2xlcyI6W10sImh0dHA6Ly93YWl0cm9zZS5jb20vY2xpZW50X2lkIjoiek1acUcxbWQyWnRTRXFWMzRDWkxDRnRaMHkzRV9WTTVnaUM5NlhTYk5nWSIsImh0dHA6Ly93YWl0cm9zZS5jb20vY3VzdG9tZXJfaWQiOiI3MDE4MjI1MDEiLCJhdWQiOiJ3YWl0cm9zZS5jb20iLCJuYmYiOjE3NjAxODczNzEsImh0dHA6Ly93YWl0cm9zZS5jb20vY3VzdG9tZXJfZW1haWwiOiJ0YXpqYW1teUBnbWFpbC5jb20iLCJzY29wZSI6WyJvcGVuaWQiXSwiZXhwIjoxNzYwMTg4MjcxLCJpYXQiOjE3NjAxODczNzEsImp0aSI6ImMyN2IzMTY2LWRkN2EtNDBmMC04ZGJlLWI1ZjUxNTcyZGRmYSJ9.al0-bL3Zv0R8H-0Ux-WcKHJT_-6pNpBta4GdGgQr9aNi4RhTWg2WMcZ6UhTL_N8Wq64Fi8G2Ia3dgRTZ91R2iKI8ITHfERay3O43-_eiw_QQYkAcbD7OTU92DWKx5JbQCxyW4uPfuIjiT8uR64NIyi9a4KRygXo-LoT2Rny28Tv4wmbQ4OhIUzxG1X2FVL2JJS4emRjbtGhloJ28lv1naiNTV_NSeAxin0Y44wYtwtMn1k2IA7MRqBQCIczLWh-9lgZNvJfgEIy_AVdd07VHyxZ52eEwjPAUELr7cDuzCNC2F1O-eOP19KquZcFoeyB7Gg0Hwh8UvROY7nHgZLGf6A',
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
