import requests


headers = {
    'User-Agent': 'MobileApp/3.3.0.12910.12910 (Google; sdk_gphone64_x86_64; Android 14)',
    'Authorization': 'Bearer eyJraWQiOiJTc2J6a2JEVFJXa19zTTk1SXc2d2hhaGZfQmt4Q2FPZmxHX3RabzNzYlZVIiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiI3MDE4MjI1MDEiLCJodHRwOi8vd2FpdHJvc2UuY29tL3ByaW5jaXBhbF9pZCI6IjcwMTgyMjUwMSIsImh0dHA6Ly93YWl0cm9zZS5jb20vdG91Y2hwb2ludCI6IldFQl9BUFAiLCJodHRwOi8vd2FpdHJvc2UuY29tL3Blcm1pc3Npb25zIjpbXSwiaXNzIjoid2FpdHJvc2UuY29tIiwiaHR0cDovL3dhaXRyb3NlLmNvbS9yb2xlcyI6W10sImh0dHA6Ly93YWl0cm9zZS5jb20vY2xpZW50X2lkIjoiek1acUcxbWQyWnRTRXFWMzRDWkxDRnRaMHkzRV9WTTVnaUM5NlhTYk5nWSIsImh0dHA6Ly93YWl0cm9zZS5jb20vY3VzdG9tZXJfaWQiOiI3MDE4MjI1MDEiLCJhdWQiOiJ3YWl0cm9zZS5jb20iLCJuYmYiOjE3NjAxODg2MjUsImh0dHA6Ly93YWl0cm9zZS5jb20vY3VzdG9tZXJfZW1haWwiOiJ0YXpqYW1teUBnbWFpbC5jb20iLCJzY29wZSI6WyJvcGVuaWQiXSwiZXhwIjoxNzYwMTg5NTI1LCJpYXQiOjE3NjAxODg2MjUsImp0aSI6IjllYzM4YzVmLWE4YTItNGUzYi1iMzYzLTJkNWM0NTViZDEzNCJ9.h2E-rd17xZIr-usLT3VgM1fwKaBl1ZHl6havgrrhj6ExmNYUm_WNroXdi9QAD-WKUI7_AC0uSjZyUoZJmE71QnnyRUBki8sEbCxGgfyAf9KzV4INhPt-Xm8bJqnOTOdELbaAY7_fSUgIgglRaJqR1oDgYzBrQEUoWkNSUkYwii33_V2bdroCLDQlPhx1qqwP0Md4Zw_ZSTssFSGsQacXpvBbdesjfi30WUkA5PaAM-Q_M8EJ_mbeRDDSnRPdjJI9tFVWBdS7BcZHYfLTrA1HfwJF4eR87cP5skksc2-u68db3JZL2iuRHZP_bHUhN7r3yIqb95FF8mZSsqCgdoaRiQ',
}



json_data = {
    "query": """
    query {
      shoppingContext {
        customerId
        customerOrderId
        customerOrderState
        defaultBranchId
      }
      addresses {
        id
        line1
        line2
        town
        region
        postalCode
        country
        addressee {
          title
          firstName
          lastName
          contactNumber
        }
      }
    }
    """
}

response = requests.post(
    'https://www.waitrose.com/api/graphql-prod/graph/live',
    headers=headers,
    json=json_data,
)

print(response.json())