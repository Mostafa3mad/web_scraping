import requests


headers = {
    'User-Agent': 'MobileApp/3.3.0.12910.12910 (Google; sdk_gphone64_x86_64; Android 14)',
    'authorization': 'Bearer eyJraWQiOiJTc2J6a2JEVFJXa19zTTk1SXc2d2hhaGZfQmt4Q2FPZmxHX3RabzNzYlZVIiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiI3MDE4MjI1MDEiLCJodHRwOi8vd2FpdHJvc2UuY29tL3ByaW5jaXBhbF9pZCI6IjcwMTgyMjUwMSIsImh0dHA6Ly93YWl0cm9zZS5jb20vdG91Y2hwb2ludCI6IldFQl9BUFAiLCJodHRwOi8vd2FpdHJvc2UuY29tL3Blcm1pc3Npb25zIjpbXSwiaXNzIjoid2FpdHJvc2UuY29tIiwiaHR0cDovL3dhaXRyb3NlLmNvbS9yb2xlcyI6W10sImh0dHA6Ly93YWl0cm9zZS5jb20vY2xpZW50X2lkIjoiek1acUcxbWQyWnRTRXFWMzRDWkxDRnRaMHkzRV9WTTVnaUM5NlhTYk5nWSIsImh0dHA6Ly93YWl0cm9zZS5jb20vY3VzdG9tZXJfaWQiOiI3MDE4MjI1MDEiLCJhdWQiOiJ3YWl0cm9zZS5jb20iLCJuYmYiOjE3NjAzNjQxNTIsImh0dHA6Ly93YWl0cm9zZS5jb20vY3VzdG9tZXJfZW1haWwiOiJ0YXpqYW1teUBnbWFpbC5jb20iLCJzY29wZSI6WyJvcGVuaWQiXSwiZXhwIjoxNzYwMzY1MDUyLCJpYXQiOjE3NjAzNjQxNTIsImp0aSI6IjgxMDhjNzg1LTEzZjItNDA1Zi1iZmE1LTYxMmYzOWIwMDhhNyJ9.ndLJ2rDhHsEeBtk_VA-9WNoI4rxsGEYg_6yXTNKNq28yeUS8abhIMRhf12A9wTGmIvyMOE297FSP0Eru6qvFcSiYzYMh1su9-0dN4uEDjyscbd53WU8_wfMyNPfCkO4NQq54zmcJMD-cgLWtrKDZm9Tc9BVbtFcD2XkY3wcXoDPjSZRxZqNhmvLG1juToYQtFxIrIPmrKKiOSBx3Ehn39pwPD77B9tV9hX0WvE815NWoYKXr0g7lpoKEFvnWUwYiI7EjeiBrYAOSor1cDK5UbmxXOyI6QMGXW78NyzEtH-C83-CPJLumQ0P3MNu2VKWG2OGjWvZ5BIP-uFi7devUCA'
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


data_customer = response.json()['data']['shoppingContext']
data_address = response.json()['data']['addresses'][0]
trolleyId = data_customer['customerOrderId']
customerId = data_customer['customerId']
branchId = data_customer['defaultBranchId']
addressId = data_address["id"]
postcode = data_address["postalCode"]

print(trolleyId)
print(customerId)
print(branchId)
print(addressId)
print(postcode)