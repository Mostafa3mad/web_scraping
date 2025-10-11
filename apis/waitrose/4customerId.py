import requests


headers = {
    'User-Agent': 'MobileApp/3.3.0.12910.12910 (Google; sdk_gphone64_x86_64; Android 14)',
    'Authorization': 'Bearer eyJraWQiOiJTc2J6a2JEVFJXa19zTTk1SXc2d2hhaGZfQmt4Q2FPZmxHX3RabzNzYlZVIiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiI3MDE4MzU3NTgiLCJodHRwOi8vd2FpdHJvc2UuY29tL3ByaW5jaXBhbF9pZCI6IjcwMTgzNTc1OCIsImh0dHA6Ly93YWl0cm9zZS5jb20vdG91Y2hwb2ludCI6IldFQl9BUFAiLCJodHRwOi8vd2FpdHJvc2UuY29tL3Blcm1pc3Npb25zIjpbXSwiaXNzIjoid2FpdHJvc2UuY29tIiwiaHR0cDovL3dhaXRyb3NlLmNvbS9yb2xlcyI6W10sImh0dHA6Ly93YWl0cm9zZS5jb20vY2xpZW50X2lkIjoiek1acUcxbWQyWnRTRXFWMzRDWkxDRnRaMHkzRV9WTTVnaUM5NlhTYk5nWSIsImh0dHA6Ly93YWl0cm9zZS5jb20vY3VzdG9tZXJfaWQiOiI3MDE4MzU3NTgiLCJhdWQiOiJ3YWl0cm9zZS5jb20iLCJuYmYiOjE3NjAxOTM4NjUsImh0dHA6Ly93YWl0cm9zZS5jb20vY3VzdG9tZXJfZW1haWwiOiJ0YWhpcmFqYW1pbDkwMEBnbWFpbC5jb20iLCJzY29wZSI6WyJvcGVuaWQiXSwiZXhwIjoxNzYwMTk0NzY1LCJpYXQiOjE3NjAxOTM4NjUsImp0aSI6ImFmNDcyYzQwLTdjMTEtNGVjNS1iMDcwLWZlOTY5OGRiNGE2MCJ9.ozp9OQYpHNVaFOLhY-GO8MN2wKTc5kXmRUF0cw1KLQO_X843n1G9l0LJvRd5QwyXaQ8yodFi8DR9aV9fOOzyCDYC7nFcJFX30b9ZZmXko2Pgpmy_Clkh3v9FfXD5QKY9kxSgygN2uMEw7v-tAWzKmK-pkr84HpFZH-aqHq1otO5Nn5DE2_mVpwzUPIVIsQ9tGwSOIZ_PFuPeEsKbXJDag_hOBCbhBNy7UkgfQGXkNFvKPXx6Mck6VEKRoWHn7Oj-w3M7R7rH5xzX0xcuGsqybXPhM5qeEw8raJCUZ2Kj_IXM7-8z-D4PB54J38Jcs90MBFj2nBRZ3SKiYiSCfZwr6w',
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