import requests


import requests


trolleyId = '1029093420'
customerId = '701822501'
branchId = "750"
postcode = "EH11 1AA"
addressId = "53322204"


headers = {
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,ar;q=0.7',
    'authorization': 'Bearer eyJraWQiOiJTc2J6a2JEVFJXa19zTTk1SXc2d2hhaGZfQmt4Q2FPZmxHX3RabzNzYlZVIiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiI3MDE4MjI1MDEiLCJodHRwOi8vd2FpdHJvc2UuY29tL3ByaW5jaXBhbF9pZCI6IjcwMTgyMjUwMSIsImh0dHA6Ly93YWl0cm9zZS5jb20vdG91Y2hwb2ludCI6IldFQl9BUFAiLCJodHRwOi8vd2FpdHJvc2UuY29tL3Blcm1pc3Npb25zIjpbXSwiaXNzIjoid2FpdHJvc2UuY29tIiwiaHR0cDovL3dhaXRyb3NlLmNvbS9yb2xlcyI6W10sImh0dHA6Ly93YWl0cm9zZS5jb20vY2xpZW50X2lkIjoiek1acUcxbWQyWnRTRXFWMzRDWkxDRnRaMHkzRV9WTTVnaUM5NlhTYk5nWSIsImh0dHA6Ly93YWl0cm9zZS5jb20vY3VzdG9tZXJfaWQiOiI3MDE4MjI1MDEiLCJhdWQiOiJ3YWl0cm9zZS5jb20iLCJuYmYiOjE3NjAzNjQxNTIsImh0dHA6Ly93YWl0cm9zZS5jb20vY3VzdG9tZXJfZW1haWwiOiJ0YXpqYW1teUBnbWFpbC5jb20iLCJzY29wZSI6WyJvcGVuaWQiXSwiZXhwIjoxNzYwMzY1MDUyLCJpYXQiOjE3NjAzNjQxNTIsImp0aSI6IjgxMDhjNzg1LTEzZjItNDA1Zi1iZmE1LTYxMmYzOWIwMDhhNyJ9.ndLJ2rDhHsEeBtk_VA-9WNoI4rxsGEYg_6yXTNKNq28yeUS8abhIMRhf12A9wTGmIvyMOE297FSP0Eru6qvFcSiYzYMh1su9-0dN4uEDjyscbd53WU8_wfMyNPfCkO4NQq54zmcJMD-cgLWtrKDZm9Tc9BVbtFcD2XkY3wcXoDPjSZRxZqNhmvLG1juToYQtFxIrIPmrKKiOSBx3Ehn39pwPD77B9tV9hX0WvE815NWoYKXr0g7lpoKEFvnWUwYiI7EjeiBrYAOSor1cDK5UbmxXOyI6QMGXW78NyzEtH-C83-CPJLumQ0P3MNu2VKWG2OGjWvZ5BIP-uFi7devUCA',
}


json_data = {
    'query': 'query slotDays($slotDaysInput: SlotDaysInput) {\n  slotDays(slotDaysInput: $slotDaysInput) {\n    content {\n      id\n      branchId\n      slotType\n      date\n      slots {\n        slotId: id\n        startDateTime\n        endDateTime\n        shopByDateTime\n        slotGridType\n        deliveryPassSlot\n        greenSlot\n        slotStatus: status\n        charge {\n          amount\n          currencyCode\n        }\n      }\n    }\n    variant\n    links {\n      rel\n      title\n      href\n    }\n    failures {\n      message\n      type\n    }\n  }\n}\n',
    'variables': {
        'slotDaysInput': {
            'branchId': branchId,
            'slotType': 'DELIVERY',
            'customerOrderId': trolleyId,
            'postcode': postcode,
            'addressId': addressId,
            'fromDate': '',
            'size': 7,
        },
    },
}

response = requests.post(
    'https://www.waitrose.com/api/graphql-prod/graph/live',
    headers=headers,
    json=json_data,
)


print(response.json())

dataslot = response.json()
slots = dataslot['data']['slotDays']['content'][0]['slots']
available_slots = [slot for slot in slots if slot['slotStatus'] == 'AVAILABLE']

if available_slots:
    first_available = available_slots[0]
    startDateTime = first_available['startDateTime']
    endDateTime = first_available['endDateTime']
    print("Start:", startDateTime)
    print("End:", endDateTime)
else:
    print("❌ No available slots found.")