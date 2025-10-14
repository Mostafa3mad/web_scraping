import requests


trolleyId = '1029093420'
customerId = '701822501'
branchId = "750"
postcode = "EH11 1AA"
addressId = "53322204"
startDateTime = "2025-10-15T11:00:00+01:00"
endDateTime = "2025-10-15T12:00:00+01:00"

slot_payload = {
    "branchId": branchId,
    "slotType": "DELIVERY",
    "customerOrderId": trolleyId,
    "customerId": customerId,
    'postcode': postcode,
    'addressId': addressId,

    "startDateTime": startDateTime,
    "endDateTime": endDateTime,
    "slotGridType": "DEFAULT_GRID",
    "greenSlot": False
}

headers = {
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,ar;q=0.7',
    'authorization': 'Bearer eyJraWQiOiJTc2J6a2JEVFJXa19zTTk1SXc2d2hhaGZfQmt4Q2FPZmxHX3RabzNzYlZVIiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiI3MDE4MjI1MDEiLCJodHRwOi8vd2FpdHJvc2UuY29tL3ByaW5jaXBhbF9pZCI6IjcwMTgyMjUwMSIsImh0dHA6Ly93YWl0cm9zZS5jb20vdG91Y2hwb2ludCI6IldFQl9BUFAiLCJodHRwOi8vd2FpdHJvc2UuY29tL3Blcm1pc3Npb25zIjpbXSwiaXNzIjoid2FpdHJvc2UuY29tIiwiaHR0cDovL3dhaXRyb3NlLmNvbS9yb2xlcyI6W10sImh0dHA6Ly93YWl0cm9zZS5jb20vY2xpZW50X2lkIjoiek1acUcxbWQyWnRTRXFWMzRDWkxDRnRaMHkzRV9WTTVnaUM5NlhTYk5nWSIsImh0dHA6Ly93YWl0cm9zZS5jb20vY3VzdG9tZXJfaWQiOiI3MDE4MjI1MDEiLCJhdWQiOiJ3YWl0cm9zZS5jb20iLCJuYmYiOjE3NjAzNjQxNTIsImh0dHA6Ly93YWl0cm9zZS5jb20vY3VzdG9tZXJfZW1haWwiOiJ0YXpqYW1teUBnbWFpbC5jb20iLCJzY29wZSI6WyJvcGVuaWQiXSwiZXhwIjoxNzYwMzY1MDUyLCJpYXQiOjE3NjAzNjQxNTIsImp0aSI6IjgxMDhjNzg1LTEzZjItNDA1Zi1iZmE1LTYxMmYzOWIwMDhhNyJ9.ndLJ2rDhHsEeBtk_VA-9WNoI4rxsGEYg_6yXTNKNq28yeUS8abhIMRhf12A9wTGmIvyMOE297FSP0Eru6qvFcSiYzYMh1su9-0dN4uEDjyscbd53WU8_wfMyNPfCkO4NQq54zmcJMD-cgLWtrKDZm9Tc9BVbtFcD2XkY3wcXoDPjSZRxZqNhmvLG1juToYQtFxIrIPmrKKiOSBx3Ehn39pwPD77B9tV9hX0WvE815NWoYKXr0g7lpoKEFvnWUwYiI7EjeiBrYAOSor1cDK5UbmxXOyI6QMGXW78NyzEtH-C83-CPJLumQ0P3MNu2VKWG2OGjWvZ5BIP-uFi7devUCA',
}


r = requests.post(
    "https://www.waitrose.com/api/slot-orchestration-prod/v1/slot-reservations",
    headers=headers,
    json=slot_payload
)

print("Slot booked:", r.status_code, r.text)


