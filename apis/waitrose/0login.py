import requests




session = requests.Session()
response = session.get('https://auth.waitrose.com/v1/csrf')
_csrf = response.json().get('token')
print("_csrf:", _csrf)
cookies = session.cookies

jsession_id = cookies.get("JSESSIONID_AS")
print("jsession_id:", jsession_id)
# #_____________________________________________________________________________________________

url = "https://auth.waitrose.com/v1/login"
headers = {
    "Host": "auth.waitrose.com",
    "X-Csrf-Token": _csrf,
}

cookies = {
    "JSESSIONID_AS": jsession_id,
}

data = {
    "_csrf": _csrf,
    "email": "tazjammy@gmail.com",
    "password": "RPbhqq6gr#@k7Q"
}
session = requests.Session()
response = session.post(url, headers=headers, cookies=cookies, data=data, allow_redirects=False)

print("Status code:", response.status_code)
print("Headers:", response.headers)
print("Text:", response.text[:1000])
override_session_url = response.headers['Location']
print(override_session_url)
print(session.cookies)

# #_____________________________________________________________________________________________
response = session.get(override_session_url, allow_redirects=False)
print("Status code:", response.status_code)
print("Headers:", response.headers)
authorization_url = response.headers['Location']
print(authorization_url)

# #_____________________________________________________________________________________________
response = session.get(authorization_url, allow_redirects=False)
print("Status code:", response.status_code)
print("Headers:", response.headers)
oauth2_url = response.headers['Location']
print(oauth2_url)
# #_____________________________________________________________________________________________
response = session.get(oauth2_url, allow_redirects=False)
print("Status code:", response.status_code)
print("Headers:", response.headers)
code_url = response.headers['Location']
print(code_url)
# #_____________________________________________________________________________________________
response = session.get(code_url, allow_redirects=False)
print("Status code:", response.status_code)
print("Headers:", response.headers)
override_session2_url = response.headers['Location']
print(override_session2_url)
JSESSIONID_TC = session.cookies.get("JSESSIONID_TC")
# #_____________________________________________________________________________________________
response = session.get(override_session2_url, allow_redirects=False)
print("Status code:", response.status_code)
print("Headers:", response.headers)
waitrose_url = response.headers['Location']
print(waitrose_url)
#_____________________________________________________________________________________________
response = session.get(waitrose_url)
print("Status code:", response.status_code)
print("Headers:", response.headers)
# #_____________________________________________________________________________________________
headers = {
    'Authorization': 'Bearer unauthenticated',
}

csrf_url = "https://www.waitrose.com/api/token-client-prod/v1/csrf"

response = session.get(csrf_url,headers=headers)

_csrf_header = response.json().get('token')
print("_csrf_header:", _csrf_header)
# #_____________________________________________________________________________________________
headers["X-Csrf-Token"] = _csrf_header

api_clint = "https://www.waitrose.com/api/token-client-prod/v1/token"


response = session.post(api_clint, headers=headers)
print("Status code:", response.status_code)
print(response.text)