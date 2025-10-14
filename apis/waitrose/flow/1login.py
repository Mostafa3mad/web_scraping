import requests
# email = "tahirajamil900@gmail.com"
# password = "d^4mEj7W%8Z@6@"

email = "tazjammy@gmail.com"
password = "RPbhqq6gr#@k7Q"

session = requests.Session()
response = session.get('https://auth.waitrose.com/v1/csrf')
_csrf = response.json().get('token')
cookies = session.cookies
jsession_id = cookies.get("JSESSIONID_AS")
#_____________________________________________________________________________________________
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
    "email": email,
    "password": password
}
session = requests.Session()
response = session.post(url, headers=headers, cookies=cookies, data=data, allow_redirects=False)
override_session_url = response.headers['Location']
#_____________________________________________________________________________________________
response = session.get(override_session_url, allow_redirects=False)
authorization_url = response.headers['Location']
#_____________________________________________________________________________________________
response = session.get(authorization_url, allow_redirects=False)
oauth2_url = response.headers['Location']
#_____________________________________________________________________________________________
response = session.get(oauth2_url, allow_redirects=False)
code_url = response.headers['Location']
#_____________________________________________________________________________________________
response = session.get(code_url, allow_redirects=False)
override_session2_url = response.headers['Location']
JSESSIONID_TC = session.cookies.get("JSESSIONID_TC")
#_____________________________________________________________________________________________
response = session.get(override_session2_url, allow_redirects=False)
waitrose_url = response.headers['Location']
#_____________________________________________________________________________________________
response = session.get(waitrose_url)
#_____________________________________________________________________________________________
headers = {
    'Authorization': 'Bearer unauthenticated',
}
csrf_url = "https://www.waitrose.com/api/token-client-prod/v1/csrf"
response = session.get(csrf_url,headers=headers)
_csrf_header = response.json().get('token')
#_____________________________________________________________________________________________
headers["X-Csrf-Token"] = _csrf_header
api_clint = "https://www.waitrose.com/api/token-client-prod/v1/token"
response = session.post(api_clint, headers=headers)

Authorization = f"Authorization : Bearer {response.json().get('accessToken')}"
print(Authorization)