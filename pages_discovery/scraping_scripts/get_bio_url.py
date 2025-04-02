import requests
import json
import urllib.parse
import execjs
import random
from params import XBOGUS_JS_FILE

user_agent = 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:136.0) Gecko/20100101 Firefox/136.0'
headers = {
    'User-Agent': user_agent,
}

params = {
    'WebIdLastTime': '1743513832',
    'abTestVersion': '[object Object]',
    'aid': '1988',
    'appType': 'm',
    'app_language': 'en',
    'app_name': 'tiktok_web',
    'browser_language': 'en-US',
    'browser_name': 'Mozilla',
    'browser_online': 'true',
    'browser_platform': 'Linux x86_64',
    'browser_version': '5.0 (X11; Ubuntu)',
    'channel': 'tiktok_web',
    'cookie_enabled': 'true',
    'data_collection_enabled': 'false',
    'device_platform': 'web_pc',
    'focus_state': 'true',
    'from_page': 'user',
    'history_len': '9',
    'is_fullscreen': 'false',
    'is_page_visible': 'true',
    'language': 'en',
    'needAudienceControl': 'true',
    # 'odinId': '7488334863024686102',
    'os': 'linux',
    #'priority_region': '',
    #'referer': '',
    'region': 'FR',
    'screen_height': '1050',
    'screen_width': '1680',
    #'secUid': '',
    'tz_name': 'Europe/Paris',
    'user_is_login': 'false',
    'verifyFp': 'verify_m8yj8hlz_Cr9yoWik_AJqE_4Aor_87M4_VbsBJdBiOLuZ',
    'webcast_language': 'en',
    'msToken': 'XMqSKbyekYaE0p-f-CAEukFxhxv9LnuvIvVJ5haqWAZZorZbZYK4dItKMHOaN3MxvdjDD37E_bZ_B_jIv-mq-vacfXy_Prg08oMILD4c6K-JOO1fDDRaP4DO-BQLPSgUB9oM30oH65uARzFcMtHk',
    'X-Bogus': 'DFSzsIVL1RiANanqthSzPuRFp2al',
    '_signature': '_02B4Z6wo00001Z7wTGQAAIDB8D.Y7LVJjAme8kjAAABY29',
}

params['device_id'] = str(random.randint(10**17, 10**18 - 1))
params['uniqueId'] = 'lemondefr'
params = urllib.parse.urlencode(params, safe='=').replace('+', '%20')
url = f'https://www.tiktok.com/api/user/detail/?{params}'

query = urllib.parse.urlparse(url).query
xbogus = execjs.compile(open(XBOGUS_JS_FILE).read()).call('sign', query, user_agent)
url = url + "&X-Bogus=" + xbogus

response = requests.get(url, headers=headers)

try:
    json_resp = json.loads(response.text)
    user_info = json_resp['userInfo']['user']
except KeyError as e:
    print("Key not found in the response, error is:", e)
except json.JSONDecodeError:
    print("Failed to decode JSON response.")
except requests.exceptions.RequestException as e:
    print(f"Request failed: {e}")

try:
    print(user_info['bioLink']['link'])
except KeyError as e:
    print("No bio for this user...")