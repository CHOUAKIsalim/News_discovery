import requests
import json
import urllib.parse
import execjs
import random
from params import XBOGUS_JS_FILE
import time


def get_bio_url(uniqueId, logger):

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
    params['uniqueId'] = uniqueId
    params = urllib.parse.urlencode(params, safe='=').replace('+', '%20')
    url = f'https://www.tiktok.com/api/user/detail/?{params}'

    query = urllib.parse.urlparse(url).query
    xbogus = execjs.compile(open(XBOGUS_JS_FILE).read()).call('sign', query, user_agent)
    url = url + "&X-Bogus=" + xbogus

    retry = 0
    while retry < 5:
        try:
            response = requests.get(url, headers=headers)
        except Exception as e:
            logger.error(f'Failed to make request.\nError is:\n' + str(e) + '\nRetrying...')
            retry +=1
            time.sleep(5)
            continue

        try:
            json_resp = json.loads(response.text)
            user_info = json_resp['userInfo']['user']
            return user_info['bioLink']['link'] if 'bioLink' in user_info else None
        except KeyError as e:
            logger.error(f"Key not found in the response, error is: {e} for uniqueId: {uniqueId}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON response, error is: {e} for uniqueId: {uniqueId}")
            retry += 1
            time.sleep(5)
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e} for uniqueId: {uniqueId}")
            retry += 1
            time.sleep(5)
        
        
    return None

def add_users_bio_url_to_posts(posts, logger):
    """
    Add the bio URL to the posts dictionary.
    """
    logger.info("Adding bio URL to posts...")
    for post in posts:
        uniqueId = post['username']
        post['bioLink'] = get_bio_url(uniqueId, logger)
    logger.info("Bio URL added to posts.")
    return posts