import json, requests, time
import pandas as pd
from os import path
from datetime import datetime,timedelta
from params import CROWDTANGLE_TOKEN, ERROR_TYPES, TIME_DELAY_CROWDTANGLE, DIR_KEYWORD_POSTS
from database_scripts.utils import COLSET_CT_POST, COLSET_TIKTOK_POST
from database_scripts.save_posts import save_posts
from crawling_scripts.utils import attribute_posts_to_keywords
import os
import re
import numpy as np

########################################################################

def get_keywords_to_search(df_news):
    df_news = df_news[df_news.first_keyword!='']
    df_searchterm1 = df_news[['first_keyword','hashed_first_kw']]
    df_searchterm2 = df_news[['second_keyword','hashed_second_kw']]
    df_searchterm1.columns = ['keyword','hashed_keyword']
    df_searchterm2.columns = ['keyword','hashed_keyword']
    df_searchterms = pd.concat([df_searchterm1,df_searchterm2],ignore_index=True).drop_duplicates(subset='keyword',keep='first')
    df_searchterms = df_searchterms[df_searchterms.keyword!='']

    return df_searchterms

########################################################################

def get_leaves(item, key=None, key_prefix=""):

    if isinstance(item, dict):
        leaves = {}
        for item_key in item.keys():
                temp_key_prefix = (
                    item_key if (key_prefix == "") else (key_prefix + "_" + str(item_key)))
                leaves.update(get_leaves(
                    item[item_key], item_key, temp_key_prefix))
        return leaves
    
    elif isinstance(item, list):
        leaves = {}
        elements = []
        for element in item:
            if isinstance(element, dict) or isinstance(element, list):
                leaves.update(get_leaves(element, key, key_prefix))
            else:
                elements.append(element)
        if len(elements) > 0:
            leaves[key] = elements
        return leaves
    else:
        return {key_prefix: item}

def format_posts(item, post_formatting, logger):

    try:
        if 'posts' in item:
            for post in item['posts']:
                post['search_term'] = item['keyword']
                post['hashed_term'] = item['hashed_keyword']
            
            posts = [get_leaves(post) for post in item['posts']]

            for post in posts:
                cols = set(post.keys())
                missing_cols = post_formatting.difference(cols)
                for c in missing_cols:
                    post[c] = ''

    except Exception as e:
        logger.error("posts_extraction, format_posts")
        logger.error(f"{ERROR_TYPES.UNDEFINED}: {str(e)}")

    return posts

def call_api(url, logger):
    resp_content = '{}'
    time_cost = 10
    trials = 0
    time_delay = 30 
    while(trials < 3):
        try:
            logger.info(f"Requesting data from {url}")
            start_time = datetime.now()
            resp = requests.get(url,timeout=60)
            if resp.status_code == 200:
                exec_time = datetime.now() - start_time
                resp_content= resp.text
                time_cost = exec_time.total_seconds()
            else:
                logger.error(f'API error {resp.status_code}')
                resp_content='{}'
        except Exception as e:
            logger.error(f'Error: {e}')
            resp_content='{}'
        trials += 1
        if resp_content!='{}':
            break
        time.sleep(time_delay*trials)
    return resp_content,time_cost

def collect_posts_next(nextpage_url, logger):
    resp_text,exec_time = call_api(nextpage_url, logger)
    jsondata = json.loads(resp_text)
    if jsondata == {}:
        return [],''

    posts = jsondata['result']['posts']
    if 'nextPage' in jsondata['result']['pagination']: 
        nextpage_url = jsondata['result']['pagination']['nextPage']
    else:
        nextpage_url = ''
    logger.info(f'API return in {exec_time} seconds - {len(posts)} posts found')
    return posts,nextpage_url

def search_term_endpoint(token, terms, startDate, endDate=None, count=100, offset=0):
    url = "https://api.crowdtangle.com/posts/search?token={}".format(token)
    url += '&searchTerm="{}"'.format(terms)
    url += '&startDate={}'.format(startDate)
    if endDate:
        url += '&endDate={}'.format(endDate)
    url += '&count={}'.format(count)
    url += '&offset={}'.format(offset)
    url += '&platforms=facebook'
    url += '&sortBy=date'
    return url

def search_posts_for_keyword_crowdtangle(term,start_date,country,token,logger):
    
    start_date_datetime = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = (start_date_datetime + timedelta(days=1)).strftime('%Y-%m-%d')

    start_date = "{}T00:00:00".format(start_date)
    end_date = "{}T00:00:00".format(end_date)

    logger.info(f'Searching posts from {start_date} to {end_date}')
    
    api_url = search_term_endpoint(token=token,terms=term,startDate=start_date,endDate=end_date,count=100)
    resp_text,exec_time = call_api(api_url, logger)    
    time_delay = 30
    pcount = 0
    jsondata = json.loads(resp_text)

    if jsondata == {}:
        logger.info(f'API return in {exec_time} seconds - 0 post found')
        return []
    
    all_posts = []
    posts = jsondata['result']['posts']

    if 'nextPage' in jsondata['result']['pagination']: 
        nextpage_url = jsondata['result']['pagination']['nextPage']
    else:
        nextpage_url = ''
        
    if len(posts) > 0:
        logger.info(f'API return in {exec_time} seconds - {len(posts)} posts found')
        pcount += len(posts)        
        all_posts.extend(posts)
        
    if len(all_posts) >= 1000: #Search api return maximum 1000 posts -> can be solved by splitting into smaller time window
        return all_posts
    
    while nextpage_url!='':
        time.sleep(time_delay)
        posts,new_nextpage_url = collect_posts_next(nextpage_url, logger)
        pcount += len(posts)
        all_posts.extend(posts)
        nextpage_url = new_nextpage_url

    return all_posts

def get_tiktok_access_token():
    """Get access token using client key and secret."""
    response = os.popen(f"curl --location --request POST 'https://open.tiktokapis.com/v2/oauth/token/' --header 'Content-Type: application/x-www-form-urlencoded' --header 'Cache-Control: no-cache' --data-urlencode 'client_key={os.environ['TIKTOK_CLIENT_KEY']}' --data-urlencode 'client_secret={os.environ['TIKTOK_CLIENT_SECRET']}' --data-urlencode 'grant_type=client_credentials'").read()
    pattern = r'"access_token":"([^"]+)"'
    match = re.search(pattern, response)
    if match:
        return match.group(1)
    else:
        raise ValueError("Failed to get access token")

def query_tiktok_api(url, params, headers, data, retry_count, logger):
    response = requests.post(url, params=params, headers=headers, data=data)
    try:
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        while retry_count>0:
            logger.error(f'HTTP error occurred: {http_err}')
            if http_err.response.status_code == 429:
                logger.error('Rate limit exceeded. Waiting for 1 minute before the next query.')
                time.sleep(60)
            return query_tiktok_api(url, params, headers, data, retry_count=retry_count-1, logger=logger)
        logger.error(f'HTTP error occurred more than 10 times or is 500: {http_err}. \nResponse is {response}')
        return response 
    except Exception as err:
        logger.error(f'Other error occurred: {err}. \nResponse is {response}')
        return response


def search_posts_for_keyword_tiktok(terms,start_date,country,token,logger):

    url = 'https://open.tiktokapis.com/v2/research/video/query/'
    params = {
        'fields': 'id,create_time,username,region_code,video_description,music_id,like_count,comment_count,share_count,view_count,effect_ids,hashtag_names,hashtag_info_list,video_mention_list,video_label,playlist_id,voice_to_text,is_stem_verified,video_duration'
    }
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    search_date = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y%m%d')

    query = {
        'and': [
            {'operation': 'IN', 'field_name': 'keyword', 'field_values': terms},
            {'operation': 'IN', 'field_name': 'region_code', 'field_values': [country]}
        ]
    }
    
    payload = {
        'query': query,
        'start_date': search_date,
        'end_date': search_date,
        'max_count': 100,
        'is_random': False,
    }

    has_more = True
    cursor = 0
    search_id = None
    all_posts = []
    while has_more:
        payload['cursor'] = cursor
        payload['search_id'] = search_id
        json_response = query_tiktok_api(url=url, params=params, headers=headers, data=json.dumps(payload), retry_count=10, logger=logger)

        try: 
            all_posts.extend(json_response['data']['videos'])
            has_more = json_response['data']['has_more']
            logger.info(f"Amount of data collected: {len(json_response['data']['videos'])}")
        except Exception as e:
            logger.info(f'An error occured while subscripting json_response: {e}. The response is {json_response}')
            logger.info(f'Payload: {payload}')
        if has_more:
            cursor = json_response['data']['cursor']
            search_id = json_response['data']['search_id']
        
    logger.info(f"Total amount of data collected for this batch of keywords: {len(all_posts)}")
    return all_posts

PLATFORMS = {
    "facebook": {
        "search_posts": search_posts_for_keyword_crowdtangle, 
        "post_formatting": COLSET_CT_POST,
        },
    "tiktok": {
        "search_posts": search_posts_for_keyword_tiktok, 
        "post_formatting": COLSET_TIKTOK_POST,
        },
}

def search_posts_for_all_keywords(dct_searchterm, start_date, country, platform, logger):

    posts_to_store = []

    if platform == "tiktok":
        keywords_list = [item['keyword'] for item in dct_searchterm if item['keyword'] is not np.nan]
        token = get_tiktok_access_token()

        batch_size = 20
        posts = []
        for offset in range(0, len(keywords_list), batch_size):
            batch_keywords = keywords_list[offset:offset + batch_size]
            logger.info(f"Searching posts for keywords: {batch_keywords}")
            posts = search_posts_for_keyword_tiktok(batch_keywords, start_date, country, token, logger)
            if len(posts) == 0:
                logger.info(f"No posts found for keywords: {batch_keywords}")
                continue
            posts_to_keywords = attribute_posts_to_keywords(posts, batch_keywords, logger)

            for item in dct_searchterm[offset:offset + batch_size]:
                if item['keyword'] not in posts_to_keywords:
                    continue
                item['posts'] = [posts[i] for i in posts_to_keywords[item['keyword']]]
                item['nb_posts'] = len(item['posts'])
                start_time = time.time()
                posts_to_store.extend(format_posts(item, PLATFORMS[platform]['post_formatting'], logger))
                save_keyword_posts_csv(item, country, platform, logger)
                exe_time = time.time() - start_time

                if len(posts_to_store) >= 400: #Save sublists of posts
                    logger.info("Saving posts to SQL database")
                    save_posts(pd.DataFrame(posts_to_store), platform, logger)
                    posts_to_store = []

    elif platform == "facebook":
        for item in dct_searchterm:
            token = CROWDTANGLE_TOKEN
            posts = search_posts_for_keyword_crowdtangle(item['keyword'], start_date, country, token, logger)
            item['posts'] = posts
            item['nb_posts'] = len(posts)
            start_time = time.time()
            posts_to_store.extend(format_posts(item, PLATFORMS[platform]['post_formatting'], logger))
            save_keyword_posts_csv(item, country, platform, logger)
            exe_time = time.time() - start_time
            if platform == "facebook":
                time.sleep(TIME_DELAY_CROWDTANGLE - exe_time)


            if len(posts_to_store) >= 400: #Save sublists of posts
                logger.info("Saving posts to SQL database")
                save_posts(pd.DataFrame(posts_to_store), platform, logger)
                posts_to_store = []


    if len(posts_to_store) > 0: ## Save remaining posts
        save_posts(pd.DataFrame(posts_to_store), platform, logger)


def save_keyword_posts_csv(item, country, platform, logger):
    posts = format_posts(item, PLATFORMS[platform]['post_formatting'], logger)
    if len(posts) == 0:
        return 
    if not path.exists(DIR_KEYWORD_POSTS[country]):
        os.makedirs(DIR_KEYWORD_POSTS[country])
    filename = path.join(DIR_KEYWORD_POSTS[country],item['hashed_keyword']+".csv")
    logger.info(f"Saving {len(posts)} posts for keyword {item['keyword']} to {filename}...")
    df = pd.DataFrame(posts)
    df.to_csv(filename,index=None)
    logger.info("Saved!")
