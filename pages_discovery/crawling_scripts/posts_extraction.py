import json, requests, time
import pandas as pd
from os import path
from datetime import datetime,timedelta
from params import CROWDTANGLE_TOKEN, ERROR_TYPES, TIME_DELAY_CROWDTANGLE, DIR_KEYWORD_POSTS
from database_scripts.utils import COLSET_CT_POST
from database_scripts.save_posts import save_posts

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

def format_posts(item):

    try:
        if 'posts' in item:
            for post in item['posts']:
                post['search_term'] = item['keyword']
                post['hashed_term'] = item['hashed_keyword']
            
            posts = [get_leaves(post) for post in item['posts']]

            for post in posts:
                cols = set(post.keys())
                missing_cols = COLSET_CT_POST.difference(cols)
                for c in missing_cols:
                    post[c] = ''

    except Exception as e:
        print("posts_extraction, format_posts")
        print("{}: {}".format(ERROR_TYPES.UNDEFINED,str(e)))

    return posts

def call_api(url):
    resp_content = '{}'
    time_cost = 10
    trials = 0
    time_delay = 30 
    while(trials < 3):
        try:
            print("Requesting data from {}".format(url))
            start_time = datetime.now()
            resp = requests.get(url,timeout=60)
            if resp.status_code == 200:
                exec_time = datetime.now() - start_time
                resp_content= resp.text
                time_cost = exec_time.total_seconds()
            else:
                print('API error {}'.format(resp.status_code))
                resp_content='{}'
        except Exception as e:
            print('Error:')
            print(str(e))
            resp_content='{}'
        trials += 1
        if resp_content!='{}':
            break
        time.sleep(time_delay*trials)
    return resp_content,time_cost

def collect_posts_next(nextpage_url ):
    resp_text,exec_time = call_api(nextpage_url)
    jsondata = json.loads(resp_text)
    if jsondata == {}:
        return [],''

    posts = jsondata['result']['posts']
    if 'nextPage' in jsondata['result']['pagination']: 
        nextpage_url = jsondata['result']['pagination']['nextPage']
    else:
        nextpage_url = ''
    print('API return in {} seconds - {} posts found'.format(exec_time,len(posts)))
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

def search_posts_for_keyword_crowdtangle(token,term,time_beg,time_end):
    
    api_url = search_term_endpoint(token=token,terms=term,startDate=time_beg,endDate=time_end,count=100)
    resp_text,exec_time = call_api(api_url)    
    time_delay = 30
    pcount = 0
    jsondata = json.loads(resp_text)

    if jsondata == {}:
        print('API return in {} seconds - 0 post found'.format(exec_time))
        return []
    
    all_posts = []
    posts = jsondata['result']['posts']

    if 'nextPage' in jsondata['result']['pagination']: 
        nextpage_url = jsondata['result']['pagination']['nextPage']
    else:
        nextpage_url = ''
        
    if len(posts) > 0:
        print('API return in {} seconds - {} posts found'.format(exec_time,len(posts)))
        pcount += len(posts)        
        all_posts.extend(posts)
        
    if len(all_posts) >= 1000: #Search api return maximum 1000 posts -> can be solved by splitting into smaller time window
        return all_posts
    
    while nextpage_url!='':
        time.sleep(time_delay)
        posts,new_nextpage_url = collect_posts_next(nextpage_url)
        pcount += len(posts)
        all_posts.extend(posts)
        nextpage_url = new_nextpage_url

    return all_posts

def search_posts_for_keyword_tiktok(token,term,time_beg,time_end):
    print('Tiktok search not implemented yet')
    import sys
    sys.exit(1)


PLATFORMS = {
    "facebook": search_posts_for_keyword_crowdtangle,
    "tiktok": search_posts_for_keyword_tiktok
}

def search_posts_for_all_keywords(dct_searchterm, start_date, country, platform):

    start_date_datetime = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = (start_date_datetime + timedelta(days=1)).strftime('%Y-%m-%d')

    start_date = "{}T00:00:00".format(start_date)
    end_date = "{}T00:00:00".format(end_date)

    print('Searching posts from {} to {}'.format(start_date,end_date))

    posts_to_store = []

    for item in dct_searchterm:
        posts = PLATFORMS[platform](CROWDTANGLE_TOKEN, item['keyword'], start_date, end_date)
        item['posts'] = posts
        item['nb_posts'] = len(posts)
        start_time = time.time()
        posts_to_store.extend(format_posts(item))
        save_keyword_posts_csv(item, country)
        exe_time = time.time() - start_time
        time.sleep(TIME_DELAY_CROWDTANGLE - exe_time)


        if len(posts_to_store) >= 400: #Save sublists of posts
            print("saving posts")
            save_posts(pd.DataFrame(posts_to_store))
            posts_to_store = []


    if len(posts_to_store) > 0: ## Save remaining posts
        save_posts(pd.DataFrame(posts_to_store))


def save_keyword_posts_csv(item, country):
    posts = format_posts(item)
    if len(posts) == 0:
        return 
    filename = path.join(DIR_KEYWORD_POSTS[country],item['hashed_keyword']+".csv")
    print("saving:", filename)
    df = pd.DataFrame(posts)
    df.to_csv(filename,index=None)
    print("")
