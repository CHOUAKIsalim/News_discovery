import pandas as pd
from datetime import datetime
from gnews import GNews
from hashlib import sha256
import yake
from cleantext.clean import fix_strange_quotes, normalize_whitespace, replace_urls, remove_emoji, clean


def remove_source(title):
    ind = title.rfind('-')
    return title[0:ind].strip()


def format_pubdate(date_str,ftm = "%a, %d %b %Y %H:%M:%S %Z"):
    d = datetime.strptime(date_str,ftm)
    return d


def get_top_news(lang, country, start_date, period='1d'):
   # start_date_datetime = datetime.strptime(start_date, "%Y-%m-%d")
   # start_date_tuple = (start_date_datetime.year, start_date_datetime.month, start_date_datetime.day)

    news_agent = GNews(language=lang, country = country, period=period, max_results=500)   
    top_news = news_agent.get_top_news()    
    return top_news


def get_categorized_news(lang, country, start_date, period='1d'):
    TOPICS = ['WORLD', 'NATION', 'BUSINESS', 'TECHNOLOGY', 'ENTERTAINMENT', 'SPORTS', 'SCIENCE', 'HEALTH']
#    start_date_datetime = datetime.strptime(start_date, "%Y-%m-%d")
#    start_date_tuple = (start_date_datetime.year, start_date_datetime.month, start_date_datetime.day)
    news_agent = GNews(language=lang, country=country, period=period,max_results=500)
    ls_news = []
    for topic in TOPICS:
        topic_news = news_agent.get_news_by_topic(topic)
        for news in topic_news:
            news['topic'] = topic
        ls_news.extend(topic_news)
    return ls_news

def get_daily_articles(lang, country, start_date, period='1d'):

    headline_news = get_top_news(lang=lang, country=country, start_date=start_date, period=period)
    headline_news = []
    categorized_news = get_categorized_news(lang=lang, country=country, start_date=start_date, period=period)
    headline_news.extend(categorized_news)
    for news in headline_news:
        try:
            news['publisher_name'] = news['publisher']['title']
            news['publisher_website'] = news['publisher']['href']
            news['title'] = remove_source(news['title'])
            news['published_date'] = format_pubdate(news['published date'])
            del news['published date']
            del news['publisher']
        except Exception as e:
            print(e)
    df_news = pd.DataFrame(headline_news)
    df_news = df_news.drop_duplicates(subset='title')
    return df_news

###########################################################################

def word_count(text):
    return len(text.split(' '))


def get_clean_text(strs):
    cl_text = clean(strs,fix_unicode=False,lower=False,no_line_breaks=True)
    cl_text = fix_strange_quotes(cl_text)
    cl_text = normalize_whitespace(cl_text)
    cl_text = replace_urls(cl_text,'')
    return remove_emoji(cl_text)


def get_keywords(text,N=4):
    ctext = get_clean_text(text)
    kw_extractor = yake.KeywordExtractor(top=N, stopwords=None)
    keywords = kw_extractor.extract_keywords(ctext)
    fl_keywords = sorted(keywords,key=lambda x:x[1], reverse=True)
    ls_keywords = [kw[0] for kw in fl_keywords]
    return list(filter(lambda w: word_count(w)>=2, ls_keywords))


def get_article_keyword(title):

    try:
        keywords = get_keywords(title)
        filtered_kw = list(filter(lambda w: word_count(w)>=2, keywords))
        return ",".join(filtered_kw)
    except Exception as e:
        print(e)
        return ''
    

def get_keyword_at(keywords, ind = 0):
    ls_keywords = keywords.split(',')
    if len(ls_keywords) < (ind+1):
        return ''
    else:
        return ls_keywords[ind]
    


def extract_keywords(df_news):

    df_news['keywords'] = df_news.title.apply(get_article_keyword)
    df_news['first_keyword'] = df_news.keywords.apply(lambda x:get_keyword_at(x,0))
    df_news['second_keyword'] = df_news.keywords.apply(lambda x:get_keyword_at(x,1))
    df_news['hashed_first_kw'] = df_news.first_keyword.apply(lambda x: get_hash_string(x))
    df_news['hashed_second_kw'] = df_news.second_keyword.apply(lambda x: get_hash_string(x))

    return df_news

#######################################################################################

def get_hash_string(keyword, date_str=''):
    if keyword == '':
        return ''
    if date_str=='':
        date_str = datetime.today().strftime('%Y-%m-%d')
    value = date_str + "." + keyword
    return sha256(value.encode('utf-8')).hexdigest()



    



