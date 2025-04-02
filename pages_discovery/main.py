import logging, uuid, sys, time
import pandas as pd
from os import path, makedirs
from datetime import datetime, timedelta
import argparse

from params import ERROR_TYPES, DIR_DAILY_NEWS
from crawling_scripts.headlines_extraction import get_daily_articles, extract_keywords, get_kw_extractor
from crawling_scripts.posts_extraction import get_keywords_to_search, search_posts_for_all_keywords
from crawling_scripts.utils import append_to_csv
from database_scripts.save_headlines import save_news_headlines
from database_scripts.save_search_terms import save_search_terms

logger = None


DAILY = None

def set_logger():
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.basicConfig(filename='logs/error.log', level=logging.DEBUG, format='%(asctime)s %(levelname)s %(name)s %(message)s')
    return logging.getLogger(__name__)

def gather_daily_news(lang, country, start_date, extractor, logger):

    filename = f"headlines_{start_date}_{extractor}.csv"
    if path.exists(path.join(DIR_DAILY_NEWS[country],filename)):
        logger.info(f"News for {start_date} already gathered")
        return pd.read_csv(path.join(DIR_DAILY_NEWS[country],filename))

    df_news = pd.DataFrame()

    try:
        if DAILY == True: #IF DAILY NO NEED TO PROVIDE START_DATE, BECAUSE IT RAISES AN ERROR
            df_news = get_daily_articles(lang, country, start_date = None, period='300d')
        else:
            df_news = get_daily_articles(lang, country, start_date)

    except Exception as e:
        logger.error("{}: {}".format(ERROR_TYPES.GET_DAILY_NEWS,str(e)))

    df_news = extract_keywords(df_news, extractor=extractor, model=get_kw_extractor(extractor, lang=lang), lang=lang)

    if not path.exists(DIR_DAILY_NEWS[country]):
        makedirs(DIR_DAILY_NEWS[country])
    desired_columns = ['title','description','url','topic','publisher_name','publisher_website','published_date','keywords','first_keyword','second_keyword','hashed_first_kw','hashed_second_kw']
    append_to_csv(df_news, desired_columns, path.join(DIR_DAILY_NEWS[country],filename))
    
    save_news_headlines(df_news)

    return df_news


def searching_posts(df_news, country, start_date, platform, logger):

    if df_news.shape[0] == 0:
        logger.info("No news headline on ", start_date)
        return

    df_searchterms = get_keywords_to_search(df_news)

    ## Save search terms to DB
    save_search_terms(df_searchterms, start_date, logger)

    dct_searchterm = df_searchterms.to_dict('records')
    search_posts_for_all_keywords(dct_searchterm, start_date, country, platform, logger)



def daily_job(lang, country, start_date, platform, extractor, logger):
    df_news = gather_daily_news(lang, country, start_date, extractor, logger)
    searching_posts(df_news, country, start_date, platform, logger)




if __name__ == "__main__":

    logger = set_logger()

    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--lang', type=str, default='en', help='Language of the news')
    parser.add_argument('--country', type=str, default='US', help='Country of the news')
    parser.add_argument('--start_date', type=str, default=(datetime.today() - timedelta(days=7)).strftime('%Y-%m-%d'), help='Start date for the news in YYYY-MM-DD format')
    parser.add_argument('--nb_days', type=int, default=1, help='Number of days to gather news for')
    parser.add_argument('--platform', type=str, default='facebook', help='Platform to search posts on (e.g., facebook, tiktok)')
    parser.add_argument('--extractor', type=str, default='yake', help='Keyword extractor to use (e.g., yake, keybert, fasttext)')

    args = parser.parse_args()

    lang = args.lang
    country = args.country
    start_date = args.start_date
    nb_days = args.nb_days
    platform = args.platform
    extractor = args.extractor 
    logger.info(f"Starting daily job for {nb_days} days from {start_date} in {country} ({lang}) on {platform} using {extractor}")

    for _ in range(nb_days):
        
        logger.info(f"NEW DAY: {start_date}")
    
        daily_job(lang, country, start_date, platform, extractor, logger)
        start_date = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        

 

