import logging, uuid, sys, time
import pandas as pd
from os import path
from datetime import datetime, timedelta

from params import ERROR_TYPES, DIR_DAILY_NEWS
from crawling_scripts.headlines_extraction import get_daily_articles, extract_keywords
from crawling_scripts.posts_extraction import get_keywords_to_search, search_posts_for_all_keywords
from database_scripts.save_headlines import save_news_headlines
from database_scripts.save_search_terms import save_search_terms

logger = None


DAILY = None

def set_logger():
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.basicConfig(filename='logs/error.log', level=logging.DEBUG, format='%(asctime)s %(levelname)s %(name)s %(message)s')
    return logging.getLogger(__name__)


def gather_daily_news(lang, country, start_date):

    df_news = pd.DataFrame()

    try:
        if DAILY == True: #IF DAILY NO NEED TO PROVIDE START_DATE, BECAUSE IT RAISES AN ERROR
            df_news = get_daily_articles(lang, country, start_date = None, period='300d')
        else:
            df_news = get_daily_articles(lang, country, start_date)

    except Exception as e:
        logger.error("{}: {}".format(ERROR_TYPES.GET_DAILY_NEWS,str(e)))

    try:
        df_news = extract_keywords(df_news)

    except Exception as e:
        logger.error("{}: {}".format(ERROR_TYPES.KEYWORD_EXTRACT,str(e)))

    try:
#        df_news['uuid'] = [str(uuid.uuid4()) for _ in range(len(df_news.index))]
        filename = "headlines_{}.csv".format(start_date)
        df_news.to_csv(path.join(DIR_DAILY_NEWS[country],filename),index=None)

        ## Save daily_news to DB
        save_news_headlines(df_news)

    except Exception as e:
        print(str(e))
        logger.error("{}: {}".format(ERROR_TYPES.DB_OPERATION,str(e)))

    return df_news


def searching_posts(df_news, start_date):

    if df_news.shape[0] == 0:
        print("No news headline on ", start_date)
        return

    df_searchterms = get_keywords_to_search(df_news)

    ## Save search terms to DB
    save_search_terms(df_searchterms, start_date)

    dct_searchterm = df_searchterms.to_dict('records')
    search_posts_for_all_keywords(dct_searchterm, start_date, country)



def daily_job(lang, country, start_date):
    df_news = gather_daily_news(lang, country, start_date)
    searching_posts(df_news, start_date)




if __name__ == "__main__":

    logger = set_logger()

    try:
        lang = sys.argv[1]
        country = sys.argv[2]
    except:
        lang = "en"
        country = "US"

    try:
        start_date = sys.argv[3]
        nb_days = int(sys.argv[4])
        DAILY = False

    except: 
        start_date = (datetime.today() - timedelta(days = 1)).strftime('%Y-%m-%d')
        nb_days = 1
        DAILY = True
 

    for _ in range(nb_days):

        print()
        print()
        print("NEW DAY:", start_date)
    
        daily_job(lang, country, start_date)
        start_date = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

 

