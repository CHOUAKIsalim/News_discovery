import os
from dotenv import load_dotenv, find_dotenv


load_dotenv(find_dotenv())

TIME_DELAY_CROWDTANGLE = 20

CROWDTANGLE_TOKEN = os.getenv("CROWDTANGLE_TOKEN")

DIR_KEYWORD_POSTS = {
    "France": 'data/keyword_posts_csv/France',
    "US": 'data/keyword_posts_csv/US',
    "RO": 'data/keyword_posts_csv/RO'
}

STOP_WORDS_FILE = {
    'ro': os.getenv("RO_STOP_WORDS_FILE"),
    'en': os.getenv("EN_STOP_WORDS_FILE"),
    }

DIR_KW_FASTTEXT = os.getenv("DIR_KW_FASTTEXT")

XBOGUS_JS_FILE = os.getenv("XBOGUS_JS_FILE")

DIR_DAILY_NEWS = { 
    'France': 'data//daily_news_csv/France',
    'US': 'data/daily_news_csv/US',
    'RO': 'data/daily_news_csv/RO'
}
MAIN_LOG_FILE = 'logs/error.log'

INPUT_DATA_FILE = 'input/daily_articles_eng_oct.csv'

class ERROR_TYPES:
    GET_DAILY_NEWS = 'GET_DAILY_NEWS'
    KEYWORD_EXTRACT = 'KEYWORD_EXTRACT'
    CROWDTANGLE_API = 'CROWDTANGLE_API'
    DB_OPERATION = 'DB_OPERATION'
    FILE_OPERATION = 'FILE_OPERATION'
    UNDEFINED = 'UNDEFINED'




