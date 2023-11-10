

from .utils import create_engine, TBL_NEWS_HEADLINE


def save_news_headlines(df_news):
        
    try:
        database_connection = create_engine()
        df_news.to_sql(name=TBL_NEWS_HEADLINE,con=database_connection,if_exists='append',index=True, index_label='id')
        database_connection.close()   

    except Exception as e:
        print(e)
