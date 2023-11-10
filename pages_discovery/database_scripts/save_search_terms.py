
from .utils import create_engine, TBL_KEYWORD
from datetime import datetime

def save_search_terms(df_searchterms, start_date):

    try:
        database_connection = create_engine()
        df_searchterms = df_searchterms.set_index('hashed_keyword')
        df_searchterms['search_date'] = start_date
        df_searchterms.to_sql(name=TBL_KEYWORD,con=database_connection,if_exists='append',
                                index=True, index_label='hashed_keyword')
        database_connection.close()

    except Exception as e:
        print(e)

