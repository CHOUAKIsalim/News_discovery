
from .utils import create_engine, TBL_CT_POST


def save_posts(posts_df):
    try:
        ignore_types = ['live_video_scheduled']
        posts_df = posts_df[~posts_df['type'].isin(ignore_types)]
        posts_df.set_index(['platformId','hashed_term'],inplace=True)
        database_connection = create_engine()
        posts_df.to_sql(name=TBL_CT_POST,con=database_connection,
                if_exists='append',index=True, index_label=['platformId','hashed_term'])
        database_connection.close()
    except Exception as e:
        print(e)
