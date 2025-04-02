
from .utils import create_engine, explode_and_save, TBL_CT_POST, TBL_TIKTOK_POST
import pandas as pd


def save_posts(posts_df, platform, logger):
    try:
        if platform == 'facebook':
            ignore_types = ['live_video_scheduled']
            posts_df = posts_df[~posts_df['type'].isin(ignore_types)]
            posts_df.set_index(['platformId','hashed_term'],inplace=True)
            database_connection = create_engine()
            posts_df.to_sql(name=TBL_CT_POST,con=database_connection,
                    if_exists='append',index=True, index_label=['platformId','hashed_term'])
        
        elif platform == 'tiktok':
            database_connection = create_engine() # connect to the database

            # The three columns below are lists of strings, so we need to save them in separate tables
            explode_and_save(posts_df, 'id', 'effect_ids', 'tiktok_effect_ids', database_connection)
            explode_and_save(posts_df, 'id', 'hashtag_names', 'tiktok_hashtag_names', database_connection)
            explode_and_save(posts_df, 'id', 'video_mention_list', 'tiktok_video_mentions', database_connection)
    
            posts_df.drop(columns=['effect_ids', 'hashtag_names', 'video_mention_list'], inplace=True)
            posts_df.set_index(['id','hashed_term'], inplace=True)
            posts_df.to_sql(name=TBL_TIKTOK_POST, con=database_connection, if_exists='append', index=True, index_label=['id','hashed_term'])

        database_connection.close()
    except Exception as e:
        logger.error(f'Exception occured in save_posts: {e}')
