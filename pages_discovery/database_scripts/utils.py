import sqlalchemy, os
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from pathlib import Path
from hashlib import sha256
from datetime import datetime


load_dotenv(find_dotenv())


#DRIVER_NAME = os.getenv("DB_DRIVER")
USER_NAME = os.getenv("DB_USER_NAME")
PASSWORD = os.getenv("DB_PASSWORD")
HOST_NAME = os.getenv("DB_HOSTNAME")
DB_NAME = os.getenv("DB_NAME")

TBL_NEWS_HEADLINE = 'gl_daily_news'
TBL_CT_POST = 'ct_found_post'
TBL_TIKTOK_POST = 'tiktok_found_post'
TBL_KEYWORD = 'search_term'


COLSET_CT_POST = set(['account_platformId','link','brandedContentSponsor_pageCreatedDate','media_type','account_verified','statistics_actual_commentCount','brandedContentSponsor_handle','statistics_expected_loveCount','brandedContentSponsor_id','account_profileImage','updated','brandedContentSponsor_subscriberCount','message','account_pageCreatedDate','platformId','statistics_expected_commentCount','videoLengthMS','statistics_expected_angryCount','caption','brandedContentSponsor_verified','subscriberCount','account_name','score','legacyId','statistics_expected_careCount','media_full','statistics_actual_careCount','statistics_expected_likeCount','account_subscriberCount','account_handle','account_pageDescription','expandedLinks_expanded','media_width','account_pageCategory','brandedContentSponsor_pageCategory','media_url','statistics_actual_hahaCount','statistics_expected_sadCount','brandedContentSponsor_profileImage','expandedLinks_original','account_platform','statistics_actual_angryCount','statistics_expected_thankfulCount','postUrl','id','account_accountType','title','description','statistics_actual_loveCount','statistics_expected_hahaCount','type','account_pageAdminTopCountry','platform','languageCode','statistics_actual_wowCount','brandedContentSponsor_name','statistics_actual_thankfulCount','account_id','imageText','brandedContentSponsor_pageDescription','media_height','statistics_expected_shareCount','liveVideoStatus','brandedContentSponsor_url','account_url','brandedContentSponsor_platform','brandedContentSponsor_platformId','statistics_actual_shareCount','statistics_actual_sadCount','brandedContentSponsor_accountType','date','statistics_actual_likeCount','brandedContentSponsor_pageAdminTopCountry','statistics_expected_wowCount'])
COLSET_TIKTOK_POST = set(['id','create_time','username','region_code','video_description','music_id','like_count','comment_count','share_count','view_count','effect_ids','hashtag_names','hashtag_info_list','video_mention_list','video_label','playlist_id','voice_to_text','is_stem_verified','video_duration'])

def generate_unique_id(row):
    unique_string = ".".join([str(row[field]) for field in row.index])
    return sha256(unique_string.encode('utf-8')).hexdigest()

def create_engine():
    connection = sqlalchemy.create_engine(f'mysql+pymysql://{USER_NAME}:{PASSWORD}@{HOST_NAME}/{DB_NAME}?charset=utf8mb4').connect()
    #connection = sqlalchemy.create_engine('sqlite:////home/benzenoug/News_discovery/pages_discovery/data/news_collection.db').connect()
    return connection

def explode_and_save(df, id_column, value_column, table_name, database_connection):
    exploded_df = df[[id_column, value_column]].explode(value_column)
    exploded_df = exploded_df[exploded_df[value_column].notna()]
    exploded_df = exploded_df[exploded_df[value_column] != '']
    exploded_df['unique_id'] = exploded_df.apply(generate_unique_id, axis=1)
    exploded_df.rename(columns={id_column: 'original_id', value_column: value_column}, inplace=True)
    exploded_df.to_sql(name=table_name, con=database_connection, if_exists='append', index=False, )


def exec_cmd(sql_cmd):
    conn = create_engine()
    cursor = conn.cursor()
    try:
        rs = cursor.execute(sql_cmd)
        conn.close()
        return rs
    except Exception as e:
        print(e)
        conn.close()
        return -1


def insert_row(table_name, values, columns=[]):
    _values = ['%s' for v in values]
    if len(columns) > 0:
        sql = """INSERT INTO `{}` ({}) VALUES ({})""".format(table_name, ", ".join(columns), ", ".join(_values))
    else:
        sql = """INSERT INTO `{}` VALUES ({})""".format(table_name, ", ".join(_values))

    if isinstance(values,list):
        values = tuple(values)
    
    conn = create_engine()
    cursor = conn.cursor()    
    try:
        print("Executing ", sql%values)
        rs = cursor.execute(sql,values)
        conn.close()
        return rs
    except Exception as e:
        print(e)
        conn.close()
        return -1

def query_data(sql_query):
    conn = create_engine()    
    try:
        print("Executing ", sql_query)
        df = pd.read_sql(sql_query, con=conn)
        conn.close()
        return df
    except Exception as e:
        print(e)
        conn.close()
        return str(e) 



    
