from newsapi import NewsApiClient
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv, find_dotenv
import pandas as pd
import os

load_dotenv(find_dotenv())


gnews_api_key =  os.getenv("GNEWS_API_KEY")



newsapi = NewsApiClient(api_key=gnews_api_key)


#CATEGORIES: general, health, science, technology, business, entertainment

def top_headlines():    
   country="fr"
   category="general"        
   top_headlines =newsapi.get_top_headlines(category=category,language='fr',country=country)     
   top_headlines=pd.json_normalize(top_headlines['articles'])   
   newdf=top_headlines[["title","url"]]    
   print(newdf.shape)
   print(newdf.columns)
   dic=newdf.set_index('title')['url'].to_dict()


top_headlines()