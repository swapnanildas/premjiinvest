import pandas as pd
import numpy as np
import json
import requests
import pprint
from dateutil import tz
from datetime import datetime as dt
from algoliasearch.search_client import SearchClient
from bs4 import BeautifulSoup 
import random
import datetime
import pymysql
import hashlib
import yaml

from sqlalchemy import create_engine


with open('secrets.yml', 'r') as file:
    prime_service = yaml.safe_load(file)

ApplicationID = prime_service['ApplicationID']
APIKey = prime_service['APIKey']
# Connect and authenticate with your Algolia app
client = SearchClient.create(ApplicationID, APIKey)
indexName = "production_STORIES"
# Create a new index and add a record
index = client.init_index(indexName)
request_options = {
    'hitsPerPage' : 20,
    'page': 0
}

headers = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246"} 
# Here the user agent is for Edge browser on windows 10. You can find your browser user agent from the above given link. 
queries = ['HDFC','Tata Motors']


def count_digit(n):
    num = n
    count=0
    while(num>0):
        count+=1
#         print(num)
        num//=10
    return count


def getIstDateFromUnix(ts):
    ts = int(ts)
    if(count_digit(ts)==16):
        unix_time = ts/1000000.0
    elif(count_digit(ts)==13):
        unix_time = ts/1000.0
    else:
        unix_time = ts
    
    
    from_zone = tz.gettz('UTC')
    to_zone = tz.gettz('Asia/Calcutta')
    utc_time = dt.utcfromtimestamp(unix_time)
    # datetime objects are 'naive' by default
    utc_time = utc_time.replace(tzinfo=from_zone)

    # Convert time zone
    ist_date = utc_time.astimezone(to_zone).date()
    return ist_date

def get_sentiment_score(text):
    return random.random()


article_df = pd.DataFrame(columns= ['aid','source','query','type','title','url','publishDate','text','score'])



for query in queries:
    data = index.search(query,request_options)
    hits = data['hits']
    for hit in hits:
        if hit['type'] == 'Article':
            title = hit['title']
            sub_url = hit['url']
            url = 'https://yourstory.com'+sub_url
            publishedAtUnix = hit['publishedAt']
            publisheDate = getIstDateFromUnix(publishedAtUnix).strftime('%Y-%m-%d')
            r = requests.get(url=url, headers=headers) 
            soup = BeautifulSoup(r.content, 'lxml')
            p_tags = soup.find_all('div',{"id":"article_container"})
            text = ""
            hash_obj = hashlib.sha256(url.encode('utf-8'))
            hex_hash = hash_obj.hexdigest()
            aid = str(hex_hash)
            for p in p_tags:
                text+=p.text
            score = get_sentiment_score(text)
            article_df = pd.concat([article_df, pd.DataFrame([[aid,'YourStory',query,'Article',title,url,publisheDate,text,score]],columns=['aid','source','query','type','title','url','publishDate','text','score'])],ignore_index=True)

    r = requests.get(url="https://backend.finshots.in/backend/search/?q="+query, headers=headers) 
    out = json.loads(r.content)
    matches = out['matches']
    for match in matches:
        title = match['title']
        publishDate = datetime.datetime.strptime(match['published_date'].split('T')[0],"%Y-%m-%d").date().strftime('%Y-%m-%d')
        url = match['post_url']
        req = requests.get(url=url, headers=headers)
        soup = BeautifulSoup(req.content, 'lxml')
        p_tags = soup.find_all('div',{"class":"post-content"})
        text = ""
        hash_obj = hashlib.sha256(url.encode('utf-8'))
        hex_hash = hash_obj.hexdigest()
        aid = str(hex_hash)
        for p in p_tags:
            text+=p.text
        score = get_sentiment_score(text)
        article_df = pd.concat([article_df, pd.DataFrame([[aid,'FinShots',query,'Article',title,url,publishDate,text,score]],columns=['aid','source','query','type','title','url','publishDate','text','score'])],ignore_index=True)


article_df['RN'] = article_df.sort_values(['publishDate'], ascending=[False]) \
             .groupby(['source','query']) \
             .cumcount() + 1

latest_articles = article_df[article_df['RN']<=5].sort_values(by='RN')

latest_articles = latest_articles.drop(columns=['RN'])

username = prime_service['username']
password = prime_service['password']
server = prime_service['server']


engine = create_engine("mysql+pymysql://"+username+":"+password+"@"+server+"/sql12709467")

db = pymysql.connect(host=server.split(':')[0],user=username,passwd=password)
cursor = db.cursor()
query = ("""
use sql12709467;

""")
cursor.execute(query)
for r in cursor:
    print(r)
db.commit()

try:
    df = pd.read_sql('SELECT * FROM articles', con=engine)
    db_articles = list(df['aid'].unique())
    new_articles = latest_articles[~latest_articles.aid.isin(db_articles)]
except:
    new_articles = latest_articles


new_articles.to_sql('articles', con=engine, if_exists='append', index=False)
db.commit()

print(df.head())
cursor = db.cursor()
query = ("""
select count(*) from articles;
""")
cursor.execute(query)
for r in cursor:
    print(r)



