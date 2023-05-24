#!/usr/bin/env python
# coding: utf-8

# # Instructions: 
# 1. Install sqlite: go to download page https://www.sqlite.org/download.html, download SQLite
# 2. Run the following commands in terminal:
#     conda install pip
#     pip install pandas
#     pip install ijson
#     pip install os-sys
#     pip install pyspark
#     pip install os-sys
# 3. Run all cells of this notebook and then run the 3 sql files to get answers to 2a, 2b, and 2c (2c is incomplete)
#     

# In[1]:


import urllib.request
import pandas as pd
import os
import requests
import urllib.request
import zipfile
from io import BytesIO
import re
import sqlite3
import json
import os 
import urllib.request
import ijson


# In[2]:


#download zip frol url
with urllib.request.urlopen('https://cricsheet.org/downloads/odis_json.zip') as dl_file:
        with open("/Users/catherinedana/Zelus/odis_json.zip", 'wb') as out_file:
            out_file.write(dl_file.read())


# In[3]:


os.listdir()


# In[5]:


meta = None
path = '/Users/catherinedana/odis_json.zip'
metadata = []
infodata = []
inningdata = []
with zipfile.ZipFile("odis_json.zip", "r") as f:
    for name in f.namelist(): 
       # print(name)
        if not name == 'README.txt':
            text = f.read(name)
            data = json.loads(text)
            
     
            metadata.append(data['meta'])
            infodata.append(data['info'])
            inningdata.append(data['innings'])

          #  else:
           #     info = pd.json_normalize(data['info'])  
          #      meta = pd.json_normalize(data['meta'])
          #      innings = pd.json_normalize(data['innings'])
print(row_keys)
meta = pd.DataFrame(metadata)
info = pd.DataFrame(infodata)
innings = pd.DataFrame(inningdata)
info = info.applymap(str)  #changing all to strings so SQLite compatible, can alter dtypes in db later
meta = meta.applymap(str)
innings = innings.applymap(str)


# In[10]:


with zipfile.ZipFile("odis_json.zip", "r") as f:
    text = f.read('64814.json')
    data = json.loads(text)
d=data['innings']
d


# In[17]:


#fix innings data

#df['all'] = df['all'].str.replace("{'team':","")
#df[['team','overs']]=df['all'].str.split("[{}]")[3]

x = pd.json_normalize(d,record_path=[['overs','over'], 
               'deliveries', 
               'batter', 'bowler', 'extras', 'runs']
                )
x
#r = pd.concat([x.drop(['overs', 'penaltyruns', 'powerplays', 'target'], 1).reset_index(drop=True), pd.json_normalize(x.overs), pd.json_normalize(x.powerplays), pd.json_normalize(x.target), pd.json_normalize(x.penaltyruns)], 1)

#overs_cols = [f'overs.{i}' for i in pd.json_normalize(x.overs).columns]    
#target_cols = [f'target.{i}' for i in pd.json_normalize(x.target).columns]

#r.columns = [*x.drop(['overs', 'target'], 1).columns , *overs_cols, *target_cols]
#r
#df
#df.all = df['all'].fillna({i: [] for i in registry.all})  # replace NaN with []
#df.explode('all')
#df[['name','id']]=df['all'].str.split(":",expand=True)
#df['id'] = df['id'].str.replace("'","").str.replace("}}","")
#df['name'] = df['name'].str.replace("'","")
#registry = df


# In[125]:


import numpy as np
#fix info data
info[['team1','team2']] = info.teams.str.split(",",expand=True)
info['team1'] = info['team1'].str.replace("['","").str.replace("'","")
info['team2'] = info['team2'].str.replace("']","").str.replace("'","")

info[['by','winner']] = info.outcome.str.split("'winner':",expand=True)
info['winner'] = info['winner'].str.replace("'}","").str.replace("'","")
info['winner'] = info['winner'].str.split(',').str[0]
info['loser']= np.where(info.winner == info.team1,'X', info.team2)

info['dates'] = info['dates'].str.replace("[","").str.replace("'","").str.replace("]","")
info['year'] = info['dates'].str.split('-').str[0]



# In[128]:


#registry data
df = pd.DataFrame(info['registry'].str.split(',').apply(Series, 1).stack(), columns =['all'])
#registry = registry.explode(list('all'))
df['all'] = df['all'].str.replace("{'","").str.replace("people':","")

#df.all = df['all'].fillna({i: [] for i in registry.all})  # replace NaN with []
df.explode('all')
df[['name','id']]=df['all'].str.split(":",expand=True)
df['id'] = df['id'].str.replace("'","").str.replace("}}","")
df['name'] = df['name'].str.replace("'","")
registry = df


# In[130]:


#convert  JSON data from dataframe to sqlite 
import sqlite3
conn = sqlite3.connect("cricSheet.db")
c = conn.cursor()
info.to_sql("info_table",conn)
registry.to_sql("registry_table",conn)
#info.to_sql("meta_table",conn)
#info.to_sql("innings_table",conn)


# In[ ]:





# In[ ]:




