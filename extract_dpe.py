#!/usr/bin/env python
# coding: utf-8

# ## Welcome to your notebook.
# 

# #### Run this cell to connect to your GIS and get started:

# In[1]:


from arcgis.gis import GIS
gis = GIS("home")
import pandas as pd
import geopandas as gpd

# In[2]:


## https://data.ademe.fr/datasets/dpe-v2-logements-existants


# In[3]:


# Item Added From Toolbar
# Title: communeref2021 | Type: Feature Service | Owner: leroseyo
item = gis.content.get("f38cf13259124c92ae6cc5431d410b90")
item= item.layers[0]

comref= item.query().sdf
comref

comref = gpd.read_file('C:/Users/BA Mouhamed/Downloads/communeref2021_export.geojson')
# #### Now you are ready to start!



import pandas as pd, requests
get_ipython().system('pip3 install geopandas')
import geopandas as gpd


# In[5]:



codeinsee = comref['CODE_INSEE'][0]
print(codeinsee)
dpe =pd.read_csv('https://data.ademe.fr/data-fair/api/v1/datasets/dpe-v2-logements-existants/lines?size=10000&page=1&q_mode=simple&qs=(Code_INSEE_%5C(BAN%5C):'+codeinsee+'*)&finalizedAt=2022-11-03T19:04:48.295Z&format=csv')
if len(dpe) > 9998:
    print('ffff',codeinsee)
else:
    print(len(dpe),codeinsee)


# In[6]:


## écrire le fichier
dpe.to_csv(codeinsee+'.csv',sep =';')


# In[ ]:


import time
i=0
codeinsees=[]
dpeX=pd.DataFrame()
maxcom=len(comref)
while i<maxcom:
    codeinsee=comref['CODE_INSEE'][i]
    print(comref['CODE_INSEE'][i])
    codeinsees.append(codeinsee)
    ##on va chercxher dpe
    dpe =pd.read_csv('https://data.ademe.fr/data-fair/api/v1/datasets/dpe-v2-logements-existants/lines?size=10000&page=1&q_mode=simple&qs=(Code_INSEE_%5C(BAN%5C):'+codeinsee+'*)&finalizedAt=2022-11-03T19:04:48.295Z&format=csv')
    dpe['codeinsee']=str(codeinsee)
    dpeX=dpeX.append(dpe)
    ii=str(i)
    if ii[:2] == '00':
        dpeX['i']=str(i)
        dpeX.to_csv('dpe_log_22.csv',sep=';')
    print(len(dpeX),str(i),codeinsee)
    time.sleep(0.1)
    i=i+1

# In[14]:

dpeX.to_csv('C:/Users/Ba Mouhamed/Downloads/dpe_log_22.csv' ,sep=';')
dpeX.to_excel('lol5.csv')


import pandas as pd
dpeX = pd.read_csv('C:/Users/Ba Mouhamed/Downloads/dpe_log_22.csv' ,sep=';')
dpeX2 = pd.read_excel('C:/Users/Ba Mouhamed/Downloads/dpe2.xlsx')
DPE = dpeX.append(dpeX2)

def tocodeinsee(x):
    x=str(x)
    if len(x) == 4:
        x='0'+x
    return x

DPE['codeinsee'] = DPE['codeinsee'].apply(lambda x: tocodeinsee(x))
DPE['dep'] = DPE.codeinsee.apply(lambda x: x[:2])
DPE.to_csv('C:/Users/Ba Mouhamed/Downloads/dpe_log_all.csv' ,sep=';')

deps=[]
for dep in range(1,96):
    print(dep)
    if dep != 20:
        if len(str(dep) )== 1:
            dep='0'+str(dep)
        else:
            dep=str(dep)
        deps.append(dep)

d=0
while d < len(deps):
    dep=deps[d]
    print(dep)
    
    DPEdep = DPE.loc[DPE.dep == dep]
    
    DPEdep.to_csv('C:/Users/Ba Mouhamed/Downloads/dpe_log_'+dep+'_.csv' ,sep=';')
    print(dep, len(DPEdep))
    d=d+1

len('1235')


# In[13]:


dpeX.to_excel('lol4.xlsx')

# In[ ]:





# In[ ]:




