# -*- coding: utf-8 -*-
"""
Created on Fri Aug 25 17:36:03 2023

@author: BA Mouhamed
"""

import pandas as pd, requests, os
from bs4 import BeautifulSoup
 #obs_neuf
 #obs_reno
 #obs_patrimoine
htmls=[]
i=0
while i<3000:
    
     url_neuf='https://www.observatoirebbc.org/batiments/neuf?type_rech=obs_neuf&favori=national&UserAccess=0&formatJson=1&query_start='+str(i)+'&tmpl=component'
     url_patrimoine='https://www.observatoirebbc.org/batiments/patrimoine?type_rech=obs_patrimoine&favori=national&UserAccess=0&formatJson=1&query_start='+str(i)+'&tmpl=component'
     url_reno='https://www.observatoirebbc.org/batiments/renovation?type_rech=obs_renovation&favori=national&UserAccess=0&formatJson=1&query_start='+str(i)+'&tmpl=component'
  
     html_neuf = requests.get(url_neuf).json()['result']
     html_reno = requests.get(url_reno).json()['result']
     html_patrimoine = requests.get(url_patrimoine).json()['result']
    
     htmls=htmls+html_neuf+html_reno+html_patrimoine
     print(i)
     i=i+30

htmls_= []
data= pd.DataFrame.from_dict(htmls)

cols=list(data)
def get_label_value_text(x,type_):
    label,value,text='','',''
    xx=list(x)
    if 'label' in x:
         label=x['label']
    if 'value' in x:
         value=x['value']
    if 'text' in x:
         text=x['text']
    if type_ == 'label':
        return label
    if type_ == 'value':
        return value
    if type_ == 'text':
        return text
okcols=[]
c=0
while c< len(cols):
    col=cols[c]
    newcol=data[cols[c]].apply(lambda x: get_label_value_text(x,'label'))[0]
    data[newcol]=data[cols[c]].apply(lambda x: get_label_value_text(x,'value'))
    newtext=data[cols[c]].apply(lambda x: get_label_value_text(x,'text'))[0]
    okcols.append(newcol)
    if newtext != '':
        okcols.append(newcol+'_text')
        data[newcol+'_text']=data[cols[c]].apply(lambda x: get_label_value_text(x,'text'))
    print(c,col)
    c=c+1

dataok=data[okcols]

dataok['url'] = 'https://www.observatoirebbc.org/bepos/'+dataok['ID'].map(str)

dataok.to_excel('F:/YO/observatoire_bbc/observatoire_bbc.xlsx')

 ## on scrap dans chaque page:
html_fiche=requests.get(dataok['url'][0]).text