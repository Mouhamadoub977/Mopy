# -*- coding: utf-8 -*-
"""
Created on Tue Apr 11 15:12:16 2023

@author: YL3D829N
"""

import geopandas as gpd
import pandas as pd
import os
from shapely.wkt import loads

def getdeps():
    deps=[]
    
    for d in range(1,96):
        if d != 20:
            if len(str(d)) ==1:
                deps.append('0'+str(d))
            else:
                deps.append(str(d))
    return deps

deps = getdeps()

  # 6 Le site est un ancien aérodrome, délaissé d’aérodrome, un ancien aéroport ou un délaissé d’aéroport
  ## délaissé aéroport = aéroport - piste aérodrome
anciennepistes=gpd.GeoDataFrame()
anciennepisteaerodromes=gpd.GeoDataFrame()
aerodromes=gpd.GeoDataFrame()
delaisses=gpd.GeoDataFrame()
delaissesmaxx=gpd.GeoDataFrame()
batisaeroports = gpd.GeoDataFrame()
for dep in deps:  
    os.chdir('D:/IGN/BDTOPOV3latest/BDTOPO_3-0_TOUSTHEMES_SHP_LAMB93_D0'+dep+'_2021-03-15/BDTOPO/1_DONNEES_LIVRAISON_2021-03-00272/BDT_3-0_SHP_LAMB93_D0'+dep+'-ED2021-03-15'+'/TRANSPORT')
         
    
    aerodrome = gpd.read_file('AERODROME.shp')
    aerodrome['surface']=aerodrome.area
    aerodrome['geometry']=aerodrome['geometry'].map(str)
    aerodrome = aerodrome.drop_duplicates()
    aerodrome['geometry']=aerodrome['geometry'].apply(lambda x: loads(x))
    aerodromes=aerodromes.append(aerodrome)
    pisteaerodrome = gpd.read_file('PISTE_D_AERODROME.shp')
    
    ## ok 
    
    anciennepiste = pisteaerodrome.loc[pisteaerodrome['ETAT'] == 'Non exploité']
    
    anciennepisteaerodrome = gpd.sjoin(anciennepiste, aerodrome, op='intersects', how='left', lsuffix='piste',rsuffix='aerodrome')
    anciennepisteaerodrome['surface']=anciennepisteaerodrome.area
    #anciennepistes=anciennepistes.append(anciennepiste )
    anciennepisteaerodromes=anciennepisteaerodromes.append(anciennepisteaerodrome)
    
    os.chdir('D:/IGN/BDTOPOV3latest/BDTOPO_3-0_TOUSTHEMES_SHP_LAMB93_D0'+dep+'_2021-03-15/BDTOPO/1_DONNEES_LIVRAISON_2021-03-00272/BDT_3-0_SHP_LAMB93_D0'+dep+'-ED2021-03-15'+'/BATI')
    bati = gpd.read_file('BATIMENT.shp')
    
    batiaerodromes = gpd.sjoin(bati, aerodromes, op='within',how='inner',lsuffix='_bati',rsuffix='_aerodrome')
    batisaeroports=batisaeroports.append(batiaerodromes)
    ### 
    delaisse = gpd.overlay( aerodrome, pisteaerodrome, how='difference')
    delaissem=delaisse.copy()
    delaisse['surface'] = delaisse.area
    delaisse=delaisse[list(aerodrome.keys())]
    delaisse.crs = aerodrome.crs
    delaisse=delaisse.to_crs(epsg=4326)
    delaisses=delaisses.append(delaisse)
    
    batiaerodromes=batiaerodromes.reset_index().drop('index', axis=1)
    
    delaissemax = gpd.overlay( delaissem, batiaerodromes, how='difference')
    delaissemax['surface'] = delaissemax.area
    delaissemax=delaissemax[list(aerodrome.keys())]
    delaissesmaxx=delaissesmaxx.append(delaissemax)
    print(dep)

aerodromes.crs = {'init' : 'epsg:2154'}
aerodrome3000 = aerodromes.copy()
aerodrome3000['geometry'] = aerodrome3000.buffer(3000)

aerodrome3000=aerodrome3000.to_crs(epsg=4326)
aerodrome3000.to_file('D:/YO/AUTOCONSO PV/aerodromes_3km.geojson', driver='GeoJSON')

aerodromes=aerodromes.to_crs(epsg=4326)
aerodromes.to_file('D:/YO/AUTOCONSO PV/aerodromes.geojson', driver='GeoJSON', encoding='utf-8')

os.chdir('D:/YO/AUTOCONSO PV/')
aerodromes = gpd.read_file('aerodromes.geojson', encoding='utf-8')

delaisses = delaisses.reset_index().drop('index', axis=1)
delaisses['geometry']=delaisses['geometry'].map(str)
delaisses['surface']=delaisses['surface'].map(int)
delaisses = delaisses.drop_duplicates()
delaisses['geometry']=delaisses['geometry'].apply(lambda x: loads(x))

delaisses.to_file('D:/YO/AUTOCONSO PV/delaisses_aerodromes.geojson', driver='GeoJSON')

delaissesmaxx.crs = aerodrome.crs
delaissesmaxx=delaissesmaxx.to_crs(epsg=4326)
delaissesmaxx.to_file('D:/YO/AUTOCONSO PV/delaisses_aerodromes_without_batis.geojson', driver='GeoJSON')


batisaeroports.crs = batiaerodromes.crs
batisaeroports=batisaeroports.to_crs(epsg=4326)
batisaeroports.to_file('D:/YO/AUTOCONSO PV/batiments_aerodromes.geojson', driver='GeoJSON', encoding='utf-8')


anciennepisteaerodromes=anciennepisteaerodromes.to_crs(epsg=4326)
anciennepisteaerodromes['ID']=anciennepisteaerodromes['ID_piste']
anciennepisteaerodromes['IDID']='old_aero_'+anciennepisteaerodromes.index.map(str)
anciennepisteaerodromes['type']='ancien aerodrome'


yodata = yodata.append(anciennepisteaerodromes[['ID','IDID','surface','type','geometry']])


anciennepisteaerodromes.to_file('D:/YO/AUTOCONSO PV/anciennes_pistes_aerodromes.geojson', driver='GeoJSON')

anciennepisteaerodromes = gpd.read_file('D:/YO/AUTOCONSO PV/anciennes_pistes_aerodromes.geojson')
delaisses = gpd.read_file('D:/YO/AUTOCONSO PV/delaisses_aerodromes.geojson', encoding='utf-8')
