# -*- coding: utf-8 -*-
"""
Created on Wed Mar  1 15:03:25 2023

@author: F04182
"""

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from shapely import geometry, ops
from rtree import index
import rtree
from shapely.strtree import STRtree
from shapely.geometry import LineString, shape
def segments_(curve):
    return list(map(LineString, zip(curve.coords[:-1], curve.coords[1:])))

from shapely.geometry import LineString, LinearRing,mapping,Point,MultiLineString
def segments_(curve):
    return list(map(LineString, zip(curve.coords[:-1], curve.coords[1:])))

def cut(line, distance):
# Cuts a line in two at a distance from its starting point
    if distance <= 0.0 or distance >= line.length:
        return [LineString(line)]
    coords = list(line.coords)
    for i, p in enumerate(coords):
        pd = line.project(Point(p))
        if pd == distance:
            return [
                LineString(coords[:i+1]),
                LineString(coords[i:])]
        if pd > distance:
            cp = line.interpolate(distance)
            return [
                LineString(coords[:i] + [(cp.x, cp.y)]),
                LineString([(cp.x, cp.y)] + coords[i:])]

def cut_piece(line,distance, lgth):
    """ From a linestring, this cuts a piece of length lgth at distance.
    Needs cut(line,distance) func from above ;-) 
    """
    precut = cut(line,distance)[1]
    result = cut(precut,lgth)[0]
    return result


def cut_gdf_by_maxlong(gdf,maxlong,idcolname):
    gdf=gdf.reset_index().drop('index', axis=1)
    resdats=gpd.GeoDataFrame()
    g=0
    while g<len(gdf):
        
        id_ = gdf[idcolname][g]
        lineok = gdf['geometry'][g]
        try:
            line_segments = segments_(lineok)
        except:
            line_segments=list(lineok)
        lines=[]
        s=0
        while s<len(line_segments):
            segments=line_segments[s]
            long = segments.length
            #print(long,'long')
            if long>maxlong:
                    
                
                ll=0
                
                d=1
                while d<segments.length:
                    try:
                        r = cut_piece(segments, d,maxlong)
                        lines.append(r)
                        #print(str(ll),str(d),str(r.length))
                    except:
                        pass
                        #print('error ',str(ll),str(d),str(segments.length))
                    d=d+maxlong
                    ll=ll+1
                    #print(str(ll))
            else:
                
                lines.append(segments)
            s=s+1
        resdat=pd.DataFrame(lines,columns=['geomline'])
        resdat=resdat.set_geometry('geomline')
        resdat[idcolname] = id_
        resdats=resdats.append(resdat)
        print(g,len(gdf))
        g=g+1
    return resdats



rrn= gpd.read_file('C:/Users/f04182/Downloads/classification-rrn-2022-shp/Classification-RRN_2022.shp')

osm= gpd.read_file('C:/Users/f04182/Downloads/export.geojson')
osmok = osm.loc[osm[ 'junction:ref'].map(str) != 'None']
osmok=osmok.to_crs(epsg=2154)

osmok.geometry = osmok.buffer(2)

bdtopo = gpd.read_file('C:/Users/f04182/Documents/BDTOPO_3-0_TOUSTHEMES_GPKG_LAMB93_D041_2022-09-15/BDTOPO_3-0_TOUSTHEMES_GPKG_LAMB93_D041_2022-09-15/BDTOPO/1_DONNEES_LIVRAISON_2022-10-00024/BDT_3-0_GPKG_LAMB93_D041-ED2022-09-15/BDT_3-0_GPKG_LAMB93_D041-ED2022-09-15.gpkg',
            layer='troncon_de_route'           )

rrnok = rrn.loc[rrn['Classifica'] == 'Bretelle Autoroute']

rrnok['ID'] = rrnok.route.map(str)+'_'+rrnok.prD.map(str)


rrnok_ = gpd.sjoin(rrnok, osmok[['geometry', 'junction:ref']],op='intersects', how='inner')


rrnok__ = rrnok_.groupby(['ID'], as_index=False).agg({'junction:ref' : lambda x: '/'.join(x.unique())})

rrnokok = rrnok.merge(rrnok__,on='ID',how='left')

deps=list(rrnokok['depPrD'].unique())
##bd topo
rrnokokdepgoOKOK = gpd.GeoDataFrame()
rrnokokdepgogo=gpd.GeoDataFrame()
d=0
while d<len(deps):
    dep=deps[d]
    print(dep,'go')
    rrnokokdep =  rrnokok.loc[rrnokok['depPrD'] == dep]
    
   # bdtopo = gpd.read_file('C:/Users/f04182/Documents/BDTOPO_3-0_TOUSTHEMES_GPKG_LAMB93_D041_2022-09-15/BDTOPO_3-0_TOUSTHEMES_GPKG_LAMB93_D041_2022-09-15/BDTOPO/1_DONNEES_LIVRAISON_2022-10-00024/BDT_3-0_GPKG_LAMB93_D041-ED2022-09-15/BDT_3-0_GPKG_LAMB93_D041-ED2022-09-15.gpkg',
       #         layer='troncon_de_route'           )
    bdtopo = gpd.read_file('D:/IGN/BDTOPOV3latest/BDTOPO_3-0_TOUSTHEMES_SHP_LAMB93_D0'+dep+'_2022-09-15/BDTOPO/1_DONNEES_LIVRAISON_2022-09-00418/BDT_3-0_SHP_LAMB93_D0'+dep+'-ED2022-09-15/TRANSPORT/TRONCON_DE_ROUTE.shp')
    print('bd topo lue')
    bdtopo.geometry = bdtopo .buffer(6)
    print('bd buffer ok')
    bdtopo=bdtopo.rename(columns={'NATURE':'nature'})
    rrnokokdep_ = gpd.sjoin(rrnokokdep, bdtopo[['geometry', 'nature']],op='intersects', how='left')
    
    rrnokokdep__= rrnokokdep_.groupby(['ID'], as_index=False).agg({'nature' : lambda x: '/'.join(x.unique())})
    
    rrnokokdepgo = rrnokokdep.merge(rrnokokdep__,on='ID',how='left')
    
    
    def is_sortie(x):
        for t in ['Route à 1 chaussée', 'Route empierrée', 
        'Rond-point', 
           'Route à 2 chaussées' ]:
            
            if t in x :
                return 'ok'
            
    rrnokokdepgo['sortie'] = rrnokokdepgo.nature.apply(lambda x: is_sortie(x))
    
    ## decoupe 10m
    
    
    rrnokokdepgo['longueur'] =rrnokokdepgo['geometry'].length
    
    rrnokokdepgogo=rrnokokdepgogo.append(rrnokokdepgo)
    
    
    rrnokokdepgo_50_cut = cut_gdf_by_maxlong(rrnokokdepgo,50,'ID')
    rrnokokdepgo_50_cut=rrnokokdepgo_50_cut.set_geometry('geomline')
    
    rrnokokdepgo_50_cut.geometry = rrnokokdepgo_50_cut.centroid
    rrnokokdepgo_50_cut.crs={'init' : 'epsg:2154'}
    
    rrnokokdepgoOK = rrnokokdepgo_50_cut.merge(rrnokokdepgo,on='ID',how='left')
    rrnokokdepgoOK=rrnokokdepgoOK.set_geometry('geomline')
    rrnokokdepgoOK['geometry'] = rrnokokdepgoOK['geometry'].map(str)
    #rrnokokdepgoOK.to_file('C:/Users/f04182/Downloads/test01.geojson',driver='GeoJSON')
    
    rrnokokdepgoOKOK=rrnokokdepgoOKOK.append(rrnokokdepgoOK)
    print(dep,'fait',len(rrnokokdepgoOKOK),d,len(deps))
    rrnokokdepgoOKOK['geometry'] = rrnokokdepgoOKOK['geometry'].map(str)
    rrnokokdepgoOKOK=rrnokokdepgoOKOK.set_geometry('geomline')
    rrnokokdepgoOKOK.to_file('C:/Users/f04182/Downloads/test'+dep+'_.geojson',driver='GeoJSON')
    
    d=d+1
rrnokokdepgogo.to_file('C:/Users/f04182/Downloads/test_line_.geojson',driver='GeoJSON')
