# -*- coding: utf-8 -*-
"""
Created on Mon Mar 13 15:55:35 2023

@author: BA Mouhamed
"""

import pandas as pd
import mapbox
from geopy.distance import distance, Point





import pandas as pd
import mapbox
import geopy.distance
from shapely.geometry import shape
# Charger les données de la couche DPE et de la couche sorties_autoroute
dpe = pd.read_csv("C:/Users/BA Mouhamed/Downloads/dpe_SIA.geocoded (2).csv", sep=";")
sorties_autoroute = pd.read_csv("C:/Users/BA Mouhamed/OneDrive/Documents/distance_auto_logem/Sortie_hf.csv")

# Créer un client Mapbox
access_token = 'pk.eyJ1IjoibW9tbzA0MjIiLCJhIjoiY2xmNm5mNHp5MHdsbTNzbzQzcHFpbzM0MSJ9.QF2kxFmxmI7qhd7EkTV-pw'
mapbox_access_token = access_token

#Vérification de la validité de la clé API
# Remplacez 'YOUR_ACCESS_TOKEN_HERE' par votre clé API Mapbox
##https://docs.mapbox.com/help/getting-started/directions/#matrix-api-requests
access_token = 'pk.eyJ1IjoibW9tbzA0MjIiLCJhIjoiY2xmNm5mNHp5MHdsbTNzbzQzcHFpbzM0MSJ9.QF2kxFmxmI7qhd7EkTV-pw'

coordlog=','.join([str(dpe['longitude'][0]),str(dpe['latitude'][0])])
coordlog2='5.54,51.4;0.43,48.12'

dpeok=dpe.loc[dpe.longitude.map(str) !='nan']
dpeok['lonlat']=dpeok['longitude'].map(str)+','+dpe['latitude'].map(str)
from shapely.geometry import Point
geom_= [Point(xy) for xy in zip(dpeok['longitude'], dpeok['latitude'])]
dpeok['geometry'] =geom_
dpeok=dpeok.set_geometry('geometry')


pt=(2.79,50.239)
dpeok['geom2']=(dpeok['longitude'].map(str)+','+dpeok['latitude'].map(str))
dpeok['dis']=dpeok.geom2.apply(lambda x: geopy.distance.distance(x,pt).m).astype(int)

sorties=dpeok.loc[dpeok.groupby('Nom__commune_(BAN)')['dis'].idxmin()].sort_values("dis", ascending=True)
sortiesok = sorties[0:5]

coordlog2=';'.join(sortiesok['lonlat'].tolist())

lend=len(sortiesok)
def makedestination(lend):
    d=[]
    for r in range(1,lend):
        d.append(str(r))
    dd=';'.join(d)
    return dd

coords="3,50;2.45,48"
destinations_=makedestination(lend)
url= "https://api.mapbox.com/directions-matrix/v1/mapbox/driving/"+coordlog+";"+coordlog2+"?sources=0&destinations="+destinations_+"&&annotations=distance,duration&access_token="+access_token

import requests

json_ = requests.get(url).json()

pd.DataFrame(json_['distances'])






# Créez une instance du client Mapbox en utilisant votre clé API
client = mapbox.Directions(access_token=access_token)

# Définir la distance seuil
seuil_distance = 1000  # en mètres

# Stocker les paires (bâtiment, sortie d'autoroute) et leur distance associée dans un dictionnaire
resultats = {}

for i, row_dpe in dpe.iterrows():
    for j, row_sortie in sorties_autoroute.iterrows():
        # Calculer la distance entre chaque bâtiment et chaque sortie d'autoroute en utilisant l'API Mapbox
        point_dpe = Point(row_dpe["latitude"], row_dpe["longitude"])
        point_sortie = Point(shape(row_sortie['geometry']).centroid.y, row_sortie["longitude"])
        d = distance(point_dpe, point_sortie).meters

        # Si la distance est inférieure à la distance seuil, stocker la paire (bâtiment, sortie d'autoroute)
        if d < seuil_distance:
            if i not in resultats:
                resultats[i] = []
            resultats[i].append((j, d))

# Afficher les résultats
for i, distances in resultats.items():
    print(f"Bâtiment {i} :")
    for j, d in distances:
        print(f"  - Sortie d'autoroute {j} : distance = {d:.2f} mètres")

"""

import pandas as pd
import mapbox
from geopy.distance import distance, Point

# Charger les données de la couche DPE et de la couche sorties_autoroute
dpe = pd.read_csv("C:/Users/BA Mouhamed/Downloads/dpe_SIA.geocoded (2).csv", sep=";")
sorties_autoroute = pd.read_csv("C:/Users/BA Mouhamed/OneDrive/Documents/distance_auto_logem/Sortie_hf.csv")

# Créer un client Mapbox
access_token = 'sk.eyJ1IjoibW9tbzA0MjIiLCJhIjoiY2xmMTN5eHdvMDRvdjNycG5mZXllMWtiOCJ9.Q0mjLS_nqAiNtMHLmGEBmQ'
mapbox_access_token = access_token

#Vérification de la validité de la clé API
# Remplacez 'YOUR_ACCESS_TOKEN_HERE' par votre clé API Mapbox
access_token = 'sk.eyJ1IjoibW9tbzA0MjIiLCJhIjoiY2xmMTN5eHdvMDRvdjNycG5mZXllMWtiOCJ9.Q0mjLS_nqAiNtMHLmGEBmQ'

# Créez une instance du client Mapbox en utilisant votre clé API
client = mapbox.Directions(access_token=access_token)

# Définir la distance seuil
seuil_distance = 1000  # en mètres

# Stocker les paires (bâtiment, sortie d'autoroute) et leur distance associée dans un dictionnaire
resultats = {}

for i, row_dpe in dpe.iterrows():
    for j, row_sortie in sorties_autoroute.iterrows():
        # Calculer la distance entre chaque bâtiment et chaque sortie d'autoroute en utilisant l'API Mapbox
        point_dpe = Point(row_dpe["latitude"], row_dpe["longitude"])
        point_sortie = Point(row_sortie["latitude"], row_sortie["longitude"])
        d = distance(point_dpe, point_sortie).meters

        # Si la distance est inférieure à la distance seuil, stocker la paire (bâtiment, sortie d'autoroute)
        if d < seuil_distance:
            if i not in resultats:
                resultats[i] = []
            resultats[i].append((j, d))

# Afficher les résultats
for i, distances in resultats.items():
    print(f"Bâtiment {i} :")
    for j, d in distances:
        print(f"  - Sortie d'autoroute {j} : distance = {d:.2f} mètres")


















"""

# Charger les données de la couche DPE et de la couche sorties_autoroute
dpe = pd.read_csv("C:/Users/BA Mouhamed/Downloads/dpe_SIA.geocoded (2).csv", sep=";")
sorties_autoroute = gpd.read_file("C:/Users/BA Mouhamed/OneDrive/Documents/distance_auto_logem/sortie_autoroute_pts.geojson")

# Créer un client Mapbox

access_token = 'sk.eyJ1IjoibW9tbzA0MjIiLCJhIjoiY2xmMTN5eHdvMDRvdjNycG5mZXllMWtiOCJ9.Q0mjLS_nqAiNtMHLmGEBmQ'
mapbox.mapbox_access_token = access_token


#Vérification de la validité de la clé API

# Remplacez 'YOUR_ACCESS_TOKEN_HERE' par votre clé API Mapbox
access_token = 'sk.eyJ1IjoibW9tbzA0MjIiLCJhIjoiY2xmMTN5eHdvMDRvdjNycG5mZXllMWtiOCJ9.Q0mjLS_nqAiNtMHLmGEBmQ'

# Créez une instance du client Mapbox en utilisant votre clé API
client = mapbox.MapMatcher(access_token=access_token)

#client = mapbox.Client(access_token="pk.eyJ1IjoibW9tbzA0MjIiLCJhIjoiY2xmNm5rOHZoMG1mbTNzbm45dmVuanR3byJ9.vu8qDf_cFmScSz7Xpur1kA")

# Définir la distance seuil
seuil_distance = 1000  # en mètres

# Stocker les paires (bâtiment, sortie d'autoroute) et leur distance associée dans un dictionnaire
resultats = {}

for i, row_dpe in dpe.iterrows():
    for j, row_sortie in sorties_autoroute.iterrows():
        # Calculer la distance entre chaque bâtiment et chaque sortie d'autoroute en utilisant l'API Mapbox
        d = client.distance_matrix(origins=[(row_dpe["longitude"], row_dpe["latitude"])], destinations=[(row_sortie["longitude"], row_sortie["latitude"])])["durations"][0][0]
        
        # Si la distance est inférieure à la distance seuil, stocker la paire (bâtiment, sortie d'autoroute)
        if d < seuil_distance:
            if i not in resultats:
                resultats[i] = []
            resultats[i].append((j, d))

# Afficher les résultats
for i, distances in resultats.items():
    print(f"Bâtiment {i} :")
    for j, d in distances:
        print(f"  - Sortie d'autoroute {j} : distance = {d:.2f} mètres")
