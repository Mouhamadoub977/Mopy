# -*- coding: utf-8 -*-
"""
Created on Sat Aug 26 17:32:50 2023

@author: BA Mouhamed
"""

import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt

parkings_05 = gpd.read_file("D:/Ciblage_parkings/Parkings/2023/parking_osm_2023_05.geojson")
parkings_04 = gpd.read_file("D:/Ciblage_parkings/Parkings/2023/parking_osm_2023_04.geojson")
parcorama_05 = gpd.read_file("D:/Ciblage_parkings/Parkings/2023/Parcorama_france_05.geojson")
parcorama_04 = gpd.read_file("D:/Ciblage_parkings/Parkings/2023/Parcorama_france_04.geojson")
gireve = pd.read_csv("D:/Ciblage_parkings/gireve/gireve_territoires_point_20230801.csv", sep=";")
commune_ref = gpd.read_file("D:/Ciblage_parkings/Communeref_2023/communeref2023.shp")



previews_file = {
    "parkings_05" : parkings_05.head(),
    "parkings_04" : parkings_04.head(),
    "parcorama_05" : parcorama_05.head(),
    "parcorama_04" : parcorama_04.head(),
    "gireve": gireve.head(),
    "commune_ref" : commune_ref.head()
    }
previews_file

#Liste des EPCI d'intérêt
epci_list = ['200067445', '200067452', '200067437']
#Filtrage des données gireve
epci_ok = commune_ref[commune_ref['SIREN_EPCI'].isin(epci_list)]
epci_ok = epci_ok.to_crs("EPSG:2154")


#Affichage des contours EPCI
fig, ax = plt.subplots(figsize=(10, 10))
epci_ok.plot(ax=ax, edgecolor='black', facecolor='none')
plt.title("Contours EPCI d'intérêt")
plt.show()

parkings_05_ok = gpd.sjoin(parkings_05, epci_ok, how="inner", op="within")
parkings_04_ok = gpd.sjoin(parkings_04, epci_ok, how="inner", op="within")
#Fusion des parkings d'intérêt
parkings_data = parkings_05_ok.append(parkings_04_ok)
parcorama = parcorama_04.append(parcorama_05)

#Calcul surface
parkings_data['surface'] = parkings_data['geometry'].area

parkings_data['nb_place_parking'] = parkings_data['surface'].apply(lambda x: int((int(x)*0.5)/12.5))
parkings_data['surf_place_parking'] = parkings_data['surface'].apply(lambda x: int((int(x)*0.5)))
parkings_data['surf_ombriere'] = parkings_data['surf_place_parking'].apply(lambda x: int((x*1.15)))
parkings_data['puissance_insta_parking'] = parkings_data['surf_ombriere'].apply(lambda x: x/5.5)


#Filtre données parcorama pour parkings d'intérêt
parcorama_ok = parcorama[parcorama['id_osm'].isin(parkings_data['id_osm'])]

# Groupement des données par identifiant de parking et comptage du nombre total de places
parkings_place = parcorama_ok.groupby('id_osm').size().reset_index(name='nombre de place')

#Suite : faire le filtre surface sup à 1500m²
#Comparaison nbre place selon parcaroma et nbre place selon methode 12.5
#Potentiel solaire si surface > 1500m² (uniquement la puissance installable en kWc, pas besoin de donner le productible)

#Etape suite : Gireve Nombre de bornes IRVE existantes sur le parking
#Calculer le ratio nombre bornes IRVE existantes / nombre de places, et mettre en place un « tag » pour chaque parking pour dire si ce ratio est inférieur ou supérieur à 5%.


print(parkings_05.crs)
print(epci_ok.crs)

#Affichage
fig, ax = plt.subplots(figsize=(12, 12))
epci_ok.plot(ax=ax, color='lightblue', edgecolor='black')
parkings_data.plot(ax=ax, color='red', markersize=100)
plt.title("Visualisation des parkings filtrés et des EPCI d'intérêt")
plt.show()


