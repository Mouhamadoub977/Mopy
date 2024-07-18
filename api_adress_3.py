# -*- coding: utf-8 -*-
"""
Created on Fri Aug 11 17:32:53 2023
@author: BA Mouhamed
"""

import pandas as pd
import requests
import time
from fuzzywuzzy import fuzz
import math  # pour calcul des distance score_similitude adresse & code naf
import re  # supprimer code postal de pdres pour meilleure evaluation similarité adresse
from statistics import mean  # pour calcul de la moy de similarité entre les adresses data_ok et pdres_ok


# Fonction pour supprimer les codes postaux dans pdres_ok
def remove_postal_code_re(address):
    return re.sub(r'\b\d{5}\b', '', address).strip()

def fuzzywuzzyyoupi(y,x):
        Ratio = fuzz.ratio(y,x)
        Partial_Ratio = fuzz.partial_ratio(y, x)
        Token_sort_ratio = fuzz.token_sort_ratio(y, x)
        token_set_ratio = fuzz.token_set_ratio(y, x)
        moy= mean([Ratio,Partial_Ratio,Token_sort_ratio,token_set_ratio])
        return moy
    

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calcule la distance en kilomètres entre deux points géographiques.

    Args:
    - lat1, lon1 : latitude et longitude du premier point
    - lat2, lon2 : latitude et longitude du deuxième point

    Returns:
    - distance en kilomètres entre les deux points
    """
    # Rayon de la Terre en kilomètres
    R = 6371.0

    # Convertir les degrés en radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    # Calculer les différences entre les latitudes et longitudes
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    # Formule haversine
    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c

    return distance


# Charger le fichier CSV contenant les données sur la consommation et les points de livraison d'énergie.
data = pd.read_csv('D:/pour_catch_heat/Donnees-de-consommation-et-de-points-de-livraison-denergie-a-la-maille-adresse-gaz_geo.csv', sep=';')

data['idid']=data['adresse'].map(str)+'_'+data['Ville'].map(str)+'_'+data.index.map(str)

# Filtrer le DataFrame pour ne garder que les lignes qui ont une adresse ou des coordonnées lat/lon et un code NAF.
data_ok = data[~data['CODE_SECTEUR_NAF2_CODE'].isna() & ((~data['adresse'].isna()) | (~data['latitude'].isna() & ~data['longitude'].isna()))]

# Pour augmenter la précision des recherches, on concatène l'adresse et la ville pour créer une adresse complète.
data_ok['adresse_complete'] = data_ok['adresse'] + ", " + data_ok['Ville']

# Charger le fichier Excel contenant les codes NAF.
naf_data = pd.read_excel("D:/pour_catch_heat/naf2008_5_niveaux.xlsx", dtype={'NIV2': str})

# Convertir la colonne des codes NAF en chaîne de caractères pour éviter les problèmes de format.
data_ok['CODE_SECTEUR_NAF2_CODE'] = data_ok['CODE_SECTEUR_NAF2_CODE'].astype(str)

# Créer un dictionnaire de correspondance entre les codes NAF de niveau 2 et les codes NAF de niveau 1.
naf2_to_letter_map = dict(zip(naf_data['NIV2'].astype(str), naf_data['NIV1']))

 
# Fonction pour effectuer une recherche textuelle à l'aide de l'API du gouvernement.
def recherche_textuelle_api(adresse, naf2_code):
    section_activite_principale = naf2_to_letter_map.get(naf2_code)
    url = f"https://recherche-entreprises.api.gouv.fr/search?q={adresse}&section_activite_principale={section_activite_principale}"
    return url


# Fonction pour effectuer une recherche géographique à l'aide de l'API du gouvernement.
def recherche_geographique_api(latitude, longitude, naf2_code, rayon=0.5):
    section_activite_principale = naf2_to_letter_map.get(naf2_code)
    url = f"https://recherche-entreprises.api.gouv.fr/near_point?lat={latitude}&long={longitude}&radius={rayon}&section_activite_principale={section_activite_principale}"
    return url


# Fonction pour appeler l'API. Si l'appel échoue, elle réessayera jusqu'à un maximum d'essais.
def appeler_api_avec_essais(url, index, essais_max=3, delai=10):
    print(f"Effectue un appel API pour la ligne {i} avec l'URL : {url}")
    for essai in range(essais_max):
        response = requests.get(url)
        if response.status_code == 200:
            print(f"Appel API pour la ligne {i} réussi.")
            return response.json()
        else:
            print(f"Erreur lors de l'appel API pour la ligne {i} (Essai {essai + 1}). Code d'erreur {response.status_code} : {response.text}")
            if essai < essais_max - 1:  # Pas besoin d'attendre après le dernier essai
                print(f"Attente de {delai} secondes avant de réessayer...")
                time.sleep(delai)
    return None  # Si tous les essais échouent


# Liste pour stocker les résultats de l'API pour chaque ligne du DataFrame.
resultats_api_debug = []

# Réinitialiser l'index du DataFrame pour avoir une séquence continue d'index.
#data_ok = data_ok.reset_index().drop('index', axis=1)

data_ok = data_ok.sample(n=15).reset_index(drop=True)

 #Test fonction pour traiter les résultats de l'API  
def traiter_resultats_api(pdres, adresse_complete, current_naf2_code, data_row):
    # Préparation pour stocker les entreprises trouvées.
    pdres_ok = pd.DataFrame()
    


    ii = 0
        # Parcourir chaque entreprise trouvée.
    while ii <len(pdres):
            print(f"Traitement de l'entreprise {ii+1} renvoyée par l'API pour la ligne {i}")
            matching_etablissements=pdres['matching_etablissements'][ii]
            for res_et in matching_etablissements:
                
                res_et['siren'] = pdres['siren'][ii]
                pdres_et = pd.DataFrame.from_dict([res_et])
                pdres_et.columns = pdres_et.columns + '_siret'
                pdres_et = pdres_et.rename(columns={'siren_siret': 'siren'})
                pdres_ = pdres.merge(pdres_et, on='siren')
                pdres_ok = pd.concat([pdres_ok, pdres_])

           # print(f"Fin du traitement de l'entreprise {ii} pour la ligne {i}")
            ii += 1

    p = 0
    pdres_ok=pdres_ok.reset_index().drop('index',axis=1)
    while p < len(pdres_ok):
            adresse_api = remove_postal_code_re(pdres_ok['adresse_siret'].iloc[p].lower() if isinstance(pdres_ok['adresse_siret'].iloc[p], str) else "")
            score_similitude = fuzzywuzzyyoupi(adresse_complete.lower(), adresse_api)
                
                
            if score_similitude > 80:
                pdres_ok.loc[p,'scoring_adress'] = score_similitude
            #code_naf_api = pdres_ok['activite_principale_siret'].iloc[p][:2]
            value = pdres_ok['activite_principale_siret'].iloc[p]
            if isinstance(value, str):
                code_naf_api = value[:2]
            else:
                code_naf_api = None

            score_similitude_naf = fuzzywuzzyyoupi(current_naf2_code, code_naf_api)
            if score_similitude_naf > 95:
                pdres_ok.loc[p,'scoring_naf'] = score_similitude_naf
    #Calcul de la distance entre coordonnées API et originales                      
            # Vérifiez d'abord si les valeurs de latitude et longitude à la position actuelle ne sont pas nulles
            if pd.notna(pdres_ok['latitude_siret'].iloc[p]) and pd.notna(pdres_ok['longitude_siret'].iloc[p]):
                lat_api = pdres_ok['latitude_siret'].iloc[p]
                lon_api = pdres_ok['longitude_siret'].iloc[p]
                
                try:
                    lat_api = float(lat_api)
                    lon_api = float(lon_api)
                    
                    distance = haversine_distance(data_ok['latitude'][i], data_ok['longitude'][i], lat_api, lon_api)
                    # Si la distance est supérieure à 100m
                    if distance > 0.2:
                        pdres_ok.loc[p,'distance'] = distance                        
                        print(f"Distance supérieure à 100m pour la ligne {i} {p}.")
                                        
                except ValueError:
                    print(f"Coordonnées API non valides pour la ligne {i}.")
            else:
                print(f"Coordonnées API manquantes pour la ligne {i}.")



            
    
            # Ici, on peut décider d'exclure ou de traiter différemment cette entreprise en fonction de la distance
           # if distance > 0.5:  # par exemple, si la distance est supérieure à 0,5 km
              # print(f"Distance entre les coordonnées API et originales pour la ligne {index}: {distance} km")
            
            p=p+1
    
    pdres_ok['idid_consos'] = data_row['idid']
    
    return pdres_ok
           
        ## regles pour choisir
        # adresse similaire ? fuzzy wuzzy
       # pdres_ok[ 'adresse_siret'][0]
       # adresse_complete
        # naf similaire ?
        # distance?
        # autre
# Parcourir chaque ligne du DataFrame.
i=0
data_ok = data_ok.reset_index().drop('index', axis=1)

while i <len(data_ok):
    print(f"Commencement du traitement de la ligne {i}")

    # Extraire les informations importantes de la ligne actuelle.
    adresse_complete = data_ok['adresse_complete'][i]
    ville = data_ok['Ville'][i].lower()
    adresse = data_ok['adresse'][i].lower() if pd.notna(data_ok['adresse'][i]) else ""
    nbpdl = data_ok['PDL'][i]
    current_naf2_code = data_ok['CODE_SECTEUR_NAF2_CODE'][i].split('.')[0]

    # Obtenir la correspondance du code NAF actuel avec la section d'activité principale.
    section_activite_principale = naf2_to_letter_map.get(current_naf2_code)

    # Si la section d'activité principale n'est pas trouvée pour le code NAF, afficher un message d'erreur.
    if not section_activite_principale:
        print(f"Ligne {i} : section_activite_principale non trouvée pour le code NAF2 {current_naf2_code}.")

    # Selon que l'adresse est présente ou non, choisir la méthode de recherche appropriée (textuelle ou géographique).
    if pd.notna(adresse_complete):
        url = recherche_textuelle_api(adresse_complete, current_naf2_code) + '&radius=0.5'
    else:
        url = recherche_geographique_api(data_ok['latitude'][i], data_ok['longitude'][i], current_naf2_code)

    # Appeler l'API avec l'URL générée.
    print(f"Préparation à appeler l'API pour la ligne {i}")
    resultat = appeler_api_avec_essais(url, i)
    print(f"Appel API terminé pour la ligne {i}")
    
    if resultat and 'results' in resultat and resultat['results']:
 
        print(f"Commencement du traitement des résultats de l'API pour la ligne {i}")
        pdres = pd.DataFrame.from_dict(resultat['results'])
        try:
            pdres_ok_ = traiter_resultats_api(pdres, adresse_complete, current_naf2_code, data_ok.iloc[i])
            resultats_api_debug.append(pdres_ok_)
        except:
            print('bug')
    print(f"Ligne {i} traitée.")

    i = i + 1
    
resultats_df_test = pd.concat(resultats_api_debug, ignore_index=True)
# Convertir la liste des résultats en DataFrame.
resultats_df = pd.DataFrame(resultats_api_debug)

# Sauvegarder les résultats dans un fichier CSV.
data_resultats_api_lol = "D:/pour_catch_heat/resultats_api2.csv"
resultats_df.to_csv(data_resultats_api_lol, index=False, sep=';')
print(f"Les résultats ont été sauvegardés dans le fichier {data_resultats_api_lol}")

# Afficher des statistiques sur les résultats obtenus.
resultats_df.describe()
