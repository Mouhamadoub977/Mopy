# -*- coding: utf-8 -*-
"""
Created on Fri Aug 11 17:32:53 2023
@author: BA Mouhamed
"""

import pandas as pd
import requests
import time
from fuzzywuzzy import fuzz
import math #pour calcul des distance score_similitude adresse & code naf
import re   #supprimer code postal de pdres pour meilleure evaluation similarité adresse
from statistics import mean  #pour calcul de la moy de similarité entre les adresses data_ok et pdres_ok





#Fonction pour supprimer les codes postaux dans pdres_ok
def remove_postal_code_re(address):
    return re.sub(r'\b\d{5}\b', '', address).strip()
    


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
data = pd.read_csv('D:/pour_catch_heat/Donnees-de-consommation-et-de-points-de-livraison-denergie-a-la-maille-adresse-gaz_geo.csv', sep =';')

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
    print(f"Effectue un appel API pour la ligne {index} avec l'URL : {url}")
    for essai in range(essais_max):
        response = requests.get(url)
        if response.status_code == 200:
            print(f"Appel API pour la ligne {index} réussi.")
            return response.json()
        else:
            print(f"Erreur lors de l'appel API pour la ligne {index} (Essai {essai + 1}). Code d'erreur {response.status_code} : {response.text}")
            if essai < essais_max - 1:  # Pas besoin d'attendre après le dernier essai
                print(f"Attente de {delai} secondes avant de réessayer...")
                time.sleep(delai)
    return None  # Si tous les essais échouent

# Liste pour stocker les résultats de l'API pour chaque ligne du DataFrame.
resultats_api_debug = []

# Réinitialiser l'index du DataFrame pour avoir une séquence continue d'index.
data_ok = data_ok.reset_index().drop('index', axis=1)

i = 0
# Parcourir chaque ligne du DataFrame.
while i < len(data_ok):
    index = data_ok.index[i]
    row = data_ok.iloc[i]
    
    # Extraire les informations importantes de la ligne actuelle.
    adresse_complete = row['adresse_complete']
    ville = row['Ville'].lower()
    adresse = row['adresse'].lower()
    nbpdl = row['PDL']
    current_naf2_code = row['CODE_SECTEUR_NAF2_CODE'].split('.')[0]
   
    # Obtenir la correspondance du code NAF actuel avec la section d'activité principale.
    section_activite_principale = naf2_to_letter_map.get(current_naf2_code)

    # Si la section d'activité principale n'est pas trouvée pour le code NAF, afficher un message d'erreur.
    if not section_activite_principale:
        print(f"Ligne {index} : section_activite_principale non trouvée pour le code NAF2 {current_naf2_code}.")

    # Selon que l'adresse est présente ou non, choisir la méthode de recherche appropriée (textuelle ou géographique).
    if pd.notna(row['adresse_complete']):
        url = recherche_textuelle_api(row['adresse_complete'], current_naf2_code) + '&radius=0.5'
    else:
        url = recherche_geographique_api(row['latitude'], row['longitude'], current_naf2_code)

    # Appeler l'API avec l'URL générée.
    resultat = appeler_api_avec_essais(url, index)

   
    # Si des entreprises sont trouvées, les traiter.
    if resultat['results'] != []:
        pdres = pd.DataFrame.from_dict(resultat['results'])

        # Préparation pour stocker les entreprises trouvées.
        pdres_ok = pd.DataFrame()

        ii = 0
        # Parcourir chaque entreprise trouvée.
        while ii < len(pdres['matching_etablissements']):
            for res_et in pdres['matching_etablissements'][ii]:
                # Associer le siren de l'entreprise principale à chaque établissement.
                res_et['siren'] = pdres['siren'][ii]

                # Convertir les informations de l'établissement en DataFrame.
                pdres_et = pd.DataFrame.from_dict([res_et])

                # Renommer les colonnes pour indiquer qu'elles proviennent des informations de l'établissement.
                pdres_et.columns = pdres_et.columns + '_siret'
                pdres_et = pdres_et.rename(columns={'siren_siret': 'siren'})

                # Fusionner les informations de l'entreprise principale et de l'établissement.
                pdres_ = pdres.merge(pdres_et, on='siren')

                # Ajouter les informations fusionnées à notre DataFrame de résultats.
                pdres_ok = pd.concat([pdres_ok, pdres_])
            
            ii = ii + 1
        def fuzzywuzzyyoupi(y,x):
            Ratio = fuzz.ratio(y,x)
            Partial_Ratio = fuzz.partial_ration(y, x)
            Token_sort_ratio = fuzz.token_sort_ratio(y, x)
            token_set_ratio = fuzz.token_set_ratio(y, x)
            moy= mean([Ratio,Partial_Ratio,Token_sort_ratio,token_set_ratio])
            return moy
        
        p=0
        while p < len(pdres_ok):
            
            adresse_api = remove_postal_code_re(pdres_ok['adresse_siret'].iloc[p].lower())
            score_similitude = fuzz.ratio(adresse_complete.lower(), adresse_api)
            
            
            pdres_ok
            # Comparer les adresses avec fuzzywuzzy pour vérifier leur similitude.
            codepos = re.search(r'\d{5}', pdres_ok['adresse_siret'][p]).group()
            #nettoyer adresse
            cleaned_address = re.sub(r'\d{5}', '', pdres_ok['adresse_siret'][p]).strip()
            score_similitude = fuzz.ratio(adresse_complete.lower(), cleaned_address.lower())
            # supp des lignes
            if re.search(r'\d{5}', pdres_ok['adresse_siret'][p]):
                codepos = re.search(r'\d{5}', pdres_ok['adresse_siret'][p]).group()
            

            score_similitude = fuzz.ratio(adresse_complete.lower(), pdres_ok['adresse_siret'][p].lower())
            score_similitude2 = fuzzywuzzyyoupi(adresse_complete.lower(), pdres_ok['adresse_siret'][0].lower())
            if score_similitude < 80:  # Seuil 80 à ajuster en fonction des résultats
               print(f"Adresse API différente de l'adresse originale pour la ligne {index}. Score de similitude : {score_similitude}")
               # Ici, on peut décider d'exclure ou de traiter différemment cette entreprise
               
               #Comparer les codes NAF et/ou évaluer leur indice de similitude (data_ok & pdres_ok)
            #Récuperer d'abord le code NAF renvoyé par l'API
            code_naf_api = pdres_ok['activite_principale_siret'].iloc[p] #Nom de la colonne contenant le code NAF
            score_similitude_naf = fuzz.ratio(current_naf2_code, code_naf_api)
            if score_similitude_naf < 95:
                print(f"Code NAF API différent du code NAF original pour la ligne {index}. Score de similitude : {score_similitude_naf}")
    
            # À ce stade, on peut ajouter d'autres règles pour filtrer ou trier les entreprises trouvées.
            # On peut, par exemple, vérifier si le code NAF est similaire, ou calculer la distance entre les adresses, etc.
     # Calcul de distance entre adresses/Code naf différents suite déduction score similitude
             #Récupérons d'abord les lat, lon retournées par l'API
            lat_api = float(pdres_ok['latitude_siret'][0])
            lon_api = float(pdres_ok['longitude_siret'][0])
            
            distance = haversine_distance(row['latitude'], row['longitude'], lat_api, lon_api)
            print(f"Distance entre les coordonnées API et originales pour la ligne {index}: {distance} km")
    
            # Ici, on peut décider d'exclure ou de traiter différemment cette entreprise en fonction de la distance
           # if distance > 0.5:  # par exemple, si la distance est supérieure à 0,5 km
              # print(f"Distance entre les coordonnées API et originales pour la ligne {index}: {distance} km")
            
            p=p+1

            
           
        ## regles pour choisir
        # adresse similaire ? fuzzy wuzzy
        pdres_ok[ 'adresse_siret'][0]
        adresse_complete
        # naf similaire ?
        # distance?
        # autre
        
        pdres_ok

      
    print(f"Ligne {index} traitée.")
    i = i + 1
    
    resultats_api_debug.append(pdres_ok)


# Convertir la liste des résultats en DataFrame.
resultats_df = pd.DataFrame(resultats_api_debug)

# Sauvegarder les résultats dans un fichier CSV.
data_resultats_api_lol = "D:/pour_catch_heat/resultats_api.csv"
resultats_df.to_csv(data_resultats_api_lol, index=False, sep=';')
print(f"Les résultats ont été sauvegardés dans le fichier {data_resultats_api_lol}")

# Afficher des statistiques sur les résultats obtenus.
resultats_df.describe()