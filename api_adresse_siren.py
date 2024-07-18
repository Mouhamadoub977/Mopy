# -*- coding: utf-8 -*-
"""
Created on Fri Aug 11 17:32:53 2023
@author: BA Mouhamed
"""

import pandas as pd
import requests
import time

# Charger le fichier CSV
data = pd.read_csv('D:/pour_catch_heat/Donnees-de-consommation-et-de-points-de-livraison-denergie-a-la-maille-adresse-gaz_geo.csv', sep =';')

# Filtrer les lignes où on a lat, lon ou adresse mais pas de code_naf (siret absent)
data_ok = data[~data['CODE_SECTEUR_NAF2_CODE'].isna() & ((~data['adresse'].isna()) | (~data['latitude'].isna() & ~data['longitude'].isna()))]

# Pour améliorer la précision de la recherche géographique, concaténer l'adresse et la ville
data_ok['adresse_complete'] = data_ok['adresse'] + ", " + data_ok['Ville']

# Charger les données NAF
naf_data = pd.read_excel("D:/pour_catch_heat/naf2008_5_niveaux.xlsx", dtype={'NIV2': str})

# Convertir la colonne CODE_SECTEUR_NAF2_CODE en chaîne de caractères
data_ok['CODE_SECTEUR_NAF2_CODE'] = data_ok['CODE_SECTEUR_NAF2_CODE'].astype(str)

# Créer un dictionnaire de correspondance entre NIV2 et NIV1 pour le mapping des codes NAF
naf2_to_letter_map = dict(zip(naf_data['NIV2'].astype(str), naf_data['NIV1']))

# Fonction pour effectuer une recherche textuelle à l'aide de l'API
def recherche_textuelle_api(adresse, naf2_code):
    section_activite_principale = naf2_to_letter_map.get(naf2_code)
    url = f"https://recherche-entreprises.api.gouv.fr/search?q={adresse}&section_activite_principale={section_activite_principale}"
    return url

# Fonction pour recherche géographique à l'aide de l'API
def recherche_geographique_api(latitude, longitude, naf2_code, rayon=0.5):
    section_activite_principale = naf2_to_letter_map.get(naf2_code)
    url = f"https://recherche-entreprises.api.gouv.fr/near_point?lat={latitude}&long={longitude}&radius={rayon}&section_activite_principale={section_activite_principale}"
    return url

# Fonction pour appeler l'API avec mécanisme de réessai
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
    return None  # Si nous avons épuisé tous nos essais

# Liste pour stocker les résultats de l'API
resultats_api_debug = []

# Parcourir le dataframe
for index, row in data_ok.iterrows():
data_ok=data_ok.reset_index().drop('index',axis=1)
i=0
while i < len(data_ok):
    index = data_ok.index[i]
    row=data_ok.iloc[i]
    current_naf2_code = row['CODE_SECTEUR_NAF2_CODE'].split('.')[0]
    section_activite_principale = naf2_to_letter_map.get(current_naf2_code)
    if not section_activite_principale:
        print(f"Ligne {index} : section_activite_principale non trouvée pour le code NAF2 {current_naf2_code}.")
        #continue
    if pd.notna(row['adresse_complete']):
        url = recherche_textuelle_api(row['adresse_complete'], current_naf2_code)+'&radius=0.5'
    else:
        url = recherche_geographique_api(row['latitude'], row['longitude'], current_naf2_code)

    resultat = appeler_api_avec_essais(url, index)
    print(resultat['results'])
    if resultat['results'] != []:
        
        pdres=pd.DataFrame.from_dict(resultat['results'])
        
        pdres_ok=pd.DataFrame()
        ii=0
        while ii < len(pdres['matching_etablissements']):
            
            for res_et in pdres['matching_etablissements'][ii]:
                res_et
                res_et['siren']=pdres['siren'][ii]
                pdres_et=pd.DataFrame.from_dict([res_et])
                pdres_et.columns=pdres_et.columns +'_siret'
                pdres_et=pdres_et.rename(columns={'siren_siret' : 'siren'})
                pdres_=pdres.merge(pdres_et,on='siren')
                pdres_ok=pd.concat([pdres_ok,pdres_])
            
            ii=ii+1
            
        ## regles pour choisir
        # adresse similaire ? fuzzy wuzzy
        # naf similaire ?
        # distance?
        # autre
        
        pdres_ok

    print(f"Ligne {index} traitée.")
    
    i=i+1

#Conversion la liste des résultats en DataFrame
resultats_df = pd.DataFrame(resultats_api_debug)

data_resultats_api_lol = "D:/pour_catch_heat/resultats_api.csv"
resultats_df.to_csv(data_resultats_api_lol, index=False, sep=';')
print(f"Les résultats ont été sauvegardés dans le fichier {data_resultats_api_lol}")


resultats_df.describe()
