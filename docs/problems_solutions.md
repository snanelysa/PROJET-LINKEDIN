## ❗ Problème 1 : Doublons dans les données

**Description :**  
Plusieurs lignes identiques (basées sur `job_id` ou `company_id`) étaient présentes dans les fichiers chargés.

**Solution appliquée :**  
Nettoyage des tables via :
```sql
CREATE OR REPLACE TABLE NOM_TABLE_CLEAN AS
SELECT DISTINCT *
FROM NOM_TABLE;
```
Cela a permis de supprimer les doublons de façon simple et efficace.

## ❗ Problème 2 : Formats de données inconsistants

**Description :** 
Certains fichiers CSV avaient des valeurs manquantes ou mal formatées (dates, salaires, champs vides).

**Solution appliquée :**
- Utilisation de la clause ```  ON_ERROR = 'CONTINUE' ``` dans la commande ``` COPY INTO ```

- Définition d’un format CSV personnalisé dans Snowflake (CSV_FORMAT) avec les bons délimiteurs et encodages.

## ❗ Problème 3 : Plotly non disponible dans Streamlit Snowflake

**Description :**
Le module plotly n’est pas supporté par la version intégrée de Streamlit dans Snowflake.

**Solution appliquée :**
Utilisation d’Altair pour créer des graphiques interactifs et compatibles avec Snowflake :
```py
chart = alt.Chart(df).mark_bar().encode(...)
```
