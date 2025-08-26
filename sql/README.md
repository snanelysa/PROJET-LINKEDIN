# Scripts SQL du Projet LinkedIn

Ce fichier contient les commandes SQL utilisées pour créer et analyser les données issues des offres d'emploi LinkedIn. Chaque section correspond à une étape technique avec une explication.

---

##  1. Création de la base et du schéma

```sql
CREATE DATABASE linkedin;
CREATE SCHEMA LINKEDIN.linkedin_job;
 ```



## 2. Configuration du stage (connexion à S3)

```sql

CREATE OR REPLACE STAGE linkedin_stage 
URL = 's3://snowflake-lab-bucket/'
FILE_FORMAT = (TYPE = 'CSV');

LIST @linkedin_stage;
```


## 3. Création des formats de fichier

```sql
--format CSV
CREATE OR REPLACE FILE FORMAT CSV_FORMAT
TYPE = 'CSV'
FIELD_DELIMITER = ','           
RECORD_DELIMITER = '\n'        
SKIP_HEADER = 1                 
FIELD_OPTIONALLY_ENCLOSED_BY = '"'  
TRIM_SPACE = TRUE               
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE  
NULL_IF = ('', 'NULL', 'null', 'N/A', 'n/a') 
EMPTY_FIELD_AS_NULL = TRUE;
--format  JSON
CREATE OR REPLACE FILE FORMAT JSON_FORMAT
TYPE = 'JSON'
STRIP_OUTER_ARRAY = TRUE        
REPLACE_INVALID_CHARACTERS = TRUE  
COMMENT = 'Format JSON pour les fichiers LinkedIn';
```
## 4. Création et chargement des tables
### Table JOB_POSTINGS – Offres d'emploi




















