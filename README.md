
# PROJET-LINKEDIN

## Description
Analyse des données LinkedIn avec Snowflake et Streamlit dans le cadre d'un projet universitaire.

## Objectifs
- Créer une base de données Snowflake
- Charger les données depuis S3
- Réaliser 5 analyses avec visualisations Streamlit

## Fichiers analysés
- job_postings.csv
- companies.json
- benefits.csv
- employee_counts.csv
- job_skills.csv
- job_industries.json
- company_specialities.json
- company_industries.json




## 1. CODE SQL :


```sql
CREATE database linkedin ;
CREATE SCHEMA LINKEDIN.linkedin_job;

CREATE OR REPLACE STAGE linkedin_stage 
URL = 's3://snowflake-lab-bucket/'
FILE_FORMAT = (TYPE = 'CSV');

LIST @linkedin_stage;


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


CREATE OR REPLACE FILE FORMAT JSON_FORMAT
TYPE = 'JSON'
STRIP_OUTER_ARRAY = TRUE        
REPLACE_INVALID_CHARACTERS = TRUE  
COMMENT = 'Format JSON pour les fichiers LinkedIn';


--1er table 
CREATE OR REPLACE TABLE JOB_POSTINGS (
    job_id BIGINT,                    
    company_id VARCHAR(100),         
    title VARCHAR(500),          
    description TEXT,              
    max_salary VARCHAR(50),     
    med_salary VARCHAR(50),       
    min_salary VARCHAR(50),         
    pay_period VARCHAR(50),          
    formatted_work_type VARCHAR(100),
    location VARCHAR(200),            
    applies VARCHAR(50),              
    original_listed_time VARCHAR(100),
    remote_allowed VARCHAR(10),      
    views VARCHAR(50),                
    job_posting_url TEXT,      
    application_url TEXT,             
    application_type VARCHAR(100),   
    expiry VARCHAR(100),             
    closed_time VARCHAR(100),         
    formatted_experience_level VARCHAR(100), 
    skills_desc TEXT,               
    listed_time VARCHAR(100),         
    posting_domain VARCHAR(200),      
    sponsored VARCHAR(10),            
    work_type VARCHAR(100),           
    currency VARCHAR(10),          
    compensation_type VARCHAR(100)  );

    
COPY INTO JOB_POSTINGS 
FROM @linkedin_stage/job_postings.csv
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE';

SELECT * FROM JOB_POSTINGS LIMIT 3;


SELECT COUNT(*) FROM JOB_POSTINGS;

-- Chercher les doublons
SELECT job_id, COUNT(*) as combien_fois
FROM JOB_POSTINGS 
GROUP BY job_id 
HAVING COUNT(*) > 1
LIMIT 5;
-- Chercher les lignes sans job_id 
SELECT COUNT(*) as jobs_sans_id
FROM JOB_POSTINGS 
WHERE job_id IS NULL;

-- Chercher les lignes sans titre
SELECT COUNT(*) as jobs_sans_titre
FROM JOB_POSTINGS 
WHERE title IS NULL OR title = '';

-- Regarder s'il y a des salaires negatifs 
SELECT COUNT(*) as salaires_negatifs
FROM JOB_POSTINGS 
WHERE TRY_CAST(max_salary AS NUMBER) < 0;



 ----table2

CREATE OR REPLACE TABLE COMPANIES (
    company_id BIGINT,            
    name VARCHAR(500),              
    description TEXT,               
    company_size INTEGER,           
    state VARCHAR(100),             
    country VARCHAR(100),         
    city VARCHAR(200),              
    zip_code VARCHAR(50),           
    address TEXT,                  
    url VARCHAR(1000)     );          
COPY INTO COMPANIES (
    company_id,
    name, 
    description,
    company_size,
    state,
    country,
    city,
    zip_code,
    address,
    url )
FROM (SELECT 
        $1:company_id::BIGINT,
        $1:name::VARCHAR(500),
        $1:description::TEXT,
        $1:company_size::INTEGER,
        $1:state::VARCHAR(100),
        $1:country::VARCHAR(100),
        $1:city::VARCHAR(200),     --plus de100 caracter 
        $1:zip_code::VARCHAR(50),
        $1:address::TEXT,
        $1:url::VARCHAR(1000)
    FROM @linkedin_stage/companies.json)
FILE_FORMAT = JSON_FORMAT;

SELECT $1 FROM @linkedin_stage/companies.json ;
SELECT * FROM COMPANIES LIMIT 3;
-- Y a-t-il des doublons ?
SELECT company_id, COUNT(*) as combien_fois
FROM COMPANIES 
GROUP BY company_id 
HAVING COUNT(*) > 1
LIMIT 5;

-- Chercher les company_id vides
SELECT COUNT(*) as entreprises_sans_id FROM COMPANIES 
WHERE company_id IS NULL;

-- Chercher les noms vides
SELECT COUNT(*) as entreprises_sans_nom FROM COMPANIES 
WHERE name IS NULL OR name = '';



--table3
CREATE OR REPLACE TABLE BENEFITS (
    job_id BIGINT,             
    inferred VARCHAR(10),          
    type VARCHAR(200)   );         

COPY INTO BENEFITS
FROM @linkedin_stage/benefits.csv
FILE_FORMAT = CSV_FORMAT ;

SELECT * FROM BENEFITS ;
SELECT job_id, type, COUNT(*) as combien_fois
FROM BENEFITS 
GROUP BY job_id, type
HAVING COUNT(*) > 1
LIMIT 5;


---table4
CREATE OR REPLACE TABLE EMPLOYEE_COUNTS (
    company_id BIGINT,             
    employee_count INTEGER,         
    follower_count INTEGER,         
    time_recorded BIGINT    );   


COPY INTO EMPLOYEE_COUNTS
FROM @linkedin_stage/employee_counts.csv
FILE_FORMAT = CSV_FORMAT 
ON_ERROR = 'CONTINUE'; 

SELECT * FROM EMPLOYEE_COUNTS ;
-- Chercher les doublons
SELECT company_id, COUNT(*) as combien_fois
FROM EMPLOYEE_COUNTS 
GROUP BY company_id
HAVING COUNT(*) > 1
LIMIT 5;


-- Créer une version propre sans doublons
CREATE OR REPLACE TABLE EMPLOYEE_COUNTS_CLEAN AS
SELECT DISTINCT *
FROM EMPLOYEE_COUNTS;

select * from EMPLOYEE_COUNTS_CLEAN ;

DROP TABLE EMPLOYEE_COUNTS;
ALTER TABLE EMPLOYEE_COUNTS_CLEAN RENAME TO EMPLOYEE_COUNTS;





---table5
CREATE OR REPLACE TABLE JOB_SKILLS (
    job_id BIGINT,                 
    skill_abr VARCHAR(100) );       
COPY INTO JOB_SKILLS
FROM @linkedin_stage/job_skills.csv
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE';

SELECT * FROM JOB_SKILLS LIMIT 5;

SELECT job_id, skill_abr, COUNT(*) as combien_fois
FROM JOB_SKILLS 
GROUP BY job_id, skill_abr
HAVING COUNT(*) > 1
LIMIT 5;


--table6

CREATE OR REPLACE TABLE JOB_INDUSTRIES (
    job_id BIGINT,                 
    industry_id INTEGER   );        


COPY INTO JOB_INDUSTRIES FROM (SELECT 
        $1:job_id::BIGINT,
        $1:industry_id::INTEGER
    FROM @linkedin_stage/job_industries.json)
FILE_FORMAT = JSON_FORMAT
ON_ERROR = 'CONTINUE';

SELECT * FROM JOB_INDUSTRIES ;

SELECT job_id, industry_id, COUNT(*) as combien_fois
FROM JOB_INDUSTRIES 
GROUP BY job_id, industry_id
HAVING COUNT(*) > 1
LIMIT 5;


--table7
CREATE OR REPLACE TABLE COMPANY_SPECIALITIES (
    company_id BIGINT,           
    speciality varchar );        


COPY INTO COMPANY_SPECIALITIES  FROM (SELECT 
        $1:company_id::BIGINT,
        $1:speciality::VARCHAR FROM @linkedin_stage/company_specialities.json)
FILE_FORMAT = JSON_FORMAT
ON_ERROR = 'CONTINUE';

SELECT * FROM COMPANY_SPECIALITIES ;

-- Chercher les doublons
SELECT company_id, speciality, COUNT(*) as combien_fois FROM COMPANY_SPECIALITIES 
GROUP BY company_id, speciality
HAVING COUNT(*) > 1 ; 

--Créer une version propre sans doublons
CREATE OR REPLACE TABLE COMPANY_SPECIALITIES_CLEAN AS
SELECT DISTINCT *
FROM COMPANY_SPECIALITIES;


-- Remplacer l'ancienne table
DROP TABLE COMPANY_SPECIALITIES;
ALTER TABLE COMPANY_SPECIALITIES_CLEAN RENAME TO COMPANY_SPECIALITIES;



---table8
CREATE OR REPLACE TABLE COMPANY_INDUSTRIES (
    company_id BIGINT,              
    industry VARCHAR(50) );        

COPY INTO COMPANY_INDUSTRIES FROM (SELECT 
        $1:company_id::BIGINT,
        $1:industry::VARCHAR(50)
    FROM @linkedin_stage/company_industries.json)
FILE_FORMAT = JSON_FORMAT
ON_ERROR = 'CONTINUE';

SELECT * FROM COMPANY_INDUSTRIES LIMIT 5;

-- Chercher les doublons
SELECT company_id, industry, COUNT(*) as combien_fois
FROM COMPANY_INDUSTRIES 
GROUP BY company_id, industry
HAVING COUNT(*) > 1
LIMIT 5;

-- Créer une version propre sans doublons
CREATE OR REPLACE TABLE COMPANY_INDUSTRIES_CLEAN AS
SELECT DISTINCT *
FROM COMPANY_INDUSTRIES;

-- Vérifier la différence
SELECT 
    (SELECT COUNT(*) FROM COMPANY_INDUSTRIES) as avant,
    (SELECT COUNT(*) FROM COMPANY_INDUSTRIES_CLEAN) as apres;

-- Remplacer l'ancienne table
DROP TABLE COMPANY_INDUSTRIES;
ALTER TABLE COMPANY_INDUSTRIES_CLEAN RENAME TO COMPANY_INDUSTRIES;

------------------------------analyse des donnée------------------------------------

--Top 10 des titres de postes les plus publiés par industrie

WITH job_counts AS (SELECT ci.industry , jp.title,  COUNT(*) as nombre_offres,
ROW_NUMBER() OVER (PARTITION BY ci.industry ORDER BY COUNT(*) DESC) as rang
    FROM JOB_POSTINGS jp
    JOIN COMPANY_INDUSTRIES ci ON jp.company_id = ci.company_id
    WHERE jp.title IS NOT NULL 
      AND ci.industry IS NOT NULL
    GROUP BY ci.industry, jp.title )
SELECT industry , title, nombre_offres, rang FROM job_counts
WHERE rang <= 10
ORDER BY industry, rang;

-- Top 10 des postes les mieux rémunérés par industrie

WITH salary_ranks AS (SELECT  ci.industry, jp.title, jp.max_salary, jp.currency,
    ROW_NUMBER() OVER (PARTITION BY ci.industry ORDER BY TRY_CAST(jp.max_salary AS NUMBER) DESC) as rang
    FROM JOB_POSTINGS jp
    JOIN COMPANY_INDUSTRIES ci ON jp.company_id = ci.company_id
    WHERE jp.max_salary IS NOT NULL 
      AND TRY_CAST(jp.max_salary AS NUMBER) > 0
      AND ci.industry IS NOT NULL)
SELECT industry, title, max_salary, currency,  rang FROM salary_ranks
WHERE rang <= 10 ORDER BY industry, rang;

--Répartition des offres par taille d'entreprise
SELECT c.company_size, COUNT(*) as nombre_offres,
ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM JOB_POSTINGS jp JOIN COMPANIES c2 ON jp.company_id = c2.company_id WHERE c2.company_size IS NOT NULL)), 2) as pourcentage
FROM JOB_POSTINGS jp
JOIN COMPANIES c ON jp.company_id = c.company_id
WHERE c.company_size IS NOT NULL
GROUP BY c.company_size ORDER BY c.company_size;


--Répartition des offres par secteur d'activité

SELECT  ci.industry , COUNT(*) as nombre_offres FROM JOB_POSTINGS jp
JOIN COMPANY_INDUSTRIES ci ON jp.company_id = ci.company_id
WHERE ci.industry IS NOT NULL
GROUP BY ci.industry ORDER BY nombre_offres DESC;

--Répartition par type d'emploi

SELECT  formatted_work_type, COUNT(*) as nombre_offres FROM JOB_POSTINGS 
WHERE formatted_work_type IS NOT NULL
GROUP BY formatted_work_type ORDER BY nombre_offres DESC;

```




## 2. CODE STREAMLIT

### IMPORTS ET CONFIGURATION
```python
import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

session = get_active_session()

#Analyse 1 : Top 10 des titres de postes les plus publiés par industrie.
st.title("Top 10 des titres  par industrie")

query = """
WITH job_counts AS (
    SELECT ci.industry, jp.title, COUNT(*) as nombre_offres,
    ROW_NUMBER() OVER (PARTITION BY ci.industry ORDER BY COUNT(*) DESC) as rang
    FROM JOB_POSTINGS jp
    JOIN COMPANY_INDUSTRIES ci ON jp.company_id = ci.company_id
    WHERE jp.title IS NOT NULL AND ci.industry IS NOT NULL
    GROUP BY ci.industry, jp.title)
SELECT industry, title, nombre_offres, rang 
FROM job_counts
WHERE rang <= 10
ORDER BY industry, rang
"""

df = session.sql(query).to_pandas()

industries = df['INDUSTRY'].unique()
selected_industry = st.selectbox("Choisir une industrie:", industries)

filtered_df = df[df['INDUSTRY'] == selected_industry]

chart = alt.Chart(filtered_df).mark_bar().encode(
    x=alt.X('NOMBRE_OFFRES:Q', title='Nombre d\'offres'),
    y=alt.Y('TITLE:N', title='Titre du poste', sort='-x'),
    color='NOMBRE_OFFRES:Q').properties(width=600, height=400)

st.altair_chart(chart, use_container_width=True)
st.dataframe(filtered_df)

#Analyse 2 - Top 10 postes les mieux rémunérés par industrie 

st.title("Top 10 des postes les mieux rémunérés par industrie")

query2 = """
WITH salary_ranks AS (
    SELECT ci.industry, jp.title, jp.max_salary, jp.currency,
    ROW_NUMBER() OVER (PARTITION BY ci.industry ORDER BY TRY_CAST(jp.max_salary AS NUMBER) DESC) as rang
    FROM JOB_POSTINGS jp
    JOIN COMPANY_INDUSTRIES ci ON jp.company_id = ci.company_id
    WHERE jp.max_salary IS NOT NULL 
    AND TRY_CAST(jp.max_salary AS NUMBER) > 0
    AND ci.industry IS NOT NULL)
SELECT industry, title, max_salary, currency, rang 
FROM salary_ranks
WHERE rang <= 10 
ORDER BY industry, rang
"""

df2 = session.sql(query2).to_pandas()

industries2 = df2['INDUSTRY'].unique()
selected_industry2 = st.selectbox("Choisir une industrie:", industries2, key="salary")

filtered_df2 = df2[df2['INDUSTRY'] == selected_industry2]

chart2 = alt.Chart(filtered_df2).mark_bar().encode(
    x=alt.X('MAX_SALARY:Q', title='Salaire Maximum'),
    y=alt.Y('TITLE:N', title='Titre du poste', sort='-x'),
    color='MAX_SALARY:Q').properties(width=600, height=400)

st.altair_chart(chart2, use_container_width=True)
st.dataframe(filtered_df2)

#Analyse 3 - Répartition par taille d'entreprise 

st.title("Répartition des offres par taille d'entreprise")

query3 = """
SELECT c.company_size, COUNT(*) as nombre_offres,
ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM JOB_POSTINGS jp JOIN COMPANIES c2 ON jp.company_id = c2.company_id WHERE c2.company_size IS NOT NULL)), 2) as pourcentage
FROM JOB_POSTINGS jp
JOIN COMPANIES c ON jp.company_id = c.company_id
WHERE c.company_size IS NOT NULL
GROUP BY c.company_size 
ORDER BY c.company_size
"""

df3 = session.sql(query3).to_pandas()

chart3 = alt.Chart(df3).mark_bar().encode(
    x=alt.X('COMPANY_SIZE:O', title='Taille d\'entreprise'),
    y=alt.Y('NOMBRE_OFFRES:Q', title='Nombre d\'offres'),
    color='COMPANY_SIZE:O').properties(width=600, height=400)

st.altair_chart(chart3, use_container_width=True)
st.dataframe(df3)

#Analyse 4 - Répartition par secteur d'activité

st.title("Répartition des offres par secteur d'activité")

query4 = """
SELECT ci.industry, COUNT(*) as nombre_offres 
FROM JOB_POSTINGS jp
JOIN COMPANY_INDUSTRIES ci ON jp.company_id = ci.company_id
WHERE ci.industry IS NOT NULL
GROUP BY ci.industry 
ORDER BY nombre_offres DESC
"""

df4 = session.sql(query4).to_pandas()

chart4 = alt.Chart(df4).mark_bar().encode(
    x=alt.X('NOMBRE_OFFRES:Q', title='Nombre d\'offres'),
    y=alt.Y('INDUSTRY:N', title='Secteur d\'activité', sort='-x'),
    color='NOMBRE_OFFRES:Q').properties(width=600, height=400)

st.altair_chart(chart4, use_container_width=True)
st.dataframe(df4)



#Analyse 5 - Répartition par type d'emploi 

st.title("Répartition des offres par type d'emploi")

query5 = """
SELECT formatted_work_type, COUNT(*) as nombre_offres 
FROM JOB_POSTINGS 
WHERE formatted_work_type IS NOT NULL
GROUP BY formatted_work_type 
ORDER BY nombre_offres DESC
"""

df5 = session.sql(query5).to_pandas()

chart5 = alt.Chart(df5).mark_bar(
    cornerRadiusTopLeft=3,
    cornerRadiusTopRight=3).encode(
    x=alt.X('NOMBRE_OFFRES:Q', title='Nombre d\'offres'),
    y=alt.Y('FORMATTED_WORK_TYPE:N', title='Type d\'emploi', sort='-x'),
    color=alt.Color('NOMBRE_OFFRES:Q', 
                   scale=alt.Scale(scheme='blues', reverse=True),
                   legend=alt.Legend(title="Nombre d'offres")),
    tooltip=['FORMATTED_WORK_TYPE:N', 'NOMBRE_OFFRES:Q']).properties(
    width=700, 
    height=450,
    title="Distribution des types d'emploi")

st.altair_chart(chart5, use_container_width=True)
st.dataframe(df5)
```

## 3. PROBLÈMES RENCONTRÉS ET SOLUTIONS

### **PROBLÈME 1 : DOUBLONS DANS LES DONNÉES**
**Solution :** Utilisation de `CREATE TABLE ... AS SELECT DISTINCT` pour nettoyer les tables

### **PROBLÈME 2 : FORMATS DE DONNÉES INCONSISTANTS**
**Solution :** Configuration des formats CSV et JSON avec gestion d'erreurs

### **PROBLÈME 3 : PLOTLY NON DISPONIBLE DANS STREAMLIT SNOWFLAKE**
**Solution :** Utilisation d'Altair comme alternative




## 4. CAPTURES D'ÉCRAN DES RÉSULTATS

### ANALYSE 1 : TOP 10 DES TITRES PAR INDUSTRIE
<img width="985" height="632" alt="image" src="https://github.com/user-attachments/assets/28ba6f65-1125-4301-a226-8e5ecbf1c92b" />
### ANALYSE 2 : TOP 10 DES POSTES LES MIEUX RÉMUNÉRÉS  
<img width="1019" height="633" alt="image" src="https://github.com/user-attachments/assets/418cf5da-2ffd-4077-a3c9-7ef92ae25280" />

### ANALYSE 3 : RÉPARTITION PAR TAILLE D'ENTREPRISE
<img width="915" height="629" alt="image" src="https://github.com/user-attachments/assets/7e73b28c-d207-4428-86bb-952f6d18069d" />


### ANALYSE 4 : RÉPARTITION PAR SECTEUR D'ACTIVITÉ
<img width="977" height="628" alt="image" src="https://github.com/user-attachments/assets/f4ccd112-6ff7-46a9-8bb1-a0a8bae8da11" />


### ANALYSE 5 : RÉPARTITION PAR TYPE D'EMPLOI
<img width="835" height="628" alt="image" src="https://github.com/user-attachments/assets/415139ad-b65a-41ed-afe5-d45839450afc" />



