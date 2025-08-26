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
### 4.1 Table JOB_POSTINGS – Offres d'emploi
```sql
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

```
#### Résultat attendu

- Table créée et données importées.
- Aperçu des 3 premières lignes.
- Détection des doublons job_id.
- Offres sans job_id ou titre signalées
- Salaires négatifs identifiés pour contrôle


### 4.2 Table COMPANIES – Entreprises
```sql
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
```
### 4.3 Table BENEFITS – Avantages proposés

```sql
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
```

### 4.4 table EMPLOYEE_COUNTS – Nombre d'employés

```sql
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
```

#### Nettoyage des doublons dans la table EMPLOYEE_COUNTS

Après avoir identifié les doublons, on crée une nouvelle table EMPLOYEE_COUNTS_CLEAN qui ne contient que des enregistrements uniques (sans doublons).
Ensuite, on supprime l’ancienne table contenant les doublons et on renomme la table propre pour qu’elle remplace l’originale.






### 4.5 Table JOB_SKILLS – Compétences requises
``` sql
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
```
### 4.6 Table JOB_INDUSTRIES – Secteurs des offres

```sql
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
```
### 4.7 Table COMPANY_SPECIALITIES

```sql
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

```
##### - Nettoyage avec DISTINCT, puis renommage




### 4.8 Table 

```sql
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
```


## 5. analyse des donnée

### 5.1 Top 10 des titres de postes les plus publiés par industrie


```sql
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
```

### 5.2 Top 10 des postes les mieux rémunérés par industrie
```sql
WITH salary_ranks AS (SELECT  ci.industry, jp.title, jp.max_salary, jp.currency,
    ROW_NUMBER() OVER (PARTITION BY ci.industry ORDER BY TRY_CAST(jp.max_salary AS NUMBER) DESC) as rang
    FROM JOB_POSTINGS jp
    JOIN COMPANY_INDUSTRIES ci ON jp.company_id = ci.company_id
    WHERE jp.max_salary IS NOT NULL 
      AND TRY_CAST(jp.max_salary AS NUMBER) > 0
      AND ci.industry IS NOT NULL)
SELECT industry, title, max_salary, currency,  rang FROM salary_ranks
WHERE rang <= 10 ORDER BY industry, rang;
```

### 5.3 Répartition des offres par taille d'entreprise
```sql
SELECT c.company_size, COUNT(*) as nombre_offres,
ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM JOB_POSTINGS jp JOIN COMPANIES c2 ON jp.company_id = c2.company_id WHERE c2.company_size IS NOT NULL)), 2) as pourcentage
FROM JOB_POSTINGS jp
JOIN COMPANIES c ON jp.company_id = c.company_id
WHERE c.company_size IS NOT NULL
GROUP BY c.company_size ORDER BY c.company_size;
```


### 5.4 Répartition des offres par secteur d'activité
```sql
SELECT  ci.industry , COUNT(*) as nombre_offres FROM JOB_POSTINGS jp
JOIN COMPANY_INDUSTRIES ci ON jp.company_id = ci.company_id
WHERE ci.industry IS NOT NULL
GROUP BY ci.industry ORDER BY nombre_offres DESC;
```

### 5.5 Répartition par type d'emploi
```sql
SELECT  formatted_work_type, COUNT(*) as nombre_offres FROM JOB_POSTINGS 
WHERE formatted_work_type IS NOT NULL
GROUP BY formatted_work_type ORDER BY nombre_offres DESC;
```




















































