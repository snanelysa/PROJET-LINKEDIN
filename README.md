# Analyse des Offres d'Emploi LinkedIn avec Snowflake

## Description du Projet

Analyse complète de données d'offres d'emploi LinkedIn en utilisant Snowflake pour stocker les données et Streamlit pour créer des graphiques. Ce projet permet de comprendre le marché de l'emploi grâce à l'analyse de milliers d'offres d'emploi.

## Objectifs

- Créer une base de données avec Snowflake
- Charger et traiter des données depuis Amazon S3  
- Faire 5 analyses avec des graphiques interactifs
- Documenter tout le processus technique

## Technologies Utilisées

- **Snowflake** - Base de données cloud
- **SQL** - Langage pour interroger les données
- **Streamlit** - Outil pour créer des graphiques
- **Python** - Programmation et traitement des données
- **Amazon S3** - Stockage des fichiers de données

## Architecture des Données

### Source des Données
- **Emplacement :** `s3://snowflake-lab-bucket/`
- **Volume :** 8 fichiers (CSV/JSON)
- **Contenu :** Offres d'emploi, entreprises, compétences, secteurs

### Fichiers Traités

| Fichier | Format | Contenu |
|---------|--------|---------|
| job_postings.csv | CSV | Détails des offres d'emploi |
| companies.json | JSON | Informations sur les entreprises |
| benefits.csv | CSV | Avantages des postes |
| employee_counts.csv | CSV | Nombre d'employés des entreprises |
| job_skills.csv | CSV | Compétences demandées |
| job_industries.json | JSON | Secteurs d'activité |
| company_specialities.json | JSON | Spécialités des entreprises |
| company_industries.json | JSON | Industries par entreprise |

## Analyses Réalisées

1. **Top 10 des titres de postes par industrie**
2. **Top 10 des postes les mieux payés par industrie**
3. **Répartition des offres par taille d'entreprise**
4. **Répartition des offres par secteur d'activité**
5. **Répartition des offres par type d'emploi**



## Méthode de Travail

1. **Configuration** - Création de la base de données et connexion à S3
2. **Création des tables** - Structure pour organiser les données
3. **Chargement** - Import des données depuis S3
4. **Nettoyage** - Correction des erreurs et suppression des doublons
5. **Analyse** - Requêtes pour extraire des informations
6. **Visualisation** - Création de graphiques avec Streamlit

## Résultats Principaux

- **124,000+** offres d'emploi analysées
- **15** secteurs d'activité identifiés
- **7** catégories de taille d'entreprise
- **Informations** sur les tendances salariales et sectorielles



## Documentation Technique

### [1. Commandes SQL](sql/)
Scripts SQL complets avec explications détaillées.

### [2. Code Streamlit](./Streamlit/app.py)
Applications de visualisation et code des graphiques interactifs.

### [3. Captures d'écran des résultats ](image/Captures.md)
Aperçus visuels des résultats, analyses et visualisations.

### [4. Problèmes et Solutions](docs/problems_solutions.md)
Documentation des difficultés rencontrées et solutions appliquées.


## Installation et Utilisation

1. Configurer l'environnement Snowflake
2. Exécuter les scripts SQL dans l'ordre
3. Lancer l'application Streamlit
4. Consulter les visualisations interactives

## Livrables

- Scripts SQL documentés
- Application Streamlit fonctionnelle
- Documentation technique complète
- Rapport d'analyse des résultats















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



