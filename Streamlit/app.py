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
