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

