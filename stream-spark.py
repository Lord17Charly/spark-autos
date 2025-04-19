import streamlit as st
import requests
import pandas as pd
import pymongo

# ------------------- CONEXIÓN MONGO ------------------- #
@st.cache_resource
def init_connection():
    try:
        client = pymongo.MongoClient(**st.secrets["mongo"])
        # Intentar listar bases para comprobar conexión
        client.list_database_names()
        return client
    except Exception as e:
        st.error(f"Error al conectar con MongoDB: {e}")
        return None

client = init_connection()

# GET de MongoDB (sin caché por defecto)
@st.cache_data(ttl=600)
def get_data_mongo():
    if client:
        try:
            db = client.autos
            items = list(db.autos.find())
            return items
        except Exception as e:
            st.error(f"Error al consultar datos de MongoDB: {e}")
            return []
    else:
        st.warning("Conexión a MongoDB no disponible.")
        return []

# ------------------- CONEXIÓN POSTGRES ------------------- #
try:
    conn = st.connection("postgresql", type="sql")
except Exception as e:
    st.error(f"Error al conectar con PostgreSQL: {e}")
    conn = None

@st.cache_data(ttl=600)
def get_data_postgres():
    if conn:
        try:
            return conn.query('SELECT * FROM autos;')
        except Exception as e:
            st.error(f"Error al consultar datos de PostgreSQL: {e}")
            return pd.DataFrame()
    else:
        st.warning("Conexión a PostgreSQL no disponible.")
        return pd.DataFrame()

# ------------------- FUNCIÓN POST ------------------- #
def trigger_post(url):
    try:
        response = requests.post(url)
        if response.status_code in [200, 201]:
            st.success(f"POST enviado a {url}")
        else:
            st.warning(f"Respuesta inesperada ({response.status_code}): {response.text}")
        st.write("Respuesta:")
        st.write(response.text)
    except Exception as e:
        st.error(f"Error al hacer POST: {e}")

# ------------------- SIDEBAR ------------------- #
st.sidebar.title("Acciones")

# MongoDB
st.sidebar.subheader("MongoDB")
if st.sidebar.button("Consultar MongoDB"):
    items = get_data_mongo()
    if items:
        st.subheader("Datos desde MongoDB")
        df_mongo = pd.DataFrame(items)
        st.dataframe(df_mongo)

if st.sidebar.button("Refrescar MongoDB"):
    st.cache_data.clear()
    items = get_data_mongo()
    if items:
        st.subheader("Datos actualizados desde MongoDB")
        df_mongo = pd.DataFrame(items)
        st.dataframe(df_mongo)

#Subir a mongoDB
if st.sidebar.button("Subir a MongoDB"):
    trigger_post("")

#PostgreSQL
st.sidebar.subheader("PostgreSQL")
if st.sidebar.button("Consultar PostgreSQL"):
    df_postgres = get_data_postgres()
    if not df_postgres.empty:
        st.subheader("Datos desde PostgreSQL")
        st.dataframe(df_postgres)

if st.sidebar.button("Refrescar PostgreSQL"):
    st.cache_data.clear()
    df_postgres = get_data_postgres()
    if not df_postgres.empty:
        st.subheader("Datos actualizados desde PostgreSQL")
        st.dataframe(df_postgres)
#Subir a PostgreSQL
if st.sidebar.button("Subir a PostgreSQL"):
    trigger_post("")

# ------------------- SPARK JOB ------------------- #
st.title("Spark & Streamlit")

st.header("spark-submit Job")

github_user  =  st.text_input('Github user')
github_repo  =  st.text_input('Github repo')
spark_job    =  st.text_input('Spark job')
github_token =  st.text_input('Github token', type='password')
code_url     =  st.text_input('Code URL')
dataset_url  =  st.text_input('Dataset URL')

def post_spark_job(user, repo, job, token, codeurl, dataseturl):
    url = f'https://api.github.com/repos/{user}/{repo}/dispatches'
    payload = {
        "event_type": job,
        "client_payload": {
            "codeurl": codeurl,
            "dataseturl": dataseturl
        }
    }
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/vnd.github.v3+json',
        'Content-type': 'application/json'
    }

    try:
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 204:
            st.success("Spark job enviado correctamente")
        else:
            st.error(f"Error al enviar Spark job: {response.status_code}")
            st.write(response.text)
    except Exception as e:
        st.error(f"Error en la solicitud del Spark job: {e}")

if st.button("POST spark submit"):
    post_spark_job(github_user, github_repo, spark_job, github_token, code_url, dataset_url)

# ------------------- RESULTADOS SPARK ------------------- #
st.header("Resultados Spark Job")

url_results = st.text_input('URL de resultados')

def get_spark_results(url_results):
    try:
        response = requests.get(url_results)
        if response.status_code == 200:
            try:
                st.json(response.json())
            except:
                st.text(response.text)
        else:
            st.error(f"Error al obtener resultados: {response.status_code}")
    except Exception as e:
        st.error(f"Error al hacer GET de resultados: {e}")

if st.button("GET spark results"):
    get_spark_results(url_results)
