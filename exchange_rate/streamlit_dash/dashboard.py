import streamlit as st
import pandas as pd
import plotly.express as px
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Configuração do PostgreSQL
def load_postgres_settings(): 
    """
    Carrega as configurações a partir de variáveis de ambiente.
    """
    # Carregar variáveis de ambiente do arquivo .env 
    load_dotenv()

    settings = {
        "db_host": os.getenv("POSTGRES_HOST"),
        "db_user": os.getenv("POSTGRES_USER"),
        "db_pass": os.getenv("POSTGRES_PASSWORD"),
        "db_name": os.getenv("POSTGRES_DB"),
        "db_port": os.getenv("POSTGRES_PORT"),
    }
    return settings

# Retorna a String de Conexão Postgres
def postgres_connection():
    settings = load_postgres_settings()
    connection_string = f"postgresql://{settings['db_user']}:{settings['db_pass']}@{settings['db_host']}:{settings['db_port']}/{settings['db_name']}"
    engine = create_engine(connection_string)    
    return engine

# Função para carregar dados do banco de dados
def load_data():
    engine = postgres_connection()
    query = """
    SELECT * FROM exchange_rates_kafka
    ORDER BY exr_date DESC;
    """
    try:
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        return pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro

# Rewriting Dataframe
def rewrite_df(df):
    """
    Reestruturando os dados para construção do Dash
    """
    df_aux01 = df[['exr_date', 'exr_usd_brl']].rename({'exr_usd_brl': 'exr_value'}, axis=1)
    df_aux01['exr_currency'] = 'exr_usd_brl'
    df_aux02 = df[['exr_date', 'exr_eur_brl']].rename({'exr_eur_brl': 'exr_value'}, axis=1)
    df_aux02['exr_currency'] = 'exr_eur_brl'
    df_dash = pd.concat([df_aux01, df_aux02], axis=0).reset_index(drop=True)
    df_dash["exr_date"] = pd.to_datetime(df_dash["exr_date"], utc=True).dt.tz_localize(None)
    df_dash['exr_value'] = df_dash['exr_value'].apply(lambda x: round(x, 2))
    return df_dash

def display_data(df, df_dash):
    # Use the full page instead of a narrow central column
    st.set_page_config(layout="wide")

    st.title("Exchange Rates Historical")

    # Dataframe and Dashboard Side By Side
    col1, col2 = st.columns(2)

    with col1: 
        st.subheader("Dataframe")
        st.dataframe(df.head())

    with col2:
        st.html('<h4>Legenda:</h4>')
        st.html("""
                    <ul>
                <li>exr_date: Data referente ao valor da taxa de câmbio entre as moedas;</li>
                <li>exr_usd_brl: Exchange Rate Dolar x Real;</li>
                <li>exr_eur_brl: Exchange Rate Euro x Real;</li>
                <li>exr_value: Valor da taxa de câmbio (Exchange Rate);</li>
                <li>run_time: Horário em que foram coletadas as informações;</li>
                    </ul>
        """)

    st.markdown("---")
    st.subheader("Dashboard")
    fig = px.line(df_dash, x="exr_date", y="exr_value", color="exr_currency", height=500)
    st.plotly_chart(fig) 

    return None

if __name__ == '__main__':
    df = load_data()
    df_dash = rewrite_df(df)
    
    display_data(df, df_dash)