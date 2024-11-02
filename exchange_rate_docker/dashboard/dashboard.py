import streamlit as st
import pandas as pd
import plotly.express as px
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import os

# PostgreSQL Settings
def load_postgres_settings(): 
    """
    Carrega as configurações a partir de variáveis de ambiente.
    """
    settings = {
        "db_host": os.environ["POSTGRES_HOST"],
        "db_user": os.environ["POSTGRES_USER"],
        "db_pass": os.environ["POSTGRES_PASSWORD"],
        "db_name": os.environ["POSTGRES_DB"],
        "db_port": os.environ["POSTGRES_PORT"],
    }
    return settings

# Postgres String Connection
def postgres_connection():
    """
    Carrega as configurações a partir de variáveis de ambiente.
    """
    settings = load_postgres_settings()
    connection_string = f"postgresql://{settings['db_user']}:{settings['db_pass']}@{settings['db_host']}:{settings['db_port']}/{settings['db_name']}"
    engine = create_engine(connection_string)    
    return engine

# Load the data from PostgreSQL Render
def load_data():
    """
    Carrega os dados que foram armazenados no Postgres Render pelo consumer kafka
    """
    engine = postgres_connection()
    query = '''
    SELECT DISTINCT ON (exr_date,"uuid") *
    FROM exchange_rates_kafka
    ORDER BY exr_date, "uuid" DESC;
    '''
    try:
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        return pd.DataFrame()  # Return an empty Dataframe if error

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
    df_dash['exr_value'] = df_dash['exr_value'].apply(lambda x: round(x, 2))
    return df_dash

# Streamlit building part. Show the Dashboard and Dataframe base.
def display_data(df, df_dash):
    """
    Parte visual do Dataframe e Dashboard, utilizando Streamlit.
    """
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
    # Load the data from PostgreSQL Render
    df = load_data()
    # Rewrite de Dataframe Base to an easier Dataframe to plot.
    df_dash = rewrite_df(df)

    
    display_data(df, df_dash)