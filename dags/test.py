from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
import pandas as pd
from sklearn.decomposition import PCA
from sqlalchemy import create_engine
from sqlalchemy import create_engine
from airflow.hooks.postgres_hook import PostgresHook

def extract():
    # Extracción de datos desde una base de datos MySQL utilizando la conexión configurada en Airflow
    hook = MySqlHook(mysql_conn_id='testsql')
    query = 'SELECT * FROM medata.tu_tabla'
    df = hook.get_pandas_df(query)
    return df

def transform(df):

    df['YearStart'] = df['YearStart'].astype(str)
    df['YearEnd'] = df['YearEnd'].astype(str)

    # Convertir las cadenas de texto en columnas 'YearStart' y 'YearEnd' a objetos datetime
    df['YearStart'] = df['YearStart'].apply(lambda x: datetime.strptime(x, '%Y'))
    df['YearEnd'] = df['YearEnd'].apply(lambda x: datetime.strptime(x, '%Y'))

    # Calcular la duración en días
    df['Duration'] = (df['YearEnd'] - df['YearStart']).dt.days


    # Transformación de los datos
    df[['Latitude', 'Longitude']] = df['GeoLocation'].str.strip('POINT ()').str.split(expand=True).astype(float)

    

    # Puedes eliminar las claves originales que ya no necesitas
    df.pop('YearStart')
    df.pop('YearEnd')
    df.pop('GeoLocation')
    df.pop('DatavalueFootnote')

    



    return df


def apply_pca(df):
    df.replace("", pd.NA, inplace=True)
    # Aplicar PCA a los datos transformados
    features = ['Latitude', 'Longitude']
    X = df[features]
    pca = PCA(n_components=2)
    X_pca = pca.fit_transform(X)
    
    # Añadir las componentes principales al DataFrame transformado
    df['PCA1'] = X_pca[:, 0]
    df['PCA2'] = X_pca[:, 1]

    # df['DataValueUnit'] = pd.to_numeric(df['DataValueUnit'], errors='coerce')
    # df['DataValueFootnoteSymbol'] = df['DataValueFootnoteSymbol'].replace('-', float('nan'))
    empty_columns = df.columns[df.isnull().all()].tolist()

    # Eliminar columnas sin datos
    df.dropna(axis=1, how='all', inplace=True)
    # Eliminar filas con valores faltantes
    df.dropna(axis=0, how='all', inplace=True)



    # # Rellenar los valores faltantes con la media de la columna
    df['DataValue'].fillna(df['DataValue'].mean(), inplace=True)
    df['DataValueAlt'].fillna(df['DataValueAlt'].mean(), inplace=True)

    # # Rellenar los valores faltantes con la mediana de la columna
    # df.fillna(df.median(), inplace=True)

    # # Rellenar los valores faltantes con un valor específico
    # df.fillna(0, inplace=True)
    df.drop('DataValueUnit', axis=1, inplace=True)
    df.drop('DataValueFootnoteSymbol', axis=1, inplace=True)
    
    




    
    return df

def load_transformed_data(df):
    conn = PostgresHook(postgres_conn_id='potsqqq')
    engine = create_engine(conn.get_uri())

    # Nombre de la tabla de destino en la base de datos PostgreSQL
    table_name = 'transformed_table'

    # Carga los datos en la tabla de la base de datos
    df.to_sql(table_name, engine, if_exists='replace', index=False)

# Definición del DAG
with DAG(dag_id="etl_dag", 
         schedule_interval=None, 
         start_date=datetime(2023, 7, 8),
         catchup=False) as dag:

    # Tarea de extracción
    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract,
        dag=dag
    )

    # Tarea de transformación
    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform,
        op_args=[extract_task.output],
        provide_context=True,
        dag=dag
    )

    # Tarea de aplicar PCA
    pca_task = PythonOperator(
        task_id="apply_pca",
        python_callable=apply_pca,
        op_args=[transform_task.output],
        provide_context=True,
        dag=dag
    )
    
    # Tarea de carga
    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_transformed_data,
        op_args=[pca_task.output],
        provide_context=True,
        dag=dag
    )

    # Definición del flujo de ejecución
    extract_task >> transform_task >> pca_task >> load_task