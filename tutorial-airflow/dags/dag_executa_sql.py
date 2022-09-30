from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG('dag_executa_sql', start_date = datetime(2021,12,1),
    schedule_interval="30 * * * *", catchup=False,
    template_searchpath = '/opt/airflow/sql') as dag:
    
    criar_tabela_db = PostgresOperator(
        task_id='criar_tabela_db',
        postgres_conn_id = 'postgres-airflow',
        sql = 'criar_tabela_db.sql'
        
    )
    
    insere_dados_tabela_db = PostgresOperator(
        task_id='insere_dados_tabela_db',
        postgres_conn_id = 'postgres-airflow',
        sql = 'insere_dados_tabela_db.sql'
        
    )

criar_tabela_db >> insere_dados_tabela_db