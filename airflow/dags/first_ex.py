from airflow.models.dag import DAG
import pendulum
from airflow.operators.python import PythonOperator
import psycopg2

def get_db_connection():
    return psycopg2.connect(
    host='postgres_u',
    database='sdulogs',
    user='karina',
    password='12345',
    port = '5433'
)

with DAG(
        "first_dag",
        default_args={"retries": 1},
        description="DAG tutorial",
        schedule="@daily",
        start_date=pendulum.datetime(2024, 12, 3, tz="UTC"),
        catchup=False,
        tags=["example"],
) as dag:
    def extract(**kwargs):
        ti = kwargs['ti']
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""CREATE TABLE IF NOT EXISTS LOGIN_LOGS (
            user_id VARCHAR(64) NOT NULL, 
            user_ip VARCHAR(45),          
            device_info TEXT,             
            log_date TIMESTAMP,           
            login_status INTEGER
        );""")

        conn.commit()



    def transform(**kwargs):
        ti = kwargs['ti']

        with open('../../de5(practise)/loginLogs-3-64.sql', 'r') as file:
            sql_queries = file.read()

        ti.xcom_push(key='sql_queries', value=sql_queries)


    def load(**kwargs):
        ti = kwargs['ti']

        sql_queries = ti.xcom_pull(task_ids="transform", key="sql_queries")

        if sql_queries:
            conn = get_db_connection()
            cur = conn.cursor()

            for query in sql_queries.split(';'):
                query = query.strip()
                if query:
                    cur.execute(query)

            conn.commit()
            cur.close()
            conn.close()

extract_task = PythonOperator(
    task_id="extract",
    python_callable=extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform",
    python_callable=transform,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load",
    python_callable=load,
    dag=dag,
)

extract_task >> transform_task >> load_task
