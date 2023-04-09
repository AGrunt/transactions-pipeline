import csv
import re
import datetime
from datetime import timedelta
import json
import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.hooks.base import BaseHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
from airflow.providers.telegram.hooks.telegram import TelegramHook
from airflow.providers.telegram.operators.telegram import TelegramOperator

""" SERVICE FUNCTIONS """

""" Function for sending notification through Telegram service """

def telegram_func(tg_message, **kwargs):
    telegram_hook = TelegramHook(telegram_conn_id='telegram_default')
    extra_hook = BaseHook.get_connection('telegram_default')
    extra_section = json.loads(extra_hook.extra)
    chat_id = extra_section['chat_id']
    telegram_hook.send_message({'text': tg_message, 'chat_id': chat_id})   

""" CALLBACKS """

""" Callback for failed task/DAG """

def on_failure_callback(context, **kwargs):
    ti = context['task_instance']
    exception_ti = context.get('exception')
    tg_message = (f'Issue.\nException: {exception_ti}\nTask id: {ti.task_id}\nDag id: {ti.dag_id}')
    telegram_func(tg_message)

""" Callback for successed task/DAG """

def on_success_callback(context, **kwargs):
    ti = context['task_instance']
    tg_message = (f'Success.\nTask id: {ti.task_id}\nDag id: {ti.dag_id}')
    telegram_func(tg_message)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # 'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
    'on_failure_callback': on_failure_callback,
    # 'on_success_callback': on_success_callback,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

dag = DAG(
    'transactions_pipeline',
    default_args = default_args,
    description='Transactions Pipeline',
    schedule_interval='0 * * * *',
    start_date = days_ago(2),
    tags=['transactions'],

)

""" Function declarations """

def get_chat_id(conn_id):
    extra_hook = BaseHook.get_connection(conn_id)
    extra_section = json.loads(extra_hook.extra)
    chat_id = extra_section['chat_id']
    return chat_id

""" Get path to the directiory with files for processing """

def get_path(conn_id):
    connection = BaseHook.get_connection(conn_id)
    path = json.loads(connection.extra)
    files_path = path['path']
    return files_path

""" List files in directory for processing """

def get_file_names():
    file_names = []
    for file_name in os.listdir(get_path('file_csv_transactions_landing')):
        if file_name.endswith('.csv'):
            file_names.append(file_name)
    return file_names

""" Getting uploded lines in table for filename """

def get_max_lines(source, connection, table):
    mysql_hook = MySqlHook(mysql_conn_id = connection, commit_every = 1)
    sql = f'SELECT COALESCE(MAX(Line),0) FROM {table} WHERE DataSource = %s'
    parameters = [source]
    max_lines = mysql_hook.get_first(sql = sql, parameters = parameters)
    return max_lines[0]

""" Extracting 'Value Date' from transaction description """

def get_value_date(txDescription):
    value_date_result = re.search('Value Date: (\d{2}/\d{2}/\d{4})', txDescription)
    if value_date_result:
        return value_date_result.group(1)
    else:
        value_date_result = None
        return value_date_result

""" Data Extraction function and inserting data to temporary table 'transaction_landing' row by row """

def extract(**kwargs):
    file_names = get_file_names()
    path = get_path('file_csv_transactions_landing')
    for file_name in file_names:
        row_counter = 1
        fields = ['TxDate', 'Amount', 'TxDescription', 'Balance', 'DataSource', 'Line']
        with open(path + file_name, 'r') as open_file:
            csvreader = csv.DictReader(open_file , fieldnames = ['Date', 'Amount', 'Description', 'Balance'])
            for row in csvreader:
                try:
                    row['DataSource'] = file_name
                    row['Line'] = row_counter
                    results = [(row['Date'], row['Amount'], row['Description'], row['Balance'], row['DataSource'], row['Line'])]
                    mysql_transactions_landing_hook = MySqlHook(mysql_conn_id = 'db_mysql_transactions_landing', commit_every = 1 )
                    mysql_transactions_landing_hook.insert_rows(table = 'transactions_landing', rows = results, target_fields = fields )
                except:
                    # TODO Check log level for this output
                    print(f'Something went wrong: {results}')
                row_counter += 1

    task_instance = kwargs['task_instance']
    task_instance.xcom_push(key = 'file_names', value = file_names)              

""" Rename processed files """

def file_rename(**kwargs):
    task_instance = kwargs['task_instance']
    files = task_instance.xcom_pull(task_ids = 'extract_task', key = 'file_names')
    path = get_path('file_csv_transactions_landing')
    for file in files:
        os.rename(path+file, path+file+'.bak')

""" Transformation of data with inserting transformed data into a final table 'transactions' row by row """

def transform(**kwargs):
    task_instance = kwargs['task_instance']
    files = task_instance.xcom_pull(task_ids = 'extract_task', key = 'file_names')
    mysql_transactions_landing_hook = MySqlHook(mysql_conn_id = 'db_mysql_transactions_landing', commit_every = 1 )
    mysql_transactions_hook = MySqlHook(mysql_conn_id = 'db_mysql_transactions', commit_every = 1 )
    for file in files:
        max_line_transactions_landing = get_max_lines(file, 'db_mysql_transactions_landing', 'transactions_landing')
        max_line_transactions = get_max_lines(file, 'db_mysql_transactions', 'transactions')
        if max_line_transactions < max_line_transactions_landing:
            sql_transactions_landing_rows = 'SELECT * FROM transactions_landing where Line > %s AND DataSource = %s'
            sql_transactions_landing_rows_parameters = [max_line_transactions, file]
            mysql_transactions_landing_rows = mysql_transactions_landing_hook.get_records(sql = sql_transactions_landing_rows, parameters = sql_transactions_landing_rows_parameters)
            for row in mysql_transactions_landing_rows:
                row_list = list(row)
                row_value = {
                    'TxDate': row_list[0],
                    'Amount': row_list[1],
                    'TxDescription': row_list[2],          
                    'Balance': row_list[3],
                    'ValueDate': get_value_date(row_list[2]),
                    'DataSource': row_list[4],
                    'Line': row_list[5]
                }

                if row_value['TxDate']:
                    row_value['TxDate'] = datetime.datetime.strptime(str(row_list[0]), '%d/%m/%Y').strftime('%Y-%m-%d')
                else:
                    row_value['TxDate'] = None
                if not row_value['Amount']:
                    row_value['Amount'] = None
                if not row_value['TxDescription']:
                    row_value['TxDescription'] = None
                if not row_value['Balance']:
                    row_value['Balance'] = None
                if get_value_date(row_list[2]):
                    row_value['ValueDate'] = datetime.datetime.strptime(str(get_value_date(row_list[2])), '%d/%m/%Y').strftime('%Y-%m-%d')
                else:
                    row_value["ValueDate"] = None
                if not row_value['DataSource']:
                    row_value['DataSource'] = None
                if not row_value['Line']:
                    row_value['Line'] = None

                fields = ['TxDate', 'Amount', 'TxDescription', 'Balance', 'ValueDate', 'DataSource', 'Line']
                results = [(row_value['TxDate'], row_value['Amount'], row_value['TxDescription'], row_value['Balance'], row_value['ValueDate'], row_value['DataSource'], row_value['Line'])]
                mysql_transactions_hook.insert_rows(table = 'transactions', rows = results, target_fields = fields )

""" DAG tasks declarations """

telegram_notification_task = TelegramOperator(
    task_id = 'telegram_notification_task',
    telegram_conn_id = 'telegram_default',
    chat_id = get_chat_id('telegram_default'),
    text = f'Started: {dag.dag_id}',
    dag = dag,
)

check_file_task = FileSensor(
    task_id = 'check_file_task',
    filepath = '*.csv',
    fs_conn_id = 'file_csv_transactions_landing',
    poke_interval = 2,
    dag = dag,
    on_failure_callback = on_failure_callback,
    on_success_callback = on_success_callback,
)

extract_task = PythonOperator(
    task_id = 'extract_task',
    provide_context = True,
    python_callable = extract,
    dag = dag,
    on_failure_callback = on_failure_callback,
    on_success_callback = on_success_callback,
)

rename_task = PythonOperator(
    task_id = 'rename_task',
    provide_context = True,
    python_callable = file_rename,
    dag = dag,
    on_failure_callback = on_failure_callback,
    on_success_callback = on_success_callback,
) 

transform_task = PythonOperator(
    task_id = 'transform_task',
    provide_context = True,
    python_callable = transform,
    dag = dag,
    on_failure_callback = on_failure_callback,
    on_success_callback = on_success_callback,
)

great_expectation_task = GreatExpectationsOperator(
    task_id = 'great_expectation_task',
    data_context_root_dir = './great_expectations/',
    checkpoint_name = "transactions_value_date_checkpoint",
    return_json_dict = True,
    dag = dag,
    on_failure_callback = on_failure_callback,
    on_success_callback = on_success_callback,
) 

telegram_notification_task >> check_file_task >> extract_task >> rename_task >> transform_task >> great_expectation_task