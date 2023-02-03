from datetime import datetime
from json import loads

from airflow.decorators import dag, task, task_group
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from clients.s3_client import S3Client
from clients.vertica_client import VerticaClient

S3_CONN = BaseHook.get_connection("S3")
S3_EXTRA = loads(S3_CONN.get_extra())
S3_CONN_INFO = {
    "service_name": S3_EXTRA.get('service_name'),
    "endpoint_url": S3_EXTRA.get('endpoint_url'),
    "aws_access_key_id": S3_CONN.login,
    "aws_secret_access_key": S3_CONN.get_password()
}

VERTICA_CONN = BaseHook.get_connection("VERTICA")
VERTICA_CONN_INFO = {
    'host': VERTICA_CONN.host,
    'port': VERTICA_CONN.port,
    'user': VERTICA_CONN.login,
    'password': VERTICA_CONN.get_password(),
    'database': VERTICA_CONN.schema
}

BUCKET_NAME = Variable.get('BUCKET_NAME')

DIALOGS_FILE_NAME = Variable.get('DIALOGS_FILE_NAME')
GROUPS_FILE_NAME = Variable.get('GROUPS_FILE_NAME')
USERS_FILE_NAME = Variable.get('USERS_FILE_NAME')
GROUP_LOG_FILE_NAME = Variable.get('GROUP_LOG_FILE_NAME')

BUCKET_FILES = [DIALOGS_FILE_NAME, GROUPS_FILE_NAME, USERS_FILE_NAME, GROUP_LOG_FILE_NAME]

PATH_TEMPLATE = "/data/{}"

STAGE_SCHEMA_NAME = Variable.get('STAGE_SCHEMA_NAME')
DWH_SCHEMA_NAME = Variable.get('DWH_SCHEMA_NAME')

VERTICA_CLIENT = VerticaClient(VERTICA_CONN_INFO, STAGE_SCHEMA_NAME, DWH_SCHEMA_NAME, PATH_TEMPLATE)


@task
def fetch_dialogs_csv(s3_client: S3Client) -> None:
    s3_client.fetch_s3_file(DIALOGS_FILE_NAME)


@task
def fetch_groups_csv(s3_client: S3Client) -> None:
    s3_client.fetch_s3_file(GROUPS_FILE_NAME)


@task
def fetch_users_csv(s3_client: S3Client) -> None:
    s3_client.fetch_s3_file(USERS_FILE_NAME)


@task
def fetch_group_logs_csv(s3_client: S3Client) -> None:
    s3_client.fetch_s3_file(GROUP_LOG_FILE_NAME)


@task_group()
def fetch_files():
    s3_client = S3Client(S3_CONN_INFO, PATH_TEMPLATE, BUCKET_NAME)
    fetch_dialogs_csv(s3_client)
    fetch_users_csv(s3_client)
    fetch_groups_csv(s3_client)
    fetch_group_logs_csv(s3_client)


@task
def load_dialogs():
    VERTICA_CLIENT.copy_file_to_vertica(DIALOGS_FILE_NAME, 'dialogs')


@task
def load_groups():
    VERTICA_CLIENT.copy_file_to_vertica(GROUPS_FILE_NAME, 'groups')


@task
def load_users():
    VERTICA_CLIENT.copy_file_to_vertica(USERS_FILE_NAME, 'users')


@task
def load_group_log():
    VERTICA_CLIENT.copy_file_to_vertica(GROUP_LOG_FILE_NAME, 'group_log')


@task_group()
def load_files_to_stage():
    load_dialogs()
    load_groups()
    load_users()
    load_group_log()


@task_group()
def load_dwh_hubs():
    # В версии 2.3.0 TaskFlow не умеет рендерить шаблоны в методы тасков,
    # пришлось делать операторами по старинке :-(
    load_h_users = PythonOperator(
        task_id='load_h_users',
        python_callable=VERTICA_CLIENT.execute_query_from_template,
        templates_dict={"query": "sql/dwh_load_h_users.sql"},
        templates_exts=[".sql"]
    )

    load_h_groups = PythonOperator(
        task_id='load_h_groups',
        python_callable=VERTICA_CLIENT.execute_query_from_template,
        templates_dict={"query": "sql/dwh_load_h_groups.sql"},
        templates_exts=[".sql"]
    )

    load_h_dialogs = PythonOperator(
        task_id='load_h_dialogs',
        python_callable=VERTICA_CLIENT.execute_query_from_template,
        templates_dict={"query": "sql/dwh_load_h_dialogs.sql"},
        templates_exts=[".sql"]
    )

    load_h_group_logs = PythonOperator(
        task_id='load_h_group_logs',
        python_callable=VERTICA_CLIENT.execute_query_from_template,
        templates_dict={"query": "sql/dwh_load_h_group_logs.sql"},
        templates_exts=[".sql"]
    )


@task_group()
def load_dwh_links():
    load_l_user_message_from = PythonOperator(
        task_id='load_l_user_message_from',
        python_callable=VERTICA_CLIENT.execute_query_from_template,
        templates_dict={"query": "sql/dwh_load_l_user_message_from.sql"},
        templates_exts=[".sql"]
    )

    load_l_user_message_to = PythonOperator(
        task_id='load_l_user_message_to',
        python_callable=VERTICA_CLIENT.execute_query_from_template,
        templates_dict={"query": "sql/dwh_load_l_user_message_to.sql"},
        templates_exts=[".sql"]
    )

    load_l_admins = PythonOperator(
        task_id='load_l_admins',
        python_callable=VERTICA_CLIENT.execute_query_from_template,
        templates_dict={"query": "sql/dwh_load_l_admins.sql"},
        templates_exts=[".sql"]
    )

    load_l_groups_dialogs = PythonOperator(
        task_id='load_l_groups_dialogs',
        python_callable=VERTICA_CLIENT.execute_query_from_template,
        templates_dict={"query": "sql/dwh_load_l_groups_dialogs.sql"},
        templates_exts=[".sql"]
    )

    load_l_group_logs_groups = PythonOperator(
        task_id='load_l_group_logs_groups',
        python_callable=VERTICA_CLIENT.execute_query_from_template,
        templates_dict={"query": "sql/dwh_load_l_group_logs_groups.sql"},
        templates_exts=[".sql"]
    )

    load_l_group_logs_users = PythonOperator(
        task_id='load_l_group_logs_users',
        python_callable=VERTICA_CLIENT.execute_query_from_template,
        templates_dict={"query": "sql/dwh_load_l_group_logs_users.sql"},
        templates_exts=[".sql"]
    )

    load_l_group_logs_users_from = PythonOperator(
        task_id='load_l_group_logs_users_from',
        python_callable=VERTICA_CLIENT.execute_query_from_template,
        templates_dict={"query": "sql/dwh_load_l_group_logs_users_from.sql"},
        templates_exts=[".sql"]
    )


@task_group()
def load_dwh_satellites():
    load_s_user_chatinfo = PythonOperator(
        task_id='load_s_user_chatinfo',
        python_callable=VERTICA_CLIENT.execute_query_from_template,
        templates_dict={"query": "sql/dwh_load_s_user_chatinfo.sql"},
        templates_exts=[".sql"]
    )

    load_s_user_socdem = PythonOperator(
        task_id='load_s_user_socdem',
        python_callable=VERTICA_CLIENT.execute_query_from_template,
        templates_dict={"query": "sql/dwh_load_s_user_socdem.sql"},
        templates_exts=[".sql"]
    )

    load_s_group_private_status = PythonOperator(
        task_id='load_s_group_private_status',
        python_callable=VERTICA_CLIENT.execute_query_from_template,
        templates_dict={"query": "sql/dwh_load_s_group_private_status.sql"},
        templates_exts=[".sql"]
    )

    load_s_admins = PythonOperator(
        task_id='load_s_admins',
        python_callable=VERTICA_CLIENT.execute_query_from_template,
        templates_dict={"query": "sql/dwh_load_s_admins.sql"},
        templates_exts=[".sql"]
    )

    load_s_group_name = PythonOperator(
        task_id='load_s_group_name',
        python_callable=VERTICA_CLIENT.execute_query_from_template,
        templates_dict={"query": "sql/dwh_load_s_group_name.sql"},
        templates_exts=[".sql"]
    )

    load_s_dialog_info = PythonOperator(
        task_id='load_s_dialog_info',
        python_callable=VERTICA_CLIENT.execute_query_from_template,
        templates_dict={"query": "sql/dwh_load_s_dialog_info.sql"},
        templates_exts=[".sql"]
    )

    load_s_auth_history = PythonOperator(
        task_id='load_s_auth_history',
        python_callable=VERTICA_CLIENT.execute_query_from_template,
        templates_dict={"query": "sql/dwh_load_s_auth_history.sql"},
        templates_exts=[".sql"]
    )


@dag(schedule_interval=None, start_date=datetime.now(), render_template_as_native_obj=True)
def sprint6_dag():
    start = EmptyOperator(task_id='start')

    print_10_lines_of_each = BashOperator(
        task_id='print_10_lines_of_each',
        bash_command="head {{ params.files }}",
        params={'files': ' '.join([PATH_TEMPLATE.format(f) for f in BUCKET_FILES])}
    )

    finish = EmptyOperator(task_id='finish')

    (
            start
            >> fetch_files()
            >> print_10_lines_of_each
            >> load_files_to_stage()
            >> load_dwh_hubs()
            >> load_dwh_links()
            >> load_dwh_satellites()
            >> finish
    )


_ = sprint6_dag()
