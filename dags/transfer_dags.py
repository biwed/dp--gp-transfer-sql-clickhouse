import os
import sys
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
import uuid
import logging
import glob
import yaml
import dateutil.parser
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime
import os
import sys

dag_folder = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, dag_folder)
from dag_libs.config.config import MODULE_NAME, POOL, VARIABLES_NAME, GP_CONNECTION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

def get_column_list(table) -> list:
    colum_list = [column for column in table.fields() if not column.startswith('_')]
    return colum_list


def get_path(table) -> str:
    from dag_libs.transfer_ods import SCHEMA_ODS
    transfer_to_click = Variable.get(VARIABLES_NAME, deserialize_json=True)
    path = f'{transfer_to_click["S3_BUCKET"]}/{MODULE_NAME}/{SCHEMA_ODS}/{table}'
    logging.info(f'path = {path}')
    return path


def migration(**kwargs):
    from infi.clickhouse_orm import Database
    from dag_libs.transfer_ods import DB_INIT, SCHEMA_ODS
    current_db = DB_INIT.copy()
    current_db['db_name'] = SCHEMA_ODS
    Database(**current_db).migrate("dag_libs.transfer_ods_migrations")


def generate_batch_guid(table, **kwargs) -> str:
    kwargs['ti'].xcom_push(key='batch_uud', value=f'{uuid.uuid4()}')
    kwargs['ti'].xcom_push(key='s3_path', value=get_path(table))


def upload_bathch_gp_s3(table, config: dict, **kwargs):
    from dag_libs.transfer_ods import ALL_TABLE
    logging.info('Start upload_bathch_gp_s3')
    logging.info(f'ALL_TABLE = {ALL_TABLE}')
    ti = kwargs['ti']
    batch_uud = ti.xcom_pull(task_ids='generate_batch_guid', key='batch_uud')
    s3_path = ti.xcom_pull(task_ids='generate_batch_guid', key='s3_path')
    logging.info(batch_uud)
    table_class = ALL_TABLE[table]
    colum_list = get_column_list(table_class)
    select_column = ", ".join(colum_list)
    sql_query = config['sql']

    sql = f"""
        DROP TABLE  IF EXISTS temp_out_table;
        CREATE TEMP TABLE temp_out_table AS
            SELECT {select_column},
                NULL::TEXT as _updated_ddtm
            FROM (
                {sql_query}
            ) as t;

        DROP EXTERNAL  TABLE  IF EXISTS temp_out_table_ext_write;
        CREATE WRITABLE EXTERNAL TEMP TABLE temp_out_table_ext_write
        (LIKE temp_out_table)
        LOCATION (
            'pxf://{s3_path}/{batch_uud}?PROFILE=s3:text&SERVER=default&COMPRESSION_CODEC=org.apache.hadoop.io.compress.GzipCodec'
            )
        ON ALL FORMAT 'TEXT' (delimiter=';') ENCODING 'UTF8';

        INSERT INTO temp_out_table_ext_write
        SELECT * FROM temp_out_table;
        /*Get row count in temp table*/
        SELECT count(*)
        FROM temp_out_table;
    """
    pg = PostgresHook(postgres_conn_id=GP_CONNECTION)
    logging.info(f"PG hook {pg}")
    # count_row = pg.get_first(sql=sql, parameters=param)[0]
    count_row = pg.get_first(sql=sql)[0]
    logging.info(sql)
    logging.info(count_row)
    kwargs['ti'].xcom_push(key='count_rows', value=count_row)


def load_batch_s3_cliclhouse(table, config: dict, **kwargs):
    from dag_libs.transfer_ods import SCHEMA_ODS, DB_INIT, SCHEMA_STAGE_S3, ALL_TABLE
    from dag_libs.custom_eng.s3_engine_infi import S3Config, S3
    from infi.clickhouse_orm import Database, UUIDField, NullableField, StringField
    
    leader_variables = Variable.get(VARIABLES_NAME, deserialize_json=True)
    s3_server = leader_variables['S3_SERVER']
    logging.info(f's3_server = {s3_server}')

    logging.info('Start load_batch_s3_cliclhouse')
    ti = kwargs['ti']
    batch_uud = ti.xcom_pull(task_ids='generate_batch_guid', key='batch_uud')
    s3_path = ti.xcom_pull(task_ids='generate_batch_guid', key='s3_path')
    count_rows = ti.xcom_pull(task_ids='upload_bathch_gp_s3', key='count_rows')
    if count_rows == 0:
        return 0
    table_class = ALL_TABLE[table]
    table_name = table_class.table_name()
    colum_list = get_column_list(table_class)
    select_column = ", ".join(colum_list)
    increment_field = config['incrementField']
    s3_param = config.get('S3')
    defaut_s3 = {
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
        "compression": 'none',
        "format": 'CSV',
        "path": f'{s3_server}{s3_path}/{batch_uud}/*'
    }
    if s3_param:
        s3_config = {
            **defaut_s3,
            **s3_param
        }

        test_config = S3Config(
            **s3_config
        )
    else:
        test_config = S3Config(
            **defaut_s3
        )

    class TableToS3(table_class):
        engine = S3(test_config)
        _updated_ddtm = NullableField(StringField())

        @classmethod
        def table_name(cls):
            return f'{table_name}_s3_{batch_uud}'

    test = DB_INIT.copy()
    test['db_name'] = SCHEMA_STAGE_S3

    db = Database(**test)
    db.create_table(TableToS3)

    class TableToStage(table_class):
        batch_uuid_tech = UUIDField()
        _updated_ddtm = NullableField(StringField())

        @classmethod
        def table_name(cls):
            return f'{table_name}_stage_{batch_uud}'

    db.create_table(TableToStage)

    sql = f"""
        INSERT INTO "{TableToStage.table_name()}" ({select_column} , "batch_uuid_tech")
        SELECT
            {select_column},
            '{batch_uud}'
        FROM
            "{TableToS3.table_name()}"
    """
    logging.info(f'Unsert into stage = {sql}')
    db.raw(sql)

    sql = f"""
        DELETE FROM "{SCHEMA_ODS}"."{table_class.table_name()}"
        WHERE `{increment_field}` >= (
                                        SELECT min(`{increment_field}`)
                                        FROM "{SCHEMA_STAGE_S3}"."{TableToStage.table_name()}"
                                    )
         AND `{increment_field}` <= (
                                        SELECT max(`{increment_field}`)
                                        FROM "{SCHEMA_STAGE_S3}"."{TableToStage.table_name()}"
                                    ) 
        SETTINGS mutations_sync = 1;
    """
    logging.info(f'DROP OLD rows in target table = {sql}')
    db.raw(sql)
    sql = f"""
        INSERT INTO "{SCHEMA_ODS}"."{table_class.table_name()}" ({select_column})
        SELECT
            {select_column}
        FROM
            "{TableToStage.table_name()}"
    """
    logging.info(f'insert data into target target table = {sql}')
    db.raw(sql)
    db.drop_table(TableToStage)
    db.drop_table(TableToS3)
    logging.info('Stop create backet')


def create_dag(dag_config: dict, dag_id, table, config: dict):
    logging.info(f'config {dag_id} =  {table} {config}')
    start_date = dateutil.parser.isoparse(dag_config['start_date'])
    end_date = dateutil.parser.isoparse('2199-01-01')
    if dag_config.get('end_date'):
        end_date = dateutil.parser.isoparse(dag_config.get('end_date'))
    args = {
        "owner": dag_config.get('owner', ""),
        "email": dag_config.get('email', []),
        "email_on_failure": True,
        "email_on_retry": False,
        "start_date": start_date,
        "end_date": end_date,
        "depends_on_past": True,
        'pool': POOL,
        'catchup': dag_config.get('catchup', True)
    }
    dag = DAG(
        dag_id=dag_id,
        default_args=args,
        max_active_runs=1,
        schedule_interval=dag_config.get('schedule', "@once"),
        tags=dag_config.get('tags', []),
    )

    migration_step = PythonOperator(
        task_id="migration",
        provide_context=True,
        python_callable=migration,
        dag=dag,
    )

    generate_batch_guid_step = PythonOperator(
        task_id="generate_batch_guid",
        provide_context=True,
        python_callable=generate_batch_guid,
        op_kwargs={"table": table},
        dag=dag,
    )

    upload_bathch_gp_s3_step = PythonOperator(
        task_id="upload_bathch_gp_s3",
        provide_context=True,
        python_callable=upload_bathch_gp_s3,
        op_kwargs={"table": table, "config": config},
        dag=dag,
    )

    load_batch_s3_cliclhouse_step = PythonOperator(
        task_id="load_batch_s3_cliclhouse",
        provide_context=True,
        python_callable=load_batch_s3_cliclhouse,
        op_kwargs={"table": table, "config": config},
        dag=dag,
    )

    migration_step >> generate_batch_guid_step >> upload_bathch_gp_s3_step >> load_batch_s3_cliclhouse_step
    logging.info(f'create dag suscess {dag_id} =  {table} {config}')
    return dag


def get_dag_key(path, file_name: str) -> str:
    key_file = (file_name[len(path):]).replace("/", "_").replace('.yaml', '')
    return key_file


config_filepath = f'{dag_folder}/transfersql_configs/'

logging.info('Start create transfer dag')
logging.info(f'config_filepath = {config_filepath}')

file_contents = []
for filename in glob.glob(f'{config_filepath}/*.yaml', recursive=True):
    dag_id = MODULE_NAME + "_" + get_dag_key(config_filepath, filename)
    logging.error(f'found dag {dag_id}')
    with open(filename) as python_file:
        file_contents.append(
            (
                dag_id,
                yaml.safe_load(python_file)
            )
        )
logging.info(f'Create file file_contents = {file_contents}')
if file_contents is None:
    @dag(
        dag_id="error_file", 
        start_date=datetime(2022, 2, 1),
        tags=["error_create_dags", "error"]
        )
    def dynamic_generated_dag():
        @task
        def print_message(message):
            print(f' file_contents = {message}')

        print_message(file_contents)
    dynamic_generated_dag()
if len(file_contents) > 0:
    for (dag_ids, config) in file_contents:
        logging.info(f'DAG id = {dag_id}')
        logging.info(f"{config.get('kind')} , {config.get('version')}")
        if config.get('kind') == 'TransferSQLToClickHouse' and str(config.get('version')) == '1.0':
            logging.info(f'{dag_ids} kind and spec is normal')
            for item, table_config in config['spec']['tables'].items():
                table_dag_id = dag_ids + '_' + item
                for key, dag_config in table_config['DAG'].items():
                    dag_id = table_dag_id + "_" + key
                    dags = create_dag(
                        dag_config=dag_config,
                        dag_id=dag_id,
                        table=item,
                        config=table_config['metaspec']
                    )
                    if dags is not None:
                        globals()[dag_id] = dags
else:
    logging.info('file_content transfer is None')
