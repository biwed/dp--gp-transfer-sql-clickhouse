from infi.clickhouse_orm import migrations, RunSQL
from ..transfer_ods import SCHEMA_STAGE_S3, Sales
operations = [
    RunSQL(f'CREATE DATABASE {SCHEMA_STAGE_S3};'),
    migrations.CreateTable(Sales)
]
