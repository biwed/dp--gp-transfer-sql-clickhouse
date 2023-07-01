import os
from infi.clickhouse_orm import (
    Model,
    LowCardinalityField, NullableField,
    DateField, StringField, DateTimeField,
    Float32Field, Float64Field,
    Int32Field, Int64Field, UInt64Field,
    MergeTree
)
from infi.clickhouse_orm import F
from .utils import uri_to_db
from inspect import isclass


STORE_BACKEND_URI = os.environ.get("CLICKHOUSE_BACKEND_URI")
SCHEMA_ODS = 'transfer_ods'
SCHEMA_STAGE_S3 = 'transfer_stage_s3'

DB_INIT = uri_to_db(STORE_BACKEND_URI)


class Sales(Model):
    id = Int32Field()
    date = DateField()
    amt = Float32Field()
    _updated_ddtm = DateTimeField(default=F.now())

    engine = MergeTree(
        date_col='date', 
        order_by=('date', 'id'),
    )

    @classmethod
    def table_name(cls):
        return 'sale_test'


ALL_TABLE = {
    c.table_name(): c for c in locals().values() if isclass(c) and issubclass(c, (Model)) and type(c()) != Model
}
