from langchain import OpenAI
from sqlalchemy import select, create_engine, MetaData, Table,inspect
from llama_index import LLMPredictor, ServiceContext, SQLDatabase, VectorStoreIndex
from llama_index.indices.struct_store.sql_query import NLSQLTableQueryEngine, BaseSQLTableQueryEngine
import environ
from sqlalchemy.engine import URL
from llama_index.objects import SQLTableNodeMapping, ObjectIndex, SQLTableSchema
import pyodbc

env = environ.Env()
environ.Env.read_env()
API_KEY = env('OPENAI_API_KEY')
USER = env('USER')
PWD = env('PWD')
HOST = env('HOST')
DBNAME = env('DBNAME')
ODBCDRIVER = env('ODBCDRIVER')

connection_uri = URL.create(
    "mssql+pyodbc",
    username=USER,
    password=PWD,
    host=HOST,
    database=DBNAME,
    query={"driver": ODBCDRIVER},
)
engine = create_engine(connection_uri)

metadata_obj = MetaData()
metadata_obj.reflect(engine, schema='dbo_v2')


sql_database = SQLDatabase(engine, schema='dbo_v2', include_tables=["fcs_computadores"])


table_node_mapping = SQLTableNodeMapping(sql_database)
table_schema_objs = [(SQLTableSchema(table_name="fcs_computadores"))]

obj_index = ObjectIndex.from_objects(
    table_schema_objs,
    table_node_mapping,
    VectorStoreIndex,
)
