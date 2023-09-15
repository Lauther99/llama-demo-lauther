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

metadata_obj = MetaData(schema='dbo_v2')
metadata_obj.reflect(engine, schema='dbo_v2')

sql_database = SQLDatabase(engine, schema='dbo_v2', include_tables=["fcs_computadores"])

llm_predictor = LLMPredictor(llm=OpenAI(temperature=0, openai_api_key=API_KEY, model="text-davinci-003"))
service_context = ServiceContext.from_defaults(llm_predictor=llm_predictor, num_output=256)


query_engine = NLSQLTableQueryEngine(
    sql_database,
    tables=["dbo_v2.fcs_computadores"],
    service_context=service_context,
    synthesize_response=True,
)

res = query_engine.query("how many computers are in the table?")
print(res)
