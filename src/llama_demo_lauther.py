from sqlalchemy import create_engine, MetaData
from llama_index import LLMPredictor, ServiceContext, SQLDatabase, VectorStoreIndex
from llama_index.indices.struct_store import SQLTableRetrieverQueryEngine
from llama_index.objects import SQLTableNodeMapping, ObjectIndex, SQLTableSchema
from langchain import OpenAI
import environ
import pyodbc
import openai
from sqlalchemy.engine import URL

env = environ.Env()
environ.Env.read_env()
API_KEY = env('OPENAI_API_KEY')
USER = env('USER')
PWD = env('PWD')
HOST = env('HOST')
DBNAME = env('DBNAME')
ODBCDRIVER = env('ODBCDRIVER')

openai.api_key  = API_KEY

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
metadata_obj.reflect(engine)

sql_database = SQLDatabase(engine)
table_node_mapping = SQLTableNodeMapping(sql_database)
table_schema_objs = []
for table_name in metadata_obj.tables.keys():
    table_schema_objs.append(SQLTableSchema(table_name=table_name))


obj_index = ObjectIndex.from_objects(
    table_schema_objs,
    table_node_mapping,
    VectorStoreIndex,
)

llm_predictor = LLMPredictor(llm=OpenAI(temperature=0, model_name="text-davinci-003"))
service_context = ServiceContext.from_defaults(llm_predictor=llm_predictor)

query_engine = SQLTableRetrieverQueryEngine(
    sql_database,
    obj_index.as_retriever(similarity_top_k=1),
    service_context=service_context,
)

response = query_engine.query("Using the table: dbo_v2.fcs_computadores, How many computers with firmware equal to 21 are in the table?")

print(response)
print(response.metadata['sql_query'])
print(response.metadata['result'])