from langchain import OpenAI
from sqlalchemy import create_engine, MetaData
from llama_index import LLMPredictor, ServiceContext, SQLDatabase, VectorStoreIndex
from llama_index.indices.struct_store import SQLTableRetrieverQueryEngine
from llama_index.objects import SQLTableNodeMapping, ObjectIndex, SQLTableSchema
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
engine = create_engine(connection_uri, schema='dbo_v2')

metadata_obj = MetaData(schema="dbo_v2")
metadata_obj.reflect(bind=engine, schema="dbo_v2")

sql_database = SQLDatabase(engine,schema='dbo_v2', metadata=metadata_obj, include_tables=["fcs_computadores", "fcs_computador_medidor"])
table_node_mapping = SQLTableNodeMapping(sql_database)
table_names_filter = ["dbo_v2.fcs_computadores", "dbo_v2.fcs_computador_medidor"]
table_schema_objs = [SQLTableSchema(table_name=table_name) for table_name in metadata_obj.tables.keys() if table_name in table_names_filter]

obj_index = ObjectIndex.from_objects(
    objects=table_schema_objs,
    object_mapping=table_node_mapping,
    index_cls=VectorStoreIndex,
)

llm_predictor = LLMPredictor(llm=OpenAI(temperature=0, openai_api_key=API_KEY, model="text-davinci-003"))
service_context = ServiceContext.from_defaults(llm_predictor=llm_predictor, num_output=256)

query_engine = SQLTableRetrieverQueryEngine(
    sql_database,
    obj_index.as_retriever(similarity_top_k=1),
    service_context=service_context,
)

response = query_engine.query("How many computers are in the table?")

# print(response)
# print(response.metadata['sql_query'])
# print(response.metadata['result'])