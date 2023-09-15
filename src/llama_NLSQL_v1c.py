from langchain import OpenAI
from langchain_experimental.sql import SQLDatabaseChain
from langchain.utilities import SQLDatabase
from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.engine import URL
import environ
import openai
import pyodbc
# from llama_index import LLMPredictor, ServiceContext, VectorStoreIndex
# from llama_index.indices.struct_store.sql_query import NLSQLTableQueryEngine


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
sql_database = SQLDatabase.from_uri(connection_uri, schema='dbo_v2', include_tables=["fcs_computadores", "fcs_computador_medidor"])
# sql_database = SQLDatabase(engine,schema='dbo_v2', metadata=metadata_obj, include_tables=["fcs_computadores", "fcs_computador_medidor"])
llm = OpenAI(temperature=0, openai_api_key=API_KEY, model="text-davinci-003")


QUERY = """
Given an input question, first create a syntactically correct SQL query to run, then look at the results of the query and return the answer.
Use the following format:

Question: Question here
SQLQuery: SQL Query to run
SQLResult: Result of the SQLQuery
Answer: Final answer here
{question}
"""

db_chain = SQLDatabaseChain(llm=llm, database=sql_database, verbose=True)
query_str = "How many meters does the computer have with IP equal to 1.1.1.1?"
response = db_chain.run(query_str)
print(response)