from google.cloud import bigquery
import json
import sys

def run_query(query):
  client = bigquery.Client()
  query_job = client.query (query)
  results = query_job.result()
  return results

if __name__ == '__main__':
    params=sys.argv[1]
    newParams=params.replace("'", "\"")
    params1=json.loads(newParams)   
   
    query = """SELECT count(*) FROM `dmgcp-ingestion-poc.transient.cvn_stress_8gb` """ 
   
    results=run_query(query)
   
    Params=str(parameters)
    newParams=Params.replace("'", "\"")
    print(newParams)
