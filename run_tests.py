from pprint import pprint
from elasticsearch import Elasticsearch
from bert_serving.client import BertClient
import pandas as pd
from laserembeddings import Laser
import sys



__author__ = "Bijin Benny"
__email__ = "bijin@ualberta.ca"
__license__ = "MIT"
__version__ = "1.0"

#Client connection to local BERT server
bc = BertClient(ip='localhost', output_fmt='list')

#Instance of the LASER language model
laser = Laser()

#Elasticsearch DB client
client = Elasticsearch(hosts="http://bijin:Samsung1!@localhost:9200/")

"""
createScript function creates custom database queries based on the search type. 
The search type includes basic TF-IDF term search, LASER vector and 
BERT vector similarity searches. The function returns a unique query 
for each of the scenarios.
Arguments :
query        : Text form of the query
search type  : Type of search, i.e term, laser or bert
query_vector : Vector form of the query for cosine similarity
"""
def createScript(query,search_type,query_vector):
    if(search_type == 'term'):
        return {  "simple_query_string" : {
        "query": query,
         "fields": ["title"],
         "default_operator": "and"
            }
        }
    elif(search_type == 'bert'):
        return {
        "script_score": {
         "query": {
             "multi_match": {
             "query": query,
                "type": "best_fields",  
                "fields": [ "title" ]   
                }
            },
        "script": {
           "source": "cosineSimilarity(params.query_vector,'bert_vector') + 1.0",
           "params": {"query_vector": query_vector}
       }
       }}
    else:   
        return { 
        "script_score": {
         "query": {
             "multi_match": {
             "query": query,
                "type": "best_fields",  
                "fields": [ "title" ]   
                }
            },
        "script": {
           "source": "cosineSimilarity(params.query_vector,'laser_vector') + 1.0",
           "params": {"query_vector": query_vector}
        }
        }  }

   
"""
doRunTest function performs the tests based on the 50 standard queries. 
The queries are loaded from the 'Recipes.csv' file and run against the 
search engine. The output is a csv file 'results_<type>.csv' containing 
the top 10 results for each query item
Arguments : 
search_type : Type of search technique to run
"""
def doRunTest(search_type):
    #Load the input test queries
    df = pd.read_csv ('Recipes.csv')

    resultMatrix = [['N/A' for i in range(50)] for j in range(10)]
    
    #Loop through each query and collect the results
    for i in range(len(df)):
        recipe = df.loc[i,"Recipe"]
        query_vector = ''

        #Generate the vector embedding of the query in case of laser or bert 
        if(search_type == 'bert'):
            query_vector = bc.encode([recipe])[0]
        elif(search_type == 'laser'):    
            query_vector = laser.embed_sentences([recipe],lang='en')[0] 
        
        #Generate the database query based on the search type
        q = createScript(recipe,search_type,query_vector)

        #Database query
        response = client.search(
         index="web-en",
         body={
            "size": 10,
            "query": q,
            "_source": {"includes": ["title"]}
            }
        )

        #Aggregate results and save to output csv file
        raw_results = response['hits']['hits']
        for j in range(len(raw_results)):
            resultMatrix[j][i] = raw_results[j]['_source']['title']

    for i in range(10):
        df['Result'+str(i+1)] = resultMatrix[i]
        
    df.to_csv('results_'+search_type+'.csv',encoding='utf-8')


#Main function
def main():
    error_msg = "**********************************\nInvalid script usage!\n \
    Usage : python interactive_query.py <Type> \nType : 'TERM', 'LASER' or  \
    'BERT'\n**********************************"
    if(len(sys.argv) < 2):
        sys.exit(error_msg)
    param = str(sys.argv[1]).lower()
    if(not(param == 'laser' or param == 'bert' or param == 'term')):
        sys.exit(error_msg) 
    doRunTest(param)


if __name__ == "__main__":
    main()    
