from elasticsearch import Elasticsearch
from bert_serving.client import BertClient
import json
from laserembeddings import Laser
import sys


__author__ = "Bijin Benny"
__email__ = "bijin@ualberta.ca"
__license__ = "MIT"
__version__ = "1.0"


LASER = 'laser_vector'
BERT  = 'bert_vector'

laser = Laser()

#Elasticsearch DB client
es = Elasticsearch(hosts="http://bijin:Samsung1!@localhost:9200/") 

#Client connection to local BERT server
bc = BertClient(ip='localhost',output_fmt='list')		

"""
doVectorize() pulls entries from the database and maps the text sequences into the 
vector space using either one of LASER or BERT based on the input parameter. 
BERT produces 768 dimensional vector while LASER outputs a 1024 dimensional vector
Argument : vector_type (String) --> bert_vector or laser_vector
"""
def doVectorize(vector_type):
	"""
	Elastic search has a max search result limit of 10000 documents 
	Hence, loop through until all documents are fetched 
	"""
	while(True):		

		#Search query to fetch all documents with empty vector field											
		response = es.search(index="web-en",size=10000,body={	
		  "query": {
		    "bool": {
		      "must_not": {
		        "exists": {
		          "field": vector_type } } }  }
		})
		print(response)
		hits = response['hits']['hits']

		#If length of hits list is 0, then all documents were fetched	
		if(len(hits) == 0):							
			break

		data = []
		for hit in hits:
			doc_map = {}
			doc_id = hit['_id']
			text = hit['_source']['title']
			if(text == '' or len(text)>200):
				text = 'N/A'
			if(vector_type is LASER):	
				text_vector = laser.embed_sentences([text],lang='en')[0]  
			else:	
				text_vector = bc.encode([text])[0]		
				
			doc_map[doc_id] = text_vector
			data.append(doc_map)
			source_to_update = {"doc" : { vector_type : text_vector } }

			#Update the document in the database with the new vector values
			r = es.update(index="web-en",id=doc_id,body=source_to_update)  
			print(str(doc_id)+" "+ str(r))
		
#Main function
def main():
	error_msg = "**********************************\nInvalid script usage!\n \
	Usage : python vectorize.py <vector_type\nVector type : 'LASER' or 'BERT'\n \
	**********************************"
	if(len(sys.argv) < 2):
		sys.exit(error_msg)
	vector = str(sys.argv[1]).lower()
	if(not(vector == 'laser' or vector == 'bert')):
		sys.exit(error_msg)
	vector = "".join([vector,"_vector"])	
	doVectorize(vector)

if __name__== "__main__":
  main()
