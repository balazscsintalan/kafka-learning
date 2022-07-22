Setup OpenSearch with the included docker-compose file.  

OpenSearch quick start:
https://opensearch.org/docs/latest/#docker-quickstart
  
Console: http://localhost:5601/app/dev_tools#/console  

Create index: 
```
PUT /my-first-index
```  

Add data:
```
PUT /my-first-index/_doc/1 
{"Description": "To be or not to be, that is the question."}
```

Retrieve data:  
```
GET /my-first-index/_doc/1
```

Delete data:
```
DELETE /my-first-index/_doc/1
```

Delete index:  
```
DELETE /my-first-index
```