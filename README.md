# Tableau Extract from MongoDB
Create a TDE using the Tableau SDK from a Mongo collection (through pyMongo)
<br />
To use this tool, point it to a collection in your MongoDB instance. It should automatically pick up columns and nested elements such as lists and dictionaries, and flatten them out to create a Tableau Data Extract that can be used for analysis on Tableau.
<br /><br />
Prerequisites -

•	Install pyMongo (https://api.mongodb.com/python/current/)

•   Install the following packages - pandas, numpy

•	Setup the Tableau SDK for Python (https://onlinehelp.tableau.com/current/api/sdk/en-us/help.htm)
<br /><br />

In case of more than one list, rows will be duplicated and an index is provided for traversing through the resulting matrix.
<br /><br />

To try a sample -

•	Import the collection 'primer-dataset.json' to your mongo instance, with the below command (without the < or >)-

    mongoimport --db <db-name> --collection <collection-name> --drop --file primer-dataset.json –host <host-name> --port <port-number>

More information at https://docs.mongodb.com/getting-started/shell/import-data/

•	Modify the vital details such as host name, port number, db name, collection name, etc.

•	Run the script
<br /><br />

Notes - 
•	In case of incremental extract, the filter can be added when querying the database using collection.find (line 65), for which syntax       can be found here (http://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.find).
<br /><br />

Acknowledgement -

Flattening JSON logic taken from https://medium.com/towards-data-science/flattening-json-objects-in-python-f5343c794b10
