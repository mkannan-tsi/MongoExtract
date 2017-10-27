# Tableau Extract from MongoDB
Creating a TDE using the Tableau SDK from a Mongo collection (through pyMongo)
<br />
To use this tool, point it to a collection in your MongoDB instance. It should automatically pick up columns nested elements such as lists and dictionaries, and flatten them out to create a Tableau Data Extract that can be used for analysis on Tableau.
<br /><br />
Prerequisites -

•	Install pyMongo (https://api.mongodb.com/python/current/)

•   Install the following ackages - pandas, numpy

•	Setup the Tableau SDK for Python (https://onlinehelp.tableau.com/current/api/sdk/en-us/help.htm)
<br /><br />

In case of more than one list, values will be duplicated and an index is provided for traversing through the resulting matrix.
<br /><br />

To try a sample -

•	Import the collection 'primer-dataset.json' to your mongo instance, with the below command (without the < or >)-

    mongoimport --db <db-name> --collection <collection-name> --drop --file primer-dataset.json –host <host-name> --port <port-number>

More information at https://docs.mongodb.com/getting-started/shell/import-data/

•	Modify the vital details such as host name, port number, db name, collection name, etc.

•	Run the script
<br /><br />

Acknowledgement -

Flattening JSON logic taken from https://medium.com/towards-data-science/flattening-json-objects-in-python-f5343c794b10
