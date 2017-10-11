# Tableau Extract from MongoDB
Creating a TDE using the Tableau SDK from a Mongo collection (through pyMongo)

To use this sample script to create a Tableau Data Extract from your MongoDB instance –

•	Import the collection 'primer-dataset.json' to your mongo instance, with the below command (without the < or >)-

    mongoimport --db <db-name> --collection <collection-name> --drop --file primer-dataset.json –host <host-name> --port <port-number>

More information at https://docs.mongodb.com/getting-started/shell/import-data/

•	Install pyMongo (https://api.mongodb.com/python/current/)

•	Setup the Tableau SDK (https://onlinehelp.tableau.com/current/api/sdk/en-us/help.htm)

•	Modify the vital details such as host name, port number, db name, collection name, etc.

•	Run the script
