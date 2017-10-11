#####################################################
from pymongo import MongoClient
from tableausdk import *
from tableausdk.Extract import *
from dateutil.parser import parse

###Defining a few vitals###
db_name = 'test'
collection_name = 'restaurants'
table_name = 'Extract'
extract_name = 'Mongo Collection.tde'

###Creating a connection the the specific collection###
client = MongoClient()
db = client [db_name]
collection = db[collection_name]

###Setting the extract schema (this can be retrieved from a file as well)###
column_count = 11
column_headers = ['Cuisine', 'Borough', 'Restaurant Name', 'Restaurant ID', 'Date of Inspection', 'Grade', 'Score', 'Address', 'Zipcode', 'Latitude', 'Longitude']
column_types = [Type.UNICODE_STRING, Type.UNICODE_STRING, Type.UNICODE_STRING, Type.UNICODE_STRING, Type.DATE, Type.UNICODE_STRING, Type.DOUBLE, Type.UNICODE_STRING, Type.INTEGER, Type.DOUBLE, Type.DOUBLE]

###Initializng the Extract API, and applying the schema to the table###
ExtractAPI.initialize()
dataExtract = Extract(extract_name)
dataSchema = TableDefinition()
for i in range(0, (column_count)):
	dataSchema.addColumn (column_headers[i], column_types[i])
table = dataExtract.addTable(table_name, dataSchema)

###Adding data to the table###
newRow = Row(dataSchema)
for i in collection.find():
	for j in i.get ('grades'):
		try:
			newRow.setString (0, i.get('cuisine')) 
			newRow.setString (1, i.get('borough'))
			newRow.setString (2, i.get('name'))
			newRow.setString (3, i.get('restaurant_id'))
			newRow.setDate (4, parse(str(j.get('date'))).year, parse(str(j.get('date'))).month, parse(str(j.get('date'))).day)
			newRow.setString (5, j.get('grade'))
			newRow.setDouble (6, j.get('score'))
			newRow.setString (7, i.get ('address', [{}]).get('building') + ", " + i.get ('address', [{}]).get('street'))
			newRow.setInteger (8, int(i.get('address', [{}]).get('zipcode')))
			newRow.setDouble (9, i.get ('address', [{}]).get('coord')[1])
			newRow.setDouble (10, i.get ('address', [{}]).get('coord')[0])
			table.insert(newRow)
		except:
			pass

###Final Procedures###
dataExtract.close()
ExtractAPI.cleanup()	
#####################################################
