#####################################################
from pymongo import MongoClient
from tableausdk import *
from tableausdk.Extract import *
from dateutil.parser import parse
from datetime import date, datetime
import pandas as pd
from pandas.io.json import json_normalize
import re
import numpy

###Defining a few vitals###
db_name = 'test'
collection_name = 'achievements'
table_name = 'Extract'
extract_name = 'Mongo Collection.tde'
host = 'localhost'
port = 27017

###Creating a connection the the specific collection###
client = MongoClient(host, port)
db = client [db_name]
collection = db[collection_name]
column_headers = []
column_types = []
column_headers_types = []

def flatten_json(y):
    out = {}

    def flatten(x, name='', origin=''):
        if type(x) is dict:
        	for a in x:
        		if type (x[a]) is dict and origin == 'list':
        			nested_dict_temp.append (name + a + '.')
        		flatten(x[a], name + a + '.', origin)
        elif type(x) is list:
        	i = 0
        	for a in x:
        		if type(a) is dict:
        			nested_dict_temp.append (name)
        		else:
        			nested_list_temp.append (name)
        		flatten(a, name + str(i) + '.', 'list')
        		i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

def manipulate (j):
	a = [(m.start(0), m.end(0)) for m in re.finditer(".\d+.", j)]
	if a:
		start = a[-1][0] + 1
		end = a[-1][1]
		s = j[:start] + j[end:] + j[start:end-1]
		return (s[:((start+1)-end)])
	else:
		return (j)

master = []
try:
	for i in collection.find (limit=10):
		nested_list_temp = []
		nested_dict_temp = []
		nested_dict = []
		nested_list = []
		flattened_columns = []
		try:
			json = flatten_json (i)
			df = json_normalize (json)
		except:
			pass

		###Manipulating nested dicts###
		for j in nested_dict_temp:
			nested_dict.append (manipulate(j))
		nested_dict = list (set (nested_dict))

		###Manipulating nested lists###
		for j in nested_list_temp:
			nested_list.append (manipulate (j))
		nested_list = list (set (nested_list))

		###Manipulating column headers to match###
		columns = {}
		for j in df.columns.values:
			a = [(m.start(0), m.end(0)) for m in re.finditer(".\d+", j)]
			if a:
				words = ""
				digits = ""
				start = 0
				for i in a:
					pos_start = i[0] + 1
					pos_end = i[1] + 1
					words = words + j[start:pos_start]
					digits = digits + j[pos_start-1:pos_end-1]
					start = pos_end
				if re.search (".\d+$", j):
					words = words[:-1]
				else:
					words = words + j[pos_end:len(j)]
				title = words + digits
				columns[j] = title
		df = df.rename(columns=columns)

		###Determining the columns that are flat post manipulation###
		for j in df.columns.values:
			if re.search (".\d+", j) is None:
				flattened_columns.append (j)	

		###Dealing with nested dicts###
		if nested_dict:
			stubnames = []
			for j in nested_dict:
				for k in df.columns.values:
					if re.search ((j+"\w+.\d+$"), k) is not None:
						a = [(m.start(0), m.end(0)) for m in re.finditer(".\d+", k)]
						start = a[-1][0] + 1
						end = a[-1][1]
						stubnames.append (k[:(start-end)])
						
			stubnames = list(set(stubnames))
			if stubnames:
				df = pd.wide_to_long (df, stubnames=stubnames,  i=flattened_columns, j='key')
				df = df.reset_index()
				df = df.drop('key', axis=1)
				for k in stubnames:
					flattened_columns.append (k)

		checker = ""
		list_key = ""
		###Dealing with nested lists###
		if nested_list:
			for j in range (len(nested_list)):
				checker = checker + '\d+.'
				list_key = list_key + nested_list[j]
			list_key = list_key + 'key'
			checker = checker [:-1]

			df = pd.wide_to_long (df, stubnames=nested_list, i=flattened_columns, j=list_key)
			df = df.reset_index()
			for j in nested_list:
				flattened_columns.append (j)
				flattened_columns.append (list_key)
		

		###Consolidating into master list, and removing duplicates in case of nested lists###
		for j in range(0, len(df)):
			flag = 0
			if nested_list:
				if df[list_key][j]:
					if re.search (checker, df[list_key][j]) is None:
						flag = 1

			if flag == 0:
				series1=df.iloc[j,:]
				master.append (series1.to_dict())	

		###Creating column headers###
		for j in df.columns.values:
			if j not in column_headers and j != "_id":
				column_headers.append (j)
				column_types.append (type (df.loc [0,j]))
except:
	pass
			
###Setting Tableau recognized data types###
for i in column_types:
	if i is numpy.int64:
		column_headers_types.append (Type.INTEGER)
	elif i is numpy.float64:
		column_headers_types.append (Type.DOUBLE)
	elif i is pd.Timestamp:
		column_headers_types.append (Type.DATETIME)	
	else:
		column_headers_types.append (Type.UNICODE_STRING)

###Initializng the Extract API, and applying the schema to the table###
ExtractAPI.initialize()
dataExtract = Extract(extract_name)
dataSchema = TableDefinition()
for i in range(0, (len(column_headers))):
	dataSchema.addColumn (column_headers[i], column_headers_types[i])
table = dataExtract.addTable(table_name, dataSchema)

###Adding data to the extract###
newRow = Row(dataSchema)
for i in master:
	try:
		for j in range(len(column_headers)):
			if column_headers_types[j] == 7:
				try:
					newRow.setInteger (j, i.get(column_headers[j]))
				except:
					newRow.setNull (j)
			elif column_headers_types[j] == 13:
				try:
					newRow.setDateTime (j, i.get(column_headers[j]).year, i.get(column_headers[j]).month, i.get(column_headers[j]).day, i.get(column_headers[j]).hour, i.get(column_headers[j]).minute, i.get(column_headers[j]).second, 0)
				except:
					newRow.setNull (j)
			elif column_headers_types[j] == 10:
				try:
					newRow.setDouble (j, i.get(column_headers[j]))
				except:
					newRow.setNull (j)
			else:
				try:
					newRow.setString (j, i.get(column_headers[j]))
				except:
					newRow.setNull (j)
		table.insert(newRow)
	except:
		pass

###Final Procedures###
client.close()
dataExtract.close()
ExtractAPI.cleanup()	
#####################################################
