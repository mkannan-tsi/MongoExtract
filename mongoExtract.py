#####################################################
from pymongo import MongoClient
from tableausdk import *
from tableausdk.Extract import *
from dateutil.parser import parse
from datetime import date, datetime
import pandas as pd
from pandas.io.json import json_normalize
import re
###Defining a few vitals###
db_name = 'test'
collection_name = 'restaurants'
table_name = 'Extract'
extract_name = 'Mongo Collection 2.tde'
host = 'localhost'
port = 27017

###Creating a connection the the specific collection###
client = MongoClient(host, port)
db = client [db_name]
collection = db[collection_name]
column_headers = []
column_types = []

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
for i in collection.find (limit=1000):
	nested_list_temp = []
	nested_dict_temp = []
	nested_dict = []
	nested_list = []
	flattened_columns = []

	a = flatten_json (i)
	df = json_normalize (a)

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
			s = ""
			o = ""
			start = 0
			for i in a:
				pos_start = i[0] + 1
				pos_end = i[1] + 1
				s = s + j[start:pos_start]
				o = o + j[pos_start-1:pos_end-1]
				start = pos_end
			if re.search (".\d+$", j):
				s = s[:-1]
			else:
				s = s + j[pos_end:len(j)]
			s = s + o
			columns[j] = s
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
	##Dealing with nested lists###
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
		if j not in column_headers:
			column_headers.append (j)
			column_types.append (type (df.loc [0,j]))
			
print master
print len(master)
print column_headers
print column_types
