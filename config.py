# -*- coding: utf-8 -*-
# @Author: Muhammad Alfin N
# @Date:   2022-08-09 11:51:41
# @Last Modified by:   aldomasendi
# @Last Modified time: 2022-08-09 11:52:02

def db_source():
	source = [
		{
			'name'	: 'sql_server1',
			'db_access' : {
			    	'address'	: '127.0.0.1',
			    	'username'	: 'user',
			    	'pass'	: 'userpassword',
			    	'db'		: 'yourdb',
	    		},
	    		'table'		: [
	    						'your_json_table.json'
	    					]
		}
	]
	return source