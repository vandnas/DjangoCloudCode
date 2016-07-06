#!/usr/bin/python

import json
from collections import OrderedDict


GRAPH=[{
            'name': 'John',
            'data': [5, 3, 4, 7, 2]
        },
 	{
            'name': 'Joe',
            'data': [3, 4, 4, 2, 5]
        }, 
	{
            'name': 'Jane',
            'data': [2, 5, 6, 2, 1]
        } 
      ]


def _byteify(data, ignore_dicts = False):
    # if this is a unicode string, return its string representation
    if isinstance(data, unicode):
	print "in unicode"
        return data.encode('utf-8')
    # if this is a list of values, return list of byteified values
    if isinstance(data, list):
	print "in list"
        return [ _byteify(item, ignore_dicts=True) for item in data ]
    # if this is a dictionary, return dictionary of byteified keys and values
    # but only if we haven't already byteified it
    if isinstance(data, dict) and not ignore_dicts:
	print "in dict"
        return {
            _byteify(key, ignore_dicts=True): _byteify(value, ignore_dicts=True)
            for key, value in data.iteritems()
        }
    # if it's anything else, return it in its original form
    return data

print("Content-Type: text/html")
print("")

print(json.dumps(GRAPH).encode('utf-8'))

#data = _byteify(GRAPH)
#print "data",data


