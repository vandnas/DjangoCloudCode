#!/usr/bin/python

import json
 
GRAPH = [
        ["Kiwi", 3],
        ["Mixed nuts", 1],
        ["Oranges", 6],
        ["Apples", 8],
        ["Pears", 4],
        ["Clementines", 4],
        ["Reddish (bag)", 1],
        ["Grapes (bunch)", 1]
]
 
print("Content-Type: text/html")
print("")
print(json.dumps(GRAPH).encode('utf-8')) 
