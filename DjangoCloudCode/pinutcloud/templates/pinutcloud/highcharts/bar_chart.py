#!/usr/bin/python

import json

GRAPH = [
            ['John', [5, 3, 4, 7, 2]],
            ['Jane' , [2, 2, 3, 2, 1]],
            ['Joe' , [3, 4, 4, 2, 5]]
        ]
print("Content-Type: text/html")
print("")
print(json.dumps(GRAPH).encode('utf-8'))


