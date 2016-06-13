import json

print "hello"

data=[
        ["Kiwi", 3],
        ["Mixed nuts", 1],
        ["Oranges", 6],
        ["Apples", 8],
        ["Pears", 4],
        ["Clementines", 4],
        ["Reddish (bag)", 1],
        ["Grapes (bunch)", 1]
]


encoded_str = json.loads( data )
print "encoded_str",encoded_str
