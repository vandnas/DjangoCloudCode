from BaseHTTPServer import HTTPServer
from BaseHTTPServer import BaseHTTPRequestHandler
import cgi
import json
 
TODOS = [
    {'id': 1, 'title': 'learn python'},
    {'id': 2, 'title': 'get paid'},
]

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

 
class RestHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        #self.wfile.write(json.dumps({'data': TODOS}))
        #self.wfile.write(json.dumps({'data': GRAPH}))
        self.wfile.write(json.dumps(GRAPH))
        return
 
    def do_POST(self):
	print "IN POST"
        new_id = max(filter(lambda x: x['id'], TODOS))['id'] + 1
        form = cgi.FieldStorage(fp=self.rfile,
                           headers=self.headers, environ={
                                'REQUEST_METHOD':'POST', 
                                'CONTENT_TYPE':self.headers['Content-Type']
                           })
        new_title = form['title'].value
        new_todo = {'id': new_id, 'title': new_title}
        TODOS.append(new_todo)
 
        self.send_response(201)
        self.end_headers()
        self.wfile.write(json.dumps(new_todo))
        return

print("Content-Type: text/html")
print("")
print("Hi") 
#httpd = HTTPServer(('127.0.0.1', 8000), RestHTTPRequestHandler)
#while True:
#    httpd.handle_request()
