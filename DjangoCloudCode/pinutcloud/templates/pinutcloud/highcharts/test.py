import requests

r = requests.post('http://localhost/mango', data = {'mango':'2'})
print r.text
~              
