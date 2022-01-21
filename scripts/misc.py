import os
from arcana.data.stores.xnat.cs import XnatViaCS

os.environ['XNAT_HOST'] = 'http://localhost:8080'
os.environ['XNAT_PASS'] = 'admin'
os.environ['XNAT_USER'] = 'admin'

store = XnatViaCS(cache_dir='/Users/tclose/Desktop/test-cache')

print(store.server)