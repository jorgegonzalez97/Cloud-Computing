#!/usr/bin/env python
# coding: utf-8

# ## Cassandra

# In[1]:


# Print dependencies
print("Dependencies required:")
print("> pip3 install cassandra-driver")


# In[9]:


print("\nRunning Cassandra script...")


# In[22]:


# Imports
from cassandra.cluster import Cluster
from datetime import datetime as dt

import string
import random


# In[25]:


class Timer():
    tStart = 0
    
    def start(self):
        self.tStart = dt.now()
        
    def timeTaken(self):
        return (dt.now() - self.tStart).total_seconds() * 1000


# In[21]:


# Functions
def rndStr(size):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=size))


# In[26]:


# Defines

# Cluster
HOST = "127.0.0.1"
PORT = 9042

# Database
DB_NAME = "cloudDB"
DB_TABLE = DB_NAME + "." + "testTbl"
REPL_FACTOR = 1
ROWS_QTY = 1000
KEY_LEN = 10
VAL_LEN = 90

# Timer
t = Timer()


# In[ ]:


# Cluster definition
cluster = Cluster([HOST], port=PORT)
session = cluster.connect()
print("Connected to cluster at {}:{}".format(HOST, PORT))


# In[ ]:


print("\nCreating database...")

session.execute("CREATE KEYSPACE IF NOT EXISTS " + DB_NAME + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : " + str(REPL_FACTOR) + "}")
session.execute("DROP TABLE IF EXISTS " + DB_TABLE)
session.execute("CREATE TABLE " + DB_TABLE + "(key text PRIMARY KEY, value text)")

print("Table {} created!".format(DB_TABLE))

