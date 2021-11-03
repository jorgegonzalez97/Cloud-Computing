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
ROWS_QTY = 1250
KEY_LEN = 10
VAL_LEN = 90

# Timer
t = Timer()


# In[ ]:


# Cluster definition
cluster = Cluster([HOST], port=PORT)
session = cluster.connect()
print("Connected to cluster at {}:{}".format(HOST, PORT))


print("Generating random key-value pairs...")
kvPairs = []
for i in range(ROWS_QTY):
    kvPairs.append([rndStr(KEY_LEN), rndStr(VAL_LEN)])


# In[ ]:


print("\nInserting data...")

t.start()
for i in range(ROWS_QTY):
    session.execute("INSERT INTO " + DB_TABLE + " (key, value) VALUES (%s, %s)", kvPairs[i])
tTaken = t.timeTaken()
print("INSERT time taken: {} ms".format(tTaken))
print("INSERT time taken (per row): {} ms".format(tTaken / ROWS_QTY))


# In[ ]:


print("\nSelecting data...")

lookupStmt = session.prepare("SELECT * FROM " + DB_TABLE + " WHERE key = ?")

t.start()
for i in range(ROWS_QTY):
    session.execute(lookupStmt, [kvPairs[i][0]])
tTaken = t.timeTaken()
print("LOOKUP time taken: {} ms".format(tTaken))
print("LOOKUP time taken (per row): {} ms".format(tTaken / ROWS_QTY))


# In[ ]:


print("\nDeleting data...")

deleteStmt = session.prepare("DELETE FROM " + DB_TABLE + " WHERE key = ?")

t.start()
for i in range(ROWS_QTY):
    session.execute(deleteStmt, [kvPairs[i][0]])
tTaken = t.timeTaken()
print("DELETE time taken: {} ms".format(tTaken))
print("DELETE time taken (per row): {} ms".format(tTaken / ROWS_QTY))