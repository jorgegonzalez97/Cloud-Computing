#!/usr/bin/env python
# coding: utf-8

# ## MongoDB

# In[2]:


# Print dependencies
print("Dependencies required:")
print("> pip3 install pymongo")


# In[10]:


print("\nRunning MongoDB script...")


# In[12]:


# Imports
from datetime import datetime as dt

import pymongo
import string
import random


# In[13]:


class Timer():
    tStart = 0
    
    def start(self):
        self.tStart = dt.now()
        
    def timeTaken(self):
        return (dt.now() - self.tStart).total_seconds() * 1000


# In[14]:


# Functions
def rndStr(size):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=size))


# In[7]:


# Defines

# Database
HOST = "127.0.0.1"
PORT = 27017
DB_NAME = "cloudDB"
DB_TABLE = "testTbl"
ROWS_QTY = 1000
KEY_LEN = 10
VAL_LEN = 90


# In[ ]:

# Timer
t = Timer()

# Initiate connection
mClient = pymongo.MongoClient("mongodb://" + HOST + ":" + str(PORT) + "/")
mDB = mClient[DB_NAME]
mCOL = mDB[DB_TABLE]


# In[ ]:


print("Generating random key-value pairs...")
kvPairs = []
for i in range(ROWS_QTY):
    kvPairs.append([rndStr(KEY_LEN), rndStr(VAL_LEN)])


# In[ ]:


print("\nInserting data...")

t.start()
for i in range(ROWS_QTY):
    mCOL.insert_one({"key": kvPairs[i][0], "value": kvPairs[i][1]})
tTaken = t.timeTaken()
print("INSERT time taken: {} ms".format(tTaken))
print("INSERT time taken (per row): {} ms".format(tTaken / ROWS_QTY))


# In[ ]:


print("\nSelecting data...")

t.start()
for i in range(ROWS_QTY):
    mCOL.find({"key": kvPairs[i][0]})
tTaken = t.timeTaken()
print("LOOKUP time taken: {} ms".format(tTaken))
print("LOOKUP time taken (per row): {} ms".format(tTaken / ROWS_QTY))


# In[ ]:


print("\nDeleting data...")

t.start()
for i in range(ROWS_QTY):
    mCOL.delete_one({"key": kvPairs[i][0]})
tTaken = t.timeTaken()
print("DELETE time taken: {} ms".format(tTaken))
print("DELETE time taken (per row): {} ms".format(tTaken / ROWS_QTY))

