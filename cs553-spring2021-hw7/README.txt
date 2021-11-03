[HOW TO] Run benchmark tests for Cassandra and MongoDB performance

------------------------------------------------------------------

 Required:
	- pip3: sudo apt install python3-pip
	- pssh: pip3 install pssh
	- pymongo: pip3 install pymongo
	- cassandra-driver: pip3 install cassandra-driver

 To consider:
	- pssh_hosts_files: Contains the IP addresses of all the VMs

------------------------------------------------------------------

[CASSANDRA]

· First run cassandraDB_create.py to set up the database:

# File: cassandraDB_create.py
# Behaviour: Execution will take place on all nodes simultaneously
> parallel-ssh -i -h ~/.pssh_hosts_files python3 cassandraDB_create.py

· Then run cassandraDB to execute the experiments (number of rows and length of the key/values
can be configured inside the code)

# File: cassandraDB.py
# Behaviour: Execution will take place on all nodes simultaneously
> parallel-ssh -i -h ~/.pssh_hosts_files python3 cassandraDB.py

-------------------------------------------------------------------------

[MONGODB]

· Unlike Cassandra, MongoDB does not require a Database or Table creation prior to the benchmark.
Therefore, just a single Python script is required to run the tests

# File: mongoDB.py
# Behaviour: Execution will take place on all nodes simultaneously
> parallel-ssh -i -h ~/.pssh_hosts_files python3 mongoDB.py