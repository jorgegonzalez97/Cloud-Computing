To run the code it is required the following commands:

First, to compile the script:


javac MySort.java


Then, to run it:


java [-Xmx8g] MySort <unsorted_file> <sorted_output> <log_path> <num_threads>


<unsorted_file>: path to the file that wans to be created

<sorted_output>: path of the file  once sorted

<log_path>: path of the log generated with the time

<num_threads>: Number of total threads deployed.



An example would be:


java -Xmx8g MySort file_4GB file_4GB_sorted MySort4GB.log 8