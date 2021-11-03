import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/* 	Very interesting: https://www.geeksforgeeks.org/map-reduce-in-hadoop/
 * 	
 * 	Hadoop MapReduce explanation: Data once stored in the HDFS also needs to be processed upon. Now suppose a query is sent to process a data set in the HDFS. 
	Now, Hadoop identifies where this data is stored, this is called Mapping. Now the query is broken into multiple parts and the results of all these multiple parts are combined 
	and the overall result is sent back to the user. 
	This is called reduce process. Thus while HDFS is used to store the data, Map Reduce is used to process the data.
*/

public class HadoopSort {

	// The Hadoop mapper processes all input records from a file and genereates the output in a valid format for the Reducer class.
	// The output are key-value pairs (input gets converted).
	// In summary, it splits the raw input data into multiple Mappers splitter into different clusters, which will be concatenated later by the Reducer into a single output.
	
	
	// Class MyMappper extends Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    public static class HadoopMapper extends Mapper < Object, Text, Text, NullWritable > {


        Text tLine = new Text();

        // Map function must always be implemented in the Mapper
        /*
         * In this case, (byte offset, entire line):
         *  
         *  
         *  
         *  Hello I am GeeksforGeeks 
         *  How can I help you
         *  
         *  (key, 		value)
         *  (0, 		Hello I am geeksforgeeks)
         *  (26, 		How can I help you)
         *  
         *  The mapper will run once for each of these pairs !!!
         *  
        */
        public void map(Object key, Text value, Context context) throws IOException,
        InterruptedException {
        	
            tLine.set(value.toString() + "\r");
            context.write(tLine, NullWritable.get());
        }
        
        
        /*
         * Once the mapper has finished with its chunk, it will apply Shuffling and Sorting.
         * 
         * Shuffling Phase: This phase combines all values associated to an identical key.
         * Sorting Phase: Once shuffling is done, the output is sent to the sorting phase where all the (key, value) pairs are sorted automatically.
         * 
         * */

    }

    // The Hadoop Reducer concatenates all mappers key-value pairs into one single output.
    // public class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    public static class HadoopReducer extends Reducer < Text, NullWritable, Text, NullWritable > {

    	private NullWritable result = NullWritable.get();
    	
    	// Reduce function must always be implemented in the Reducer
        public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException,
        InterruptedException {


            context.write(key, result);

        }

    }


    public static void main(String[] args) throws Exception {

    	// Creates a new configuration resource in XML format (name/value pair)
        Configuration config = new Configuration();
        
        // Start measuring time
        long start = System.currentTimeMillis();

        // Creates a new Job with the name "Sort"
        Job job = Job.getInstance(config, "Sort");
        job.setJarByClass(HadoopSort.class);


        // Mapper for the job
        job.setMapperClass(HadoopMapper.class);

        // Reducer for the job
        job.setReducerClass(HadoopReducer.class);


        // Output key and value pairs will be of Text type
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        // Input and Output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);
        
        // Measure time
        long duration = System.currentTimeMillis() - start;
        System.out.println("\r\n >>>>>>>>>>>>>>> Compute time is " + duration + " ms <<<<<<<<<<<<<<<<<<<\r\n");
    }

}