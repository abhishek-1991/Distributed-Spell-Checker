import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.util.StringTokenizer;
import java.io.IOException;
import java.util.*;
import java.io.File;
import java.util.HashSet;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Set;

public class SpellChecker {

    public static void main(String[] args) throws Exception {
        // Check for correct usage
        if (args.length != 1) {
			System.err.println("Usage: SpellChecker <input dir>");
            System.exit(1);
        }
		
		// Create configuration
		Configuration conf = new Configuration();
		
		// Input and output paths for each job
		Path inputPath = new Path(args[0]);
		Path wcInputPath = inputPath;
		Path wcOutputPath = new Path("output/WrongCount");
		
		// Get/set the number of documents (to be used in the SpellChecker MapReduce job)
        FileSystem fs = inputPath.getFileSystem(conf);
        FileStatus[] stat = fs.listStatus(inputPath);
		String numDocs = String.valueOf(stat.length);
		conf.set("numDocs", numDocs);
		
		// Delete output paths if they exist
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(wcOutputPath))
			hdfs.delete(wcOutputPath, true);
		
		// Create and execute spell check  job
		System.out.println("Working with task");
		Job job = Job.getInstance(conf,"spell check");
		job.setJarByClass(SpellChecker.class);
		//Set spell check Mapper and reducer class
		job.setMapperClass(WCMapper.class);
		job.setReducerClass(WCReducer.class);
		//Set output key and value class
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job,wcInputPath);
		FileOutputFormat.setOutputPath(job,wcOutputPath);
		job.waitForCompletion(true);
		System.out.println("Task Completed!");
    }
	
	/*
	 * Creates a (key,value) pair for every word in the document 
	 *
	 *
	 * word = an individual word in the document
	 * document = the filename of the document
	 */
	public static class WCMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		System.out.println("Entered map function");
		 String docText = value.toString().replaceAll("[^a-zA-Z ]", "");
  	    
		String[] words = docText.split(" ");
		String doc_name = ((FileSplit) context.getInputSplit()).getPath().getName();
		
		for (String word : words){
        	context.write(new Text(doc_name), new Text(word));
		}
	}
    }

    /*
	 * For each identical key (word@document), reduces the values (1) into a sum (wordCount)
	 *
	 * Input:  ( (word@document) , 1 )
	 * Output: ( (word@document) , wordCount )
	 *
	 * wordCount = number of times word appears in document
	 */
	public static class WCReducer extends Reducer<Text, Text, Text, Text> {
	
		Set<String> dictonary = new HashSet<String>();
		
		protected void setup(Context context) throws IOException, InterruptedException {
			System.out.println("Setup function");
			Path pt = new Path("hdfs://node-master:9000/user/ajain28/dictonary/d1.txt");
			FileSystem fs = FileSystem.get(context.getConfiguration());
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line = br.readLine();
			while (line!=null) {
				dictonary.add(line.toLowerCase());
				line = br.readLine();
			}
			dictonary.add(" ");
		}

    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {	
		System.out.println( "Dictonary size " + dictonary.size());
		System.out.println("Reducer called");
		for(Text t : values){
			System.out.println("t value is: " + t.toString());
			if(!dictonary.contains(t.toString().toLowerCase())){
				if(!t.equals(" "))
					context.write(key,t);
			}
		}
    }
}
}
