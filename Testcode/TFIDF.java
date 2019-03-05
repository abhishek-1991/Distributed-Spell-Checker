/*
* CSC548 - HW4 Question 1- Calculate TFIDF using the hadoop on set of document
*  Author: ajain28 Abhash Jain
*
*/
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

/*
 * Main class of the TFIDF MapReduce implementation.
 * Author: Tyler Stocksdale
 * Date:   10/18/2017
 */
public class TFIDF {

    public static void main(String[] args) throws Exception {
        // Check for correct usage
        if (args.length != 1) {
			System.err.println("Usage: TFIDF <input dir>");
            System.exit(1);
        }
		
		// Create configuration
		Configuration conf = new Configuration();
		
		// Input and output paths for each job
		Path inputPath = new Path(args[0]);
		Path wcInputPath = inputPath;
		Path wcOutputPath = new Path("output/WordCount");
		Path dsInputPath = wcOutputPath;
		Path dsOutputPath = new Path("output/DocSize");
		Path tfidfInputPath = dsOutputPath;
		Path tfidfOutputPath = new Path("output/TFIDF");
		
		// Get/set the number of documents (to be used in the TFIDF MapReduce job)
        FileSystem fs = inputPath.getFileSystem(conf);
        FileStatus[] stat = fs.listStatus(inputPath);
		String numDocs = String.valueOf(stat.length);
		conf.set("numDocs", numDocs);
		
		// Delete output paths if they exist
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(wcOutputPath))
			hdfs.delete(wcOutputPath, true);
		if (hdfs.exists(dsOutputPath))
			hdfs.delete(dsOutputPath, true);
		if (hdfs.exists(tfidfOutputPath))
			hdfs.delete(tfidfOutputPath, true);
		
		/************ YOUR CODE HERE ************/
		
		// Create and execute Word Count job
		Job job = Job.getInstance(conf,"word count");
		job.setJarByClass(TFIDF.class);
		//Set wordcount Mapper and reducer class
		job.setMapperClass(WCMapper.class);
		job.setReducerClass(WCReducer.class);
		//Set output key and value class
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job,wcInputPath);
		FileOutputFormat.setOutputPath(job,wcOutputPath);
		job.waitForCompletion(true);

		/************ YOUR CODE HERE ************/
		
		// Create and execute Document Size job
		Job dsJob = Job.getInstance(conf,"Document Size");
		dsJob.setJarByClass(TFIDF.class);
		//set mapper and reducer for Doc size 
		dsJob.setMapperClass(DSMapper.class);
		dsJob.setReducerClass(DSReducer.class);
		dsJob.setOutputKeyClass(Text.class);
		dsJob.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(dsJob,dsInputPath);
		FileOutputFormat.setOutputPath(dsJob,dsOutputPath);
		dsJob.waitForCompletion(true); //Execute the job
		//Create and execute TFIDF job
		
			/************ YOUR CODE HERE ************/
		
		Job tfidfJob = Job.getInstance(conf,"TFIDF");
		tfidfJob.setJarByClass(TFIDF.class);
		tfidfJob.setMapperClass(TFIDFMapper.class);
		//set mapper and reducer for TFIDF
		tfidfJob.setReducerClass(TFIDFReducer.class);
		tfidfJob.setOutputKeyClass(Text.class);
		tfidfJob.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(tfidfJob,tfidfInputPath);
		FileOutputFormat.setOutputPath(tfidfJob,tfidfOutputPath);
		tfidfJob.waitForCompletion(true);
		
    }
	
	/*
	 * Creates a (key,value) pair for every word in the document 
	 *
	 * Input:  ( byte offset , contents of one line )
	 * Output: ( (word@document) , 1 )
	 *
	 * word = an individual word in the document
	 * document = the filename of the document
	 */
	public static class WCMapper extends Mapper<Object, Text, Text, IntWritable> {
		
		/************ YOUR CODE HERE ************/
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//Split each word and mark its count to 1.
			StringTokenizer itr = new StringTokenizer(value.toString());
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken()+"@"+fileName);
				context.write(word,one);
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
	public static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		/************ YOUR CODE HERE ************/
	private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {	
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		result.set(sum);
		context.write(key,result);
	}
		
    }
	
	/*
	 * Rearranges the (key,value) pairs to have only the document as the key
	 *
	 * Input:  ( (word@document) , wordCount )
	 * Output: ( document , (word=wordCount) )
	 */
	public static class DSMapper extends Mapper<Object, Text, Text, Text> {
		private Text word = new Text();
		private Text val = new Text();
		//Mapper function for Document Size
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String inputString = value.toString();
			String []strArr = inputString.split(" |\\@|\\t");
			word.set(strArr[1]);
			val.set(strArr[0]+"="+strArr[2]);
			context.write(word,val);
		}
		/************ YOUR CODE HERE ************/
		
    }

    /*
	 * For each identical key (document), reduces the values (word=wordCount) into a sum (docSize) 
	 *
	 * Input:  ( document , (word=wordCount) )
	 * Output: ( (word@document) , (wordCount/docSize) )
	 *
	 * docSize = total number of words in the document
	 */
	public static class DSReducer extends Reducer<Text, Text, Text, Text> {
		
		/************ YOUR CODE HERE ************/
		//Reducer function for Document size
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			int sum = 0;
			//Creating an array list to string in a list from iterable
			ArrayList<String> alist = new ArrayList<String>();
			for(Text t : values){
				alist.add(t.toString());
				String str = t.toString();
				//split the values
				String s[] = str.split("=");
				sum+=Integer.parseInt(s[1]);
			}
			for(String str : alist){
				//parse the Intermediate result and add it to the context in the required format for Document size
				Text newKey = new Text();
				Text newValue = new Text();
				String s[] = str.split("=");
				newKey.set(s[0]+"@"+key.toString());
				newValue.set(s[1] + "/" + sum);
				context.write(newKey,newValue);
			}
		}
    }
	
	/*
	 * Rearranges the (key,value) pairs to have only the word as the key
	 * 
	 * Input:  ( (word@document) , (wordCount/docSize) )
	 * Output: ( word , (document=wordCount/docSize) )
	 */
	public static class TFIDFMapper extends Mapper<Object, Text, Text, Text> {

		/************ YOUR CODE HERE ************/
		private Text word = new Text();
		private Text val = new Text();
		//Mapper function for TFID 
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String valueStr = value.toString();
			//split the value based on various delimeter
			String str[] = valueStr.split(" |\\@|\\t|\\/");
			word.set(str[0]);
			val.set(str[1]+"="+str[2] + "/" + str[3]);
			context.write(word,val);
		}
    }

    /*
	 * For each identical key (word), reduces the values (document=wordCount/docSize) into a 
	 * the final TFIDF value (TFIDF). Along the way, calculates the total number of documents and 
	 * the number of documents that contain the word.
	 * 
	 * Input:  ( word , (document=wordCount/docSize) )
	 * Output: ( (document@word) , TFIDF )
	 *
	 * numDocs = total number of documents
	 * numDocsWithWord = number of documents containing word
	 * TFIDF = (wordCount/docSize) * ln(numDocs/numDocsWithWord)
	 *
	 * Note: The output (key,value) pairs are sorted using TreeMap ONLY for grading purposes. For
	 *       extremely large datasets, having a for loop iterate through all the (key,value) pairs 
	 *       is highly inefficient!
	 */
	public static class TFIDFReducer extends Reducer<Text, Text, Text, Text> {
		
		private static int numDocs;
		private Map<Text, Text> tfidfMap = new HashMap<>();
		
		// gets the numDocs value and stores it
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			numDocs = Integer.parseInt(conf.get("numDocs"));
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			/************ YOUR CODE HERE ************/
	 
			ArrayList<String> valuelist = new ArrayList<String>();
			int numDocsWithWord=0;
			//Count the number of docs and create a copy of value of numDocswithwords
			for(Text t : values){
				numDocsWithWord++;
				valuelist.add(t.toString());
			}
			for (String s: valuelist){
				Text newKey = new Text();
				Text newValue = new Text();
				String valueArr[] = s.split("=|\\/");
				newKey.set(valueArr[0]+"@"+key.toString());
				//calculate the TFID
				double tfidf = (Double.parseDouble(valueArr[1])/Double.parseDouble(valueArr[2])) * Math.log((double)numDocs/numDocsWithWord);
				newValue.set(Double.toString(tfidf));
				//Put the output (key,value) pair into the tfidfMap instead of doing a context.write
				tfidfMap.put(newKey,newValue);
			}
			//tfidfMap.put(/*document@word*/, /*TFIDF*/);
		}
		
		// sorts the output (key,value) pairs that are contained in the tfidfMap
		protected void cleanup(Context context) throws IOException, InterruptedException {
            Map<Text, Text> sortedMap = new TreeMap<Text, Text>(tfidfMap);
			for (Text key : sortedMap.keySet()) {
                context.write(key, sortedMap.get(key));
            }
        }
		
    }
}
