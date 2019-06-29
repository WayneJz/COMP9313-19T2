package assignment1;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class Assignment1 {
	private static int numberOfGram;
	private static int minSupport;
	private static HashMap<String, TreeSet<String>> WordInFiles = new HashMap<String, TreeSet<String>>();

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    	
	        String[] textWords = value.toString().split("\\s+");
	        ArrayList<String> ngramQueue = new ArrayList<String>();
	        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	        
	        for (int i = 0; i < textWords.length; i ++) {
	        	String singleWord = textWords[i];
	        	if (ngramQueue.size() < numberOfGram) {
	        		ngramQueue.add(singleWord);
	        	}
	        	else {
	        		String ngramWord = "";
	        		for (int j = 0; j < ngramQueue.size(); j ++) {
	        			if (j > 0) {
	        				ngramWord += " ";
	        			}
	        			ngramWord += ngramQueue.get(j);
	        		}
	        		if (WordInFiles.containsKey(ngramWord)) {
	        			if (! WordInFiles.get(ngramWord).contains(fileName)) {
		        			WordInFiles.get(ngramWord).add(fileName);
	        			}
	        		}
	        		else {
	        			TreeSet<String> fileVec = new TreeSet<String>();
	        			fileVec.add(fileName);
	        			WordInFiles.put(ngramWord, fileVec);
	        		}
	        		
	        		word.set(ngramWord);
	        		context.write(word, one);
	        		ngramQueue.remove(0);
	        		ngramQueue.add(singleWord);
	        	}
	        }
	    }
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, String> {
		public void reduce(Text Wordkey, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			String result = "";
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			if (sum < minSupport) {
				return;
			}
			result += Integer.toString(sum) + " ";
			TreeSet<String> fileNames = WordInFiles.get(Wordkey.toString());
			
			
			Iterator<String> it = fileNames.iterator();
			while (it.hasNext()) {
				result += it.next() + " ";
			}
			context.write(Wordkey, result);
		}
	}

    public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, "word count");
    	job.setJarByClass(Assignment1.class);
    	job.setMapperClass(TokenizerMapper.class);
    	job.setReducerClass(IntSumReducer.class);
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(IntWritable.class);
    	numberOfGram = Integer.parseInt(args[0]);
    	minSupport = Integer.parseInt(args[1]);
    	FileInputFormat.addInputPath(job, new Path(args[2]));
    	FileOutputFormat.setOutputPath(job, new Path(args[3]));
    	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
