package assignment1;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class Assignment1 {
    private static int numberOfGram;	// Argument 0, number of gram
    private static int minSupport;		// Argument 1, the minimum count of n-gram

    public static class nGramMapper extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {   	
            String[] textWords = value.toString().split("\\s+");	// Tokenize the text file into string array
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();	// Get the file name
            ArrayList<String> ngramQueue = new ArrayList<String>();		// Initialize nGram queue
            
            /*
                n-gram queue:
                If the length of queue is smaller than numberOfGram, then push word into it
                Else, all words inside n-gram queue consist of the n-gram word
                record the n-gram word to context, and pop the first word in the queue, then push the new word
                and do next loop
            */

            for (int i = 0; i < textWords.length; i ++) {
                String singleWord = textWords[i];
                if (ngramQueue.size() < numberOfGram) {
                    ngramQueue.add(singleWord);
                }
                else {
                    String ngramWord = "";	// N-gram word to be recorded
                    for (int j = 0; j < ngramQueue.size(); j ++) {
                        if (j > 0) {
                            ngramWord += " ";
                        }
                        ngramWord += ngramQueue.get(j);
                    }
                    Text mapperValue = new Text(fileName + " ");
                    Text mapperKey = new Text(ngramWord);
                    context.write(mapperKey, mapperValue);		// Pass key and value into mapper
                    ngramQueue.remove(0);					// Pop the first word in the queue
                    ngramQueue.add(singleWord);				// Push the new word
                }
            }
        }
    }

    public static class TextReducer extends Reducer<Text, Text, Text, String> {
        public void reduce(Text Wordkey, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String result = "";		// The output value string
            int sum = 0;			// The sum of n-gram word
            TreeSet<String> fileNames = new TreeSet<String>();		// Handling file names in order, avoid duplicates
            
            // Iterate values, sum the count and add the file names
            for (Text val : values) {
                sum += 1;
                fileNames.add(val.toString());
            }

            // If count smaller than minimum count then return
            if (sum < minSupport) {
                return;
            }
            result += Integer.toString(sum) + " ";			// Concatenate sums

            Iterator<String> it = fileNames.iterator();		// Concatenate file names by treeset iterator
            while (it.hasNext()) {
                result += it.next() + " ";
            }
            context.write(Wordkey, result);					// Write the result
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        
        // Definition of Map-reduce classes
        job.setJarByClass(Assignment1.class);
        job.setMapperClass(nGramMapper.class);
        job.setReducerClass(TextReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        // Input by command-line arguments, exception handling
        try{
            numberOfGram = Integer.parseInt(args[0]);
            minSupport = Integer.parseInt(args[1]);
            FileInputFormat.addInputPath(job, new Path(args[2]));
            FileOutputFormat.setOutputPath(job, new Path(args[3]));
        }
        catch (NumberFormatException convertError){
            System.out.println("Command-line Arguments malformed!");
            System.exit(1);
        }

        // Wait for map-reduce job completed
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
