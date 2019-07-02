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
                Push the word into it
                Then if the length of queue is smaller than numberOfGram, continue
                Else, all words inside n-gram queue consist of the n-gram word
                record the n-gram word to context, and pop the first word in the queue, and do next loop
            */

            for (int i = 0; i < textWords.length; i ++) {
                String singleWord = textWords[i];
                ngramQueue.add(singleWord);			// Push the new word
                
                if (ngramQueue.size() < numberOfGram) {
                    continue;
                }
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
                ngramQueue.remove(0);						// Pop the first word in the queue       				
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
            result += Integer.toString(sum) + "  ";			// Concatenate sums

            Iterator<String> it = fileNames.iterator();		// Concatenate file names by treeset iterator
            while (it.hasNext()) {
                result += it.next();
            }
            result = result.trim();							// Remove trailing spaces
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
        
        // Check if command-line arguments length correct
        if (args.length != 4) {
            System.out.println("Make sure 4 command-line arguments are provided!");
            System.exit(1);
        }
        
        // Input by command-line arguments, exception handling
        try{
            numberOfGram = Integer.parseInt(args[0]);
            minSupport = Integer.parseInt(args[1]);
        }
        catch (NumberFormatException convertError){
            System.out.println("The first two command-line arguments malformed! Integers expected.");
            System.exit(1);
        }
        
        Path inputDir = new Path(args[2]);
        Path outputDir = new Path(args[3]); 

        FileInputFormat.addInputPath(job, inputDir);
        FileOutputFormat.setOutputPath(job, outputDir);

        // Wait for map-reduce job completed
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
