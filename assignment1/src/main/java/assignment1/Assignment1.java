package assignment1;

import java.io.*;
import java.util.*;
import org.apache.hadoop.conf Configuration;




public class Assignment1 {
	/*

	private HashMap<String, TreeSet<String>> wordInFiles = new HashMap<String, TreeSet<String>>();
	private HashMap<String, Integer> wordCount = new HashMap<String, Integer>();

	private void fileReader (String fileName, int nGram) {
		try {
			String fileLine = "";
			String content = "";
			InputStream input = new FileInputStream(fileName);
			BufferedReader reader = new BufferedReader(new InputStreamReader(input));
			fileLine = reader.readLine();

			while (fileLine != null) {
				content = content.concat(fileLine);
				fileLine = reader.readLine();
			}
			tokenizer(content, fileName, nGram);
		}
		catch (IOException error) {
			System.out.println("Failed to open file." + fileName);
		}
	}

	private void tokenizer (String fileContent, String fileName, int nGram) {
		String[] tokens = fileContent.split("\\s+");
		LinkedList<String> nGramQueue = new LinkedList<String>();

		for (String s : tokens){
			if (nGramQueue.size() == nGram){
				String gramStr = "";
				for (String gramSubStr : nGramQueue){
					gramStr = gramStr.concat(gramSubStr);
				}

				if (wordInFiles.containsKey(gramStr)){
					wordInFiles.get(gramStr).add(fileName);
				}
				else{
					TreeSet<String> fileNameSet = new TreeSet<String>();
					fileNameSet.add(fileName);
					wordInFiles.put(gramStr, fileNameSet);
				}

				if (wordCount.containsKey(gramStr)){
					int prevCount = wordCount.get(gramStr);
					wordCount.put(gramStr, prevCount + 1);
				}
				else{
					wordCount.put(gramStr, 0);
				}

				nGramQueue.removeFirst();
				nGramQueue.addLast(s);
			}
			else{
				nGramQueue.addLast(s);
			}
		}
	}

	public void getResult(){

	}

	*/


	public static void main (String[] arguments) {
//		Assignment1 ngram = new Assignment1();
//		ngram.fileReader("input/file01.txt", 2);


		System.out.println("wocao");
	}
}