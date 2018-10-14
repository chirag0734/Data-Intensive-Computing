package course.dataintensive.tfidf;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>  {
	
	/*
	 * This method reads document by document in the input directory and tokenize each documents.
	 * The Output key here is in the format ---- <word> : <document name> : <total_words_in_document>
	 * The Output Value is 1.
	 * */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
		String docName = ((FileSplit) context.getInputSplit()).getPath().getName();

		StringTokenizer tokenizer = new StringTokenizer(value.toString(), " ()<>,./:");			
		
		int noOfWordsInDoc = tokenizer.countTokens();											// count number of tokens in the tokenizer beforehand 
		
		while(tokenizer.hasMoreTokens()){
			String word = tokenizer.nextToken();
			context.write(new Text(word.toLowerCase() + ":" + docName + ":" + noOfWordsInDoc), new IntWritable(1));
		}
    }
}
