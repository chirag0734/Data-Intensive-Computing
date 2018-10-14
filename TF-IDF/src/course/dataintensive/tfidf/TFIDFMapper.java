package course.dataintensive.tfidf;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TFIDFMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	/*
	 * This method takes the output file generated from WordCountReducer.
	 * Parse the file and generates a new key value pair of form:
	 * (<word>, <document_ name> : <count> / <total_words_in_document>)
	 * */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] wdtf = value.toString().split("\t");
		String[] wd = wdtf[0].split(":");                 
		context.write(new Text(wd[0]), new Text(wd[1] + ":" + wdtf[1]));
    }
}
