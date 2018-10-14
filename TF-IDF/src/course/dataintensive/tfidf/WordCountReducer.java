package course.dataintensive.tfidf;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends
		Reducer<Text, IntWritable, Text, Text> {
	
	/*
	 * This method counts the appearance of a word in a document with the help of respective key.
	 * The Output key is ---  <word> : <document> 
	 * Output value is count of that word in that particular document + "/" + total number of words in that particular document. 
	 * The Output is a generated file following the "key" \t "value" format.
	 * */
	
	public void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int count = 0;
		
		for (IntWritable value : values) {
			count += value.get();
		}	
		
		String[] keyCount = key.toString().split(":");
		
		context.write(new Text(keyCount[0] + ":" + keyCount[1]), new Text(count + "/" + keyCount[2]));
	}
}
