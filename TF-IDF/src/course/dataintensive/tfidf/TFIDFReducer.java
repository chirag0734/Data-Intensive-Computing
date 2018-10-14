package course.dataintensive.tfidf;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TFIDFReducer extends Reducer<Text, Text, Text, Text> {

	public int noOfDocuments;
	public Text wordAndDoc = new Text();
	public Text result = new Text();
	public static Set<String> vocab;

	/*
	 * This method is called once and intended to load given vocab file words into a Hashset and also count the number of files in the input directory.
	 * */
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);

		vocab = new HashSet<String>();

		FileSystem fileSys = FileSystem.get(context.getConfiguration());
		Path vocabPath = new Path("/vocab.txt");
		BufferedReader bufferRead = new BufferedReader(new InputStreamReader(
				fileSys.open(vocabPath)));
		String word = null;
		while ((word = bufferRead.readLine()) != null) {
			vocab.add(word);
		}

		Path inputPath = new Path(context.getConfiguration().get("corpus"));
		FileSystem fs = inputPath.getFileSystem(context.getConfiguration());
		FileStatus[] files = fs.listStatus(inputPath);
		noOfDocuments = files.length;
	}

	/*
	 * This method calculates the required TF-IDF score for the word and generated a file having the output format as:
	 * <word> <document_name> \t <TF-IDF score> */
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		int tfCounter = 0;
		
		Map<String, String> dfMap = new HashMap<String, String>();

		if (vocab.contains(key.toString().toLowerCase())) {
			for (Text value : values) {
				String[] df = value.toString().split(":");
				tfCounter++;
				dfMap.put(df[0], df[1]);
			}

			for (String doc : dfMap.keySet()) {
				String[] count = dfMap.get(doc).split("/");

				double tf = Double.valueOf(Double.valueOf(count[0])
						/ Double.valueOf(count[1]));

				double idf = (double) noOfDocuments / (double) tfCounter;

				double tfIdf = noOfDocuments == tfCounter ? tf : tf
						* Math.log(idf) / Math.log(2);

				String res = Double.toString(tfIdf);

				wordAndDoc.set(key + " " + doc);
				result.set(res);

				context.write(wordAndDoc, result);
			}
		}
	}
}
