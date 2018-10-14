# Term Frequency (TF) – Inverse Document Frequency (IDF)

The project implements TF-IDF using map-reduce paradigm and is built using Java and Hadoop MapReduce Framework. 
We ran the project on multi-cluster environment with 1 master node and 2 workers node. 
The task is simplified into two jobs, the output file from first job(Word Count job) is given as an input to the second job(TFIDF job). 
 Assumptions to run the jar file:
1. The map-reduce-corpus folder is placed on hdfs root directory.
2. File vocab.txt is placed on hdfs root directory.

Approach:
Our approach to the solution is as follows:
JOB 1: Word Count Job
This job takes list of files/documents from given input directory as input and process them. Following are the Map and Reduce method input-output formats for the job:
WordCountMapper: 
Input – (document, contents)
	Output – ((<word>:<document>:<total_words_in_document>), 1)
WordCountReducer:
	Output – ((<word>:<document>:<total_words_in_document>), <word_count>)

This job generates a file called “wordCount” in output directory specified and it is used in next job i.e., TFIDF job.

JOB 2: TFIDF Job
This job takes file generated in previous job and process it. 
TFIDFMapper:
Input – ((<word>:<document>:<total_words_in_document>), <word_count>)
	Output – ((<word>:<document>:<word_count>  /  <total_words_in_document>))
TFIDFReducer:
	Output – (<word> <document>/t<TFIDF>)

This job generated the result file “tf-idf” containing the word, the document and its associated tfidf. 
Also, we only print words and its data that are there in “vocab.txt” as per requirement.
In last step, the Term Frequency(tf) is calculated by dividing the <word_count> over <total_words_in_document> and Inverse document frequency(idf) is calculated by taking log2 of division of <Total_number_of_documents> over <Number_of_documents_containing_word>.

TFIDF = tf * idf

How to run?
1. Place the input directory on hdfs.
2. Place vocab.txt on hdfs root directory.
3. hadoop "<jarName>.jar" "/<inputDirectoryPath>"  "/<outputDirectoryPath>"


