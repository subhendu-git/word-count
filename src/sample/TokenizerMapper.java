package sample;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	private final static String[] stopwords = {"a","above","about","again","against","after","all","and","an","any","am",
			"are","as","at","b","be","been","by","before","beside","behind","both","but","busy","bad","c","com",
			"can","could","come","do","ever","else","every","for","from","full","go","how","hi","hello","has",
			"had","have","i","is","in","into","it","its","it's","joy","k","keen","keep","low","lol","left","loose","me",
			"my","m","man","more","met","new","not","on","of","over","or","off","out","put","see","saw","the",
			"this","that","to","too","up","was","what","where","when","who","with","will","would","why"};
	
	private final static List<String> stopWordList = Arrays.asList(stopwords);
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		//StringTokenizer itr = new StringTokenizer(value.toString());
		
		String[] itr = value.toString().split(" ");
		
		for (String token : itr) {
			token = token.toLowerCase();
			
			//using only hashtags
			if(//token.startsWith("#") && 
					(token.length()>1)){
				token = token.replaceAll("\\s+", "");
				token = token.replaceAll("\"+", "");
				token = token.replaceAll(",+", "");
				token = token.replaceAll(";+", "");
				
				if(stopWordList.contains(token))
					continue;
				
				word.set(token);
				context.write(word, one);
			}
		}
	}
}