package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopKReducer extends  Reducer<Text, IntWritable, Text, IntWritable> {
    public PriorityQueue<WordComparable> top10;
	public void setup(Context context) {
		top10 = new PriorityQueue<>();
	}

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
        for (IntWritable value : values) {
            top10.add(new WordComparable(new Text(key), new IntWritable(value.get())));
        }
	}

	public void cleanup(Context context) throws IOException, InterruptedException {
        while(top10.size() > 10){
            top10.poll();
        }
        List<WordComparable> top10NonSorted = new ArrayList<WordComparable>(10);
		while (top10.size() > 0) {
			WordComparable wordcount = top10.poll();
			top10NonSorted.add(wordcount);
		}
        for(int i = top10NonSorted.size() - 1; i >= 0; i--){
            WordComparable wordcount = top10NonSorted.get(i);
            context.write(wordcount.word, wordcount.total_count);
        }
	}
}
