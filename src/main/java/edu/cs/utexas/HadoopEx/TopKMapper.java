package edu.cs.utexas.HadoopEx;
import java.io.IOException;
import java.util.PriorityQueue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopKMapper  extends Mapper<Text, Text, Text, IntWritable> {

    public PriorityQueue<WordComparable> top10;
	public void setup(Context context) {
		top10 = new PriorityQueue<>();
	}

    public void map(Text word, Text word_count, Context context)
			throws IOException, InterruptedException {
        top10.add(new WordComparable(new Text(word), new IntWritable(Integer.parseInt(word_count.toString()))));
	}

	public void cleanup(Context context) throws IOException, InterruptedException {
        while(top10.size() > 10){
            top10.poll();
        }

		while (top10.size() > 0) {
			WordComparable wordcount = top10.poll();
			context.write(wordcount.word, wordcount.total_count);
		}
	}
}
