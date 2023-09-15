package edu.cs.utexas.HadoopEx;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

public class WordComparable implements Comparable<WordComparable>  {
    public Text word;
    public IntWritable total_count;

    public WordComparable(Text word, IntWritable total_count) {
        this.word = word;
        this.total_count = total_count;
    }

    public int compareTo(WordComparable other) {

        int compare = total_count.get() - other.total_count.get();
        if(compare == 0){
            return 0;
        }
        else if(compare < 0){
            return -1;
        }
        else{
            return 1;
        }
    }
}
