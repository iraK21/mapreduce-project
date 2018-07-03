package summarization.numerical;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import utils.XMLParser;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * This class implements a simple WordCount program, using the MapReduce API in Hadoop.
 * It parses through input files and outputs all words encountered, alongside of their count.
 */
public class WordCount {

    /**
     * Mapper class for WordCount.
     * It takes in a line of text and maps it to a count of 1.
     */
    public static class WordCountMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        /** This field serves as a placeholder for every word. */
        private Text word = new Text();

        /**
         * This is called once for each (line - text-in-line) pair of the input.
         * It parses the XML into a HashMap, then tokenizes the comment text and maps every word to a value of 1
         */
        public void map(Object key,
                        Text value,
                        Context context)
                throws IOException, InterruptedException {
            Map<String, String> parsed = XMLParser.transformXmlToMap(value.toString());

            String text = parsed.get("Text");
            if (text == null)
                return;

            StringTokenizer itr = new StringTokenizer(text, " ");
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, new IntWritable(1));
            }
        }
    }

    /**
     * Reducer class for Word Count.
     * This class aggregates all the counts corresponding to a given key (word), and writes the sum to the output.
     * Note that this class can also be used as a combiner.
     */
    public static class WordCountReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        /**
         * This method iterates through all values corresponding to a given key.
         * It thens sums them up to give the final word count, and writes the count to output.
         */
        public void reduce(Text key,
                           Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) sum += val.get();
            result.set(sum);
            context.write(key, result);
        }
    }

}
