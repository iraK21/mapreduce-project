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

    public static class WordCountMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private Text word = new Text();

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

    public static class WordCountReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

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
