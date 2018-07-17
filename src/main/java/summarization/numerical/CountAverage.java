package summarization.numerical;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import util.XMLParser;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * This class aims at finding the number of times a user commented, along with the average length of each comment.
 * See src/resources/Comments.xml for data
 */
public class CountAverage {

    public static class CountAverageMapper extends Mapper<Object, Text, IntWritable, CountAverageTuple> {

        private IntWritable outHour = new IntWritable();
        private CountAverageTuple tuple = new CountAverageTuple();

        private static final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            Map<String, String> map = XMLParser.transformXmlToMap(value.toString());

            String date = map.get("CreationDate");
            String comment = map.get("Text");

            if(date == null || comment == null) return;

            Date creationDate = new Date();

            try {
                creationDate = format.parse(date);
            } catch (ParseException e) {
                e.printStackTrace();
            }

            outHour.set(creationDate.getHours());

            tuple.setCount(1);
            tuple.setAverage(comment.length());

            context.write(outHour, tuple);

        }
    }

    public static class CountAverageReducer extends Reducer<IntWritable, CountAverageTuple, IntWritable, CountAverageTuple> {

        private CountAverageTuple result = new CountAverageTuple();

        @Override
        protected void reduce(IntWritable key, Iterable<CountAverageTuple> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            double count = 0;

            for(CountAverageTuple tuple: values) {
                sum += tuple.getCount() * tuple.getAverage();
                count += tuple.getCount();
            }

            result.setCount(count);
            result.setAverage(sum/count);

            context.write(key, result);

        }
    }
}