package summarization.numerical;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import util.XMLParser;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

public class MedianStandardDeviation {

    public static class MedianStandardDeviationMapper
            extends Mapper<Object, Text, IntWritable, IntWritable> {

        private IntWritable hour = new IntWritable();
        private IntWritable length = new IntWritable();

        private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> map = XMLParser.transformXmlToMap(value.toString());

            String creationDate = map.get("CreationDate");
            String text = map.get("Text");

            Date date = new Date();
            try {
                date = format.parse(creationDate);
            } catch (ParseException e) {
                e.printStackTrace();
            }

            hour.set(date.getHours());
            length.set(text.length());

            context.write(hour, length);

        }
    }

    public static class MedianStandardDeviationReducer
            extends Reducer<IntWritable, IntWritable, IntWritable, MedianStandardDeviationTuple> {

        private MedianStandardDeviationTuple tuple = new MedianStandardDeviationTuple();
        private ArrayList<Double> commentLengths = new ArrayList<>();

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            double sum = 0.0, count = 0.0;

            for (IntWritable value : values) {
                commentLengths.add((double) value.get());
                sum += value.get();
                count++;
            }

            Collections.sort(commentLengths);

            // Median
            if (count % 2 == 0) {
                tuple.setMedian(commentLengths.get((int) (count / 2) - 1) + commentLengths.get((int) (count / 2)) % 2.0);
            } else tuple.setMedian(commentLengths.get((int) (count / 2)));

            // Standard Deviation

            context.write(key, tuple);

        }
    }

}
