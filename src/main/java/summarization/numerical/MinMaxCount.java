package summarization.numerical;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MinMaxCount {

    public static class MinMaxCountMapper extends Mapper<Object, Text, Text, MinMaxCountTuple> {

        private Text outID = new Text();
        private MinMaxCountTuple tuple = new MinMaxCountTuple();

        private final static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] strings = value.toString().split(",");
            String userID = strings[0];
            String date = strings[1];

            Date creationDate = new Date();
            try {
                creationDate = format.parse(date);
            } catch (ParseException e) {
                e.printStackTrace();
            }

            tuple.setMinimum(creationDate);
            tuple.setMaximum(creationDate);
            tuple.setCount(1);

            outID.set(userID);

            context.write(outID, tuple);
        }
    }

    public static class MinMaxCountReducer extends Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {

        private MinMaxCountTuple result = new MinMaxCountTuple();

        @Override
        public void reduce(Text key, Iterable<MinMaxCountTuple> values, Context context) throws IOException, InterruptedException {

            result.setMinimum(null);
            result.setMaximum(null);
            result.setCount(0);

            int sum = 0;

            for(MinMaxCountTuple tuple: values) {
                if(result.getMinimum() == null ||
                        tuple.getMinimum().compareTo(result.getMinimum())<0)
                    result.setMinimum(tuple.getMinimum());

                if(result.getMaximum() == null ||
                        tuple.getMaximum().compareTo(result.getMaximum())>0)
                    result.setMaximum(tuple.getMaximum());

                sum += tuple.getCount();
            }

            result.setCount(sum);
            context.write(key, result);

        }
    }

}
