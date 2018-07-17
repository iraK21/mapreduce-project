package filtering.distinct;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import util.XMLParser;

import java.io.IOException;
import java.util.Map;

public class DistinctUser {

    public static class DistinctUserMapper extends Mapper<Object, Text, Text, NullWritable> {

        private Text text = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            Map<String, String> map = XMLParser.transformXmlToMap(value.toString());

            if(map == null) return;

            String userID = map.get("Id");

            if(userID == null) return;

            text.set(userID);

            context.write(text, NullWritable.get());
        }
    }

    public static class DistinctUserReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

        @Override
        public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

}
