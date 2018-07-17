package filtering.top_ten;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import util.XMLParser;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

// TODO Find use of new Text(Text text)

public class TopTen {

    public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {

        private TreeMap<Integer, Text> map = new TreeMap<>();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> stringMap = XMLParser.transformXmlToMap(value.toString());

            if(stringMap == null) return;

            String userId = stringMap.get("Id");
            String reputation = stringMap.get("Reputation");

            if(userId == null || reputation == null) return;

            map.put(Integer.parseInt(reputation), new Text(value));

            if(map.size() > 10) map.remove(map.firstKey());

        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Text text : map.values()) context.write(NullWritable.get(), text);
        }
    }

    public static class TopTenReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

        private TreeMap<Integer, Text> map = new TreeMap<>();

        @Override
        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text value : values) {
                Map<String, String> stringMap = XMLParser.transformXmlToMap(value.toString());

                map.put(Integer.parseInt(stringMap.get("Reputation")), new Text(value));

                if(map.size() > 10) map.remove(map.firstKey());
            }

            for (Text text : map.descendingMap().values())
                context.write(NullWritable.get(), text);

        }
    }

}
