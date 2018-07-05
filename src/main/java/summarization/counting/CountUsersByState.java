package summarization.counting;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.XMLParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

/**
 * This class counts the number of users from each state, using Hadoop Counters.
 * See src/resources/Users.xml for data.
 * Also view commented driver boilerplate below.
 */
public class CountUsersByState {

    public static class CountUsersByStateMapper extends Mapper<Object, Text, NullWritable, NullWritable> {

        public static final String STATE_COUNTER_GROUP = "State";
        private static final String UNKNOWN_COUNTER = "Unknown";
        private static final String NULL_OR_EMPTY_COUNTER = "Null or Empty";

        private String[] statesArray = new String[]{"AL", "AK", "AZ", "AR",
                "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN",
                "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
                "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND",
                "OH", "OK", "OR", "PA", "RI", "SC", "SF", "TN", "TX", "UT",
                "VT", "VA", "WA", "WV", "WI", "WY"};

        private HashSet<String> states = new HashSet<>(Arrays.asList(statesArray));

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            Map<String, String> map = XMLParser.transformXmlToMap(value.toString());

            String location = map.get("Location");

            if (location == null || location.isEmpty())
                context.getCounter(STATE_COUNTER_GROUP, NULL_OR_EMPTY_COUNTER).increment(1);
            else {
                String[] tokens = location.toUpperCase().split("\\s");

                boolean unknown = true;

                for (String state : tokens) {
                    if (states.contains(state)) {
                        context.getCounter(STATE_COUNTER_GROUP, state).increment(1);
                        unknown = false;
                        break;
                    }
                }

                if (unknown)
                    context.getCounter(STATE_COUNTER_GROUP, UNKNOWN_COUNTER).increment(1);
            }

        }
    }

    /*
    public static void main(String[] args) {

        //-- Set up job normally --

        int code = job.waitForCompletion(true) ? 0 : 1;

        if (code == 0)
            for (Counter counter : job.getCounters().
                    getGroup(CountUsersByState.CountUsersByStateMapper.STATE_COUNTER_GROUP))
                System.out.println(counter.getDisplayName() + "\t" + counter.getValue());

        System.exit(code);


    }
    */
}