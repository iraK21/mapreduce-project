package filtering.filtering;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Random;

/**
 * The Simple Random Sampler grabs a subset of a larger data set, with every record in the dataset having equal
 * probability of being selected in the sample.
 */
public class SimpleRandomSampler {

    public static class SimpleRandomSamplerMapper extends Mapper<Object, Text, NullWritable, Text> {

        private Random random = new Random();
        private Double percentage  = 0.0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String string = context.getConfiguration().get("filter_percentage");
            percentage = Double.parseDouble(string) / 100.0;
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if(random.nextDouble() < percentage)
                context.write(NullWritable.get(), value);
        }
    }

}