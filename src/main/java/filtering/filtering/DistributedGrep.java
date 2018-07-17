package filtering.filtering;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * This class demonstrates how to apply a regular expression to every line.
 * View commented driver boilerplate.
 */
public class DistributedGrep {

    public static class GrepMapper extends Mapper<Object, Text, NullWritable, Text> {

        private String mapRegex = null;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            mapRegex = context.getConfiguration().get("mapregex");
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if(value.toString().matches(mapRegex))
                context.write(NullWritable.get(), value);
        }

    }

}

/*
    public static void main(String[] args) {

        -- Create configuration object conf --

        -- conf.set("nameOfProperty", "valueOfProperty"); --

        conf.set("regex", "someText");

        -- Set up job normally --

    }
 */