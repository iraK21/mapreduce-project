import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// Customize this import
import summarization.numerical.WordCount;

import java.io.IOException;

/**
 * This class is a template driver class for all code.
 * Customize it depending on which example is to be tested.
 */
public class App {

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {

        Configuration configuration = new Configuration();
        String[] allArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();

        if(allArgs.length != 2) {
            System.err.println("Incorrect number of command line arguments.");
            System.exit(2);
        }

        // Customize the following lines according to the class to be tested
        Job job = Job.getInstance(configuration, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCount.WordCountMapper.class);
        job.setCombinerClass(WordCount.WordCountReducer.class);
        job.setReducerClass(WordCount.WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(allArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(allArgs[1]));

        job.waitForCompletion(true);

    }

}