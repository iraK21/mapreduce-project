package summarization.numerical;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This class encapsulates a Writable to output count and average values for given key.
 */
public class CountAverageTuple implements Writable {

    private double count = 0;
    private double average = 0;

    public double getCount() {
        return count;
    }

    public void setCount(double count) {
        this.count = count;
    }

    public double getAverage() {
        return average;
    }

    public void setAverage(double average) {
        this.average = average;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(count);
        out.writeDouble(average);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        count = in.readDouble();
        average = in.readDouble();
    }

    @Override
    public String toString() {
        return count + "\t" + average;
    }

}