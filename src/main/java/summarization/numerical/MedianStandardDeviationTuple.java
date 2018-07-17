package summarization.numerical;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MedianStandardDeviationTuple implements Writable {

    private double median = 0.0;
    private double standardDeviation = 0.0;

    public double getMedian() {
        return median;
    }

    public void setMedian(double median) {
        this.median = median;
    }

    public double getStandardDeviation() {
        return standardDeviation;
    }

    public void setStandardDeviation(double standardDeviation) {
        this.standardDeviation = standardDeviation;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(median);
        out.writeDouble(standardDeviation);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        median = in.readDouble();
        standardDeviation = in.readDouble();
    }

    @Override
    public String toString() {
        return median + "\t" + standardDeviation;
    }

}
