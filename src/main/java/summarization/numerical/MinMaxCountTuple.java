package summarization.numerical;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * This class encapsulates a Writable for the output value of finding minimum, maximum
 * and count of the given key.
 */
public class MinMaxCountTuple implements Writable {

    private Date minimum = new Date();
    private Date maximum = new Date();
    private long count = 0;

    public Date getMinimum() {
        return minimum;
    }

    public void setMinimum(Date minimum) {
        this.minimum = minimum;
    }

    public Date getMaximum() {
        return maximum;
    }

    public void setMaximum(Date maximum) {
        this.maximum = maximum;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    private static final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(minimum.getTime());
        out.writeLong(maximum.getTime());
        out.writeLong(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        minimum = new Date(in.readLong());
        maximum = new Date(in.readLong());
        count = in.readLong();
    }

    @Override
    public String toString() {
        return format.format(minimum) + '\t' + format.format(maximum) + '\t' + count;
    }
}