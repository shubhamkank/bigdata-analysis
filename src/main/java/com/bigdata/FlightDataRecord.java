package com.bigdata;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by shubham.kankaria on 01/02/16.
 */
public class FlightDataRecord implements Writable {

    public Text tag = new Text();
    public Text origin = new Text();
    public Text dest = new Text();
    public Text airlineId = new Text();
    public Text uniqCarrier = new Text();
    public Text date = new Text();
    public IntWritable depTime = new IntWritable();
    public DoubleWritable delay = new DoubleWritable();

    public FlightDataRecord(Text tag, Text origin, Text dest, Text airlineId, Text uniqCarrier, Text date, IntWritable depTime,
                             DoubleWritable delay) {
        this.tag = tag;
        this.origin = origin;
        this.dest = dest;
        this.airlineId = airlineId;
        this.uniqCarrier = uniqCarrier;
        this.date = date;
        this.depTime = depTime;
        this.delay = delay;
    }

    public FlightDataRecord() {

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.tag.write(dataOutput);
        this.origin.write(dataOutput);
        this.dest.write(dataOutput);
        this.airlineId.write(dataOutput);
        this.uniqCarrier.write(dataOutput);
        this.date.write(dataOutput);
        this.depTime.write(dataOutput);
        this.delay.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.tag.readFields(dataInput);
        this.origin.readFields(dataInput);
        this.dest.readFields(dataInput);
        this.airlineId.readFields(dataInput);
        this.uniqCarrier.readFields(dataInput);
        this.date.readFields(dataInput);
        this.depTime.readFields(dataInput);
        this.delay.readFields(dataInput);
    }

    @Override
    public String toString() {
        return "FlightDataRecord{" +
                "tag=" + tag +
                ", origin=" + origin +
                ", dest=" + dest +
                ", airlineId=" + airlineId +
                ", uniqCarrier=" + uniqCarrier +
                ", date=" + date +
                ", depTime=" + depTime +
                ", delay=" + delay +
                '}';
    }
}
