package com.bigdata;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by shubham.kankaria on 01/02/16.
 */
public class TaggedKey implements Writable, WritableComparable<TaggedKey> {

    public Text joinKey = new Text();
    public IntWritable tag = new IntWritable();

    @Override
    public int compareTo(TaggedKey taggedKey) {
        int compareValue = this.joinKey.compareTo(taggedKey.joinKey);
        if (compareValue == 0) {
            compareValue = this.tag.compareTo(taggedKey.tag);
        }
        return compareValue;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.joinKey.write(dataOutput);
        this.tag.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.joinKey.readFields(dataInput);
        this.tag.readFields(dataInput);
    }

}
