package com.bigdata;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

/**
 * Created by shubham.kankaria on 01/02/16.
 */
public class JoiningMapper extends Mapper<Object, Text, TaggedKey, FlightDataRecord> {

    private static Splitter splitter = Splitter.on(',');
    private TaggedKey taggedKey = new TaggedKey();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        List<String> values = Lists.newArrayList(splitter.split(value.toString()));
        String joinKey;

        FlightDataRecord record = new FlightDataRecord();
        record.origin = new Text(values.get(1));
        record.dest = new Text(values.get(2));
        record.airlineId = new Text(values.get(3));
        record.uniqCarrier = new Text(values.get(4));
        record.date = new Text(values.get(5));
        record.depTime = new IntWritable(Integer.valueOf(values.get(6)));
        record.delay = new DoubleWritable(Double.valueOf(values.get(7)));

        if (values.get(0).equals("0")) {
            joinKey = values.get(2);
            taggedKey.joinKey = new Text(joinKey);
            taggedKey.tag = new IntWritable(0);
            record.tag = new Text("0");
            if(record.depTime.get() > 1200) {
                return;
            }
            context.write(taggedKey, record);
        }
        else if(values.get(0).equals("1")) {
            joinKey = values.get(1);
            taggedKey.joinKey = new Text(joinKey);
            taggedKey.tag = new IntWritable(1);
            record.tag = new Text("1");
            if(record.depTime.get() < 1200) {
                return;
            }
            context.write(taggedKey, record);
        }
    }
}
