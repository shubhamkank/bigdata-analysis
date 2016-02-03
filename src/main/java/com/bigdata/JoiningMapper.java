package com.bigdata;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

/**
 * Created by shubham.kankaria on 01/02/16.
 */
public class JoiningMapper extends Mapper<Object, Text, TaggedKey, Text> {

    private static Splitter splitter = Splitter.on(',');
    private TaggedKey taggedKey = new TaggedKey();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        List<String> values = Lists.newArrayList(splitter.split(value.toString()));

        if (values.get(0).equals("0")) {
            taggedKey.joinKey = new Text(values.get(2));
            taggedKey.tag = new IntWritable(0);
            if(Integer.valueOf(values.get(6)) > 1200) {
                return;
            }
            context.write(taggedKey, value);
        }
        else if(values.get(0).equals("1")) {
            taggedKey.joinKey = new Text(values.get(1));
            taggedKey.tag = new IntWritable(1);
            if(Integer.valueOf(values.get(6)) < 1200) {
                return;
            }
            context.write(taggedKey, value);
        }
    }
}
