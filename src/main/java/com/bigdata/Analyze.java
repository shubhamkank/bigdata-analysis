package com.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by shubham.kankaria on 30/01/16.
 */
public class Analyze extends Configured implements Tool {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text outKey = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString(), ",");

            List<String> tokens = new ArrayList<>();

            while(tokenizer.hasMoreTokens()) {
                tokens.add(tokenizer.nextToken());
            }

            StringBuilder sb = new StringBuilder();

            for(int i = 0; i < tokens.size()-1; i++) {
                if(i == tokens.size()-2) {
                    sb.append(tokens.get(i));
                }
                else {
                    sb.append(tokens.get(i) + "$");
                }
            }

            DoubleWritable outValue = new DoubleWritable(Double.valueOf(tokens.get(tokens.size()-1)));
            outKey.set(sb.toString());
            context.write(outKey, outValue);
        }
    }

    public static class DoubleAvgReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context) throws IOException, InterruptedException {

            double sum = 0, count = 0;

            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }

            result.set(sum/count);

            context.write(new Text(key.toString().replace('$', '\t')), result);
        }
    }

    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.err.println("Usage: analyze <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "analyze");

        job.setJarByClass(Analyze.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(DoubleAvgReducer.class);
        job.setReducerClass(DoubleAvgReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }
}
