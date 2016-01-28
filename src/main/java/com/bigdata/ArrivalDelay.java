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
import java.util.StringTokenizer;

public class ArrivalDelay extends Configured implements Tool {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            String airlineId = itr.nextToken();
            DoubleWritable delay = new DoubleWritable(Double.valueOf(itr.nextToken()));
            word.set(airlineId);
            context.write(word, delay);
        }
    }

    public static class IntSumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context) throws IOException, InterruptedException {

            double sum = 0, count = 0;

            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }

            result.set(sum/count);
            context.write(key, result);
        }
    }

    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.err.println("Usage: arrivalDelay <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "arrival delay");

        job.setJarByClass(ArrivalDelay.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }
}
