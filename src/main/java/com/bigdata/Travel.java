package com.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

/**
 * Created by shubham.kankaria on 01/02/16.
 */
public class Travel extends Configured implements Tool {

    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.err.println("Usage: travel <in>,<in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "travel");

        job.setJarByClass(Travel.class);
        job.setMapperClass(JoiningMapper.class);
        job.setReducerClass(JoiningReducer.class);
        job.setOutputKeyClass(TaggedKey.class);
        job.setOutputValueClass(FlightDataRecord.class);
        job.setPartitionerClass(TaggerJoiningPartitioner.class);
        job.setGroupingComparatorClass(TaggedJoiningGroupingComparator.class);

        FileInputFormat.addInputPaths(job, otherArgs[1]);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }
}
