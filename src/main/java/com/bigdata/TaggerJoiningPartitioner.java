package com.bigdata;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by shubham.kankaria on 01/02/16.
 */
public class TaggerJoiningPartitioner extends Partitioner<TaggedKey, Text> {

    @Override
    public int getPartition(TaggedKey taggedKey, Text record, int numPartitions) {
        return taggedKey.joinKey.hashCode() % numPartitions;
    }
}