package com.bigdata;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by shubham.kankaria on 01/02/16.
 */
public class TaggedJoiningGroupingComparator extends WritableComparator {

    protected TaggedJoiningGroupingComparator() {
        super(TaggedKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TaggedKey taggedKey1 = (TaggedKey) a;
        TaggedKey taggedKey2 = (TaggedKey) b;
        return taggedKey1.joinKey.compareTo(taggedKey2.joinKey);
    }

}