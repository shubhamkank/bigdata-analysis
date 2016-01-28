package com.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Application {

    public static void main(String [] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new ArrivalDelay(), args);
        System.exit(res);
    }
}
