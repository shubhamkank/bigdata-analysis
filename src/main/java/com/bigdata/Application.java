package com.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Application {

    public static void main(String [] args) throws Exception {

        if(args[0].equals("arrivalDelay")) {
            int res = ToolRunner.run(new Configuration(), new ArrivalDelay(), args);
            System.exit(res);
        }
        else if(args[0].equals("analyze")) {
            int res = ToolRunner.run(new Configuration(), new Analyze(), args);
            System.exit(res);
        }
    }
}
