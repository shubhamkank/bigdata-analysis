package com.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Application {

    public static void main(String [] args) throws Exception {

        System.out.println("Use any one of the following:");
        System.out.println("1. arrivalDelay <in> <out>");
        System.out.println("2. analyze <in> <out>");
        System.out.println("3. popularAirport <in> <out>");

        if(args[0].equals("arrivalDelay")) {
            int res = ToolRunner.run(new Configuration(), new ArrivalDelay(), args);
            System.exit(res);
        }
        else if(args[0].equals("analyze")) {
            int res = ToolRunner.run(new Configuration(), new Analyze(), args);
            System.exit(res);
        }
        else if(args[0].equals("popularAirport")) {
            int res = ToolRunner.run(new Configuration(), new PopularAirport(), args);
            System.exit(res);
        }
        else if(args[0].equals("travel")) {
            int res = ToolRunner.run(new Configuration(), new Travel(), args);
            System.exit(res);
        }
    }
}
