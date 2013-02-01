package test.readhtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Main {
    public static void main(String [] args) throws Exception{
        int mr;
        mr = ToolRunner.run(new Configuration(),new HtableDriver(),args);
        System.exit(mr);
    }
}
