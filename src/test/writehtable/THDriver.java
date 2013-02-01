package test.writehtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;

public class THDriver extends Configured implements Tool{

    @Override
    public int run(String[] arg0) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = HBaseConfiguration.create();
//        conf.set("zookeeper.znode.parent", "/idcserver");  
//        System.out.println(conf.get("zookeeper.znode.parent"));
//        conf.setInt("zookeeper.session.timeout", 500000);
//        conf.setInt("hbase.zookeeper.property.clientPort", 2181); 
        
        Job job = new Job(conf,"Txt-to-Hbase");
        job.setJarByClass(TxtHbase.class);
        
        Path in = new Path("hdfs://localhost:8021/user/root/intput01");
        
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, in);
        
        job.setMapperClass(THMapper.class);
        job.setReducerClass(THReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        TableMapReduceUtil.initTableReducerJob("s4", THReducer.class, job);
        
       job.waitForCompletion(true);
       return 0;
    }
    
}
