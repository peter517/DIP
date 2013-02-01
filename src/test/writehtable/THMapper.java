package test.writehtable;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class THMapper extends Mapper<LongWritable,Text,Text,Text>{
        public void map(LongWritable key,Text value,Context context){
            String[] items = value.toString().split(" ");
            String k = items[0];
            String v = items[1];
            System.out.println("key:"+k+","+"value:"+v);
            try {
                
                context.write(new Text(k), new Text(v));
                
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

    }

}
