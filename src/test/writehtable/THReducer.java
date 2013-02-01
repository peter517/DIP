package test.writehtable;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text; 

public class THReducer extends TableReducer<Text,Text,ImmutableBytesWritable>{
	
    public void reduce(Text key,Iterable<Text> value,Context context){
        String k = key.toString();
        String v = value.iterator().next().toString(); //由数据知道value就只有一行
        Put putrow = new Put(k.getBytes());
        putrow.add("n".getBytes(), "qualifier".getBytes(), v.getBytes());
        try {
            
            context.write(new ImmutableBytesWritable(key.getBytes()), putrow);
            
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }

}
