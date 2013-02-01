package ip.mapreduce.imgmatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import model.GlobalVars;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

import tools.HbaseTableTools;

public class ImgMatchReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

	public void reduce(Text sim, Iterable<Text> paths, Context context) {

		for (Text val : paths) {
			System.out.println("key:" + sim.toString() + "," + "value:" + val.toString());
		}
		List<Put> prList = new ArrayList<Put>();
		Put putrow = new Put(GlobalVars.getSrcPath().getBytes());
		
		int rankNum = 0;
		StringBuffer pathSb = new StringBuffer();
		for (Text path : paths) {
			
			if(rankNum++ > GlobalVars.MOST_SIM_NUM){
				break;
			}
			pathSb.append(path.toString());
			if (rankNum < GlobalVars.MOST_SIM_NUM){
				pathSb.append(GlobalVars.SEPARATOR_STRUCT);
			}
		}
		
		putrow.add(GlobalVars.FAMILY_DST_IMG.getBytes(), GlobalVars.COLUMN_SIM_PATH.getBytes(), pathSb.toString().getBytes());
		prList.add(putrow);
		
		try {
			HbaseTableTools.addKeyValue(GlobalVars.TABLE_SIM_IMG, prList);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
