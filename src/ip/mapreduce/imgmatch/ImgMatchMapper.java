package ip.mapreduce.imgmatch;

import ip.sift.Feature;
import ip.sift.SIFT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Vector;

import model.GlobalVars;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;

import service.SIFTExtractService;

public class ImgMatchMapper extends TableMapper<Text, Text> {

	public void map(ImmutableBytesWritable key, Result result, Context context) {

		for (KeyValue kv : result.raw()) {

			String column = new String((kv.getQualifier()));
			if (column.equals(GlobalVars.COLUMN_SIFT_FEATURE)) {
				
				Vector<Feature> dstFvector = SIFTExtractService.getFeatureByCmpStr(new String(kv.getValue()));
				Text row = new Text(kv.getRow());

				try {
					// get sim of two FeatureVector
					float sim = SIFT.getMatchRadio(GlobalVars.getSrcFVector(), dstFvector);
					context.write(new Text(String.valueOf(sim)), row);
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			}
		}
	}
	
	
}
