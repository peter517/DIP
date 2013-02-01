package ip.mapreduce.sift2hbase;

import ip.sift.Feature;
import ip.sift.LSHForSIFT;
import ip.sift.SIFT;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import javax.imageio.ImageIO;

import model.GlobalVars;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;

import tools.HbaseTableTools;

public class Sift2HbaseMapper extends Mapper<Text, BytesWritable, NullOutputFormat, NullOutputFormat> {

	public void map(Text filename, BytesWritable content, Context context) {

		InputStream in = new ByteArrayInputStream(content.getBytes());

		try {

			BufferedImage img = ImageIO.read(in);
			Vector<Feature> fsVector = SIFT.getFeatures(img);
			LSHForSIFT.getLSH(fsVector);
			
//			lshToHBase(filename, fsVector);
//			siftToHBase(filename, content, fsVector);

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * write lsh to hbase
	 * 
	 * @param filename
	 * @param fsVector
	 */
	private void lshToHBase(Text filename, Vector<Feature> fsVector) {

		Set<String> strSet = LSHForSIFT.getLSH(fsVector);

		List<Put> prList = new ArrayList<Put>();

		Iterator<String> iter = strSet.iterator();
		while (iter.hasNext()) {

			String str = iter.next();
			// lsh is the rowkey
			Put putrow = new Put(str.getBytes());
			try {
				List<KeyValue> kvList = HbaseTableTools.getKeyValue(GlobalVars.TABLE_LSH_IMG, str);
				if (kvList.size() != 0) {
					// must only one item
					KeyValue kv = kvList.get(0);
					StringBuffer value = new StringBuffer(new String(kv.getValue()));
					value.append(GlobalVars.SEPARATOR_ITEM);
					value.append(filename.toString());

					putrow.add(GlobalVars.FAMILY_DST_IMG.getBytes(), GlobalVars.COLUMN_LSH_PATH.getBytes(), value.toString().getBytes());

				} else {
					putrow.add(GlobalVars.FAMILY_DST_IMG.getBytes(), GlobalVars.COLUMN_LSH_PATH.getBytes(), filename.getBytes());
				}
				prList.add(putrow);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		try {
			HbaseTableTools.addKeyValue(GlobalVars.TABLE_LSH_IMG, prList);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * write sift and img data to hbase
	 * 
	 * @param filename
	 * @param content
	 * @param fsVector
	 */
	private void siftToHBase(Text filename, BytesWritable content, Vector<Feature> fsVector) {

		List<Put> prList = new ArrayList<Put>();
		Put putrow = new Put(filename.toString().getBytes());

		StringBuffer featureVectorSb = new StringBuffer();

		int len = fsVector.size();
		int num = 0;
		for (Feature fs : fsVector) {
			// get scale
			StringBuffer featureSb = new StringBuffer();
			String scale = String.valueOf(fs.scale);
			// get descriptor
			StringBuffer descriptor = new StringBuffer(String.valueOf(fs.descriptor[0]));
			for (int i = 1; i < SIFT.getFdTotalLen(); i++) {
				descriptor.append(String.valueOf(GlobalVars.SEPARATOR_ARRAY + fs.descriptor[i]));
			}

			// get feature
			featureSb.append(scale);
			featureSb.append(GlobalVars.SEPARATOR_ITEM);
			featureSb.append(descriptor);

			// add to feature vector
			featureVectorSb.append(featureSb);
			if (++num != len) {
				featureVectorSb.append(GlobalVars.SEPARATOR_STRUCT);
			}

		}

		// add sift and img binary data to htable
		putrow.add(GlobalVars.FAMILY_INFO.getBytes(), GlobalVars.COLUMN_SIFT_FEATURE.getBytes(), featureVectorSb.toString().getBytes());
		putrow.add(GlobalVars.FAMILY_INFO.getBytes(), GlobalVars.COLUMN_IMG_DATA.getBytes(), content.getBytes());
		prList.add(putrow);

		try {
			HbaseTableTools.addKeyValue(GlobalVars.TABLE_IMG_INFO, prList);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
