package service;

import ip.sift.Feature;
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

import tools.HbaseTableTools;

public class HbaseService {

	
	private static double threshold = 0.7;
	private static int simNum = 10;
	/**
	 * 根据图像id 获得图像数据
	 * @param strList
	 * @return
	 */
	public static List<BufferedImage> getImgByID(List<String> strList) {

		List<BufferedImage> imgList = new ArrayList<BufferedImage>();

		for (String id : strList) {

			List<KeyValue> kvList;
			try {
				kvList = HbaseTableTools.getKeyValue(GlobalVars.TABLE_IMG_INFO, id);

				for (KeyValue kv : kvList) {
					// must only one item
					if (new String(kv.getQualifier()).equals(GlobalVars.COLUMN_IMG_DATA)) {
						InputStream in = new ByteArrayInputStream(kv.getValue());
						imgList.add(ImageIO.read(in));
						in.close();
					}

				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return imgList;
	}

	
	/**
	 * 获取相似图像
	 * @param img
	 * @return
	 */
	public static List<String> getSimImg(BufferedImage img) {

		List<String> srtList = new ArrayList<String>();
		
		Vector<Feature> ftList = SIFT.getFeatures(img);
		Set<String> strSet = SIFTExtractService.getSimPath(ftList);

		Iterator<String> iter = strSet.iterator();
		int count = 0;
		while (iter.hasNext()) {
			String imgId = iter.next();
			Vector<Feature> dstList = SIFTExtractService.getImgFeatureByID(imgId);
			if (dstList != null) {
				float sim = SIFT.getMatchRadio(ftList, dstList);
				if (sim > threshold) {
					if (count++ > simNum) {
						break;
					}
					srtList.add(imgId);
					System.out.println(imgId + " = " + sim);
				}
			}

		}

		return srtList;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
