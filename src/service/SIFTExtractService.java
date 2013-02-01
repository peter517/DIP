package service;

import ip.sift.Feature;
import ip.sift.LSHForSIFT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.hbase.KeyValue;

import model.GlobalVars;
import tools.HbaseTableTools;

public class SIFTExtractService {

	/**
	 * get sift feature by composited string
	 * @param value
	 * @return
	 */
	public static Vector<Feature> getFeatureByCmpStr(String value){
		
		Vector<Feature> dstFvector = new Vector<Feature>();
		
		// extract feature
		StringTokenizer fToker = new StringTokenizer(value, GlobalVars.SEPARATOR_STRUCT);
		List<String> fStrList = new ArrayList<String>();
		while (fToker.hasMoreTokens()) {
			fStrList.add(fToker.nextToken());
		}
		for (String fStr : fStrList) {
			// extract scale and descriptor
			StringTokenizer iToker = new StringTokenizer(fStr, GlobalVars.SEPARATOR_ITEM);
			String[] iStrArr = new String[2];
			int k = 0;
			while (iToker.hasMoreTokens()) {
				iStrArr[k++] = iToker.nextToken();
			}

			Feature f = new Feature();
			// extract scale
			f.scale = Float.valueOf(iStrArr[0]);

			// extract descriptor
			StringTokenizer aToker = new StringTokenizer(iStrArr[1], GlobalVars.SEPARATOR_ARRAY);
			int m = 0;
			while (aToker.hasMoreTokens()) {
				f.descriptor[m++] = Float.valueOf(aToker.nextToken());
			}

			dstFvector.add(f);
		}
		return dstFvector;
	}
	
	/**
	 * 	get the images of same lsh
	 * @param ftList
	 * @return
	 */
		public static Set<String> getSimPath(Vector<Feature> ftList) {

			Map<String, Integer> dstMap = new HashMap<String, Integer>();
			
			LSHForSIFT.bucketCnt = 32;
			Set<String> strSet = LSHForSIFT.getLSH(ftList);
			Iterator<String> iter = strSet.iterator();
			while (iter.hasNext()) {
				try {
					String str = iter.next();
					List<KeyValue> kvList = HbaseTableTools.getKeyValue(GlobalVars.TABLE_LSH_IMG, str);
					for (KeyValue kv : kvList) {
						// must only one item
						StringTokenizer strToker = new StringTokenizer(new String(kv.getValue()), GlobalVars.SEPARATOR_ITEM);
						while (strToker.hasMoreTokens()) {
							String token = strToker.nextToken();
							Integer num = dstMap.get(token);
							if (num == null){
								dstMap.put(token, new Integer(1));
							}else{
								dstMap.put(token, ++num);
							}
							
						}
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			//put item into set
			Set<String> dstSet = new HashSet<String>();
			Iterator<String> iterMap = dstMap.keySet().iterator();
			while(iterMap.hasNext()){
				String key = iterMap.next();
				if (dstMap.get(key) != 1){
					dstSet.add(key);
				}
			}
			
			return dstSet;
		}

		/**
		 *  get feature data by the image id
		 * @param id
		 * @return
		 */
		public static Vector<Feature> getImgFeatureByID(String id) {

			List<KeyValue> kvList;
			try {
				kvList = HbaseTableTools.getKeyValue(GlobalVars.TABLE_IMG_INFO, id);

				for (KeyValue kv : kvList) {
					// must only one item
					if (new String(kv.getQualifier()).equals(GlobalVars.COLUMN_SIFT_FEATURE)) {
						String fStr = new String(kv.getValue());
						return SIFTExtractService.getFeatureByCmpStr(fStr);
					}

				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			return null;

		}
		
}
