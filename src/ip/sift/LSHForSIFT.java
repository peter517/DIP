package ip.sift;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;


public class LSHForSIFT {

	// the num of hash bucket
	public static int bucketCnt = 4;
	// the num of sampling
	private static int samplingCnt = 8;
	// feature descriptor total length
	private static int fdTotalLen = 128;
	// avgValue for str->01
	private static double avgValue = 0.2f;

	public static Set<String> getLSH(Vector<Feature> ftList) {

		if (bucketCnt == 0 || samplingCnt == 0) {
			return null;
		}

		Set<String> setSet = new HashSet<String>();

		int len = ftList.size();
		int step = ftList.size() / samplingCnt;
		
		for (int k = 0; k < bucketCnt; k++) {
			
			StringBuilder sb = new StringBuilder();
			for (int i = k; i < len; i += step) {

				Feature ft = ftList.get(i);
				double buf = 0;

				for (int j = 0; j < fdTotalLen; j++) {
					buf += Math.pow(ft.descriptor[j], 2);
				}
				if (buf < avgValue) {
					sb.append("0");
				} else {
					sb.append("1");
				}

				if (sb.length() == samplingCnt) {
					break;
				}
			}
			if (sb.length() < samplingCnt) {
				int addNum = samplingCnt - sb.length();
				for (int i = 0; i < addNum; i++) {
					sb.append("1");
				}
			}
			
			setSet.add(sb.toString());
		}

		return setSet;

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
