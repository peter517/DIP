package ip.sift;

import java.awt.image.BufferedImage;
import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Vector;

import javax.imageio.ImageIO;

public class SIFT {

	// steps
	private static int steps = 3;
	// initial sigma
	private static float initial_sigma = 1.6f;
	// feature descriptor size
	private static int fdsize = 4;
	// feature descriptor orientation bins
	private static int fdbins = 8;
	// feature descriptor total length
	private static int fdTotalLen = fdsize * fdsize * fdbins;

	public static int getFdTotalLen() {
		return fdTotalLen;
	}

	// size restrictions for scale octaves, use octaves < max_size and >
	// min_size only
	private static int min_size = 64;
	private static int max_size = 1024;

	/**
	 * Set true to double the size of the image by linear interpolation to (
	 * with * 2 + 1 ) * ( height * 2 + 1 ). Thus we can start identifying DoG
	 * extrema with $\sigma = INITIAL_SIGMA / 2$ like proposed by
	 * \citet{Lowe04}.
	 * 
	 * This is useful for images scmaller than 1000px per side only.
	 */
	private static boolean upscale = false;

	private static FloatArray2D img2FloatArray(BufferedImage img) {

		int width = img.getWidth();
		int height = img.getHeight();
		float[] data = new float[width * height];

		for (int i = 0; i < img.getHeight(); i++) {
			for (int j = 0; j < img.getWidth(); j++) {

				int pixel = img.getRGB(j, i);
				int r = (pixel & 0xff0000) >> 16;
				int g = (pixel & 0xff00) >> 8;
				int b = (pixel & 0xff);

				data[width * i + j] = (30 * r + 59 * g + 11 * b) / 100;

			}
		}

		return new FloatArray2D(data, width, height);
	}

	public static Vector<Feature> getFeatures(BufferedImage img) {

		Vector<Feature> fs;

		FloatArray2DSIFT sift = new FloatArray2DSIFT(fdsize, fdbins);

		FloatArray2D fa = img2FloatArray(img);
		Filter.enhance(fa, 1.0f);

		if (upscale) {
			FloatArray2D fat = new FloatArray2D(fa.width * 2 - 1, fa.height * 2 - 1);
			FloatArray2DScaleOctave.upsample(fa, fat);
			fa = fat;
			fa = Filter.computeGaussianFastMirror(fa, (float) Math.sqrt(initial_sigma * initial_sigma - 1.0));
		} else
			fa = Filter.computeGaussianFastMirror(fa, (float) Math.sqrt(initial_sigma * initial_sigma - 0.25));

		// long start_time = System.currentTimeMillis();
		// System.out.println("processing SIFT ...");
		sift.init(fa, steps, initial_sigma, min_size, max_size);
		fs = sift.run(max_size);
		Collections.sort(fs);
		// System.out.println("extract_sift time:	" +
		// (System.currentTimeMillis() - start_time) + "ms");
		// System.out.println(fs.size() + " features identified and processed");

		return fs;
	}

	public static float getMatchRadio(Vector<Feature> srcFVector, Vector<Feature> dstFvector) {

		// long start_time = System.currentTimeMillis();
		// System.out.println("identifying correspondences using brute force ...");
		float matchNum = FloatArray2DSIFT.createMatches(srcFVector, dstFvector, 1.5f);
		// System.out.println("match time:	" + (System.currentTimeMillis() -
		// start_time) + "ms");

		return matchNum / srcFVector.size();

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		long start_time = System.currentTimeMillis();
		// 读取源图像
		BufferedImage img1 = null;
		BufferedImage img2 = null;
		try {
			img1 = ImageIO.read(new File("10.jpg"));
			Vector<Feature> fs1 = getFeatures(img1);
			File file = new File("image/");
			File[] array = file.listFiles();

			for (int i = 0; i < array.length; i++) {
				img2 = ImageIO.read(array[i]);

				
				Vector<Feature> fs2 = getFeatures(img2);

//				System.out.println("fs1 num :" + fs1.size());
//				System.out.println("fs2 num :" + fs2.size());
//				long start_time1 = System.currentTimeMillis();
				float matchNum = getMatchRadio(fs1, fs2);
//				System.out.println("match " + (System.currentTimeMillis() - start_time1) + "ms");
				if (matchNum < 0.7) continue;
				System.out.println(array[i] + " " + matchNum);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		System.out.println("total " + (System.currentTimeMillis() - start_time) + "ms");

	}

}
