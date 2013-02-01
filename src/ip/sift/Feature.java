package ip.sift;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;

/**
 * SIFT feature container
 */
public class Feature implements Comparable<Feature>, Serializable {
	public float scale;
	public float orientation;
	public float[] location;
	public float[] descriptor;

	/** Dummy constructor for Serialization to work properly. */
	public Feature() {
		descriptor = new float[SIFT.getFdTotalLen()];
	}

	public Feature(float s, float o, float[] l, float[] d) {
		scale = s;
		orientation = o;
		location = l;
		descriptor = d;
	}

	/**
	 * comparator for making Features sortable please note, that the comparator
	 * returns -1 for this.scale &gt; o.scale, to sort the features in a
	 * descending order
	 */
	public int compareTo(Feature f) {
		return scale < f.scale ? 1 : scale == f.scale ? 0 : -1;
	}

	public float descriptorDistance(Feature f) {
		float d = 0;
		for (int i = 0; i < descriptor.length; ++i) {
			float a = descriptor[i] - f.descriptor[i];
			d += a * a;
		}
		return (float) Math.sqrt(d);
	}

//	@Override
//	public void readFields(DataInput input) throws IOException {
//		scale = input.readFloat();
//		orientation = input.readFloat();
//		location[0] = input.readFloat();
//		location[1] = input.readFloat();
//		for (int i = 0; i < SIFT.getFdTotalLen(); i++) {
//			descriptor[i] = input.readFloat();
//		}
//	}
//
//	@Override
//	public void write(DataOutput out) throws IOException {
//		out.writeFloat(scale);
//		out.writeFloat(orientation);
//		out.writeFloat(location[0]);
//		out.writeFloat(location[1]);
//		for (int i = 0; i < SIFT.getFdTotalLen(); i++) {
//			out.writeFloat(descriptor[i]);
//		}
//
//	}

}
