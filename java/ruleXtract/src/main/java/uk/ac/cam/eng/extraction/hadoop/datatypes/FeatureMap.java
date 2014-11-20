package uk.ac.cam.eng.extraction.hadoop.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.EnumMap;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import uk.ac.cam.eng.rule.features.Feature;

public class FeatureMap extends EnumMap<Feature, ProvenanceProbMap> implements Writable {

	private static final Feature[] enums = Feature.values();
	
	public FeatureMap() {
		super(Feature.class);
	}
	
	public FeatureMap(FeatureMap other){
		this();
		for (Entry<Feature, ProvenanceProbMap> entry : other.entrySet()){
			put(entry.getKey(), new ProvenanceProbMap(entry.getValue()));
		}
	}

	private static final long serialVersionUID = 1L;
	
	static final FeatureMap EMPTY = new FeatureMap() {

		private static final long serialVersionUID = 1L;

		public ProvenanceProbMap put(Feature key, ProvenanceProbMap value) {
			throw new UnsupportedOperationException();
		};
	};
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		clear();
		int noOfKeys = WritableUtils.readVInt(in);
		for(int i=0;i<noOfKeys;++i){
			int ordinal = WritableUtils.readVInt(in);
			Feature f = enums[ordinal];
			ProvenanceProbMap map = new ProvenanceProbMap();
			map.readFields(in);
			put(f, map);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVInt(out, size());
		for(Entry<Feature, ProvenanceProbMap> entry : entrySet()){
			WritableUtils.writeVInt(out,entry.getKey().ordinal());
			entry.getValue().write(out);
		}
	}
	
	void merge(FeatureMap other) {
		int expectedSize = size() + other.size();
		putAll(other);
		if (expectedSize != size()) {
			throw new RuntimeException("Two features with the same id: " + this
					+ " " + other + " expected size = " + expectedSize);
		}
	}

}
