package uk.ac.cam.eng.rule.features;

import org.apache.hadoop.io.ByteWritable;

import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.rule.features.FeatureFunctionRegistry.FeatureFunctionInputData;

public final class FeatureFunctions {

	static final double[] one = new double[] { 1 };

	static final double[] minusOne = new double[] { -1 };

	static final double[] empty = new double[] { 0 };

	private static final ByteWritable zeroIndex = new ByteWritable((byte) 0);

	static double[] insertScale(Rule r) {
		// deletion rule
		if (r.getTargetWords().size() == 1 && r.getTargetWords().get(0) == 0) {
			return minusOne;
		}
		// oov rule
		else if (r.getTargetWords().size() == 0) {
			return minusOne;
		}
		return empty;
	}

	static double[] ruleCount1(Rule r, FeatureFunctionInputData data) {
		// 0 element is count over the entire data
		int count = data.counts.get(zeroIndex)
				.get();
		if (count == 1) {
			return one;
		} else
			return empty;
	}

	static double[] ruleCount2(Rule r, FeatureFunctionInputData data) {
		// 0 element is count over the entire data
		int count = data.counts.get(zeroIndex)
				.get();
		if (count == 2) {
			return one;
		} else
			return empty;
	}

	static double[] ruleGreaterThan2(Rule r, FeatureFunctionInputData data) {
		// 0 element is count over the entire data
		int count = data.counts.get(zeroIndex)
				.get();
		if (count > 2) {
			return one;
		}
		return empty;
	}

	static double[] noOfWords(Rule r, FeatureFunctionInputData data) {
		return new double[]{r.nbTargetWords()};
	}

}