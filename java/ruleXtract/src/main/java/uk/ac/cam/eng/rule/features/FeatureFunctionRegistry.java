package uk.ac.cam.eng.rule.features;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.extraction.hadoop.datatypes.AlignmentCountMapWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.FeatureMap;
import uk.ac.cam.eng.extraction.hadoop.datatypes.ProvenanceCountMap;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleData;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;
import uk.ac.cam.eng.rule.features.Feature.ComputeLocation;

/**
 * 
 * Feature functions for all retrieval time features
 * 
 * @author Aurelien Waite
 *
 */
public final class FeatureFunctionRegistry {

	static class FeatureFunctionInputData {

		AlignmentCountMapWritable alignments;

		FeatureMap features;
		
		ProvenanceCountMap counts;

		Feature requested;

		FeatureRegistry fReg;

		private FeatureFunctionInputData() {

		}

		FeatureFunctionInputData build(RuleData data, Feature requested, FeatureRegistry fReg) {
			alignments = data.getAlignments();
			features = data.getFeatures();
			counts = data.getProvCounts();
			this.requested = requested;
			this.fReg = fReg;
			return this;
		} 

	}
	
	private static ThreadLocal<FeatureFunctionInputData> ffInput = new ThreadLocal<FeatureFunctionInputData>(){

		@Override
		protected FeatureFunctionInputData initialValue() {
			return new FeatureFunctionInputData();
		}
		
	};
	

	private static Map<Feature, BiFunction<Rule, FeatureFunctionInputData, double[]>> featureFunctions = new HashMap<>();

	static double[] computeFeature(Feature feature, RuleWritable rule,
			RuleData data, FeatureRegistry fReg) {
		if (ComputeLocation.MAP_REDUCE == feature.computed) {
			throw new UnsupportedOperationException(
					"Attempting to compute the MapReduce featue: "
							+ feature.getConfName() + " at retrieval time");
		}
		BiFunction<Rule, FeatureFunctionInputData, double[]> f
			= featureFunctions.get(feature);
		if (f == null) {
			return null;
		}
		return f.apply(new Rule(rule), ffInput.get().build(data, feature, fReg));
	}
	

	private static void registerFunction(Feature f,
			BiFunction<Rule, FeatureFunctionInputData, double[]> ff) {
		if (Feature.ComputeLocation.RETRIEVAL != f.computed) {
			throw new UnsupportedOperationException(
					"Trying to register feature functions for a non-retrieval time feature");
		}
		featureFunctions.put(f, ff);
	}
	
	
	
	//Register functions!	
	static{
		registerFunction(Feature.RULE_COUNT_1, 
				FeatureFunctions::ruleCount1);
		registerFunction(Feature.RULE_COUNT_2, 
				FeatureFunctions::ruleCount2);
		registerFunction(Feature.RULE_COUNT_GREATER_THAN_2, FeatureFunctions::ruleGreaterThan2);
		registerFunction(Feature.WORD_INSERTION_PENALTY, 
				FeatureFunctions::noOfWords);
	}
}
