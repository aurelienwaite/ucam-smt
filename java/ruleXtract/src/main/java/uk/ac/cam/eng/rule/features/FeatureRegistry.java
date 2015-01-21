package uk.ac.cam.eng.rule.features;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;

import uk.ac.cam.eng.extraction.Rule;
import uk.ac.cam.eng.extraction.hadoop.datatypes.FeatureMap;
import uk.ac.cam.eng.extraction.hadoop.datatypes.IntWritableCache;
import uk.ac.cam.eng.extraction.hadoop.datatypes.ProvenanceProbMap;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleData;

public class FeatureRegistry {

	// Used for provenance features which don't have a probability
	private final static double DEFAULT_S2T_PHRASE_LOG_PROB = -4.7;

	private final static double DEFAULT_T2S_PHRASE_LOG_PROB = -7;
	
	private final static double DEFAULT_LEX_VALUE = -40;

	private final List<Feature> allFeatures;

	private final Map<Feature, int[]> indexMappings = new HashMap<>();

	private final int noOfProvs;

	private final double[] zeroNonProv = new double[] { 0 };

	private final double[] zeroProv;

	private final SortedMap<Integer, Double> defaultFeatures;

	private final SortedMap<Integer, Double> defaultOOVFeatures;

	private final SortedMap<Integer, Double> defaultPassThroughFeatures;

	private final SortedMap<Integer, Double> defaultDeletionFeatures;

	private final SortedMap<Integer, Double> defaultGlueFeatures;

	private final SortedMap<Integer, Double> defaultGlueStartOrEndFeatures;

	private final boolean hasLexicalFeatures;

	public FeatureRegistry(String featureString, String provenanceString) {
		String[] featureSplit = featureString.split(",");
		noOfProvs = provenanceString.split(",").length;
		List<Feature> features = new ArrayList<>();
		int indexCounter = 1; // 1-based
		boolean lexFeatures = false;
		for (String fString : featureSplit) {
			int[] mappings;
			Feature f = Feature.findFromConf(fString);
			lexFeatures |= Feature.ComputeLocation.LEXICAL_SERVER == f.computed;
			features.add(f);
			if (Feature.Scope.PROVENANCE == f.scope) {
				mappings = new int[noOfProvs];
				for (int i = 0; i < noOfProvs; ++i) {
					mappings[i] = indexCounter++;
				}
			} else {
				mappings = new int[] { indexCounter++ };
			}
			indexMappings.put(f, mappings);
		}
		allFeatures = Collections.unmodifiableList(features);
		zeroProv = new double[noOfProvs];
		hasLexicalFeatures = lexFeatures;
		Arrays.fill(zeroProv, 0.0);
		defaultFeatures = createDefaultData();
		defaultOOVFeatures = createOOVDefaultData();
		defaultPassThroughFeatures = createPassThroughDefaultData();
		defaultDeletionFeatures = createDeletionDefaultData();
		defaultGlueFeatures = createGlueDefaultData();
		defaultGlueStartOrEndFeatures = createGlueStartOrEndDefaultData();
	}

	public int[] getFeatureIndices(Feature... features) {
		List<int[]> mappings = new ArrayList<int[]>(features.length);
		int totalSize = 0;
		for (Feature feature : features) {
			if (!indexMappings.containsKey(feature)) {
				throw new IllegalArgumentException("Feature "
						+ feature.getConfName() + " is not in the registry");
			}
			int[] mapping = indexMappings.get(feature);
			mappings.add(mapping);
			totalSize += mapping.length;
		}
		int[] result = new int[totalSize];
		int counter = 0;
		for (int[] mapping : mappings) {
			for (int index : mapping) {
				result[counter++] = index;
			}
		}
		return result;
	}

	public boolean containsFeature(Feature f) {
		return allFeatures.contains(f);
	}

	public List<Feature> getFeatures() {
		return allFeatures;
	}

	/**
	 * The number of provanences, not including the global (all) provenance.
	 * 
	 * @return
	 */
	public int getNoOfProvs() {
		return noOfProvs;
	}

	/**
	 * An array of zeros appropriately sized for provenance.
	 * 
	 * Do not write to the arrays returned from this function. They are cached
	 * to reduce object allocation during retrieval
	 * 
	 * @param f
	 * @return An array of zeros
	 */
	public double[] getZeros(Feature f) {
		if (Feature.Scope.PROVENANCE == f.scope) {
			return zeroProv;
		} else {
			return zeroNonProv;
		}
	}

	private void addDefault(Feature f, SortedMap<Integer, Double> vals,
			double val) {
		if (allFeatures.contains(f)) {
			int[] mappings = getFeatureIndices(f);
			for (int mapping : mappings) {
				vals.put(mapping, val);
			}
		}
	}

	/**
	 * If any default values needed to be created then put them in this function
	 * 
	 * @return
	 */
	private SortedMap<Integer, Double> createDefaultData() {
		// Provenance phrase probabilities need default values
		SortedMap<Integer, Double> defaultFeatures = new TreeMap<Integer, Double>();
		addDefault(Feature.PROVENANCE_SOURCE2TARGET_PROBABILITY,
				defaultFeatures, DEFAULT_S2T_PHRASE_LOG_PROB);
		addDefault(Feature.PROVENANCE_TARGET2SOURCE_PROBABILITY,
				defaultFeatures, DEFAULT_T2S_PHRASE_LOG_PROB);
		addDefault(Feature.RULE_INSERTION_PENALTY, defaultFeatures, 1d);
		return defaultFeatures;
	}

	/**
	 * Create data for OOVs with default vals for the lexical probs
	 * 
	 * @return
	 */
	private SortedMap<Integer, Double> createPassThroughDefaultData() {
		// We need to add default values for lexical probs
		SortedMap<Integer, Double> defaultFeatures = new TreeMap<Integer, Double>();
		addDefault(Feature.SOURCE2TARGET_LEXICAL_PROBABILITY, defaultFeatures,
				DEFAULT_LEX_VALUE);
		addDefault(Feature.TARGET2SOURCE_LEXICAL_PROBABILITY, defaultFeatures,
				DEFAULT_LEX_VALUE);
		addDefault(Feature.PROVENANCE_SOURCE2TARGET_LEXICAL_PROBABILITY,
				defaultFeatures, DEFAULT_LEX_VALUE);
		addDefault(Feature.PROVENANCE_TARGET2SOURCE_LEXICAL_PROBABILITY,
				defaultFeatures, DEFAULT_LEX_VALUE);
		return defaultFeatures;
	}

	private SortedMap<Integer, Double> createOOVDefaultData() {
		SortedMap<Integer, Double> defaultFeatures = new TreeMap<Integer, Double>();
		addDefault(Feature.INSERT_SCALE, defaultFeatures, -1d);
		return defaultFeatures;
	}

	private SortedMap<Integer, Double> createDeletionDefaultData() {
		SortedMap<Integer, Double> defaultFeatures = new TreeMap<Integer, Double>();
		addDefault(Feature.INSERT_SCALE, defaultFeatures, -1d);
		return defaultFeatures;
	}

	private SortedMap<Integer, Double> createGlueDefaultData() {
		SortedMap<Integer, Double> defaultFeatures = new TreeMap<Integer, Double>();
		addDefault(Feature.GLUE_RULE, defaultFeatures, 1d);
		return defaultFeatures;
	}

	private SortedMap<Integer, Double> createGlueStartOrEndDefaultData() {
		SortedMap<Integer, Double> defaultFeatures = new TreeMap<Integer, Double>();
		addDefault(Feature.RULE_COUNT_GREATER_THAN_2, defaultFeatures, 1d);
		addDefault(Feature.RULE_INSERTION_PENALTY, defaultFeatures, 1d);
		addDefault(Feature.WORD_INSERTION_PENALTY, defaultFeatures, 1d);
		return defaultFeatures;
	}

	public SortedMap<Integer, Double> getDefaultFeatures() {
		return new TreeMap<Integer, Double>(defaultFeatures);
	}

	public SortedMap<Integer, Double> getDefaultOOVFeatures() {
		return new TreeMap<Integer, Double>(defaultOOVFeatures);
	}

	public SortedMap<Integer, Double> getDefaultDeletionFeatures() {
		return new TreeMap<Integer, Double>(defaultDeletionFeatures);
	}

	public SortedMap<Integer, Double> getDefaultGlueFeatures() {
		return new TreeMap<Integer, Double>(defaultGlueFeatures);
	}

	public SortedMap<Integer, Double> getDefaultGlueStartOrEndFeatures() {
		return new TreeMap<Integer, Double>(defaultGlueStartOrEndFeatures);
	}

	public SortedMap<Integer, Double> getDefaultPassThroughRuleFeatures() {
		return new TreeMap<Integer, Double>(defaultPassThroughFeatures);
	}

	private static final ProvenanceProbMap checkedGetProbs(Feature f,
			FeatureMap features) {
		ProvenanceProbMap probs = features.get(f);
		if (probs == null) {
			throw new RuntimeException("No data for feature " + f.getConfName());
		}
		return probs;
	}

	/**
	 * If we find a pass through rule in the data then we use its lexical
	 * features but nothing else. Slightly crazy!
	 * 
	 * @param features
	 * @param defaults
	 * @return
	 */
	public SortedMap<Integer, Double> createFoundPassThroughRuleFeatures(
			FeatureMap features) {
		SortedMap<Integer, Double> defaults = getDefaultPassThroughRuleFeatures();
		allFeatures
				.stream()
				.filter((f) -> Feature.ComputeLocation.LEXICAL_SERVER == f.computed)
				.forEach(
						(f) -> {
							int[] mappings = indexMappings.get(f);
							ProvenanceProbMap probs = checkedGetProbs(f,
									features);
							for (int index : mappings) {
								IntWritable indexIntW = IntWritableCache
										.createIntWritable(index);
								double ffVal = 0.0;
								if (probs.containsKey(indexIntW)) {
									ffVal = probs.get(indexIntW).get();
								}
								if (ffVal != 0.0) {
									defaults.put(index, ffVal);
								}
							}
						});
		return defaults;
	}

	private static void setVal(int mapping, double val,
			SortedMap<Integer, Double> features) {
		// Default val in sparse tuple arc is 0. Delete default val.
		if (val == 0.0) {
			features.remove(mapping);
		} else {
			features.put(mapping, val);
		}
	}

	public SortedMap<Integer, Double> processFeatures(Rule rule,
			RuleData data) {
		SortedMap<Integer, Double> processedFeatures = getDefaultFeatures();
		for (Feature f : allFeatures) {
			int[] mappings = indexMappings.get(f);
			if (Feature.ComputeLocation.RETRIEVAL == f.computed) {
				double[] results = FeatureFunctionRegistry.computeFeature(f,
						rule, data, this);
				if(results == null){
					continue;
				}
				for (int i = 0; i < results.length; ++i) {
					setVal(mappings[i], results[i], processedFeatures);
				}
			} else {
				ProvenanceProbMap probs = checkedGetProbs(f, data.getFeatures());
				for (int i = 0; i < mappings.length; ++i) {
					//Provenances are 1-indexed with the 0th element reserved for the global
					//scope.
					int index= Feature.Scope.PROVENANCE == f.scope ? i+1 : i;
					if (probs
							.containsKey(IntWritableCache.createIntWritable(index))) {
						double ffVal = probs
								.get(IntWritableCache
										.createIntWritable(index)).get();
						setVal(mappings[i], ffVal, processedFeatures);
					}
				}
			}
		}
		return processedFeatures;
	}

	public boolean hasLexicalFeatures() {
		return hasLexicalFeatures;
	}

}
