package uk.ac.cam.eng.rule.features;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;

import uk.ac.cam.eng.extraction.hadoop.datatypes.FeatureMap;
import uk.ac.cam.eng.extraction.hadoop.datatypes.IntWritableCache;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleData;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

public class FeatureRegistry {

	// Used for provenance features which don't have a probability
	private final static double DEFAULT_PHRASE_LOG_PROB = -4.7;

	private final static double DEFAULT_LEX_VALUE = -40;

	private final List<Feature> allFeatures = new ArrayList<>();

	private final Map<Feature, int[]> indexMappings = new HashMap<>();

	private final int noOfProvs;

	private final double[] emptyNonProv = new double[] { 0 };

	private final double[] emptyProv;

	private final SortedMap<Integer, Double> defaultFeatures;

	private final SortedMap<Integer, Double> defaultOOVFeatures;

	private final SortedMap<Integer, Double> defaultPassThroughFeatures;

	private final SortedMap<Integer, Double> defaultDeletionFeatures;

	private final SortedMap<Integer, Double> defaultGlueFeatures;

	private final SortedMap<Integer, Double> defaultGlueStartOrEndFeatures;

	public FeatureRegistry(String featureString, String provenanceString) {
		String[] featureSplit = featureString.split(",");
		noOfProvs = provenanceString.split(",").length;
		List<Feature> features = new ArrayList<>();
		int indexCounter = 1; // 1-based
		for (String fString : featureSplit) {
			int[] mappings;
			Feature f = Feature.findFromConf(fString);
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
		emptyProv = new double[noOfProvs];
		Arrays.fill(emptyProv, 0.0);
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
	public double[] getEmpty(Feature f) {
		if (Feature.Scope.PROVENANCE == f.scope) {
			return emptyProv;
		} else {
			return emptyNonProv;
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
				defaultFeatures, DEFAULT_PHRASE_LOG_PROB);
		addDefault(Feature.PROVENANCE_TARGET2SOURCE_PROBABILITY,
				defaultFeatures, DEFAULT_PHRASE_LOG_PROB);
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
		addDefault(Feature.SOURCE_2_TARGET_LEXICAL_PROBABILITY,
				defaultFeatures, DEFAULT_LEX_VALUE);
		addDefault(Feature.TARGET_2_SOURCE_LEXICAL_PROBABILITY,
				defaultFeatures, DEFAULT_LEX_VALUE);
		addDefault(Feature.PROVENANCE_SOURCE_2_TARGET_LEXICAL_PROBABILITY,
				defaultFeatures, DEFAULT_LEX_VALUE);
		addDefault(Feature.PROVENANCE_TARGET_2_SOURCE_LEXICAL_PROBABILITY,
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

	/**
	 * If we find a pass through rule in the data then we use its lexical features but nothing else.
	 * Slightly crazy!
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
							int[] indexMappings = getFeatureIndices(f);
							for (int index : indexMappings) {
								IntWritable indexIntW = IntWritableCache
										.createIntWritable(index);
								double ffVal = 0.0;
								if (features.containsKey(indexIntW)) {
									ffVal = features.get(indexIntW).get();
								}
								if (ffVal != 0.0) {
									defaults.put(index, ffVal);
								}
							}
						});
		return defaults;
	}

	public SortedMap<Integer, Double> processFeatures(RuleWritable rule,
			RuleData data) {
		SortedMap<Integer, Double> processedFeatures = getDefaultFeatures();
		FeatureMap features = data.getFeatures();
		for (Feature f : allFeatures) {
			int[] indexMappings = getFeatureIndices(f);
			if (Feature.ComputeLocation.RETRIEVAL == f.computed) {
				double[] results = FeatureFunctionRegistry.computeFeature(f,
						rule, data, this);
				for (int i = 0; i < results.length; ++i) {
					if (results[i] != 0.0) {
						processedFeatures.put(indexMappings[i], results[i]);
					}
				}
			} else {
				for (int index : indexMappings) {
					double ffVal = features.get(
							IntWritableCache.createIntWritable(index)).get();
					if (ffVal != 0.0) {
						processedFeatures.put(index, ffVal);
					}
				}
			}
		}
		return processedFeatures;
	}

}
