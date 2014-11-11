/*******************************************************************************
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use these files except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright 2014 - Juan Pino, Aurelien Waite, William Byrne
 *******************************************************************************/
/**
 * 
 */

package uk.ac.cam.eng.rule.retrieval;

// TODO remove hard coded indices

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Consumer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleData;
import uk.ac.cam.eng.extraction.hadoop.datatypes.FeatureMap;
import uk.ac.cam.eng.extraction.hadoop.datatypes.IntWritableCache;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;
import uk.ac.cam.eng.rule.features.Feature;
import uk.ac.cam.eng.rule.features.FeatureRegistry;
import uk.ac.cam.eng.util.CLI;
import uk.ac.cam.eng.util.Pair;

/**
 * This class filters rules according to constraints
 * 
 * @author Juan Pino
 * @author Aurelien Waite
 * @date 28 May 2014
 */
class RuleFilter {

	private static class RuleCountComparator implements
			Comparator<Pair<RuleWritable, RuleData>> {

		private final IntWritable countIndex;

		public RuleCountComparator(IntWritable countIndex) {
			this.countIndex = countIndex;
		}

		public int compare(Pair<RuleWritable, RuleData> a,
				Pair<RuleWritable, RuleData> b) {
			// We want descending order!
			int countDiff = b.getSecond().getProvCounts().get(countIndex)
					.compareTo(a.getSecond().getProvCounts().get(countIndex));
			if (countDiff != 0) {
				return countDiff;
			} else {
				return (a.getFirst().compareTo(b.getFirst()));
			}
		}
	}

	private static class SourcePhraseConstraint {
		// Number of Occurrences
		final int nOcc;
		// Number of Translations
		final int nTrans;

		public SourcePhraseConstraint(String nOcc, String nTrans) {
			this.nOcc = Integer.parseInt(nOcc);
			this.nTrans = Integer.parseInt(nTrans);
		}
	}

	// TODO put the default in the code
	private double minSource2TargetPhrase;
	private double minTarget2SourcePhrase;
	private double minSource2TargetRule;
	private double minTarget2SourceRule;
	// allowed patterns
	private Set<RulePattern> allowedPatterns = new HashSet<RulePattern>();
	private Map<SidePattern, SourcePhraseConstraint> sourcePatternConstraints = new HashMap<>();
	// decides whether to keep all the rules that fall within the number
	// of translations per source threshold in case of a tie

	private final int[] s2tIndices;
	private final int[] t2sIndices;
	// cache the comparators
	private final RuleCountComparator[] comparators;

	public RuleFilter(CLI.FilterParams params, FeatureRegistry fReg)
			throws FileNotFoundException, IOException {
		// when using provenance features, we can either keep the rules coming
		// from the main table and add features corresponding to the provenance
		// tables (default) or keep the union of the rules coming from the main
		// and provenance tables
		// TODO: Need prov union check here
		if (params.provenanceUnion) {
			s2tIndices = fReg.getFeatureIndices(
					Feature.SOURCE2TARGET_PROBABILITY,
					Feature.PROVENANCE_SOURCE2TARGET_PROBABILITY);
			t2sIndices = fReg.getFeatureIndices(
					Feature.TARGET2SOURCE_PROBABILITY,
					Feature.PROVENANCE_TARGET2SOURCE_PROBABILITY);
		} else {
			s2tIndices = fReg
					.getFeatureIndices(Feature.SOURCE2TARGET_PROBABILITY);
			t2sIndices = fReg
					.getFeatureIndices(Feature.TARGET2SOURCE_PROBABILITY);
		}
		// No of provanences + the global scope
		comparators = new RuleCountComparator[fReg.getNoOfProvs() + 1];
		for (int i = 0; i < comparators.length; ++i) {
			comparators[i] = new RuleCountComparator(
					IntWritableCache.createIntWritable(i));
		}
		loadConfig(params.filterConfig,
				line -> allowedPatterns.add(RulePattern.parsePattern(line)));
		loadConfig(params.sourcePatterns,
				line -> {
					String[] parts = line.split(" ");
					if (parts.length != 3) {
						throw new RuntimeException(
								"line should have 3 fields: " + line);
					}
					sourcePatternConstraints.put(
							SidePattern.parsePattern(parts[0]),
							new SourcePhraseConstraint(parts[1], parts[2]));
				});
	}

	private void loadConfig(String fileName, Consumer<String> block)
			throws FileNotFoundException, IOException {
		try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
			for (String line = br.readLine(); line != null; line = br
					.readLine()) {
				if (line.startsWith("#") || line.isEmpty()) {
					continue;
				}
				block.accept(line);
			}
		}
	}

	public boolean filterSource(Text source) {
		SidePattern sourcePattern = SidePattern.getPattern(source.toString());
		if (sourcePattern.isPhrase()) {
			return false;
		} else if (sourcePatternConstraints.containsKey(sourcePattern)) {
			return false;
		}
		return true;
	}

	private List<Pair<RuleWritable, RuleData>> filterRulesBySource(
			SidePattern sourcePattern,
			SortedSet<Pair<RuleWritable, RuleData>> rules, int provMapping) {
		List<Pair<RuleWritable, RuleData>> results = new ArrayList<>();
		int numberTranslations = 0;
		int numberTranslationsMonotone = 0; // case with more than 1 NT
		int numberTranslationsInvert = 0;
		int prevCount = -1;
		IntWritable countIndex = IntWritableCache
				.createIntWritable(provMapping);
		for (Pair<RuleWritable, RuleData> entry : rules) {
			// number of translations per source threshold
			// in case of ties we either keep or don't keep the ties
			// depending on the config

			RulePattern rulePattern = RulePattern.getPattern(entry.getFirst(),
					entry.getFirst());
			int count = (int) entry.getSecond().getProvCounts().get(countIndex)
					.get();
			if (!sourcePattern.isPhrase() && sourcePattern.hasMoreThan1NT()
					&& count != prevCount) {
				double nTransConstraint = sourcePatternConstraints
						.get(sourcePattern).nTrans;
				if (nTransConstraint <= numberTranslationsMonotone
						&& nTransConstraint <= numberTranslationsInvert
						&& nTransConstraint <= numberTranslations) {
					break;
				}
			}
			results.add(entry);
			if (sourcePattern.hasMoreThan1NT()) {
				if (rulePattern.isSwappingNT()) {
					numberTranslationsInvert++;
				} else {
					numberTranslationsMonotone++;
				}
			}
			numberTranslations++;
			prevCount = count;
		}
		return results;
	}

	/**
	 * Indicates whether we should filter a rule. The decision is based on a
	 * particular provenance.
	 * 
	 * @param sourcePattern
	 *            The source pattern (e.g. w X)
	 * @param rule
	 *            The rule
	 * @param features
	 *            The features for the rule. The filtering criteria are based on
	 *            these features.
	 * @param provMapping
	 *            The provenance used as a criterion for filtering.
	 * @return
	 */
	private boolean filterRule(SidePattern sourcePattern, RuleWritable rule,
			FeatureMap features, int provMapping) {
		int s2tIndex = s2tIndices[provMapping];
		IntWritable source2targetProbabilityIndex = IntWritableCache
				.createIntWritable(s2tIndex);
		// if the rule does not have that provenance, we automatically filter it
		if (!features.containsKey(source2targetProbabilityIndex)) {
			return true;
		}
		int t2sIndex = t2sIndices[provMapping];
		IntWritable target2sourceProbabilityIndex = IntWritableCache
				.createIntWritable(t2sIndex);
		IntWritable countIndex = IntWritableCache
				.createIntWritable(s2tIndex + 1);

		double source2targetProbability = features.get(
				source2targetProbabilityIndex).get();
		double target2sourceProbability = features.get(
				target2sourceProbabilityIndex).get();
		int numberOfOccurrences = (int) features.get(countIndex).get();

		RulePattern rulePattern = RulePattern.getPattern(rule, rule);
		if (!sourcePattern.isPhrase() && !allowedPatterns.contains(rulePattern)) {
			return true;
		}
		if (sourcePattern.isPhrase()) {
			// source-to-target threshold
			if (source2targetProbability <= minSource2TargetPhrase) {
				return true;
			}
			// target-to-source threshold
			if (target2sourceProbability <= minTarget2SourcePhrase) {
				return true;
			}
		} else {
			// source-to-target threshold
			if (source2targetProbability <= minSource2TargetRule) {
				return true;
			}
			// target-to-source threshold
			if (target2sourceProbability <= minTarget2SourceRule) {
				return true;
			}
			// minimum number of occurrence threshold
			if (numberOfOccurrences < sourcePatternConstraints
					.get(sourcePattern).nOcc) {
				return true;
			}
		}
		return false;
	}

	public List<Pair<RuleWritable, RuleData>> filter(SidePattern sourcePattern,
			Iterable<Pair<RuleWritable, RuleData>> toFilter) {
		Set<RuleWritable> existingRules = new HashSet<>();
		List<Pair<RuleWritable, RuleData>> allFiltered = new ArrayList<>();

		// Assume that all mapping arrays (s2tIndices, t2sIndices, countIndices)
		// have the same length
		for (int i = 0; i < s2tIndices.length; ++i) {
			SortedSet<Pair<RuleWritable, RuleData>> rules = new TreeSet<Pair<RuleWritable, RuleData>>(
					comparators[i]);
			for (Pair<RuleWritable, RuleData> entry : toFilter) {
				RuleWritable rule = entry.getFirst();
				RuleData rawFeatures = entry.getSecond();
				if (filterRule(sourcePattern, rule, rawFeatures.getFeatures(), i)) {
					continue;
				}
				rules.add(Pair.createPair(new RuleWritable(rule), rawFeatures));
			}
			List<Pair<RuleWritable, RuleData>> filtered = filterRulesBySource(
					sourcePattern, rules, i);
			for (Pair<RuleWritable, RuleData> ruleFiltered : filtered) {
				if (!existingRules.contains(ruleFiltered.getFirst())) {
					allFiltered.add(ruleFiltered);
					existingRules.add(ruleFiltered.getFirst());
				}
			}
		}
		return allFiltered;
	}

	public Collection<SidePattern> getPermittedSourcePatterns() {
		return sourcePatternConstraints.keySet();
	}
}
