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

package uk.ac.cam.eng.rulebuilding.features;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.extraction.hadoop.datatypes.FeatureMap;

/**
 * 
 * @author Juan Pino
 * @date 28 May 2014
 */
public class RuleCount2 implements Feature {

	private final static String featureName = "rule_count_2";

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * uk.ac.cam.eng.rulebuilding.features.Feature#value(uk.ac.cam.eng.extraction
	 * .datatypes.Rule, org.apache.hadoop.io.ArrayWritable)
	 */
	@Override
	public Map<Integer, Double> value(Rule r, FeatureMap mapReduceFeatures,
			Configuration conf) {
		Map<Integer, Double> res = new HashMap<>();
		// the count always comes after the s2t probability
		IntWritable mapreduceFeatureIndex = new IntWritable(conf.getInt(
				"source2target_probability-mapreduce", 0) + 1);
		int count = 0;
		if (mapReduceFeatures.containsKey(mapreduceFeatureIndex)) {
			count = (int) mapReduceFeatures.get(mapreduceFeatureIndex).get();
		}
		int featureIndex = conf.getInt(featureName, 0);
		if (count == 2) {
			res.put(featureIndex, 1d);
		}
		return res;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * uk.ac.cam.eng.rulebuilding.features.Feature#valueAsciiOovDeletion(uk.
	 * ac.cam.eng.extraction.datatypes.Rule, org.apache.hadoop.io.ArrayWritable)
	 */
	@Override
	public Map<Integer, Double> valueAsciiOovDeletion(Rule r,
			FeatureMap mapReduceFeatures, Configuration conf) {
		return new HashMap<>();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see uk.ac.cam.eng.rulebuilding.features.Feature#valueGlue(uk.ac.cam.eng.
	 * extraction.datatypes.Rule, org.apache.hadoop.io.ArrayWritable)
	 */
	@Override
	public Map<Integer, Double> valueGlue(Rule r, FeatureMap mapReduceFeatures,
			Configuration conf) {
		return new HashMap<>();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * uk.ac.cam.eng.rulebuilding.features.Feature#getNumberOfFeatures(org.apache
	 * .hadoop.conf.Configuration)
	 */
	@Override
	public int getNumberOfFeatures(Configuration conf) {
		return 1;
	}
}
