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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.extraction.hadoop.datatypes.FeatureMap;

/**
 * 
 * @author Juan Pino
 * @date 7 August 2014
 */
class ProvenanceSource2TargetAddOneSmoothedProbability implements Feature {

	private final static String featureName = "provenance_source2target_addonesmoothed_probability";

	private final static double defaultS2t = -4.7;

	private String provenance;

	public ProvenanceSource2TargetAddOneSmoothedProbability(String provenance) {
		this.provenance = provenance;
	}

	@Override
	public Map<Integer, Double> value(Rule r, FeatureMap mapReduceFeatures,
			Configuration conf) {
		Map<Integer, Double> res = new HashMap<>();
		IntWritable mapreduceFeatureIndex = new IntWritable(conf.getInt(
				featureName + "-" + provenance + "-mapreduce", 0));
		double s2t = 0;
		if (mapReduceFeatures.containsKey(mapreduceFeatureIndex)) {
			s2t = ((DoubleWritable) mapReduceFeatures
					.get(mapreduceFeatureIndex)).get();
		}
		int featureIndex = conf.getInt(featureName + "-" + provenance, 0);
		res.put(featureIndex, s2t == 0 ? defaultS2t : Math.log(s2t));
		return res;
	}

	@Override
	public Map<Integer, Double> valueAsciiOovDeletion(Rule r,
			FeatureMap mapReduceFeatures, Configuration conf) {
		return new HashMap<>();
	}

	@Override
	public Map<Integer, Double> valueGlue(Rule r, FeatureMap mapReduceFeatures,
			Configuration conf) {
		return new HashMap<>();
	}

	@Override
	public int getNumberOfFeatures(Configuration conf) {
		return 1;
	}
}
