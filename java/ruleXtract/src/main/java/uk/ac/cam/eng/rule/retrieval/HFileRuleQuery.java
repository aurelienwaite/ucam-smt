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
package uk.ac.cam.eng.rule.retrieval;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;

import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.hbase.util.BloomFilter;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;

import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleData;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;
import uk.ac.cam.eng.extraction.hadoop.features.lexical.TTableClient;
import uk.ac.cam.eng.extraction.hadoop.merge.MergeComparator;
import uk.ac.cam.eng.rulebuilding.features.EnumRuleType;
import uk.ac.cam.eng.util.CLI;
import uk.ac.cam.eng.util.Pair;

/**
 * 
 * @author Aurelien Waite
 * @date 28 May 2014
 */
class HFileRuleQuery implements Runnable {

	private static final int BATCH_SIZE = 1000;

	private final HFileRuleReader reader;

	private final BloomFilter bf;

	private final BufferedWriter out;

	private final Collection<Text> query;

	private final RuleRetriever retriever;

	private final TTableClient s2tClient;

	private final TTableClient t2sClient;

	private final LinkedList<Pair<RuleWritable, RuleData>> queue = new LinkedList<>();

	private DataOutputBuffer tempOut = new DataOutputBuffer();

	public HFileRuleQuery(HFileRuleReader reader, BloomFilter bf,
			BufferedWriter out, Collection<Text> query,
			RuleRetriever retriever, CLI.ServerParams params) {
		this.reader = reader;
		this.bf = bf;
		this.out = out;
		this.query = query;
		this.retriever = retriever;
		this.s2tClient = new TTableClient();
		this.t2sClient = new TTableClient();
		s2tClient.setup(params, retriever.fReg, true);
		t2sClient.setup(params, retriever.fReg, false);
	}

	private void drainQueue() throws IOException {
		s2tClient.queryRules(queue);
		t2sClient.queryRules(queue);
		for (Pair<RuleWritable, RuleData> entry : queue) {
			RuleWritable rule = entry.getFirst();
			RuleData rawFeatures = entry.getSecond();
			if (retriever.passThroughRules.contains(rule)) {
				RuleWritable asciiRule = new RuleWritable(rule);
				asciiRule.setLeftHandSide(new Text(
						EnumRuleType.PASSTHROUGH_OOV_DELETE.getLhs()));
				synchronized (retriever.foundPassThroughRules) {
					retriever.foundPassThroughRules.add(asciiRule);
				}
				retriever.writeRule(rule, retriever.fReg
						.createFoundPassThroughRuleFeatures(rawFeatures
								.getFeatures()), out);
			} else {
				SortedMap<Integer, Double> processed = retriever.fReg
						.processFeatures(rule, rawFeatures);
				retriever.writeRule(rule, processed, out);
			}

		}
		queue.clear();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		List<Text> sortedQuery = new ArrayList<>(query);
		query.clear();
		StopWatch stopWatch = new StopWatch();
		System.out.println("Sorting query");
		stopWatch.start();
		Collections.sort(sortedQuery, new MergeComparator());
		System.out.printf("Query sort took %d seconds\n",
				stopWatch.getTime() / 1000);
		stopWatch.reset();
		stopWatch.start();
		try {
			for (Text source : sortedQuery) {
				SidePattern sourcePattern = SidePattern.getPattern(source
						.toString());

				tempOut.reset();
				source.write(tempOut);
				if (!bf.contains(tempOut.getData(), 0, tempOut.getLength(),
						null)) {
					continue;
				}
				if (reader.seek(source)) {
					if (retriever.testVocab.contains(source)) {
						synchronized (retriever.foundTestVocab) {
							retriever.foundTestVocab.add(source);
						}
					}
					Iterable<Pair<RuleWritable, RuleData>> rules = reader
							.getRulesForSource();
					List<Pair<RuleWritable, RuleData>> filtered = retriever.filter
							.filter(sourcePattern, rules);
					queue.addAll(filtered);
					if (queue.size() > BATCH_SIZE) {
						drainQueue();
					}
				}
			}
			drainQueue();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		System.out
				.printf("Query took %d seconds\n", stopWatch.getTime() / 1000);

	}
}
