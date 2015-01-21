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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.hbase.util.BloomFilter;
import org.apache.hadoop.io.DataOutputBuffer;

import uk.ac.cam.eng.extraction.Rule;
import uk.ac.cam.eng.extraction.WritableArrayBuffer;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleData;
import uk.ac.cam.eng.extraction.hadoop.features.lexical.TTableClient;
import uk.ac.cam.eng.extraction.hadoop.merge.MergeComparator;
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

	private final Collection<WritableArrayBuffer> query;

	private final RuleRetriever retriever;

	private final TTableClient s2tClient;

	private final TTableClient t2sClient;

	private final Map<Rule, RuleData> queue = new HashMap<>();

	private DataOutputBuffer tempOut = new DataOutputBuffer();

	public HFileRuleQuery(HFileRuleReader reader, BloomFilter bf,
			BufferedWriter out, Collection<WritableArrayBuffer> query,
			RuleRetriever retriever, CLI.ServerParams params) {
		this.reader = reader;
		this.bf = bf;
		this.out = out;
		this.query = query;
		this.retriever = retriever;
		this.s2tClient = new TTableClient();
		this.t2sClient = new TTableClient();
		if (retriever.fReg.hasLexicalFeatures()) {
			s2tClient.setup(params, retriever.fReg.getNoOfProvs(), true);
			t2sClient.setup(params, retriever.fReg.getNoOfProvs(), false);
		}
	}

	private void drainQueue() throws IOException {
		if (retriever.fReg.hasLexicalFeatures()) {
			s2tClient.queryRules(queue);
			t2sClient.queryRules(queue);
		}
		for (Entry<Rule, RuleData> entry : queue.entrySet()) {
			Rule rule = entry.getKey();
			RuleData rawFeatures = entry.getValue();
			if (retriever.passThroughRules.contains(rule)) {
				Rule asciiRule = new Rule(rule);
				synchronized (retriever.foundPassThroughRules) {
					retriever.foundPassThroughRules.add(asciiRule);
				}
				RuleRetriever.writeRule(EnumRuleType.PASSTHROUGH_OOV_DELETE.getLhs(), rule, retriever.fReg
						.createFoundPassThroughRuleFeatures(rawFeatures
								.getFeatures()), out);
			} else {
				SortedMap<Integer, Double> processed = retriever.fReg
						.processFeatures(rule, rawFeatures);
				RuleRetriever.writeRule(EnumRuleType.EXTRACTED.getLhs(), rule, processed, out);
			}

		}
		queue.clear();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		List<WritableArrayBuffer> sortedQuery = new ArrayList<>(query);
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
			for (WritableArrayBuffer source : sortedQuery) {
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
					List<Pair<Rule, RuleData>> rules = new ArrayList<>();
					for(Pair<Rule, RuleData> entry : reader.getRulesForSource()){
						rules.add(Pair.createPair(new Rule(entry.getFirst()), new RuleData(entry.getSecond())));
					}
					Map<Rule, RuleData> filtered = retriever.filter
							.filter(source.toPattern(), rules);
					queue.putAll(filtered);
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
