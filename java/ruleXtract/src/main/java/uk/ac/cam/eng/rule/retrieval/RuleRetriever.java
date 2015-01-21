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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.util.BloomFilter;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import uk.ac.cam.eng.extraction.Rule;
import uk.ac.cam.eng.extraction.S;
import uk.ac.cam.eng.extraction.Symbol;
import uk.ac.cam.eng.extraction.WritableArrayBuffer;
import uk.ac.cam.eng.extraction.X;
import uk.ac.cam.eng.extraction.hadoop.util.Util;
import uk.ac.cam.eng.rule.features.FeatureRegistry;
import uk.ac.cam.eng.util.CLI;
import uk.ac.cam.eng.util.Pair;

import com.beust.jcommander.ParameterException;

/**
 * 
 * @author Juan Pino
 * @author Aurelien Waite
 * @date 28 May 2014
 * 
 *       Aurelien's improvements to the retriever over Juan's original
 *       implementation include: 1) A better search algorithm that does not
 *       query unigrams out of order of the main search 2) A more compact HFile
 *       format, which results in much smaller HFiles and faster access 3)
 *       Multithreaded access to HFile partitions.
 */
public class RuleRetriever {

	private BloomFilter[] bfs;

	private HFileRuleReader[] readers;

	private Partitioner<WritableArrayBuffer, NullWritable> partitioner = new HashPartitioner<>();

	RuleFilter filter;

	private String passThroughRulesFileName;

	Set<Rule> passThroughRules;

	Set<Rule> foundPassThroughRules = new HashSet<>();

	Set<WritableArrayBuffer> testVocab;

	Set<WritableArrayBuffer> foundTestVocab = new HashSet<>();

	private int maxSourcePhrase;

	FeatureRegistry fReg;

	private void setup(String testFile, CLI.RuleRetrieverParameters params)
			throws FileNotFoundException, IOException {
		fReg = new FeatureRegistry(params.features.features,
				params.rp.prov.provenance);
		passThroughRulesFileName = params.passThroughRules;
		filter = new RuleFilter(params.fp, fReg);
		maxSourcePhrase = params.rp.maxSourcePhrase;
		passThroughRules = getPassThroughRules();
		Set<WritableArrayBuffer> fullTestVocab = getTestVocab(testFile);
		Set<WritableArrayBuffer> passThroughVocab = getPassThroughVocab();
		testVocab = new HashSet<>();
		for (WritableArrayBuffer word : fullTestVocab) {
			if (!passThroughVocab.contains(word)) {
				testVocab.add(word);
			}
		}
	}

	private void loadDir(String dirString) throws IOException {
		File dir = new File(dirString);
		Configuration conf = new Configuration();
		CacheConfig cacheConf = new CacheConfig(conf);
		if (!dir.isDirectory()) {
			throw new IOException(dirString + " is not a directory!");
		}
		File[] names = dir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith("hfile");
			}
		});
		if (names.length == 0) {
			throw new IOException("No hfiles in " + dirString);
		}
		bfs = new BloomFilter[names.length];
		readers = new HFileRuleReader[names.length];
		for (File file : names) {
			String name = file.getName();
			int i = Integer.parseInt(name.substring(7, 12));
			HFile.Reader hfReader = HFile.createReader(
					FileSystem.getLocal(conf), new Path(file.getPath()),
					cacheConf);
			bfs[i] = BloomFilterFactory.createFromMeta(
					hfReader.getBloomFilterMetadata(), hfReader);
			readers[i] = new HFileRuleReader(hfReader);
		}
	}

	private Set<Rule> getPassThroughRules() throws IOException {
		Set<Rule> res = new HashSet<>();
		try (BufferedReader br = new BufferedReader(new FileReader(
				passThroughRulesFileName))) {
			String line;
			Pattern regex = Pattern.compile(".*: (.*) # (.*)");
			Matcher matcher;
			while ((line = br.readLine()) != null) {
				matcher = regex.matcher(line);
				if (matcher.matches()) {
					String[] sourceString = matcher.group(1).split(" ");
					String[] targetString = matcher.group(2).split(" ");
					if (sourceString.length != targetString.length) {
						System.err
								.println("Malformed pass through rules file: "
										+ passThroughRulesFileName);
						System.exit(1);
					}
					List<Symbol> source = new ArrayList<Symbol>();
					List<Symbol> target = new ArrayList<Symbol>();
					int i = 0;
					while (i < sourceString.length) {
						if (i % maxSourcePhrase == 0 && i > 0) {
							Rule rule = new Rule(source, target);
							res.add(rule);
							source.clear();
							target.clear();
						}
						source.add(Symbol.deserialise(Integer.parseInt(sourceString[i])));
						target.add(Symbol.deserialise(Integer.parseInt(targetString[i])));
						i++;
					}
					Rule rule = new Rule(source, target);
					res.add(rule);
				} else {
					System.err.println("Malformed pass through rules file: "
							+ passThroughRulesFileName);
					System.exit(1);
				}
			}
		}
		return res;
	}

	private Set<WritableArrayBuffer> getPassThroughVocab() throws IOException {
		// TODO simplify all template writing
		// TODO getAsciiVocab is redundant with getAsciiConstraints
		Set<WritableArrayBuffer> res = new HashSet<>();
		try (BufferedReader br = new BufferedReader(new FileReader(
				passThroughRulesFileName))) {
			String line;
			Pattern regex = Pattern.compile(".*: (.*) # (.*)");
			while ((line = br.readLine()) != null) {
				Matcher matcher = regex.matcher(line);
				if (matcher.matches()) {
					String[] sourceString = matcher.group(1).split(" ");
					// only one word
					if (sourceString.length == 1) {
						WritableArrayBuffer v = new WritableArrayBuffer();
						v.add(Symbol.deserialise(sourceString[0]));
						res.add(v);
					}
				} else {
					System.err
							.println("Malformed pass through rules constraint file: "
									+ passThroughRulesFileName);
					System.exit(1);
				}
			}
		}
		return res;
	}

	private Set<WritableArrayBuffer> getTestVocab(String testFile)
			throws FileNotFoundException, IOException {
		Set<WritableArrayBuffer> res = new HashSet<>();
		try (BufferedReader br = new BufferedReader(new FileReader(testFile))) {
			String line;
			while ((line = br.readLine()) != null) {
				String[] parts = line.split("\\s+");
				for (String part : parts) {
					WritableArrayBuffer v = new WritableArrayBuffer();
					v.add(Symbol.deserialise(part));
					res.add(v);
				}
			}
		}
		return res;
	}

	public void writeGlueRules(BufferedWriter out) {
		List<Symbol> sideGlueRule1 = new ArrayList<Symbol>();
		sideGlueRule1.add(Symbol.deserialise(-4));
		sideGlueRule1.add(Symbol.deserialise(-1));
		Rule glueRule1 = new Rule(sideGlueRule1, sideGlueRule1);
		writeRule("-4", glueRule1,
				fReg.getDefaultGlueFeatures(), out);
		List<Symbol> sideGlueRule2 = new ArrayList<Symbol>();
		sideGlueRule2.add(Symbol.deserialise(-1));
		Rule glueRule2 = new Rule(sideGlueRule2, sideGlueRule2);
		writeRule("-1",glueRule2, new TreeMap<Integer,Double>(),
				out);
		List<Symbol> sideGlueRule3 = new ArrayList<>();
		sideGlueRule3.add(Symbol.deserialise(-1));
		Rule glueRule3 = new Rule(sideGlueRule3, sideGlueRule3);
		writeRule("-4", glueRule3,
				new TreeMap<Integer,Double>(), out);
		List<Symbol> startSentenceSide = new ArrayList<Symbol>();
		startSentenceSide.add(Symbol.deserialise(1));
		Rule startSentence = new Rule(startSentenceSide, startSentenceSide);
		writeRule("-1", startSentence,
				fReg.getDefaultGlueStartOrEndFeatures(), out);
		List<Symbol> endSentenceSide = new ArrayList<Symbol>();
		endSentenceSide.add(Symbol.deserialise(2));
		Rule endSentence = new Rule(endSentenceSide, endSentenceSide);
		writeRule("-1", endSentence,
				fReg.getDefaultGlueStartOrEndFeatures(), out);
	}

	private List<Set<WritableArrayBuffer>> generateQueries(String testFileName,
			CLI.RuleRetrieverParameters params) throws IOException {
		PatternInstanceCreator patternInstanceCreator = new PatternInstanceCreator(
				params, filter.getPermittedSourcePatterns());
		List<Set<WritableArrayBuffer>> queries = new ArrayList<>(readers.length);
		for (int i = 0; i < readers.length; ++i) {
			queries.add(new HashSet<WritableArrayBuffer>());
		}
		try (BufferedReader reader = new BufferedReader(new FileReader(
				testFileName))) {
			int count = 0;
			for (String line = reader.readLine(); line != null; line = reader
					.readLine()) {
				StopWatch stopWatch = new StopWatch();
				stopWatch.start();
				Set<Rule> rules = patternInstanceCreator
						.createSourcePatternInstances(line);
				Collection<WritableArrayBuffer> sources = new ArrayList<>(rules.size());
				for (Rule rule : rules) {
					WritableArrayBuffer source = rule.source();
					sources.add(source);
				}
				for (WritableArrayBuffer source : sources) {
					if (filter.filterSource(source)) {
						continue;
					}
					int partition = partitioner.getPartition(source, null,
							queries.size());
					queries.get(partition).add(source);
				}
				System.out.println("Creating patterns for line " + ++count
						+ " took " + (double) stopWatch.getTime() / 1000d
						+ " seconds");
			}
		}
		return queries;
	}

	public static void writeRule(String LHS, Rule rule,
			SortedMap<Integer, Double> processedFeatures, BufferedWriter out) {
		StringBuilder res = new StringBuilder();
		res.append(LHS).append(" ").append(rule);
		for (int featureIndex : processedFeatures.keySet()) {
			double featureValue = processedFeatures.get(featureIndex);
			// one-based index
			int index = featureIndex;
			if (Math.floor(featureValue) == featureValue) {
				int featureValueInt = (int) featureValue;
				res.append(String.format(" %d@%d", featureValueInt, index));
			} else {
				res.append(String.format(" %f@%d", featureValue, index));
			}
		}
		res.append("\n");
		synchronized (out) {
			try {
				out.write(res.toString());
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws FileNotFoundException
	 * @throws InterruptedException
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 */
	public static void main(String[] args) throws FileNotFoundException,
			IOException, InterruptedException, IllegalArgumentException,
			IllegalAccessException {
		CLI.RuleRetrieverParameters params = new CLI.RuleRetrieverParameters();
		try {
			Util.parseCommandLine(args, params);
		} catch (ParameterException e) {
			return;
		}
		RuleRetriever retriever = new RuleRetriever();
		retriever.loadDir(params.hfile);
		retriever.setup(params.testFile, params);
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		System.err.println("Generating query");
		List<Set<WritableArrayBuffer>> queries = retriever.generateQueries(params.testFile,
				params);
		System.err.printf("Query took %d seconds to generate\n",
				stopWatch.getTime() / 1000);
		System.err.println("Executing queries");
		try (BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
				new GZIPOutputStream(new FileOutputStream(params.rules))))) {
			ExecutorService threadPool = Executors
					.newFixedThreadPool(params.retrievalThreads);

			for (int i = 0; i < queries.size(); ++i) {
				HFileRuleQuery query = new HFileRuleQuery(retriever.readers[i],
						retriever.bfs[i], out, queries.get(i), retriever,
						params.sp);
				threadPool.execute(query);
			}
			threadPool.shutdown();
			threadPool.awaitTermination(1, TimeUnit.DAYS);
			// Add pass through rule not already found in query
			for (Rule passThroughRule : retriever.passThroughRules) {
				if (!retriever.foundPassThroughRules.contains(passThroughRule)) {
					writeRule(EnumRuleType.PASSTHROUGH_OOV_DELETE.getLhs(), passThroughRule,
							retriever.fReg.getDefaultPassThroughRuleFeatures(),
							out);
				}
			}
			// Add Deletetion and OOV rules
			Rule deletionRuleWritable = new Rule();
			WritableArrayBuffer zero = new WritableArrayBuffer();
			zero.add(Symbol.deserialise(0));
			deletionRuleWritable.setTarget(zero);
			Rule oovRuleWritable = new Rule();
			oovRuleWritable.setTarget(new WritableArrayBuffer());
			for (WritableArrayBuffer source : retriever.testVocab) {
				// Write deletion rule
				if (retriever.foundTestVocab.contains(source)) {
					deletionRuleWritable.setSource(source);
					writeRule(EnumRuleType.PASSTHROUGH_OOV_DELETE.getLhs(), deletionRuleWritable,
							retriever.fReg.getDefaultDeletionFeatures(), out);
					// Otherwise is an OOV
				} else {
					oovRuleWritable.setSource(source);
					writeRule(EnumRuleType.PASSTHROUGH_OOV_DELETE.getLhs(), oovRuleWritable,
							retriever.fReg.getDefaultOOVFeatures(), out);
				}
			}
			// Glue rules
			retriever.writeGlueRules(out);
		
		}
		System.out.println(retriever.foundPassThroughRules);
		System.out.println(retriever.foundTestVocab);
	}

}
