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

import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;
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

	private Partitioner<Text, NullWritable> partitioner = new HashPartitioner<>();

	RuleFilter filter;

	private String passThroughRulesFileName;

	Set<RuleWritable> passThroughRules;

	Set<RuleWritable> foundPassThroughRules = new HashSet<>();

	Set<Text> testVocab;

	Set<Text> foundTestVocab = new HashSet<>();

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
		Set<Text> fullTestVocab = getTestVocab(testFile);
		Set<Text> passThroughVocab = getPassThroughVocab();
		testVocab = new HashSet<>();
		for (Text word : fullTestVocab) {
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

	private Set<RuleWritable> getPassThroughRules() throws IOException {
		Set<RuleWritable> res = new HashSet<>();
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
					List<Integer> source = new ArrayList<Integer>();
					List<Integer> target = new ArrayList<Integer>();
					int i = 0;
					while (i < sourceString.length) {
						if (i % maxSourcePhrase == 0 && i > 0) {
							Rule rule = new Rule(-1, source, target);
							res.add(new RuleWritable(rule));
							source.clear();
							target.clear();
						}
						source.add(Integer.parseInt(sourceString[i]));
						target.add(Integer.parseInt(targetString[i]));
						i++;
					}
					Rule rule = new Rule(-1, source, target);
					res.add(new RuleWritable(rule));
				} else {
					System.err.println("Malformed pass through rules file: "
							+ passThroughRulesFileName);
					System.exit(1);
				}
			}
		}
		return res;
	}

	private Set<Text> getPassThroughVocab() throws IOException {
		// TODO simplify all template writing
		// TODO getAsciiVocab is redundant with getAsciiConstraints
		Set<Text> res = new HashSet<>();
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
						res.add(new Text(sourceString[0]));
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

	private Set<Text> getTestVocab(String testFile)
			throws FileNotFoundException, IOException {
		Set<Text> res = new HashSet<>();
		try (BufferedReader br = new BufferedReader(new FileReader(testFile))) {
			String line;
			while ((line = br.readLine()) != null) {
				String[] parts = line.split("\\s+");
				for (String part : parts) {
					res.add(new Text(part));
				}
			}
		}
		return res;
	}

	public Collection<Pair<RuleWritable, SortedMap<Integer, Double>>> getGlueRules() {
		List<Pair<RuleWritable, SortedMap<Integer, Double>>> res = new ArrayList<>();
		List<Integer> sideGlueRule1 = new ArrayList<Integer>();
		sideGlueRule1.add(-4);
		sideGlueRule1.add(-1);
		Rule glueRule1 = new Rule(-4, sideGlueRule1, sideGlueRule1);
		res.add(Pair.createPair(new RuleWritable(glueRule1),
				fReg.getDefaultGlueFeatures()));
		List<Integer> sideGlueRule2 = new ArrayList<Integer>();
		sideGlueRule2.add(-1);
		Rule glueRule2 = new Rule(-1, sideGlueRule2, sideGlueRule2);
		res.add(Pair.createPair(new RuleWritable(glueRule2),
				new TreeMap<Integer,Double>()));
		List<Integer> sideGlueRule3 = new ArrayList<>();
		sideGlueRule3.add(-1);
		Rule glueRule3 = new Rule(-4, sideGlueRule3, sideGlueRule3);
		res.add(Pair.createPair(new RuleWritable(glueRule3),
				new TreeMap<Integer,Double>()));
		List<Integer> startSentenceSide = new ArrayList<Integer>();
		startSentenceSide.add(1);
		Rule startSentence = new Rule(-1, startSentenceSide, startSentenceSide);
		res.add(Pair.createPair(new RuleWritable(startSentence),
				fReg.getDefaultGlueStartOrEndFeatures()));
		List<Integer> endSentenceSide = new ArrayList<Integer>();
		endSentenceSide.add(2);
		Rule endSentence = new Rule(-1, endSentenceSide, endSentenceSide);
		res.add(Pair.createPair(new RuleWritable(endSentence),
				fReg.getDefaultGlueStartOrEndFeatures()));
		return res;
	}

	private List<Set<Text>> generateQueries(String testFileName,
			CLI.RuleRetrieverParameters params) throws IOException {
		PatternInstanceCreator patternInstanceCreator = new PatternInstanceCreator(
				params, filter.getPermittedSourcePatterns());
		List<Set<Text>> queries = new ArrayList<>(readers.length);
		for (int i = 0; i < readers.length; ++i) {
			queries.add(new HashSet<Text>());
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
				Collection<Text> sources = new ArrayList<>(rules.size());
				for (Rule rule : rules) {
					Text source = (new RuleWritable(rule)).getSource();
					sources.add(source);
				}
				for (Text source : sources) {
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

	public static void writeRule(RuleWritable rule,
			SortedMap<Integer, Double> processedFeatures, BufferedWriter out) {
		StringBuilder res = new StringBuilder();
		res.append(rule);
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
		List<Set<Text>> queries = retriever.generateQueries(params.testFile,
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
			for (RuleWritable passThroughRule : retriever.passThroughRules) {
				if (!retriever.foundPassThroughRules.contains(passThroughRule)) {
					writeRule(passThroughRule,
							retriever.fReg.getDefaultPassThroughRuleFeatures(),
							out);
				}
			}
			// Add Deletetion and OOV rules
			RuleWritable deletionRuleWritable = new RuleWritable();
			deletionRuleWritable.setLeftHandSide(new Text(
					EnumRuleType.PASSTHROUGH_OOV_DELETE.getLhs()));
			deletionRuleWritable.setTarget(new Text("0"));
			RuleWritable oovRuleWritable = new RuleWritable();
			oovRuleWritable.setLeftHandSide(new Text(
					EnumRuleType.PASSTHROUGH_OOV_DELETE.getLhs()));
			oovRuleWritable.setTarget(new Text(""));
			for (Text source : retriever.testVocab) {
				// Write deletion rule
				if (retriever.foundTestVocab.contains(source)) {
					deletionRuleWritable.setSource(source);
					writeRule(deletionRuleWritable,
							retriever.fReg.getDefaultDeletionFeatures(), out);
					// Otherwise is an OOV
				} else {
					oovRuleWritable.setSource(source);
					writeRule(oovRuleWritable,
							retriever.fReg.getDefaultOOVFeatures(), out);
				}
			}
			// Glue rules
			for (Pair<RuleWritable, SortedMap<Integer, Double>> glueRule : retriever
					.getGlueRules()) {
				writeRule(glueRule.getFirst(), glueRule.getSecond(), out);
			}
		}
		System.out.println(retriever.foundPassThroughRules);
		System.out.println(retriever.foundTestVocab);
	}

}
