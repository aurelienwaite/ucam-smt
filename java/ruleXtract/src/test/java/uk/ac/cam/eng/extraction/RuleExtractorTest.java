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

package uk.ac.cam.eng.extraction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.ac.cam.eng.extraction.hadoop.datatypes.TextArrayWritable;
import uk.ac.cam.eng.extraction.hadoop.features.phrase.Source2TargetJob;
import uk.ac.cam.eng.extraction.hadoop.features.phrase.Target2SourceJob;
import uk.ac.cam.eng.util.CLI;
import uk.ac.cam.eng.util.Pair;

/**
 * @author aaw35
 *
 */
public class RuleExtractorTest {

	private static final String LOCAL_URI = "file:///";
	private static final String TRAINING_DATA = "/uk/ac/cam/eng/data/unit_testing_training_data";

	@ClassRule
	public static TemporaryFolder folder = new TemporaryFolder();
	public static Configuration conf;
	public static File trainingDataFile;

	@BeforeClass
	public static void setupFileSystem() throws IOException {
		// Ensure hadoop to use local file system
		conf = new Configuration();
		FileSystem.setDefaultUri(conf, LOCAL_URI);
		FileSystem fs = FileSystem.get(conf);
		fs.setWorkingDirectory(new Path(folder.getRoot().getAbsolutePath()));
		trainingDataFile = folder.newFile();
		try (OutputStream writer = new FileOutputStream(trainingDataFile)) {
			try (InputStream rulesFile = conf.getClass().getResourceAsStream(
					TRAINING_DATA)) {
				for (int in = rulesFile.read(); in != -1; in = rulesFile.read()) {
					writer.write(in);
				}
			}
		}
	}
	
	@AfterClass
	public static void cleanUp() throws IOException{
		folder.delete();
	}

	public void testExtraction() throws IOException {
		try (SequenceFile.Reader reader = new SequenceFile.Reader(
				FileSystem.get(conf), new Path(trainingDataFile.getPath()),
				conf)) {
			MapWritable key = new MapWritable();
			TextArrayWritable val = new TextArrayWritable();
			reader.next(key, val);
			key.forEach((k, v) -> System.out.println(k + ": " + v));
			for (Writable t : val.get()) {
				System.out.println(t);
			}
		}
	}
	
	private boolean isContigious(List<Rule> rules, Function<Rule, List<Symbol>> getStr){
		Set<List<Symbol>> prevs = new HashSet<>();
		List<Symbol> prev = getStr.apply(rules.get(0));
		prevs.add(prev);
		for(Rule rule : rules){
			List<Symbol> str = getStr.apply(rule);
			if(!(str.equals(prev) || prevs.add(str))){
				return false;
			}
			prev = str;
		}
		return true;
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testRuleComparator() throws IOException {
		try (SequenceFile.Reader reader = new SequenceFile.Reader(
				FileSystem.get(conf), new Path(trainingDataFile.getPath()),
				conf)) {
			MapWritable key = new MapWritable();
			TextArrayWritable val = new TextArrayWritable();
			List<Rule> rules = new ArrayList<>();
			ExtractOptions opts = new ExtractOptions(9, 5, 5, 10, true);
			int count = 0;
			while (reader.next(key, val) && count < 1000) {
				String src = val.get()[0].toString();
				String trg = val.get()[1].toString();
				String a = val.get()[2].toString();
				List<Pair<Rule, Alignment>> extracted = Extract.extractJava(opts, src, trg, a);
				for(Pair<Rule, Alignment> pair : extracted){
					rules.add(pair.getFirst());
				}
				++count; 
			}
			Assert.assertEquals(472100, rules.size());
			Assert.assertFalse(isContigious(rules, r -> r.getSource()));
			rules.sort(new Source2TargetJob.Source2TargetComparator());
			Assert.assertTrue(isContigious(rules, r -> r.getSource()));
			rules.sort(new Target2SourceJob.Target2SourceComparator());
			Assert.assertTrue(isContigious(rules, r -> r.getTarget()));
		}
	}




}
