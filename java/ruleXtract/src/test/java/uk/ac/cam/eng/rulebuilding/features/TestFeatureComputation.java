package uk.ac.cam.eng.rulebuilding.features;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.extraction.hadoop.datatypes.FeatureMap;
import uk.ac.cam.eng.extraction.hadoop.datatypes.ProvenanceCountMap;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleData;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.TargetFeatureList;
import uk.ac.cam.eng.extraction.hadoop.extraction.ExtractorJob;
import uk.ac.cam.eng.extraction.hadoop.features.phrase.Source2TargetJob;
import uk.ac.cam.eng.extraction.hadoop.features.phrase.Source2TargetJob.Source2TargetComparator;
import uk.ac.cam.eng.extraction.hadoop.features.phrase.Target2SourceJob;
import uk.ac.cam.eng.extraction.hadoop.features.phrase.Target2SourceJob.Target2SourceComparator;
import uk.ac.cam.eng.extraction.hadoop.merge.MergeJob;
import uk.ac.cam.eng.util.Pair;

public class TestFeatureComputation {

	private static final String LOCAL_URI = "file:///";
	private static final String TEST_RULES = "/uk/ac/cam/eng/extraction/testdata/wmt13frenrules.seq";
	private static final String COMPRESSED_RULES = "/uk/ac/cam/eng/extraction/testdata/compressedRules.gz";
	private static final String S2T = "/uk/ac/cam/eng/extraction/testdata/s2t.gz";
	private static final String T2S = "/uk/ac/cam/eng/extraction/testdata/t2s.gz";
	private static final String TEST_CONFIG = "/uk/ac/cam/eng/extraction/testdata/config-xtract";
	private static Configuration conf;

	public static final double DELTA = 1e-5;

	@ClassRule
	public static TemporaryFolder folder = new TemporaryFolder();

	public static File rulesFile;

	public static File compressedRules;

	public static File s2t;

	public static File t2s;

	public static String mapWritableToString(MapWritable value) {
		StringBuilder builder = new StringBuilder("{");
		for (Entry<Writable, Writable> entry : value.entrySet()) {
			builder.append(entry.getKey()).append("=").append(entry.getValue())
					.append(", ");
		}
		builder.delete(builder.length() - 2, builder.length());
		builder.append("}");
		return builder.toString();
	}

	private static void writeGzippedFile(String fileName, File output)
			throws IOException {
		try (OutputStream writer = new FileOutputStream(output)) {
			try (InputStream rulesFile = new GZIPInputStream(conf.getClass()
					.getResourceAsStream(fileName))) {
				for (int in = rulesFile.read(); in != -1; in = rulesFile.read()) {
					writer.write(in);
				}
			}
		}
	}

	@BeforeClass
	public static void setupFileSystem() throws IOException {
		// Ensure hadoop to use local file system
		conf = new Configuration();
		FileSystem.setDefaultUri(conf, LOCAL_URI);
		Properties props = new Properties();
		props.load(conf.getClass().getResourceAsStream(TEST_CONFIG));

		FileSystem fs = FileSystem.get(conf);
		fs.setWorkingDirectory(new Path(folder.getRoot().getAbsolutePath()));
		rulesFile = folder.newFile();
		try (OutputStream writer = new FileOutputStream(rulesFile)) {
			try (InputStream rulesFile = conf.getClass().getResourceAsStream(
					TEST_RULES)) {
				for (int in = rulesFile.read(); in != -1; in = rulesFile.read()) {
					writer.write(in);
				}
			}
		}
		compressedRules = folder.newFile();
		writeGzippedFile(COMPRESSED_RULES, compressedRules);
		s2t = folder.newFile();
		writeGzippedFile(S2T, s2t) ;
		t2s = folder.newFile();
		writeGzippedFile(T2S, t2s);

	}



	@Test
	public void testRuleCompression() throws IOException,
			ClassNotFoundException, InterruptedException {
		Job job = ExtractorJob.getJob(conf);
		job.setMapperClass(Mapper.class);
		FileInputFormat.setInputPaths(job, rulesFile.getPath());
		FileOutputFormat.setOutputPath(job, new Path("compression"));
		job.waitForCompletion(true);
		Assert.assertTrue(job.isSuccessful());

		int totalCount = 0;
		int moreThanOne = 0;

		try (SequenceFile.Reader reader = new SequenceFile.Reader(
				FileSystem.get(conf), new Path("compression/part-r-00000"),
				conf)) {
			RuleWritable key = new RuleWritable();
			ProvenanceCountMap value = new ProvenanceCountMap();
			while (reader.next(key, value)) {
				// System.out.println(key +"\t"+ value);
				totalCount += 1;
				if (value.size() > 2) {
					moreThanOne += 1;
				}
			}
		}
		Assert.assertEquals(56753, totalCount);
		Assert.assertEquals(631, moreThanOne);
	}

	@Test
	public void testRuleComparator() throws IOException {
		List<RuleWritable> rules = new ArrayList<>();
		try (SequenceFile.Reader reader = new SequenceFile.Reader(
				FileSystem.get(conf), new Path(compressedRules.getPath()), conf)) {
			RuleWritable key = new RuleWritable();
			while (reader.next(key)) {
				rules.add(new RuleWritable(key));
			}
		}
		Random rnd = new Random(47384738294l);
		Collections.shuffle(rules, rnd);
		Source2TargetComparator s2t = new Source2TargetComparator();
		Target2SourceComparator t2s = new Target2SourceComparator();
		RuleWritable rule1 = new RuleWritable(new Rule("-1",
				"9_739_14_431_13_9_202_105_15", "3_1579_6_25299_3_208_29_3"));
		RuleWritable rule2 = new RuleWritable(new Rule("-1",
				"9_739_14_431_13_9_202_105_15", "3_1579_6_25299_3_208_29"));
		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);
		rule1.write(dataOut);
		byte[] rule1b = bytesOut.toByteArray();
		bytesOut.reset();
		rule2.write(dataOut);
		byte[] rule2b = bytesOut.toByteArray();
		Assert.assertEquals(t2s.compare(rule1, rule2),
				t2s.compare(rule1b, 0, rule1b.length, rule2b, 0, rule2b.length));

		RuleWritable prev = rules.get(0);
		ByteArrayOutputStream prevBytes = new ByteArrayOutputStream();
		for (RuleWritable rule : rules) {
			@SuppressWarnings("resource")
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			DataOutputStream out = new DataOutputStream(bytes);
			// Adding an offset to make sure MRComparator can deal with an
			// offset
			out.writeLong(-1l);
			rule.write(out);
			byte[] prevb = prevBytes.toByteArray();
			byte[] b = bytes.toByteArray();
			if (prevb.length > 0) {
				// 8 is the length in bytes of a long
				int byteResult = s2t.compare(prevb, 8, prevb.length - 8, b, 8,
						b.length - 8);
				int objResult = prev.compareTo(rule);
				Assert.assertTrue(objResult != 0);
				Assert.assertEquals(objResult, byteResult);
				byteResult = t2s.compare(prevb, 8, prevb.length - 8, b, 8,
						b.length - 8);
				objResult = t2s.compare(prev, rule);
				Assert.assertEquals(objResult, byteResult);
			}
			prev = rule;
			prevBytes = bytes;
		}

	}

	@Test
	public void testNewSource2Target() throws IOException,
			ClassNotFoundException, InterruptedException {
		Job job = (new Source2TargetJob()).getJob(conf);
		FileInputFormat.setInputPaths(job, compressedRules.getPath());
		FileOutputFormat.setOutputPath(job, new Path("s2t"));
		job.waitForCompletion(true);
		Assert.assertTrue(job.isSuccessful());
		try (SequenceFile.Reader reader = new SequenceFile.Reader(
				FileSystem.get(conf), new Path("s2t/part-r-00000"), conf)) {
			RuleWritable key = new RuleWritable();
			FeatureMap value = new FeatureMap();
			// Test that the 9970 record is computed correctly
			for (int i = 0; i < 9971; ++i) {
				reader.next(key, value);
			}
			Assert.assertEquals(0.62068965, value.get(0).get(), DELTA);
			Assert.assertEquals(18d, value.get(1).get(), DELTA);
			Assert.assertEquals(0.6666666666, value.get(38).get(), DELTA);
			Assert.assertEquals(14d, value.get(39).get(),DELTA);
			Assert.assertEquals(1d, value.get(36).get(),DELTA);
			Assert.assertEquals(4d, value.get(37).get(),DELTA);
		}
	}

	@Test
	public void testNewTarget2Source() throws IOException,
			ClassNotFoundException, InterruptedException {
		Job job = (new Target2SourceJob()).getJob(conf);
		FileInputFormat.setInputPaths(job, compressedRules.getPath());
		FileOutputFormat.setOutputPath(job, new Path("t2s"));
		job.waitForCompletion(true);
		Assert.assertTrue(job.isSuccessful());
		try (SequenceFile.Reader reader = new SequenceFile.Reader(
				FileSystem.get(conf), new Path("t2s/part-r-00000"), conf)) {
			RuleWritable key = new RuleWritable();
			FeatureMap value = new FeatureMap();
			// Picking entry 9569 as it has lots of features
			for (int i = 0; i < 9570; ++i) {
				reader.next(key, value);
			}
			Assert.assertEquals(0.875, value.get(2).get(), DELTA);
			Assert.assertEquals(14d, value.get(3).get(), DELTA);
			Assert.assertEquals(1d, value.get(62).get(), DELTA);
			Assert.assertEquals(3d, value.get(63));
			Assert.assertEquals(0.83333333333, value.get(66).get(), DELTA);
			Assert.assertEquals(5d, value.get(67).get(), DELTA);
			Assert.assertEquals(0.8571428571428571, value.get(68).get(), DELTA);
			Assert.assertEquals(6d, value.get(69).get(),DELTA);
		}
	}

	@Test
	public void testMerge() throws IOException, ClassNotFoundException,
			InterruptedException {
		Job job = MergeJob.getJob(conf);
		FileInputFormat.setInputPaths(job, s2t.getPath() + "," + t2s.getPath());
		FileOutputFormat.setOutputPath(job, new Path("merge"));
		job.waitForCompletion(true);
		Assert.assertTrue(job.isSuccessful());
		CacheConfig cacheConf = new CacheConfig(conf);
		try (HFile.Reader hfReader = HFile.createReader(FileSystem.get(conf),
				new Path("merge/part-r-00000.hfile"), cacheConf)) {
			DataInputBuffer in = new DataInputBuffer();
			Text key = new Text();
			TargetFeatureList value = new TargetFeatureList();
			int count = 0;
			RuleWritable rule = new RuleWritable();
			HFileScanner scanner = hfReader.getScanner(false, false);
			scanner.seekTo();
			do {

				in.reset(scanner.getKey().array(), scanner.getKey()
						.arrayOffset(), scanner.getKey().limit());
				key.readFields(in);
				rule.setLeftHandSide(new Text("0"));
				rule.setSource(key);
				in.reset(scanner.getValue().array(), scanner.getValue()
						.arrayOffset(), scanner.getValue().limit());
				value.readFields(in);
				if (value.isEmpty()) {
					System.out.println(key + " is empty");
				}
				for (Pair<Text, RuleData> entry : value) {
					++count;
					rule.setTarget(entry.getFirst());
					if (count == 2180) {
						FeatureMap features = entry.getSecond().getFeatures();
						Assert.assertEquals(0.25, features.get(0).get(), DELTA);
						Assert.assertEquals(3d, features.get(1).get(), DELTA);
						Assert.assertEquals(0.04918032786885246,
								features.get(2).get(), DELTA);
						Assert.assertEquals(3d, features.get(3).get(), DELTA);
						Assert.assertEquals(0.5, features.get(34).get(), DELTA);
						Assert.assertEquals(2d, features.get(35).get(), DELTA);
						Assert.assertEquals(0.2, features.get(38).get(), DELTA);
						Assert.assertEquals(1d, features.get(39).get(), DELTA);
						Assert.assertEquals(0.13333333333333333,
								features.get(62).get(), DELTA);
						Assert.assertEquals(2d, features.get(63).get(), DELTA);
						Assert.assertEquals(0.1111111111111111,
								features.get(66).get(), DELTA);
						Assert.assertEquals(1d, features.get(67).get(), DELTA);
					}
				}
			} while (scanner.next());
			Assert.assertEquals(56753, count);
		}
	}
}
