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
package uk.ac.cam.eng.extraction.hadoop.merge;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.ac.cam.eng.extraction.Rule;
import uk.ac.cam.eng.extraction.WritableArrayBuffer;
import uk.ac.cam.eng.extraction.hadoop.datatypes.ExtractedData;
import uk.ac.cam.eng.extraction.hadoop.datatypes.FeatureMap;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleData;
import uk.ac.cam.eng.extraction.hadoop.datatypes.TargetFeatureList;
import uk.ac.cam.eng.extraction.hadoop.util.SimpleHFileOutputFormat;
import uk.ac.cam.eng.extraction.hadoop.util.Util;
import uk.ac.cam.eng.util.CLI;
import uk.ac.cam.eng.util.Pair;

import com.beust.jcommander.ParameterException;

/**
 * 
 * @author Aurelien Waite
 * @date 28 May 2014
 */
public class MergeJob extends Configured implements Tool {

	private static class MergeFeatureMapper extends
			Mapper<Rule, FeatureMap, Rule, RuleData> {

		private RuleData ruleData = new RuleData();

		@Override
		protected void map(Rule key, FeatureMap value, Context context)
				throws IOException, InterruptedException {
			ruleData.setFeatures(value);
			context.write(key, ruleData);
		}

	}

	private static class MergeRuleMapper extends
			Mapper<Rule, ExtractedData, Rule, RuleData> {

		private RuleData ruleData = new RuleData();

		@Override
		protected void map(Rule key, ExtractedData value,
				Context context) throws IOException, InterruptedException {
			ruleData.setProvCounts(value.getProvenanceCountMap());
			ruleData.setAlignments(value.getAlignmentCountMapWritable());
			context.write(key, ruleData);
		}
	}

	private static class MergeCombiner extends
			Reducer<Rule, RuleData, Rule, RuleData> {

		private RuleData ruleData = new RuleData();

		@Override
		protected void reduce(Rule key, Iterable<RuleData> values,
				Context context) throws IOException, InterruptedException {
			ruleData.clear();
			for (RuleData value : values) {
				ruleData.merge(value);
			}
			context.write(key, ruleData);
		}
	}

	private static class MergeReducer extends
			Reducer<Rule, RuleData, WritableArrayBuffer, TargetFeatureList> {

		private TargetFeatureList list = new TargetFeatureList();

		private WritableArrayBuffer source = new WritableArrayBuffer();

		@Override
		protected void reduce(Rule key, Iterable<RuleData> values,
				Context context) throws IOException, InterruptedException {
			// First rule!
			if (source.javaSize() == 0) {
				source.set(key.source());
			}
			if (!source.equals(key.source())) {
				context.write(source, list);
				list.clear();
				source.set(key.source());
			}
			RuleData ruleData = new RuleData();
			for (RuleData value : values) {
				ruleData.merge(value);
			}
			WritableArrayBuffer target = new WritableArrayBuffer();
			target.set(key.target());
			list.add(Pair.createPair(target, ruleData));
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			super.cleanup(context);
			context.write(source, list);
		}
	}

	public static Job getJob(Configuration conf) throws IOException {

		conf.set("mapred.map.child.java.opts", "-Xmx200m");
		conf.set("mapred.reduce.child.java.opts", "-Xmx10240m");

		Job job = new Job(conf);
		job.setJarByClass(MergeJob.class);
		job.setJobName("Merge");
		job.setSortComparatorClass(MergeComparator.class);
		job.setPartitionerClass(MergePartitioner.class);
		job.setReducerClass(MergeReducer.class);
		job.setCombinerClass(MergeCombiner.class);
		job.setMapOutputKeyClass(Rule.class);
		job.setMapOutputValueClass(RuleData.class);
		job.setOutputKeyClass(Rule.class);
		job.setOutputValueClass(WritableArrayBuffer.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SimpleHFileOutputFormat.class);
		return job;
	}

	public int run(String[] args) throws IllegalArgumentException,
			IllegalAccessException, IOException, ClassNotFoundException,
			InterruptedException {
		CLI.MergeJobParameters params = new CLI.MergeJobParameters();
		try {
			Util.parseCommandLine(args, params);
		} catch (ParameterException e) {
			return 1;
		}
		Configuration conf = getConf();
		Util.ApplyConf(params, conf);
		Job job = getJob(conf);

		String[] featurePathNames = params.inputFeatures.split(",");
		Path[] featurePaths = StringUtils.stringToPath(featurePathNames);
		for (Path featurePath : featurePaths) {
			MultipleInputs.addInputPath(job, featurePath,
					SequenceFileInputFormat.class, MergeFeatureMapper.class);
		}
		Path rulePath = new Path(params.inputRules);
		MultipleInputs.addInputPath(job, rulePath,
				SequenceFileInputFormat.class, MergeRuleMapper.class);

		FileOutputFormat.setOutputPath(job, new Path(params.output));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new MergeJob(), args);
		System.exit(res);
	}
}
