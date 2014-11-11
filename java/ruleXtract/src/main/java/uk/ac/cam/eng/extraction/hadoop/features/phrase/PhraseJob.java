package uk.ac.cam.eng.extraction.hadoop.features.phrase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import uk.ac.cam.eng.extraction.hadoop.util.Util;
import uk.ac.cam.eng.util.CLI;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

/**
 * Common functionality between the phrase (s2t, t2s) jobs
 * @author Aurelien Waite
 *
 */

public abstract class PhraseJob extends Configured implements Tool {

	public abstract Job getJob(Configuration conf) throws IOException;
	
	@Override
	public int run(String[] args) throws IllegalArgumentException,
	IllegalAccessException, IOException, ClassNotFoundException,
	InterruptedException {
		CLI.MarginalReducerParameters params = new CLI.MarginalReducerParameters();
		JCommander cmd = new JCommander(params);
		try {
			cmd.parse(args);
		} catch (ParameterException e) {
			System.err.println(e.getMessage());
			cmd.usage();
		}
		Configuration conf = getConf();
		Util.ApplyConf(cmd, conf);
		Job job = getJob(conf);
		FileInputFormat.setInputPaths(job, params.input);
		FileOutputFormat.setOutputPath(job, new Path(params.output));
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
