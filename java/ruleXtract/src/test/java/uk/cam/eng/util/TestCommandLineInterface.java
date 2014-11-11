package uk.cam.eng.util;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.ac.cam.eng.util.CLI;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

public class TestCommandLineInterface {

	private static final String TEST_CONFIG="/uk/cam/eng/util/TestConfigFile";
	
	public static File testConfig;
	
	@ClassRule
	public static TemporaryFolder folder = new TemporaryFolder();
	
	@BeforeClass
	public static void setup() throws IOException{
		testConfig = folder.newFile();
		try (OutputStream writer = new FileOutputStream(testConfig)) {
			try (InputStream configFile = writer.getClass().getResourceAsStream(
					TEST_CONFIG)) {
				for (int in = configFile.read(); in != -1; in = configFile.read()) {
					writer.write(in);
				}
			}
		}
	}
	
	@Test
	public void testConfigFile() {
		CLI.ExtractorJobParameters params = new CLI.ExtractorJobParameters();
		String[] args = ("--output=foo --input=bar @" + testConfig.getAbsolutePath()).split(" ");
		JCommander cmd = new JCommander();
		cmd.setAcceptUnknownOptions(true);
		cmd.addObject(params);
		try {
			cmd.parse(args);
		} catch (ParameterException e) {
			fail(e.getMessage());
		}
		System.out.print(params.rp.prov.provenance);
	}

}
