package uk.ac.cam.eng

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfterAll
import org.scalatest.ConfigMap
import uk.ac.cam.eng.extraction.RuleExtractorTest
import uk.ac.cam.eng.extraction.hadoop.extraction.ExtractorJob
import org.apache.hadoop.conf.Configuration
import uk.ac.cam.eng.util.CLI
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.Writable
import org.apache.hadoop.util.ReflectionUtils
import uk.ac.cam.eng.extraction.hadoop.features.phrase.Source2TargetJob
import uk.ac.cam.eng.extraction.hadoop.features.phrase.Target2SourceJob
import uk.ac.cam.eng.extraction.hadoop.merge.MergeJob
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import uk.ac.cam.eng.extraction.hadoop.merge.MergeJob.MergeFeatureMapper
import uk.ac.cam.eng.extraction.hadoop.datatypes.FeatureMap
import uk.ac.cam.eng.extraction.Rule
import uk.ac.cam.eng.extraction.hadoop.datatypes.ExtractedData
import uk.ac.cam.eng.extraction.hadoop.merge.MergeJob.MergeRuleMapper
import scala.collection.immutable.HashSet
import org.apache.hadoop.hbase.io.hfile.HFile
import org.apache.hadoop.hbase.io.hfile.CacheConfig
import uk.ac.cam.eng.rule.retrieval.HFileRuleReader
import scala.collection.JavaConversions._
import uk.ac.cam.eng.rule.features.Feature
import org.apache.hadoop.io.IntWritable

class PipelineTest extends FunSuite with BeforeAndAfterAll {

  override def beforeAll(configMap: ConfigMap) {
    RuleExtractorTest.folder.create()
    RuleExtractorTest.setupFileSystem()
  }

  override def afterAll(configMap: ConfigMap) {
    RuleExtractorTest.folder.delete()
  }

  def printSequenceFile(f: String, conf: Configuration) = {
    val reader = new SequenceFile.Reader(FileSystem.get(conf), new Path(f), conf);
    val key = ReflectionUtils.newInstance(
      reader.getKeyClass(), conf).asInstanceOf[Writable]
    val value = ReflectionUtils.newInstance(
      reader.getValueClass(), conf).asInstanceOf[Writable]
    while (reader.next(key, value)) {
      System.out.println(key + "\t" + value);
    }
  }

  def assertWithDelta(expected: Double)(result: Double) = {
    val delta = 1D / 1024D
    assert(Math.abs(expected - result) < delta)
  }

  test("The rule extraction job") {
    val conf = new Configuration
    conf.setInt(CLI.RuleParameters.MAX_SOURCE_PHRASE, 9)
    conf.setInt(CLI.RuleParameters.MAX_SOURCE_ELEMENTS, 5)
    conf.setInt(CLI.RuleParameters.MAX_TERMINAL_LENGTH, 5)
    conf.setInt(CLI.RuleParameters.MAX_NONTERMINAL_SPAN, 10)
    conf.setBoolean(CLI.ExtractorJobParameters.REMOVE_MONOTONIC_REPEATS, true)
    conf.set(CLI.Provenance.PROV, "all");
    val job = ExtractorJob.getJob(conf)
    FileInputFormat.setInputPaths(job, new Path(RuleExtractorTest.trainingDataFile.getAbsolutePath))
    val extractOut = new Path("extractOut")
    FileOutputFormat.setOutputPath(job, extractOut);
    println(FileSystem.get(conf).getWorkingDirectory.getParent)
    println(FileSystem.get(conf).getWorkingDirectory.getName)
    println(FileSystem.get(conf).getWorkingDirectory)
    job.waitForCompletion(true);
    //printSequenceFile("extractOut/part-r-00000", conf)
    //S2T
    val s2tOut = new Path("s2t")
    val s2tJob = (new Source2TargetJob).getJob(conf)
    FileInputFormat.setInputPaths(s2tJob, extractOut)
    FileOutputFormat.setOutputPath(s2tJob, s2tOut);
    s2tJob.waitForCompletion(true)
    val t2sOut = new Path("t2s")
    val t2sJob = (new Target2SourceJob).getJob(conf)
    FileInputFormat.setInputPaths(t2sJob, extractOut)
    FileOutputFormat.setOutputPath(t2sJob, t2sOut);
    t2sJob.waitForCompletion(true)
    val mergeJob = MergeJob.getJob(conf);
    for (featurePath <- List(s2tOut, t2sOut)) {
      MultipleInputs.addInputPath(mergeJob, featurePath,
        classOf[SequenceFileInputFormat[Rule, FeatureMap]], classOf[MergeFeatureMapper]);
    }
    MultipleInputs.addInputPath(mergeJob, extractOut,
      classOf[SequenceFileInputFormat[Rule, ExtractedData]], classOf[MergeRuleMapper]);
    val mergeOut = new Path("mergeOut")
    FileOutputFormat.setOutputPath(mergeJob, mergeOut);
    mergeJob.waitForCompletion(true)

    val cacheConf = new CacheConfig(conf);
    val hfReader = HFile.createReader(FileSystem.get(conf),
      new Path(mergeOut, "part-r-00000.hfile"), cacheConf);
    val reader = new HFileRuleReader(hfReader);
    var count = 0
    var notTested = 0
    for (entry <- reader) {
      count += 1
      val data = entry.getSecond
      entry.getFirst match {
        case Rule("57228 1663") => {
          assertWithDelta(-0.6931471805599453)(data.getFeatures.get(Feature.SOURCE2TARGET_PROBABILITY).get(new IntWritable(0)).get)
          assertWithDelta(-1.3862943611198906)(data.getFeatures.get(Feature.TARGET2SOURCE_PROBABILITY).get(new IntWritable(0)).get)
        }
        case Rule("2825_280_11431 6_668_6_3_910_974_6") => {
          assertWithDelta(-1.3862943611198906)(data.getFeatures.get(Feature.SOURCE2TARGET_PROBABILITY).get(new IntWritable(0)).get)
          assertWithDelta(0.0)(data.getFeatures.get(Feature.TARGET2SOURCE_PROBABILITY).get(new IntWritable(0)).get)
        }
        case Rule("125_4368_X_6 X_131_562_8") => {
          assertWithDelta(0.0)(data.getFeatures.get(Feature.SOURCE2TARGET_PROBABILITY).get(new IntWritable(0)).get)
          assertWithDelta(-0.6931471805599453)(data.getFeatures.get(Feature.TARGET2SOURCE_PROBABILITY).get(new IntWritable(0)).get)
        }
        case Rule("69040_6_67946_52926 2684_8_10719_201") => {
          assertWithDelta(-0.6931471805599453)(data.getFeatures.get(Feature.SOURCE2TARGET_PROBABILITY).get(new IntWritable(0)).get)
          assertWithDelta(-0.6931471805599453)(data.getFeatures.get(Feature.TARGET2SOURCE_PROBABILITY).get(new IntWritable(0)).get)
        }
        case Rule("X1_31_317_X2 X1_18_7_3_450_X2") => {
          assertWithDelta(0.0)(data.getFeatures.get(Feature.SOURCE2TARGET_PROBABILITY).get(new IntWritable(0)).get)
          assertWithDelta(0.0)(data.getFeatures.get(Feature.TARGET2SOURCE_PROBABILITY).get(new IntWritable(0)).get)
        }
        case Rule("X1_1176_X2_4 X1_1341_4_X2_5") => {
          assertWithDelta(-0.7472144018302211)(data.getFeatures.get(Feature.SOURCE2TARGET_PROBABILITY).get(new IntWritable(0)).get)
          assertWithDelta(0.0)(data.getFeatures.get(Feature.TARGET2SOURCE_PROBABILITY).get(new IntWritable(0)).get)
        }
        case Rule("94345_X1_5_X2_4 4066102_X1_6_X2_5") => {
          assertWithDelta(0.0)(data.getFeatures.get(Feature.SOURCE2TARGET_PROBABILITY).get(new IntWritable(0)).get)
          assertWithDelta(0.0)(data.getFeatures.get(Feature.TARGET2SOURCE_PROBABILITY).get(new IntWritable(0)).get)
        }
        case Rule("117102_6_191_141_10220 87048_8_118_74_10895") => {
          assertWithDelta(0.0)(data.getFeatures.get(Feature.SOURCE2TARGET_PROBABILITY).get(new IntWritable(0)).get)
          assertWithDelta(0.0)(data.getFeatures.get(Feature.TARGET2SOURCE_PROBABILITY).get(new IntWritable(0)).get)
        }
        case Rule("198_X_5212_3_130 58_X_1758_4_65") => {
          assertWithDelta(0.0)(data.getFeatures.get(Feature.SOURCE2TARGET_PROBABILITY).get(new IntWritable(0)).get)
          assertWithDelta(0.0)(data.getFeatures.get(Feature.TARGET2SOURCE_PROBABILITY).get(new IntWritable(0)).get)
        }
        case Rule("39_38_7738_5_1937_3513 63_1896_9_904_23_3") => {
          assertWithDelta(-0.6931471805599453)(data.getFeatures.get(Feature.SOURCE2TARGET_PROBABILITY).get(new IntWritable(0)).get)
          assertWithDelta(-0.6931471805599453)(data.getFeatures.get(Feature.TARGET2SOURCE_PROBABILITY).get(new IntWritable(0)).get)
        }
        case _ => notTested += 1
      }
    }
    assertResult(6180048)(count)
    assertResult(10)(count - notTested)
  }
}