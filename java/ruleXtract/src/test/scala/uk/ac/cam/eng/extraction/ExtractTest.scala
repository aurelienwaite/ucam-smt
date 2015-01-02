package uk.ac.cam.eng.extraction

import resource._
import scala.collection.JavaConverters._
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.MapWritable
import uk.ac.cam.eng.extraction.hadoop.datatypes.TextArrayWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import uk.ac.cam.eng.util.CLI
import uk.ac.cam.eng.extraction.datatypes.SentencePair

object ExtractTest extends App {

  val opt = new ExtractOptions(9, 5, 5, 10, true)

  val extract = Extract.extractWithDupes(opt)_

  val conf = new Configuration();
  conf.setInt(CLI.RuleParameters.MAX_SOURCE_PHRASE, 9);
  conf.setInt(CLI.RuleParameters.MAX_SOURCE_ELEMENTS, 5);
  conf.setInt(CLI.RuleParameters.MAX_TERMINAL_LENGTH, 5);
  conf.setInt(CLI.RuleParameters.MAX_NONTERMINAL_SPAN, 10);
  conf.setBoolean(CLI.ExtractorJobParameters.REMOVE_MONOTONIC_REPEATS,
    true);
  val oldExtractor = new RuleExtractor(conf);

  RuleExtractorTest.folder.create()
  RuleExtractorTest.setupFileSystem()

  for (
    reader <- managed(new SequenceFile.Reader(
      FileSystem.get(RuleExtractorTest.conf), new Path(RuleExtractorTest.trainingDataFile.getPath()),
      RuleExtractorTest.conf))
  ) {
    val key = new MapWritable()
    val value = new TextArrayWritable()

    var count =0
    while (reader.next(key, value)) {
      count += 1
      val strings = value.get.map { _ match { case t: Text => t.toString() } }
      val src = strings(0)
      val trg = strings(1)
      val align = strings(2)
      val newString = extract(src, trg, align).map { _ match { case (r, aSet) => "0 " + r.toString() } }.toList.sorted.mkString("\n")

      val oldResults = oldExtractor.extract(src, trg, align)
      val oldString = oldResults.asScala.map { _.toString().replace("-1", "X").replace("-2", "X1").replace("-3", "X2") }.toList.sorted.mkString("\n")
      //println(count)
      if (oldString != newString) {
        println(List(src, trg, align).mkString("\n"))
        //println(newString)
        println()
        //println(oldString)
        System.exit(1)
      }
    }
  }

  RuleExtractorTest.folder.delete()

  /*val src = "45187 82073 15 22 28500 18 2575 31846 3 102 25017 133794 19 21379 5 566 957608 3532 5 26635 155153 725236 4"
  val trg = "5023 8107 12 11 1547 14 205 55755 25 12 1226 22 11 36053 26 158559 16746 53 6119 9 3 16497 14412 115 10105 113 6 3 2904 514343 16497 5"
  val align = "0-0 1-1 2-2 3-2 3-3 4-4 5-5 6-6 7-7 9-8 10-9 11-10 15-11 10-13 16-13 16-14 11-15 16-15 11-16 12-17 13-18 14-19 15-21 16-21 16-22 16-23 19-24 20-28 16-29 17-29 21-29 21-30 22-31"
  //println(extract.extract(opt)(src, trg, align).map{ _ match {case(r,a, p) => "0 " + r.toString() + " " + a + " " + p} }.toList.sorted.mkString("\n"))
  println(extract(src, trg, align).map{ _ match {case(r,aSet) => "0 " + r.toString() } }.toList.sorted.mkString("\n"))*/

}