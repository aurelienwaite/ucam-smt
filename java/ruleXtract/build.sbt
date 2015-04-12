name := "ruleXtract"

version := "1.0"

organization := "University of Cambridge"

EclipseKeys.withSource := true

// uncomment this for debugging
// javacOptions += "-g"

// gives bug when generating .project/.classpath for eclipse
// ("eclipse cannot nest output folder")
//target := file("bin")

test in assembly := {}

// depends on hadoop, hbase and junit
libraryDependencies ++= Seq(
		    "com.beust" % "jcommander" % "1.35",
		    "org.apache.hadoop" % "hadoop-common" % "2.6.0" intransitive(),
		    "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.6.0"  intransitive(),
		    "org.apache.hadoop" % "hadoop-annotations" % "2.6.0" intransitive(), 
		    "org.apache.hbase" % "hbase-server" % "0.96.2-hadoop2" intransitive(), 
		    "org.apache.hbase" % "hbase-common" % "0.96.2-hadoop2"  intransitive(), 
		    "org.apache.hbase" % "hbase-client" % "0.96.2-hadoop2" intransitive(),
		    "commons-configuration" % "commons-configuration" % "1.6" exclude("commons-beanutils", "commons-beanutils-core"),
		    "com.google.guava" % "guava" % "11.0.2",
		    "org.slf4j" % "slf4j-api" % "1.7.5",
		    "junit" % "junit" % "4.11" % "test",
		    "com.novocode" % "junit-interface" % "0.10" % "test",
		    "com.jsuereth" % "scala-arm_2.11" % "1.4" % "test",
		    "org.scalatest" % "scalatest_2.11" % "2.2.1" % "test"
		    )

assemblyExcludedJars in assembly := { 
  val cp = (fullClasspath in assembly).value
  cp filter {_.data.getName == "commons-beanutils-1.7.0.jar"}
}

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

scalaVersion := "2.11.4"

// output jar is here: target/ruleXtract.jar
assemblyOutputPath in assembly := file("target/ruleXtract.jar") 


// we want a jar without a main class so we can run it as "hadoop jar class args"
mainClass in (Compile, packageBin) := None
// this works too, see http://stackoverflow.com/questions/18325181/suppress-main-class-in-sbt-assembly
//packageOptions in (Compile, packageBin) ~= { os =>
//	       os filterNot {_.isInstanceOf[Package.MainClass]} }
