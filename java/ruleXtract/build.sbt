name := "ruleXtract"

// workaround for artifact name in camel case (https://groups.google.com/forum/#!msg/liftweb/n1nstGkfkKQ/0gBOV-yrBjoJ)
moduleName := name.value

version := "1.0"

organization := "University of Cambridge"

EclipseKeys.withSource := true

// uncomment this for debugging
// javacOptions += "-g"

// gives bug when generating .project/.classpath for eclipse
// ("eclipse cannot nest output folder")
//target := file("bin")

test in assembly := {}

resolvers += Resolver.sonatypeRepo("snapshots")

// depends on hadoop, hbase and junit
libraryDependencies ++= Seq(
		    "com.beust" % "jcommander" % "1.35",
		    "org.apache.hadoop" % "hadoop-core" % "1.2.1" % "provided",
		    "org.apache.hbase" % "hbase" % "0.92.0" intransitive(),
		    "junit" % "junit" % "4.11" % "test",
		    "com.novocode" % "junit-interface" % "0.10" % "test",
		    "com.jsuereth" % "scala-arm_2.11" % "1.4",
		    "org.scalatest" % "scalatest_2.11" % "2.2.1" % "test"
		    )

scalaVersion := "2.11.4"

// output jar name is simply ruleXtract.jar
artifactName in (Compile, packageBin) := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
	     artifact.name + "." + artifact.extension
}

// output jar is here: target/ruleXtract.jar
artifactPath in (Compile, packageBin) ~= { defaultPath =>
	     file("target") / defaultPath.getName
}

// we want a jar without a main class so we can run it as "hadoop jar class args"
mainClass in (Compile, packageBin) := None
// this works too, see http://stackoverflow.com/questions/18325181/suppress-main-class-in-sbt-assembly
//packageOptions in (Compile, packageBin) ~= { os =>
//	       os filterNot {_.isInstanceOf[Package.MainClass]} }
