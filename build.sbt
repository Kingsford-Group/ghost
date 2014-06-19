import AssemblyKeys._

assemblySettings

jarName in assembly := "Ghost.jar"

test in assembly := {}

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("com", "sun", "jna", xs @ _*) => MergeStrategy.first
    case x => old(x)
  }
}

mainClass in assembly := Some("edu.umd.cbcb.ghost.align.LocalAligner")

organization := "University of Maryland"

name := "GHOST"

version := "1.0"

scalaVersion := "2.11.1"

crossScalaVersions := Seq("2.11")

unmanagedSources in Compile ~= { srcFiles => srcFiles.filterNot{ x => x.name.contains("GenerateBioPlotFile") } }

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", 
                      "-language:implicitConversions", "-language:higherKinds", "-language:postfixOps")

resolvers += "ScalaTools snapshots at Sonatype" at "https://oss.sonatype.org/content/repositories/snapshots/"

resolvers += "Scala Tools Nexus Snapshots" at "http://nexus.scala-tools.org/content/repositories/snapshots/"

resolvers += "Scala Tools Nexus Releases" at "http://nexus.scala-tools.org/content/repositories/releases/"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "Maven Repository" at "http://repo1.maven.org/maven2"

resolvers += "ScalaNLP Repository" at "http://repo.scalanlp.org/repo/"

resolvers += "JGraphT Repository" at "http://maven.irisa.fr/artifactory/list/repo/"

resolvers += "NativeLibs4Java Repository" at "http://nativelibs4java.sourceforge.net/maven/"

resolvers += Resolver.sonatypeRepo("public")

autoCompilerPlugins := true

// addCompilerPlugin("com.nativelibs4java" % "scalacl-compiler-plugin" % "0.2")

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.3.3",
  "org.scala-lang" % "scala-actors" % "2.11.1",
  "org.scala-lang" % "scala-actors-migration_2.11" % "1.1.0",
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,
  "javax.transaction" % "jta" % "1.1",
  "jgrapht" % "jgrapht" % "0.8.2" ,
  "org.scala-lang.modules" %% "scala-xml" % "1.0.2",
  "com.googlecode.netlib-java" % "netlib-java" % "0.9.3" ,
  "netlib" % "arpack-combo" % "0.1" ,
  "net.java.dev.jna" % "jna" % "3.3.0" ,
  "com.googlecode.efficient-java-matrix-library" % "ejml" % "0.17" ,
  "org.clapper" %% "grizzled-scala" % "1.2" ,
  "com.github.scopt" %% "scopt" % "3.2.0" ,
  "com.github.scala-incubator.io" %% "scala-io-core" % "0.4.3",
  "com.github.scala-incubator.io" %% "scala-io-file" % "0.4.3",
  "org.rogach" %% "scallop" % "0.9.5"
)


//makeInJarFilter <<= (makeInJarFilter) {
//  (makeInJarFilter) => {
//    (file) => file match {
//      case "arpack_combined_all-0.1.jar" => makeInJarFilter(file) + ",!META-INF/**,!org/**"
//      case "jgrapht-jdk1.6.jar" => makeInJarFilter(file) + ",!org/**"
//      case _ => makeInJarFilter(file) + ",!**/*.RSA,!**/*.SF,!**/*.INF"
//    }
//  }
//}

//proguardOptions ++= Seq(
//  keepMain("edu.umd.cbcb.ghost.align.ComputeSubgraphSignatures"),
//  keepMain("edu.umd.cbcb.ghost.align.LocalAligner"),
//  keepAllScala
//)
