name := "dl-scala"
version := "0.1"
scalaVersion := "2.11.8"
organization := "Belcorp"

resolvers ++= Seq(
  "Redshift" at "http://redshift-maven-repository.s3-website-us-east-1.amazonaws.com/release",
  "jitpack.io" at "https://jitpack.io",
  "Artima Maven Repository" at "http://repo.artima.com/releases"
)

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.2" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2" % "provided"

libraryDependencies += "com.github.databricks" % "spark-redshift" % "8adfe95a25"
libraryDependencies += "org.rogach" %% "scallop" % "3.1.2"

libraryDependencies += "com.zaxxer" % "HikariCP" % "3.1.0"
libraryDependencies += "com.amazon.redshift" % "redshift-jdbc42" % "1.2.15.1025"
libraryDependencies += "org.jooq" % "jooq" % "3.11.4"
libraryDependencies += "org.jooq" % "jooq-meta" % "3.11.4"

libraryDependencies += "com.amazonaws" % "aws-java-sdk-sns" % "1.11.443"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-lambda" % "1.11.443"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.443"
libraryDependencies += "com.amazonaws" % "dynamodb-lock-client" % "1.0.0"

libraryDependencies += "com.typesafe.play" %% "play-json" % "2.6.7"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @_*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

//assemblyJarName in assembly := "semantix-belcorp.jar"

test in assembly := {}
