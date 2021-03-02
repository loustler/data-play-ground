inThisBuild(
  Seq(
    organization := "io.loustler",
    scalaVersion := "2.12.12",
    version := "0.1.0",
    javacOptions ++= Seq("-source", "11", "-target", "11")
  )
)

addCommandAlias(
  "ci",
  "clean; reload; coverage; compile; test:compile; test; coverageReport; scalafmtCheckAll; scalafmtSbtCheck"
)

lazy val root = project
  .in(file("."))
  .settings(
    name := "data-play-ground",
    scalacOptions := Seq(
      "-deprecation",
      "-encoding",
      "UTF-8",
      "-feature",
      "-unchecked"
    ),
    libraryDependencies ++= Seq(
      "org.apache.spark"        %% "spark-core"   % Versions.Spark,
      "org.apache.spark"        %% "spark-sql"    % Versions.Spark,
      "org.apache.spark"        %% "spark-avro"   % Versions.Spark,
      "org.apache.hadoop"        % "hadoop-aws"   % Versions.Hadoop,
      "org.apache.logging.log4j" % "log4j-core"   % Versions.Log4j,
      "info.picocli"             % "picocli"      % Versions.PicoCli,
      "com.typesafe"             % "config"       % Versions.TypeSafeConfig,
      "com.github.pureconfig"   %% "pureconfig"   % Versions.PureConfig,
      "software.amazon.awssdk"   % "aws-sdk-java" % Versions.AwsSdk excludeAll (
        ExclusionRule("com.fasterxml.jackson.core"),
        ExclusionRule("com.fasterxml.jackson.dataformat")
      ),
      "org.scalatest"  %% "scalatest"  % Versions.ScalaTest  % Test,
      "org.scalacheck" %% "scalacheck" % Versions.ScalaCheck % Test
    )
  )
  .settings(
    assemblyJarName in assembly := s"${name.value}.jar",
    test in assembly := {},
    assembledMappings in assembly += {
      sbtassembly.MappingSet(
        None,
        Vector(
          file("conf/release.conf") -> "application.conf"
        )
      )
    },
    assemblyMergeStrategy in assembly := {
      case PathList("org", "apache", "spark", _)     => MergeStrategy.discard
      case PathList("META-INF", _)                   => MergeStrategy.discard
      case PathList("scala", "test", "resources", _) => MergeStrategy.discard
      case PathList(_, "test", _)                    => MergeStrategy.discard
      case _                                         => MergeStrategy.first
    }
  )
