inThisBuild(
  Seq(
    organization := "io.loustler",
    scalaVersion := "2.12.12",
    version := "0.1.0"
  )
)

lazy val root = project
    .in(file("."))
    .settings(
        name := "data-factory",
        scalacOptions := Seq(
            "-deprecation",
            "-encoding",
            "UTF-8",
            "-feature",
            "-unchecked"
        ),
        libraryDependencies ++= Seq(
            "org.scalatest" %% "scalatest" % "3.2.3" % Test
        )
    )