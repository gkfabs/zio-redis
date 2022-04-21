addSbtPlugin("ch.epfl.scala"                     % "sbt-bloop"        % "1.4.13")
addSbtPlugin("ch.epfl.scala"                     % "sbt-scalafix"     % "0.9.34")
addSbtPlugin("com.eed3si9n"                      % "sbt-buildinfo"    % "0.11.0")
addSbtPlugin("com.github.sbt"                    % "sbt-ci-release"   % "1.5.10")
addSbtPlugin("com.github.sbt"                    % "sbt-unidoc"       % "0.5.0")
addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings" % "3.0.2")
addSbtPlugin("de.heikoseeberger"                 % "sbt-header"       % "5.7.0")
addSbtPlugin("org.scalameta"                     % "sbt-mdoc"         % "2.3.2")
addSbtPlugin("org.scalameta"                     % "sbt-scalafmt"     % "2.4.6")
addSbtPlugin("pl.project13.scala"                % "sbt-jmh"          % "0.4.3")

libraryDependencies += "org.snakeyaml" % "snakeyaml-engine" % "2.3"
