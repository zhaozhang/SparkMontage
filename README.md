SparkMontage
============

# Building

We use maven to build. You can build with:

```
mvn clean package
```

We require a local jar to be installed the first time you build. We provide this jar in the repository. Install this jar with:

```
mvn install:install-file -Dfile=lib/jfits-0.94.jar -DgroupId="org.esa" -DartifactId="fits" -Dpackaging="jar" -Dversion="0.94"
```

# Single threaded Madd

The single threaded Madd is packaged using Appassembler. To run, do:

```
sh target/appassembler/bin/madd
```

# Spark Madd

The spark version has a provided submit script. To run, do:

```
bin/madd-submit
```

You will need to have a copy of the Spark binaries. Set `$SPARK_HOME` to the path to the binaries.