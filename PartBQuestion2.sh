#!/usr/bin/env bash

# for python
spark-submit question2.py /tweets /out


# for spark
sbt package
spark-submit --class "StructuredStreaming" target/scala-2.11/partb2_2.11-1.0.jar /tweets output