This repository contains the source code implementation of the paper "[Redundancy Elimination in Distributed Matrix Computation](https://dl.acm.org/doi/10.1145/3514221.3517877)", which appeared at SIGMOD 2022.

# ReMac

ReMac is a distributed matrix computation library developed based on [SystemDS 2.0.0](http://systemds.apache.org/docs/2.0.0/index), 
which automatically and adaptively eliminates redundancy in execution plans to improve performance.

## Installation

* Prerequisite: Java 8

* [Download](https://archive.apache.org/dist/hadoop/common/hadoop-3.2.2/hadoop-3.2.2.tar.gz), [setup and start](https://hadoop.apache.org/docs/r3.2.2/hadoop-project-dist/hadoop-common/ClusterSetup.html) Hadoop 3.2.2 on your cluster. You need to set up HDFS, but YARN is not necessary. (`config/hadoop/` lists the configuration used in the paper.)

* [Download](https://archive.apache.org/dist/spark/spark-3.0.1/spark-3.0.1-bin-hadoop3.2.tgz), [setup and start](https://spark.apache.org/docs/3.0.1/spark-standalone.html#installing-spark-standalone-to-a-cluster) Spark 3.0.1 on your cluster. (`config/spark/` lists the configuration used in the paper.)

  For [Spark configuration](https://spark.apache.org/docs/3.0.1/configuration.html), in the file of `spark-defaults.conf`, you need to specify `spark.driver.memory`, `spark.executor.cores` and `spark.executor.instances` which are essential to the Optimizer of ReMac.

  In addition, we recommend adding:
  * `spark.serializer org.apache.spark.serializer.KryoSerializer` to employ the Kryo serialization library.
  * `spark.driver.extraJavaOptions "-Xss256M"` and `spark.executor.extraJavaOptions "-Xss256M"` in case of java.lang.StackOverflowError.

* Download the source code of [SystemDS 2.0.0](https://github.com/apache/systemds/archive/98b21a4923793e7458dfe13c2bc0a10d15f9fe72.zip) as a zip file and not git clone.

* Replace the original `src` of SystemDS with the `remac/src` in this repository.

* Follow the [installation guide](https://systemds.apache.org/docs/2.0.0/site/install#build-the-project) of SystemDS to build the project. The building artifact `SystemDS.jar` is in the `target` folder.

## Algorithms and Datasets

The scripts implementing the algorithms used in our experiments are in the folder `scripts`.

The datasets used in our experiments are described in Section 6.1.
For a quick start, you can use `scripts/data.dml` to generate random datasets that have the same metadata as mentioned in the paper.

## Running ReMac

The running command of ReMac is the same as [that of SystemDS](https://systemds.apache.org/docs/2.0.0/site/run#executing-the-dml-script).

In addition, there are options in running ReMac.

* The default matrix sparsity estimator is MNC. To use the metadata-based estimator, you need to add `-metadata` in the command line.

* ReMac uses the dynamic programming-based method for adaptive elimination in default. To use the enumeration method, you need to add `-optimizer force` in the command line.

  For example, the command to run the DFP algorithm on the criteo1 dataset with the metadata-based estimator and the enumeration method is
  ```shell
  spark-submit /path/to/SystemDS.jar -metadata -optimizer force -stats -f ./scripts/dfp.dml -nvargs name=criteo1 
  ```
  (note: You need to add `spark-submit` to the environment variable `PATH`, and run this command from the `target` directory.)

* In particular, ReMac employs the block-wise search for redundancy. To employ the tree-wise search, you need to use the folder `remac-tree_search/src` to override `src` and rebuild the project.
