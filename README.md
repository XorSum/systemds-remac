# ReMac

ReMac is a distributed matrix computation library developed based on [SystemDS 2.0.0](http://systemds.apache.org/docs/2.0.0/index), 
which automatically and adaptively eliminates redundancy in execution plans to improve performance.

## Installation

Download the source code of [SystemDS 2.0.0](https://github.com/apache/systemds/tree/98b21a4923793e7458dfe13c2bc0a10d15f9fe72) and the folder `src` in this repository.

Replace the original `src` of SystemDS with the `src` of ReMac.

Follow the [installation guide](https://systemds.apache.org/docs/2.0.0/site/install#build-the-project) of SystemDS to build the project.

## Datasets and Algorithms

The datasets used in our experiments are described in Section 6.1.

The scripts implementing the algorithms used in our experiments are in the folder `scripts`.

## Running ReMac

The running command of ReMac is the same as [that of SystemDS](https://systemds.apache.org/docs/2.0.0/site/run#executing-the-dml-script).

In addition, there are optional arguments for ReMac:

* The default estimator of matrix sparsity is metadata-based. To use the MNC estimator, you need to add `-mnc`.

* ReMac uses the dynamic programming-based method for adaptive elimination in default. To use the enumeration method, you need to add `-optimizer force`.

For example, the command to run the DFP algorithm on the criteo1 dataset with the MNC estimator and the dynamic programming-based method is
```shell
spark-submit SystemDS.jar -mnc -optimizer dp -stats -f ./scripts/dfp.dml -nvargs name=criteo1 
```
