
# ReMac

ReMac is a distributed matrix computation library developed upon [SystemML 2.0.0](http://systemds.apache.org/docs/2.0.0/index), 
which eliminates implicit redundant subexpressions to accelerate the execution for iterative algorithms.

## Compilation

Just download the source code and use `mvn clean package` to compile the project.

## Datasets and Algorithms

The datasets used in our experiments are described in Section 6.1.

The scripts implementing the algorithms used in our experiments are in the folder `dmls`.

## Run 

### Configuration

The usage of System DS can be found at [SystemML 2.0.0](http://systemds.apache.org/docs/2.0.0/index).

ReMac's default matrix sparsity estimators is metadata estimator, and you can use the mnc estimator via the `-mnc` option.

ReMac's default optimize method is dynamic programming, and you can use the brute force method by `-optimizer force`. 

ReMac's default worker number and core number for the cost estimator is 6 and 24, and you can set the worker number and the core number by `-worker_num  6` and `-core_num 24` .

There are some specific elimination methods built in ReMac as the experiment baseline, which can be used by
`-optimizer manual -manual_type <manual-type-name>` , and the manual-type-name options are: 
`dfp`, `dfp-ata`, `dfp-ata-dtd`, `dfp-hata`, `dfp-ata-dtd`, `bfgs`, `bfgs-ata`, `bfgs-ata-dtd`, `gd-ata`, `gd-atb`.

### Examples

Use the dynamic programming optimizer, the mnc sparsity optimizer, to run the dfp algorithm on criteo1 dataset.
```
spark-submit --executor-memory 30G --driver-memory 30G  SystemDS.jar -optimizer dp -mnc -explain recompile_runtime -stats -f ./dmls/dfp.dml -nvargs name=criteo1 
```

Use the brute force optimizer, the metadata sparsity optimizer, to run the bfgs algorithm on criteo1 dataset.
```
spark-submit --executor-memory 30G --driver-memory 30G  SystemDS.jar -optimizer dp -explain recompile_runtime -stats -f ./dmls/bfgs.dml -nvargs name=criteo1 
```

Use the built in `gd-ata` elimination methods, to run the gd algorithm on criteo1 dataset.
```
spark-submit --executor-memory 30G --driver-memory 30G  SystemDS.jar -optimizer manual -manual_type gd-ata -explain recompile_runtime -stats -f ./dmls/gd.dml -nvargs name=criteo1 
```



