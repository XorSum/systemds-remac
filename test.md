
`spark-shell --executor-memory 4G --driver-memory 4G --jars SystemML.jar`

| size  | optimized | unoptimized |   rate  |
|-------|-----------|-------------|---------|
|100    |    0.064  |    0.071    |  0.9050 |
|1000   |    0.751  |    1.307    |  0.5748 |
|10000  |  124.772  |  143.510    |  0.8694 |


`spark-shell  --master spark://10.11.1.209:7077 --executor-memory 4G --driver-memory 4G --jars SystemML.jar`

| size    |  optimized   |  unoptimized |  rate  |
|---------|--------------|--------------|--------|
| 100     |     0.105    |      0.020   | 5.25   |
| 1000    |     0.126    |      0.120   | 1.05   |
| 10000   |    10.564    |     25.191   | 0.4193 |
| 100000  |    23.645    |     47.133   | 0.5016 |


`spark-shell  --master spark://10.11.1.209:7077 --jars SystemML.jar`


(10000,1.111418539,4.801518735,0.23147229040229914), 
(20000,1.972673142,4.80303003,0.41071430527782893),
(30000,3.282501805,4.233336636,0.7753935222362978),
(40000,4.848421775,6.008589545,0.8069151235391966), 
(50000,7.220544821,8.618753671,0.837771341034536), 
(60000,9.404428332,11.870371349,0.7922606678006128), 
(70000,12.523316069,15.843215196,0.7904529424154898), 
(80000,16.369973991,20.30124405,0.8063532437067569), 
(90000,20.095341798,25.688616059,0.7822664230663995)
(100000,24.865135565,34.328775201,0.7243234114649006)


``` scala
import org.apache.sysml.api.mlcontext._
import org.apache.sysml.api.mlcontext.ScriptFactory._
val ml = new MLContext(spark)

import scala.collection.mutable.ListBuffer

def time[A](f: => A): Double = {
  var rounds = 10
  val start_time = System.nanoTime
  for (_ <- 1 to rounds) {
    f
  }
  val end_time = System.nanoTime
  val average_time = (end_time - start_time) / rounds / 1e6
  println(" Time: " + average_time + "ms")
  average_time
}

def run_dml_a(s:Int): Unit = {
  val script = ScriptFactory.dml("X = rand(rows=$s, cols=$s, min=0, max=10, sparsity=1)\nY = rand(rows=$s, cols=1, min=0, max=10, sparsity=1)\nW = (X %*% Y)\nZ = (W %*% t(W) )/ (t(W) %*% Y)").in("$s",s).out("Z")
  ml.execute(script)
}

def run_dml_b(s:Int): Unit = {
  val script = ScriptFactory.dml("X = rand(rows=$s, cols=$s, min=0, max=10, sparsity=1)\nY = rand(rows=$s, cols=1, min=0, max=10, sparsity=1)\nZ = (X %*% Y %*% t(Y) %*% t(X)) / (t(Y) %*% t(X) %*% Y)").in("$s",s).out("Z")
  ml.execute(script)
}


var statistics = new ListBuffer[(Int, Double,Double,Double)]()

for (i:Int <- 2 to 6) {
  var s = Math.pow(10,i).toInt
  var time_a = time(run_dml_a(s))
  var time_b = time(run_dml_b(s))
  var deta = (time_b-time_a)/time_b
  statistics.append((i, time_a,time_b,deta))
  println("Size = "+s +" Time = " + time_a+" "+time_b,deta)
  println(statistics.toList)
}
println(statistics.toList)
```