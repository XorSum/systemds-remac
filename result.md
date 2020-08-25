Z = (X %*% Y) %*% (t(Y) %*% t(X)) %*% (X %*% Y)
// 发现x*y, 未发现ty*tx可以转换为t(x*y)

PROGRAM
--MAIN PROGRAM
----GENERIC (lines 1-8) [recompile=false]
------(26) u(print) [-1,-1,-1,-1] [0,0,0 -> 0MB]
------(17) dg(rand) [10,1,-1,10] [0,0,0 -> 0MB] y
------(19) r(t) (17) [1,10,-1,10] [0,0,0 -> 0MB] ty
------(8) dg(rand) [10,10,-1,100] [0,0,0 -> 0MB] x
------(20) r(t) (8) [10,10,-1,100] [0,0,0 -> 0MB] tx
------(18) ba(+*) (8,17) [10,1,-1,-1] [0,0,0 -> 0MB] x*y
------(21) ba(+*) (20,18) [10,1,-1,-1] [0,0,0 -> 0MB] tx*x*y
------(22) ba(+*) (19,21) [1,1,-1,-1] [0,0,0 -> 0MB] ty*tx*x*y
------(29) u(castdts) (22) [0,0,0,-1] [0,0,0 -> 0MB]
------(30) b(*) (29,18) [10,1,-1,-1] [0,0,0 -> 0MB] u(ty*tx*x*y) * x*y
------(27) TOSTRING (30) [0,0,-1,-1] [0,0,0 -> 0MB]
------(28) u(print) (27) [-1,-1,-1,-1] [0,0,0 -> 0MB]

Z = (X %*% Y) %*% t(Y) %*% t(X) %*% (X %*% Y)
// 发现了(x*y)， 但是未发现ty*tx 
------(17) dg(rand) [10,1,-1,10] [0,0,0 -> 0MB] y
------(19) r(t) (17) [1,10,-1,10] [0,0,0 -> 0MB] t(y)
------(8) dg(rand) [10,10,-1,100] [0,0,0 -> 0MB] x
------(21) r(t) (8) [10,10,-1,100] [0,0,0 -> 0MB] t(x)
------(18) ba(+*) (8,17) [10,1,-1,-1] [0,0,0 -> 0MB] x*y
------(20) ba(+*) (21,18) [10,1,-1,-1] [0,0,0 -> 0MB] t(x)*x*y
------(22) ba(+*) (19,20) [1,1,-1,-1] [0,0,0 -> 0MB] t(y)*t(x)*x*y
------(29) u(castdts) (22) [0,0,0,-1] [0,0,0 -> 0MB] 
------(30) b(*) (29,18) [10,1,-1,-1] [0,0,0 -> 0MB]  x*y*t(y)*t(x)*x*y
------(27) TOSTRING (30) [0,0,-1,-1] [0,0,0 -> 0MB]
------(28) u(print) (27) [-1,-1,-1,-1] [0,0,0 -> 0MB]


Z = X %*% Y %*% t(Y) %*% t(X) %*% X %*% Y
// 未发现子表达式
PROGRAM
--MAIN PROGRAM
----GENERIC (lines 1-8) [recompile=false]
------(8) dg(rand) [10,10,-1,100] [0,0,0 -> 0MB] x
------(17) dg(rand) [10,1,-1,10] [0,0,0 -> 0MB] y
------(19) r(t) (17) [1,10,-1,10] [0,0,0 -> 0MB] t(y)
------(21) r(t) (8) [10,10,-1,100] [0,0,0 -> 0MB] t(x)
------(18) ba(+*) (8,17) [10,1,-1,-1] [0,0,0 -> 0MB] x*y
------(20) ba(+*) (21,18) [10,1,-1,-1] [0,0,0 -> 0MB] tx*x*y
------(22) ba(+*) (19,20) [1,1,-1,-1] [0,0,0 -> 0MB] ty*tx*x*y
------(29) u(castdts) (22) [0,0,0,-1] [0,0,0 -> 0MB] 
------(30) b(*) (29,17) [10,1,-1,-1] [0,0,0 -> 0MB] y*ty*tx*x*y
------(24) ba(+*) (8,30) [10,1,-1,-1] [0,0,0 -> 0MB] x*y*ty*tx*x*y
------(27) TOSTRING (24) [0,0,-1,-1] [0,0,0 -> 0MB]
------(28) u(print) (27) [-1,-1,-1,-1] [0,0,0 -> 0MB]


Z = (X %*% Y) %*% t(X %*% Y) %*% (X %*% Y)
// 发现了x*y
--MAIN PROGRAM
----GENERIC (lines 1-8) [recompile=false]
------(25) u(print) [-1,-1,-1,-1] [0,0,0 -> 0MB]
------(8) dg(rand) [10,10,-1,100] [0,0,0 -> 0MB]
------(17) dg(rand) [10,1,-1,10] [0,0,0 -> 0MB]
------(18) ba(+*) (8,17) [10,1,-1,-1] [0,0,0 -> 0MB]
------(20) r(t) (18) [1,10,-1,-1] [0,0,0 -> 0MB]
------(21) ba(+*) (20,18) [1,1,-1,-1] [0,0,0 -> 0MB]
------(28) u(castdts) (21) [0,0,0,-1] [0,0,0 -> 0MB]
------(29) b(*) (28,18) [10,1,-1,-1] [0,0,0 -> 0MB]
------(26) TOSTRING (29) [0,0,-1,-1] [0,0,0 -> 0MB]
------(27) u(print) (26) [-1,-1,-1,-1] [0,0,0 -> 0MB]

Z = X %*% Y %*% t(X %*% Y) %*% X %*% Y
// 发现x*y
PROGRAM
--MAIN PROGRAM
----GENERIC (lines 1-8) [recompile=false]
------(25) u(print) [-1,-1,-1,-1] [0,0,0 -> 0MB]
------(8) dg(rand) [10,10,-1,100] [0,0,0 -> 0MB]
------(17) dg(rand) [10,1,-1,10] [0,0,0 -> 0MB]
------(18) ba(+*) (8,17) [10,1,-1,-1] [0,0,0 -> 0MB]
------(20) r(t) (18) [1,10,-1,-1] [0,0,0 -> 0MB]
------(22) ba(+*) (20,18) [1,1,-1,-1] [0,0,0 -> 0MB]
------(28) u(castdts) (22) [0,0,0,-1] [0,0,0 -> 0MB]
------(29) b(*) (28,18) [10,1,-1,-1] [0,0,0 -> 0MB]
------(26) TOSTRING (29) [0,0,-1,-1] [0,0,0 -> 0MB]
------(27) u(print) (26) [-1,-1,-1,-1] [0,0,0 -> 0MB]

Z = (X %*% Y %*% t(Y) %*% t(X)) / (t(Y) %*% t(X) %*% Y)
// 未发现子表达式
PROGRAM
--MAIN PROGRAM
----GENERIC (lines 1-11) [recompile=false]
------(29) u(print) [-1,-1,-1,-1] [0,0,0 -> 0MB]
------(8) dg(rand) [10,10,-1,100] [0,0,0 -> 0MB] x
------(30) TOSTRING (8) [0,0,-1,-1] [0,0,0 -> 0MB] 
------(31) u(print) (30) [-1,-1,-1,-1] [0,0,0 -> 0MB]
------(33) u(print) [-1,-1,-1,-1] [0,0,0 -> 0MB]
------(17) dg(rand) [10,1,-1,10] [0,0,0 -> 0MB] y
------(34) TOSTRING (17) [0,0,-1,-1] [0,0,0 -> 0MB]
------(35) u(print) (34) [-1,-1,-1,-1] [0,0,0 -> 0MB]
------(37) u(print) [-1,-1,-1,-1] [0,0,0 -> 0MB]
------(20) ba(+*) (8,17) [10,1,-1,-1] [0,0,0 -> 0MB] x*y
------(19) r(t) (17) [1,10,-1,10] [0,0,0 -> 0MB] ty
------(21) r(t) (8) [10,10,-1,100] [0,0,0 -> 0MB] tx
------(18) ba(+*) (19,21) [1,10,-1,-1] [0,0,0 -> 0MB] ty*tx
------(22) ba(+*) (20,18) [10,10,-1,-1] [0,0,0 -> 0MB]  x*y*ty*tx
------(25) ba(+*) (21,17) [10,1,-1,-1] [0,0,0 -> 0MB] tx*y
------(26) ba(+*) (19,25) [1,1,-1,-1] [0,0,0 -> 0MB] ty*tx*y
------(40) u(castdts) (26) [0,0,0,-1] [0,0,0 -> 0MB] (x*y*ty*tx)/(ty*tx*y)
------(27) b(/) (22,40) [10,10,-1,-1] [0,0,0 -> 0MB] 
------(38) TOSTRING (27) [0,0,-1,-1] [0,0,0 -> 0MB]
------(39) u(print) (38) [-1,-1,-1,-1] [0,0,0 -> 0MB]

Z = ((X %*% Y) %*% (t(Y) %*% t(X))) / ((t(Y) %*% t(X)) %*% Y)
// 把(t(Y) %*% t(X))作为子表达式，但是未转化为t(x*y)
PROGRAM
--MAIN PROGRAM
----GENERIC (lines 1-11) [recompile=false]
------(29) u(print) [-1,-1,-1,-1] [0,0,0 -> 0MB]
------(8) dg(rand) [10,10,-1,100] [0,0,0 -> 0MB] x
------(30) TOSTRING (8) [0,0,-1,-1] [0,0,0 -> 0MB]
------(31) u(print) (30) [-1,-1,-1,-1] [0,0,0 -> 0MB]
------(33) u(print) [-1,-1,-1,-1] [0,0,0 -> 0MB]
------(17) dg(rand) [10,1,-1,10] [0,0,0 -> 0MB] y
------(34) TOSTRING (17) [0,0,-1,-1] [0,0,0 -> 0MB]
------(35) u(print) (34) [-1,-1,-1,-1] [0,0,0 -> 0MB]
------(37) u(print) [-1,-1,-1,-1] [0,0,0 -> 0MB]
------(18) ba(+*) (8,17) [10,1,-1,-1] [0,0,0 -> 0MB] x*y
------(19) r(t) (17) [1,10,-1,10] [0,0,0 -> 0MB] ty
------(20) r(t) (8) [10,10,-1,100] [0,0,0 -> 0MB] tx
------(21) ba(+*) (19,20) [1,10,-1,-1] [0,0,0 -> 0MB] ty*tx
------(22) ba(+*) (18,21) [10,10,-1,-1] [0,0,0 -> 0MB] x*y*ty*tx
------(26) ba(+*) (21,17) [1,1,-1,-1] [0,0,0 -> 0MB] ty*tx*y
------(40) u(castdts) (26) [0,0,0,-1] [0,0,0 -> 0MB]
------(27) b(/) (22,40) [10,10,-1,-1] [0,0,0 -> 0MB] (x*y*ty*tx)/(ty*tx*y)
------(38) TOSTRING (27) [0,0,-1,-1] [0,0,0 -> 0MB]
------(39) u(print) (38) [-1,-1,-1,-1] [0,0,0 -> 0MB]


Z = ((X %*% Y) %*% (t(Y) %*% t(X))) / ((t(Y) %*% t(X)) %*% Y)

Z = t(Y) %*% t(X)




X = rand(rows=10, cols=1, min=0, max=10, sparsity=1)
Y = rand(rows=1, cols=10, min=0, max=10, sparsity=1)

Z = X %*% Y %*% X %*% Y %*% X %*% Y %*% X %*% Y 
 
PROGRAM
--MAIN PROGRAM
----GENERIC (lines 1-6) [recompile=false]
------(8) dg(rand) [10,1,-1,10] [0,0,0 -> 0MB] x
------(17) dg(rand) [1,10,-1,10] [0,0,0 -> 0MB] y 
------(21) ba(+*) (17,8) [1,1,-1,-1] [0,0,0 -> 0MB] y*x 
------(29) u(castdts) (21) [0,0,0,-1] [0,0,0 -> 0MB] 
------(37) b(*) (29,29) [0,0,0,-1] [0,0,0 -> 0MB] 
------(38) b(*) (29,37) [0,0,0,-1] [0,0,0 -> 0MB]
------(39) b(*) (17,38) [1,10,-1,-1] [0,0,0 -> 0MB]
------(24) ba(+*) (8,39) [10,10,-1,-1] [0,0,0 -> 0MB]
------(25) TOSTRING (24) [0,0,-1,-1] [0,0,0 -> 0MB]
------(26) u(print) (25) [-1,-1,-1,-1] [0,0,0 -> 0MB]





Z = ((X %*% Y) %*% (t(Y) %*% t(X))) / ((t(Y) %*% t(X)) %*% Y)
Z = (X %*% Y %*% t(Y) %*% t(X)) / (t(Y) %*% t(X) %*% Y)


a = rand(rows=10, cols=10, min=0, max=10, sparsity=1)
b = rand(rows=10, cols=10, min=0, max=10, sparsity=1)
c = rand(rows=10, cols=10, min=0, max=10, sparsity=1)

d = a %*% b %*% c %*% a %*% b %*% c




X = rand(rows=$s, cols=$s, min=0, max=10, sparsity=1)
Y = rand(rows=$s, cols=1, min=0, max=10, sparsity=1)
W = X %*% Y
Z = (W %*% t(W)) / (t(W) %*% Y)
print(toString(Z))


X = rand(rows=10, cols=10, min=0, max=10, sparsity=1)
Y = rand(rows=10, cols=1, min=0, max=10, sparsity=1)
Z = (X %*% Y %*% t(Y) %*% t(X)) / (t(Y) %*% t(X) %*% Y)
print(toString(Z))


a = rand(rows = 100,cols = 10)
b = rand(rows = 100, cols = 1)
x = matrix(0,rows=10,cols = 1)
i = 1
alpha = 0.1
while(i<10) {
    d = t(a)%*%(a%*%x - b)
    x = x - alpha * d
    i = i + 1
}
print(toString(x))

PROGRAM
--MAIN PROGRAM
----GENERIC (lines 1-5) [recompile=false]
------(8) dg(rand) [100,10,-1,1000] [0,0,0 -> 0MB]
------(9) TWrite a (8) [100,10,-1,1000] [0,0,0 -> 0MB]
------(18) dg(rand) [100,1,-1,100] [0,0,0 -> 0MB]
------(19) TWrite b (18) [100,1,-1,100] [0,0,0 -> 0MB]
------(28) dg(rand) [10,1,-1,0] [0,0,0 -> 0MB]
------(29) TWrite x (28) [10,1,-1,0] [0,0,0 -> 0MB]
------(31) TWrite i [0,0,-1,-1] [0,0,0 -> 0MB]
----WHILE (lines 6-10)
------(34) TRead i [-1,-1,-1,-1] [0,0,0 -> 0MB]
------(36) b(<) (34) [0,0,-1,-1] [0,0,0 -> 0MB]
------(37) TWrite __pred (36) [0,0,0,-1] [0,0,0 -> 0MB]
------GENERIC (lines 7-9) [recompile=false]
--------(38) TRead x [10,1,-1,-1] [0,0,0 -> 0MB]
--------(39) TRead a [100,10,-1,1000] [0,0,0 -> 0MB]
--------(43) r(t) (39) [10,100,-1,1000] [0,0,0 -> 0MB]
--------(41) TRead b [100,1,-1,100] [0,0,0 -> 0MB]
--------(57) ba(+*) (43,41) [10,1,-1,-1] [0,0,0 -> 0MB]
--------(58) b(-) (57) [10,1,-1,-1] [0,0,0 -> 0MB]
--------(62) t(-*) (38,58) [10,1,-1,-1] [0,0,0 -> 0MB]
--------(49) TWrite x (62) [10,1,-1,-1] [0,0,0 -> 0MB]
--------(40) TRead i [-1,-1,-1,-1] [0,0,0 -> 0MB]
--------(51) b(+) (40) [0,0,-1,-1] [0,0,0 -> 0MB]
--------(52) TWrite i (51) [0,0,-1,-1] [0,0,0 -> 0MB]
----GENERIC (lines 11-11) [recompile=false]
------(53) TRead x [10,1,-1,-1] [0,0,0 -> 0MB]
------(54) TOSTRING (53) [0,0,-1,-1] [0,0,0 -> 0MB]
------(55) u(print) (54) [-1,-1,-1,-1] [0,0,0 -> 0MB]



a = rand(rows = 100,cols = 10)
b = rand(rows = 100, cols = 1)
x = matrix(0,rows=10,cols = 1)
i = 1
alpha = 0.1
while(i<10) {
    m = t(a)%*%a
    n = t(a)%*%b
    d = m%*%x - n
    x = x - alpha * d
    i = i + 1
}
print(toString(x))

PROGRAM
--MAIN PROGRAM
----GENERIC (lines 1-5) [recompile=false]
------(8) dg(rand) [100,10,-1,1000] [0,0,0 -> 0MB]
------(9) TWrite a (8) [100,10,-1,1000] [0,0,0 -> 0MB]
------(18) dg(rand) [100,1,-1,100] [0,0,0 -> 0MB]
------(19) TWrite b (18) [100,1,-1,100] [0,0,0 -> 0MB]
------(28) dg(rand) [10,1,-1,0] [0,0,0 -> 0MB]
------(29) TWrite x (28) [10,1,-1,0] [0,0,0 -> 0MB]
------(31) TWrite i [0,0,-1,-1] [0,0,0 -> 0MB]
----WHILE (lines 6-12)
------(34) TRead i [-1,-1,-1,-1] [0,0,0 -> 0MB]
------(36) b(<) (34) [0,0,-1,-1] [0,0,0 -> 0MB]
------(37) TWrite __pred (36) [0,0,0,-1] [0,0,0 -> 0MB]
------GENERIC (lines 7-11) [recompile=false]
--------(38) TRead x [10,1,-1,-1] [0,0,0 -> 0MB]
--------(39) TRead a [100,10,-1,1000] [0,0,0 -> 0MB]
--------(43) r(t) (39) [10,100,-1,1000] [0,0,0 -> 0MB]
--------(41) TRead b [100,1,-1,100] [0,0,0 -> 0MB]
--------(46) ba(+*) (43,41) [10,1,-1,-1] [0,0,0 -> 0MB]
--------(48) b(-) (46) [10,1,-1,-1] [0,0,0 -> 0MB]
--------(59) t(-*) (38,48) [10,1,-1,-1] [0,0,0 -> 0MB]
--------(51) TWrite x (59) [10,1,-1,-1] [0,0,0 -> 0MB]
--------(40) TRead i [-1,-1,-1,-1] [0,0,0 -> 0MB]
--------(53) b(+) (40) [0,0,-1,-1] [0,0,0 -> 0MB]
--------(54) TWrite i (53) [0,0,-1,-1] [0,0,0 -> 0MB]
----GENERIC (lines 13-13) [recompile=false]
------(55) TRead x [10,1,-1,-1] [0,0,0 -> 0MB]
------(56) TOSTRING (55) [0,0,-1,-1] [0,0,0 -> 0MB]
------(57) u(print) (56) [-1,-1,-1,-1] [0,0,0 -> 0MB]

