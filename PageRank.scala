val pr=sc.textFile("/user/rsd352/pagerank.txt")
val pr1=pr.map(line => line.split("\t"))
val pr2=pr1.map(line => (line(0),line(1).split(",")))
val pr3=pr2.flatMap(a => a._2.flatMap(b => Array((a._1.toInt,b.toInt))))
val n=pr3.groupByKey().count
val pr4=pr2.map({case (i,j) => (i,1)}).reduceByKey(_+_)
val pr6=pr4.map(a => (a._1.toInt,a._2))
val pr5=pr3.join(pr6)
val m=pr5.map({case (a,(b,c)) => ((a,b),1.0/c)})
val pr7=(1 to n.toInt).toList
val pr8=sc.parallelize(pr7)
val v=pr8.map({case (i) => ((i,1),1.0/n)})
var v1=v.map({case ((j,k),v) => (j,(k,v))})
val m1=m.map({case ((i,j),k)  => ((j,i),k)})
val m2=m1.map({case ((i,j),v) => (j,(i,v))})
for(i <- 0 until 10){
val mult=m2.join(v1)
val mult1=mult.map({case (_,((i,x),(k,y))) => ((i,k),x*y)})
val mult2=mult1.reduceByKey(_+_)
mult2.collect
v1=mult2.map({case ((i,k),x) => (k,(i,(0.2/n)+0.8*x))})
}
v1.collect