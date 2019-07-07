package org.inceptez.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object corefunctions1 {
  
  def main(args:Array[String])
  {
   val conf = new SparkConf().setMaster("local[*]").setAppName("local-lab01")
   val sc = new SparkContext(conf)
   sc.setLogLevel("ERROR")
   val rdd = sc.textFile("file:/home/hduser/hive/data/txns",10)
   rdd.take(5)
   val rddsplit=rdd.map(x=>x.split(","))
   val rddexerjump = rddsplit.filter(x => x(4).toUpperCase.contains("EXERCISE") || x(5).toUpperCase.startsWith("JUMP"))
   val rddexerjumpcnt= rddexerjump.count()
   //val rddexerjumpsum= rddexerjump.to map(x=>x._3)
  println(s"No of lines with exercise or jumping: $rddexerjumpcnt")
   
  val rddcredit = rddsplit.filter(x => !x.contains("credit"))
  val cnt = rddcredit.count()
  println(s"No of lines that does not contain Credit: $cnt")
   
   
     val rdd2 = rddsplit.filter(x => x(7) == "California" && x(8) == "cash")
     val rdd3 = rdd2.map(x => x(3).toDouble)
     // cache n persist RDD
     rdd3.cache()
     rdd3.unpersist()
     rdd3.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
     val sumofsales = rdd3.sum()
     println("Sum of Sales: " + sumofsales)
     
     val maxofsales = rdd3.max()
     println("Max sales value : " + maxofsales)
     
     val totalsales = rdd3.count()     
     println("Total no fo sales: " + totalsales)
         
     val minofsales = rdd3.min()
     println("Min sales value : " + minofsales)
     val avgofsales = sumofsales/rdd3.count()
     println("Avg sales value : " + avgofsales)
   
   val rddtrimupper=rdd.map(x=>x.split(",")).map(x=>(x(0),x(1),x(2),x(3),charlen(x(4))))
   rddtrimupper.take(10).foreach(println)

     /*val rddunion = rddsplit.union(rddexerjump).map(x=>(x(0),x(1),x(2),x(3),x(4),x(5),x(6)))
  rddunion.take(10).foreach(println);*/
   //val rddunion2cols = rddunion.map(x=>(x._3,x._4))
   
   //rddunion2cols.countByKey().take(10).foreach(println)

   //rdd.foreach(println)
   //rdd.collect().foreach(println)
  }
  
  def charlen(a:String):String=
  {
    return(a.trim().toUpperCase())
  }
  
}