package com.test.test

import com.test.saprkSql.RDD_DataFrame_DataSet.Person
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, RelationalGroupedDataset, SparkSession}

object CombineOrder {
  def main(args: Array[String]): Unit = {
    //1,准备环境
    val sparkSession: SparkSession = SparkSession.builder().appName("group").master("local[*]").getOrCreate()
    val sc: SparkContext = sparkSession.sparkContext
    sc.setLogLevel("WARN")
    import sparkSession.implicits._

    //2，加载数据
    val lines: RDD[String] = sc.textFile("D:\\test\\javatest\\TestProject\\sparkTest\\src\\main\\scala\\com\\test\\test\\test.txt")

    //3，处理数据
    val orderDS: Dataset[Order] = lines.map(line => {
      val arr: Array[String] = line.split("\t")
      Order(arr(0).toInt, arr(1), arr(2), arr(3))
    }).toDS()

    orderDS.createOrReplaceTempView("order")
    val orderWithFullBirthDF: DataFrame = sparkSession.sql("select id,name,dateOfBirth,orderItem from " +
  "(select id,name,dateOfBirth,orderItem,row_number() over (partition by id,name order by dateOfBirth)rank from " +
      "(select * from order where dateOfBirth != 'no value' and length(dateOfBirth) = 10))t where t.rank =1")
    orderWithFullBirthDF.createOrReplaceTempView("orderWithFullBirth")

    val orderWithNotFullBirthDF: DataFrame = sparkSession.sql("select * from order where dateOfBirth != 'no value' and length(dateOfBirth) < 10")
    orderWithNotFullBirthDF.createOrReplaceTempView("orderWithNotFullBirth")

    val combineDF: DataFrame = sparkSession.sql("select ofb.id,ofb.name,ofb.dateOfBirth,concat_ws(',',ofb.orderItem,onfb.orderItem) orderItem " +
      "from orderWithFullBirth ofb,orderWithNotFullBirth onfb " +
      "where ofb.id = onfb.id and ofb.name = onfb.name and ofb.dateOfBirth like concat(onfb.dateOfBirth,'%')")
    combineDF.createOrReplaceTempView("combine")

    val resultDF: DataFrame = sparkSession.sql("select * from " +
      "(select * from order o where not exists(select * from combine c where o.id = c.id and o.name =c.name and o.dateOfBirth = c.dateOfBirth) " +
      "and (o.dateOfBirth = 'no value' or length(o.dateOfBirth) =10) " +
      "union select * from combine)t order by t.id,t.name,t.dateOfBirth")
    resultDF.show()

  }

  case class Order(id: Int, name: String, dateOfBirth: String, orderItem: String)
}
