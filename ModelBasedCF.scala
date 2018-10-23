import java.io.{BufferedWriter, FileWriter, PrintWriter}

import Niharika_Gajam_ModelBasedCF.dropHeader
import au.com.bytecode.opencsv.{CSVParser, CSVWriter}
import breeze.numerics.abs
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import scala.math.Integral
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.clustering.KMeans

import scala.math.sqrt
import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap

object Niharika_Gajam_ModelBasedCF{

  var stringmap = HashMap.empty[String, Int]
  var intmap = HashMap.empty[Int, String]

  def dropHeader(data: RDD[String]): RDD[String] = {
    data.mapPartitionsWithIndex((idx, lines) => {
      if (idx == 0) {
        lines.drop(1)
      }
      lines
    })
  }

  def main(args: Array[String]) : Unit = {

    val spark_config = new SparkConf().setAppName("Model Based Recommendation").setMaster("local[1]")
    val spark_context = new SparkContext(spark_config)

    val traincsv = spark_context.textFile(args(0)).cache()
    val testcsv = spark_context.textFile(args(1)).cache()

    val withoutHeadertrain = dropHeader(traincsv)
    val withoutHeadertest = dropHeader(testcsv)


    val outputFile = new BufferedWriter(new FileWriter("Niharika_Gajam_ModelBasedCF.txt"))
    val fileWriter = new PrintWriter(outputFile)

    var i = 0

    val trainpartitions = withoutHeadertrain.mapPartitions(lines => {
      val parser = new CSVParser(',')
      lines.map(line => {
        val columns = parser.parseLine(line)
       // var userval = hash(columns(0))//.toInt  // hash it to integer
        if(!(stringmap contains(columns(0)))){
          stringmap(columns(0)) = i
          intmap(i) = columns(0)
          i = i+1
        }
        var userval = stringmap(columns(0))

        if(!(stringmap contains(columns(1)))){
          stringmap(columns(1)) = i
          intmap(i) = columns(1)
          i = i+1
        }
        val itemval = stringmap(columns(1))//.toInt
        val stars = columns(2).toDouble

        (userval,itemval,stars)
      })
    })



    val testpartitions = withoutHeadertest.mapPartitions(lines => {
      val parser = new CSVParser(',')
      lines.map(line => {
        val columns = parser.parseLine(line)

        if(!(stringmap contains(columns(0)))){
          stringmap(columns(0)) = i
          intmap(i) = columns(0)
          i = i+1
        }
        var userval = stringmap(columns(0))


        if(!(stringmap contains(columns(1)))){
          stringmap(columns(1)) = i
          intmap(i) = columns(1)
          i = i+1
        }
        val itemval = stringmap(columns(1))//.toInt

        val stars = columns(2).toDouble
        (columns(0),userval,columns(1),itemval,stars)
      })
    })

    val ratings = trainpartitions.map{case (user_id, business_id, stars) =>  Rating(user_id, business_id, stars) }



    // Build the recommendation model using ALS
   val rank = 20
    val numIterations = 20
    val als_model = ALS.train(ratings, rank, numIterations, 0.3)


    // Evaluate the model on rating data
    val usersProducts = testpartitions.map{case(user_id,hash_user_id, business_id,hash_business_id, stars) => (hash_user_id,hash_business_id)}
    val testratings = testpartitions.map{case(user_id,hash_user_id, business_id,hash_business_id, stars) => ((hash_user_id,hash_business_id),stars.toDouble)}

    val predictions =
      als_model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate.toDouble)
      }


    var train_ratings_map  = trainpartitions.map(elem=>((elem._1,elem._2),elem._3)).collect().toMap //= HashMap.empty[(Int,Int) , Double]
    val user_item_map = trainpartitions.map(d=>(d._1,d._2)).groupByKey().collectAsMap()
    val item_user_map= trainpartitions.map(d=>(d._2,d._1)).groupByKey().collectAsMap()

    def getUserAvgrating(user: Int, item_list: Iterable[Int]): Double ={
      val item_list_size = item_list.size
      var sum: Double = 0.0
      for(l<-item_list)
        sum += train_ratings_map(user,l)

      val avg = sum/item_list_size

      avg.toDouble
    }
    var user_avg_ratings = HashMap.empty[Int, Double]
    user_item_map.map(elem=> {
      val user = elem._1
      val  item_list = elem._2
      val avg_rating = getUserAvgrating(user,item_list)

      user_avg_ratings(user) = avg_rating
      (elem._1,elem._2)
    })


    def getItemAvgrating(item: Int, user_list: Iterable[Int] ):Double={
      val user_list_size = user_list.size
      var sum:Double = 0.0
      for(user<- user_list)
         sum += train_ratings_map(user,item)
      val avg = sum/user_list_size
      avg.toDouble
    }
    var item_avg_ratings = HashMap.empty[Int, Double]
    item_user_map.map(elem=> {
      val item = elem._1
      val user_list = elem._2
      val avg_rating = getItemAvgrating(item,user_list)

      item_avg_ratings(item) = avg_rating
      (elem._1,elem._2)
    })

    val pred_map = predictions.collectAsMap()
    val test_data_not_predicted = testratings.filter(elem=> !pred_map.contains(elem._1))

    val trainusers = trainpartitions.map(elem=>(elem._1,1)).collect().toMap
    val trainitems = trainpartitions.map(elem=>(elem._2,1)).collect().toMap

    val data_not_predicted = test_data_not_predicted.map(elem => {

      val user = elem._1._1
      val given_item = elem._1._2
      var avg_rating = 3.0

      if(trainusers.contains(user) && trainitems.contains(given_item)){

         var sum_item_ratings=0.0
         val items_list_user_rated = user_item_map(user)
         val num = items_list_user_rated.size
         var sum = 0.0
         var get_sum1 = items_list_user_rated.map { item =>
           sum_item_ratings += train_ratings_map(user,item)
           item
         }
         var sum_user_ratings = 0.0
         val users_list_foritem_rated = item_user_map(given_item)
         var get_sum2 = users_list_foritem_rated.map {u =>
           sum_user_ratings += train_ratings_map(u, given_item)
           u
         }
         var totalsum = items_list_user_rated.size + users_list_foritem_rated.size
         avg_rating = (sum_user_ratings + sum_item_ratings)/totalsum

       }
      else if(trainitems.contains(given_item)){
       avg_rating = item_avg_ratings(given_item) //((user,given_item),item_avg_ratings(given_item))
     }else if(trainusers.contains(user)){
        avg_rating = user_avg_ratings(user) //((user,given_item),user_avg_ratings(user))
      }

       ((user,given_item),avg_rating)
    })

    val totalpredictions = predictions ++ data_not_predicted
    val ratesAndPreds = testratings.join(totalpredictions)

    //getting the errors from predicted and ground truth
   val errortypes = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val e = abs(r1 - r2)
      val errtype = if(e >=0 && e<1) 1.toInt else if(e >=1 && e<2) 2.toInt  else if(e >=2 && e<3) 3.toInt else if(e >=3 && e<4) 4.toInt  else if(e >=4) 5.toInt
      (errtype.toString,1)
    }.reduceByKey {case (x,y) => x + y}.sortBy{case (k,v) => k}
      .map{ case(k,v)=> {
        val strname = if(k.equals(1.toString)) ">=0 and <1:" else if (k.equals(2.toString)) ">=1 and <2:" else if(k.equals(3.toString)) ">=2 and <3:" else if(k.equals(4.toString)) ">=3 and <4:" else if(k.equals(5.toString)) ">=4:"
        (strname.toString,v)
      }}.collect()


    val s = "User ID,Business ID,Stars\n"
    fileWriter.write(s)

    val collect_prediction = totalpredictions.map(elem => {
      val user = intmap(elem._1._1)
      val item = intmap(elem._1._2)
      ((user,item),elem._2)
    }).sortBy{elem => (elem._1._1,elem._1._2)}.collect()

    for(l <- collect_prediction){
      val user = l._1._1
      val item = l._1._2
      val rating = l._2
      val s = user + ","+ item +"," + rating + "\n"
      fileWriter.write(s)
    }

   val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()

    for(i <- errortypes){
      val str = i._1
      val num = i._2
      println(str + " " + num)
    }
    val sqrterr = sqrt(MSE)
    println(s"RMSE: $sqrterr")
    fileWriter.close()

  }


}