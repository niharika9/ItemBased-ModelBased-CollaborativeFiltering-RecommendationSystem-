import java.io.{BufferedWriter, FileWriter, PrintWriter}

import au.com.bytecode.opencsv.{CSVParser, CSVWriter}
import breeze.numerics.abs
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap, ListBuffer}
import scala.math.sqrt

object Niharika_Gajam_ItemBasedCF{


  val user_stringmap = HashMap.empty[String, Int]
  val user_intmap =  HashMap.empty[Int, String]

  val item_stringmap = HashMap.empty[String, Int]
  val item_intmap =  HashMap.empty[Int, String]

  def dropHeader(data: RDD[String]): RDD[String] = {
    data.mapPartitionsWithIndex((idx, lines) => {
      if (idx == 0) {
        lines.drop(1)
      }
      lines
    })
  }

  def main(args: Array[String]) : Unit = {

    val t1 = System.nanoTime

    val spark_config = new SparkConf().setAppName("Model Based Recommendation").setMaster("local[1]")
    val spark_context = new SparkContext(spark_config)

    val traincsv = spark_context.textFile(args(0)).cache()
    val testcsv = spark_context.textFile(args(1)).cache()

    val outputFile = new BufferedWriter(new FileWriter("Niharika_Gajam_ItemBasedCF.txt"))
    val fileWriter = new PrintWriter(outputFile)

    val withoutHeadertrain = dropHeader(traincsv)
    val withoutHeadertest = dropHeader(testcsv)


    var i = 0
    var j = 0

    val trainpartitions = withoutHeadertrain.mapPartitions(lines => {
      val parser = new CSVParser(',')
      lines.map(line => {
        val columns = parser.parseLine(line)

        if(!user_stringmap.contains(columns(0))){
          user_stringmap(columns(0)) = i
          user_intmap(i) = columns(0)
          i = i+1
        }
        var userval = user_stringmap(columns(0))

        if(!item_stringmap.contains(columns(1))){
          item_stringmap(columns(1)) = j
          item_intmap(j) = columns(1)
          j=j+1
        }
        val itemval = item_stringmap(columns(1))//.toInt
        val stars = columns(2).toDouble
        (userval,itemval,stars)
      })
    })

    val testpartitions = withoutHeadertest.mapPartitions(lines => {
      val parser = new CSVParser(',')
      lines.map(line => {
        val columns = parser.parseLine(line)

        if(!user_stringmap.contains(columns(0))){
          user_stringmap(columns(0)) = i
          user_intmap(i) = columns(0)
          i = i+1
        }
        var userval = user_stringmap(columns(0))
        if(!item_stringmap.contains(columns(1))){
          item_stringmap(columns(1)) = i
          item_intmap(i) = columns(1)
          i = i+1
        }
        val itemval = item_stringmap(columns(1))//.toInt
        val stars = columns(2).toDouble
        (columns(0),userval,columns(1),itemval,stars)
      })
    })


    val testratings = testpartitions.map{case(user_id,hash_user_id, business_id,hash_business_id, stars) => ((hash_user_id,hash_business_id),stars.toDouble)}
    var train_ratings_map  = trainpartitions.map(elem=>((elem._1,elem._2),elem._3)).collect().toMap

    val user_item_map = trainpartitions.map(d=>(d._1,d._2)).groupByKey().collectAsMap()
    val item_user_map= trainpartitions.map(d=>(d._2,d._1)).groupByKey().collectAsMap()


    /*get the average rating given by every user */
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
      val item_list = elem._2
      val avg_rating = getUserAvgrating(user,item_list)

      user_avg_ratings(user) = avg_rating
      (elem._1,elem._2)
    })

    /*This function gets the similarity between two items (Pearson Similarity)  */
    var items_similarity_map = HashMap.empty[(Int,Int), Double]
    def similarity_items_weight(item1: Int,item2: Int): Double = {

          var itemA : Int = item1
          var itemB : Int = item2

         /* if(itemA > itemB){
            itemA = item2
            itemB = item1
          }*/

          if(items_similarity_map.contains(itemA,itemB)) return items_similarity_map((itemA,itemB))

          // check to see if the item vector is zero or not
          if(!item_user_map.contains(item1)){  // if it has atleast has one user in the list , else zero weightage
            items_similarity_map((item1,item2)) = 0.0
            return 0.0
          }

          val item1_users_list  = item_user_map(item1).toSet
          val item2_users_list = item_user_map(item2).toSet

          val corated_users_list = item2_users_list.intersect(item1_users_list)   // corrated users

          var sum_ratings_item1 : Double = 0.0
          var sum_ratings_item2 : Double = 0.0

          corated_users_list.map{u =>
            sum_ratings_item1 += train_ratings_map(u,item1)
            sum_ratings_item2 += train_ratings_map(u,item2)
            (u)
          }

          // if this item isn't rated by use
          // get vectors for both and eval similarity
          var avg_rating_user1 = sum_ratings_item1/item1_users_list.size
          var avg_rating_user2 = sum_ratings_item2/item2_users_list.size


          var numerator : Double = 0.0
          var denominator : Double = 0.0
          var denominator_left : Double = 0.0
          var denominator_right : Double = 0.0

          corated_users_list.map(elem=> {
            numerator += (train_ratings_map(elem, item1) - avg_rating_user1) * (train_ratings_map(elem, item2) - avg_rating_user2)
            denominator_left = math.pow(train_ratings_map(elem, item1) - avg_rating_user1, 2)
            denominator_right = math.pow(train_ratings_map(elem, item2) - avg_rating_user2, 2)
            (numerator,denominator_left,denominator_right)
          })

          denominator = math.sqrt(denominator_left) * math.sqrt(denominator_right)
          var weight : Double = 0.0;
          if(numerator != 0.toDouble && denominator != 0.toDouble){
            weight = numerator/denominator
          }
          items_similarity_map((itemA,itemB)) = weight

          weight // returns weight
    }

    // *************** Code  for ItemBased CF begins here ***********
    val  predictions = testratings.map{ case ((u, b), true_rating) =>
       val user = u
       val given_item = b

       /*    -------------LOGIC--------------------
         1.get the items this user rated
         2.of all get the items similar to this item with a near neighborhood and maintain a vector
          now the above is big
         3.now multiply corated items with the item similarity weight  and divide by sum of item sim weight
       */

       val items_list_user_rated = user_item_map(user)

       var similar_items_to_given = items_list_user_rated.map { item =>
         var similarity = similarity_items_weight(given_item, item)

         (item, similarity)
       }.toList.sortBy(elem => -elem._2).take(25) // getting the similar elements from near neighborhood

       var numerator: Double = 0.0
       var denominator: Double = 0.0

       similar_items_to_given.map(elem => {
         val item = elem._1
         val similarity_weight = elem._2
         numerator += train_ratings_map((user, item)) * similarity_weight
         denominator += math.abs(similarity_weight)
         (item, similarity_weight)
       })


       var predicted_rating: Double = 0.0
       if (numerator != 0.toDouble && denominator != 0.toDouble) {
         predicted_rating = numerator/denominator

       } else {
         // if there is new user ,get the average rating
         predicted_rating = user_avg_ratings(user)
       }
       ((u,b),true_rating,predicted_rating)
    }


    val s = "User ID,Business ID,Stars\n"
    fileWriter.write(s)
    val x = predictions.map(elem => {
      val user = user_intmap(elem._1._1)
      val item = item_intmap(elem._1._2)
      ((user,item),elem._3)
    }).sortBy{elem => (elem._1._1,elem._1._2)}.collect()


    for(elem <- x){
      val user_string = elem._1._1
      val item_string = elem._1._2

      val predicted_rating = elem._2
      val str = user_string+"," + item_string+","+ predicted_rating+"\n"
      fileWriter.write(str)
    }

     val MSE = predictions.map { case ((user,item),r1, r2) =>
       val err = r1-r2
       err * err
     }.mean()

     val errortypes = predictions.map { case ((user,item),r1, r2) =>
       val e = abs(r1 - r2)
       val errtype = if(e >=0 && e<1) 1.toInt else if(e >=1 && e<2) 2.toInt  else if(e >=2 && e<3) 3.toInt else if(e >=3 && e<4) 4.toInt  else if(e >=4) 5.toInt
       (errtype.toString,1)
     }.reduceByKey {case (x,y) => x + y}.sortBy{case (k,v) => k}
       .map{ case(k,v)=> {
         val strname = if(k.equals(1.toString)) ">=0 and <1:" else if (k.equals(2.toString)) ">=1 and <2:" else if(k.equals(3.toString)) ">=2 and <3:" else if(k.equals(4.toString)) ">=3 and <4:" else if(k.equals(5.toString)) ">=4:"
         (strname.toString,v)
       }}


    for(i <- errortypes){
      val str = i._1
      val num = i._2
      println(str + " " + num)
    }
     val sqrterr = sqrt(MSE)
     println(s"RMSE: $sqrterr")

    val duration = (System.nanoTime - t1) / 1e9d
    println("Time: " + duration + " secs")
    fileWriter.close()

  }
}
