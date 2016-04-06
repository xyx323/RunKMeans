/**
  * Created by xyx on 3/24/16.
  */
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.clustering.StreamingKMeansModel
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.mllib.linalg._
//import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.streaming.dstream.DStream
//import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.clustering.StreamingKMeans
//import org.apache.spark.mllib.regression.{StreamingLinearRegressionWithSGD, LabeledPoint}
//import breeze.linalg.DenseVector


object SimpleStreamingApp {

  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Simple").setMaster("local[2]"))

    //    val stream = ssc.textFileStream("/home/xyx/Desktop/data/")
    //    val sc = new SparkContext(new SparkConf().setAppName("K-means").setMaster("local"))
        val rawData = sc.textFile("file:///home/xyx/Desktop/kddcup.data_10_percent_corrected")
    ////    //
//        val numClusters = 150
//        /** ********the value numDimensions must be correct ***********/
//        val numDimensions = 38
        val model = new StreamingKMeans()
//          .setK(numClusters)
//          .setDecayFactor(1.0)
//          .setRandomCenters(numDimensions, 0.0)
        //originalmodel(model,150,1.0,38)

        //model.latestModel().clusterCenters.foreach(println)
       //clusteringTakeOffline(rawData,model)
    //    model.latestModel().clusterCenters.foreach(println)
    //
    //    model.latestModel().save(sc,"/home/xyx/Desktop/model/")
    //
       val saveMOodel = KMeansModel.load(sc,"/home/xyxDesktop/model")

    //     //test(model)
     //   run(sc,model)
    //    clusteringTake1(stream, model)
    //print(stream)
    //clusteringTake0(stream)

//        ssc.start()
//        ssc.awaitTermination()

  }

  //for testing
  def test(model: StreamingKMeans): Unit = {

  }

  def run(sc:SparkContext,model: StreamingKMeans): Unit = {
    print("here")
    val ssc = new StreamingContext(sc, Seconds(10))
    val stream = ssc.socketTextStream("localhost", 9999)

    //stream.print()
    clusteringTake1(stream, model)

    //
    //clusteringTake0(stream)

    ssc.start()
    ssc.awaitTermination()

  }

  def clusteringTake0(rawData: DStream[String]): Unit = {

    val labelsAndData = rawData.map { line =>
      val buffer = line.split(',').toBuffer
      buffer.remove(1, 3)
      val label = buffer.remove(buffer.length - 1)
      //val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
      val features = Vectors.dense(buffer.map(_.toDouble).toArray)
      //LabeledPoint(label = y, features = Vectors.dense(features))
      (label, features)
    }

    //labelsAndData.print()

    val numClusters = 10
    /** ********the value numDimensions must be correct ***********/
    val numDimensions = 38
    val model = new StreamingKMeans()
      .setK(numClusters)
      .setDecayFactor(1.0)
      .setRandomCenters(numDimensions, 0.0)


    model.trainOn(labelsAndData.map { line => line._2 })
    //model.predictOn(labelsAndData.map { line => line._2 }).print()


    //print clusterCenters
    //    labelsAndData.foreachRDD { (rdd, time) =>
    //      val cluster = model.latestModel().clusterCenters.foreach(println)
    //    }

    //use transform to create a stream with model error rates
    val clusterLabelCount = labelsAndData.transform { rdd =>
      val latest = model.latestModel()
      rdd.map { point =>
        val label = point._1
        val datum = point._2
        val cluster = latest.predict(datum)
        (cluster, label)
      }
    }.countByValue().print()

    //print the average distance to Centriod
    //    labelsAndData.foreachRDD { rdd =>
    //      val latestModel = model.latestModel()
    //      print(rdd.map { case (label, datum) =>
    //        distToCentroid(datum, latestModel)
    //      }.mean())
    //    }


    //save data for visualization
    //    val sample = labelsAndData.transform { rdd =>
    //      val latest = model.latestModel()
    //      rdd.map { point =>
    //        latest.predict(point._2) + "," + point._2.toArray.mkString(",")
    //      }.sample(false,1)
    //    }
    //    sample.saveAsTextFiles("file:///home/xyx/Desktop/sample/")

    //    clusterLabelCount.map{ case ((cluster, label), count) =>
    //      println(f"$cluster%1s$label%18s$count%8s")
    //    }
    //    println("=========")


    //    val Data = rawData.map { line =>
    //      val buffer = line.split(',').toBuffer
    //      buffer.remove(1, 3)
    //      buffer.remove(buffer.length - 1)
    //      val y = 1.0
    //      //val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
    //      val features = buffer.map(_.toDouble).toArray
    //      LabeledPoint(label = y, features = Vectors.dense(features))
    //      // vector
    //    }
    //
    //    //Data.print()
    //
    //    //val data = labelsAndData.values
    //
    //    val numClusters = 10
    //    /**********the value numDimensions must be correct***********/
    //    val numDimensions = 38
    //    val model = new StreamingKMeans()
    //      .setK(numClusters)
    //      .setDecayFactor(1.0)
    //      .setRandomCenters(numDimensions, 0.0)
    //
    //
    //
    //    model.trainOn(Data.map(lp=>lp.features))
    //    model.predictOn(Data.map(lp=>lp.features)).print()

  }

  def distance(a: Vector, b: Vector) =
    math.sqrt(a.toArray.zip(b.toArray).map(p => p._1 - p._2).map(d => d * d).sum)

  def distToCentroid(datum: Vector, model: StreamingKMeans) = {
    val cluster = model.latestModel().predict(datum)
    val centroid = model.latestModel().clusterCenters(cluster)
    distance(centroid, datum)
  }


  def clusteringTake1(rawData: DStream[String], model: StreamingKMeans): Unit = {

    val labelsAndData = rawData.map { line =>
      val buffer = line.split(',').toBuffer
      buffer.remove(1, 3)
      val label = buffer.remove(buffer.length - 1)
      //val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
      val features = Vectors.dense(buffer.map(_.toDouble).toArray)
      //LabeledPoint(label = y, features = Vectors.dense(features))
      (label, features)
    }
    //print clusterCenters
    labelsAndData.foreachRDD { (rdd, time) =>
      val cluster = model.latestModel().clusterCenters.foreach(println)
    }

    model.trainOn(labelsAndData.map { line => line._2 })

    labelsAndData.foreachRDD { (rdd, time) =>
      val cluster = model.latestModel().clusterCenters.foreach(println)
    }
//     labelsAndData.map { case (label, datum) =>
//       val cluster = model.latestModel().clusterCenters.foreach(println)
//
//       model.latestModel().predict(datum)
//     }
//
    //use transform to create a stream with model error rates
    val clusterLabelCount = labelsAndData.transform { rdd =>
      val latest = model.latestModel()
      rdd.map { point =>
        val label = point._1
        val datum = point._2
        val cluster = latest.predict(datum)
        (cluster, label)
      }
    }.countByValue().print()

    //print the average distance to Centriod
//        labelsAndData.foreachRDD { rdd =>
    //          val latestModel = model.latestModel()
    //          print(rdd.map { case (label, datum) =>
    //            distToCentroid(datum, latestModel)
    //          }.mean())
    //        }

  }
  def originalmodel(model: StreamingKMeans,numClusters: Int,DecayFacor: Double,numDimensions: Int): Unit ={
    model
      .setK(numClusters)
      .setDecayFactor(DecayFacor)
      .setRandomCenters(numDimensions, 0.0)
  }
  def ClusteringScore(data: RDD[Vector],model: StreamingKMeans,numClusters: Int,DecayFacor: Double,numDimensions: Int): Unit ={
    model
      .setK(numClusters)
      .setDecayFactor(DecayFacor)
      .setRandomCenters(numDimensions, 0.0)

    model.latestModel().update(data, 1.0, "batches")
    data.map(datum => distToCentroid(datum, model)).mean()
  }

  def clusteringTakeOffline(rawData: RDD[String], SKmeans: StreamingKMeans): Unit = {

    //rawData.map(_.split(',').last).countByValue().toSeq.sortBy(_._2).reverse.foreach(println)

    val labelsAndData = rawData.map { line =>
      val buffer = line.split(',').toBuffer
      buffer.remove(1, 3)
      val label = buffer.remove(buffer.length - 1)
      val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
      (label, vector)
    }

    val data = labelsAndData.values.cache()
    //println(labelsAndData.first())
//     val numClusters = 150
//    /** ********the value numDimensions must be correct ***********/
//     val numDimensions = 38
//    SKmeans
//      .setK(numClusters)
//      .setDecayFactor(1.0)
//      .setRandomCenters(numDimensions, 0.0)
    originalmodel(SKmeans,150,1.0,38)

   //SKmeans.trainOn(data.)
   SKmeans.latestModel().update(data, 1.0, "batches")



    val clusterLabelCount = labelsAndData.map { case (label, datum) =>
      val cluster = SKmeans.latestModel().predict(datum)
      (cluster, label)
    }.countByValue()

    clusterLabelCount.toSeq.sorted.foreach { case ((cluster, label), count) =>
      println(f"$cluster%1s$label%18s$count%8s")
    }

    val me = labelsAndData.map { case (label, datum) =>
      distToCentroid(datum, SKmeans)
    }.mean()
    print(me)






    data.unpersist()
  }





}