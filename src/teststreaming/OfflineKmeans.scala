import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by xyx on 3/30/16.
  */

object OfflineKmeans{
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("K-means").setMaster("local"))
    val rawData = sc.textFile("file:///home/xyx/Desktop/kddcup.data_10_percent_corrected")
    //testSavedModel(rawData,sc)
    //clusteringTake0(rawData,sc)
    // clusteringTake1(rawData)
     //   clusteringTake2(rawData)
     //   clusteringTake3(rawData)
    //    clusteringTake4(rawData)
      //  anomalies(rawData,sc)
    //teststandardscaler(rawData)
    buildLatestModel(rawData,sc)

  }
  def testSavedModel(rawData: RDD[String],sc:SparkContext): Unit = {
    val labelsAndData = rawData.map { line =>
      val buffer = line.split(',').toBuffer
      buffer.remove(1, 3)
      val label = buffer.remove(buffer.length - 1)
      val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
      (label, vector)
    }
    val offlinemodel = KMeansModel.load(sc, "/home/xyx/Desktop/model/")
    offlinemodel.clusterCenters.foreach(println)
    offlinemodel.predict(labelsAndData.map(lp=>lp._2)).take(30).foreach(println)
  }

  // Clustering, Take 0

  def clusteringTake0(rawData: RDD[String],sc:SparkContext): Unit = {

    rawData.map(_.split(',').last).countByValue().toSeq.sortBy(_._2).reverse.foreach(println)

    val labelsAndData = rawData.map { line =>
      val buffer = line.split(',').toBuffer
      buffer.remove(1, 3)
      val label = buffer.remove(buffer.length - 1)
      val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
      (label, vector)
    }

    val data = labelsAndData.values.cache()

    val kmeans = new KMeans()
    val model = kmeans.run(data)

    model.clusterCenters.foreach(println)

    val clusterLabelCount = labelsAndData.map { case (label, datum) =>
      val cluster = model.predict(datum)
      (cluster, label)
    }.countByValue()

    clusterLabelCount.toSeq.sorted.foreach { case ((cluster, label), count) =>
      println(f"$cluster%1s$label%18s$count%8s")
    }

    model.save(sc,"/home/xyx/Desktop/model/")

    data.unpersist()
  }

  // Clustering, Take 1

  def distance(a: Vector, b: Vector) =
    math.sqrt(a.toArray.zip(b.toArray).map(p => p._1 - p._2).map(d => d * d).sum)

  def distToCentroid(datum: Vector, model: KMeansModel) = {
    val cluster = model.predict(datum)
    val centroid = model.clusterCenters(cluster)
    distance(centroid, datum)
  }

  def clusteringScore(data: RDD[Vector], k: Int): Double = {
    val kmeans = new KMeans()
    kmeans.setK(k)
    val model = kmeans.run(data)
    data.map(datum => distToCentroid(datum, model)).mean()
  }

  def clusteringScore2(data: RDD[Vector], k: Int): Double = {
    val kmeans = new KMeans()
    kmeans.setK(k)
    //kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)
    val model = kmeans.run(data)
    data.map(datum => distToCentroid(datum, model)).mean()
  }

  def clusteringTake1(rawData: RDD[String]): Unit = {

    val data = rawData.map { line =>
      val buffer = line.split(',').toBuffer
      buffer.remove(1, 3)
      buffer.remove(buffer.length - 1)
      Vectors.dense(buffer.map(_.toDouble).toArray)
    }.cache()

//    (5 to 30 by 5).map(k => (k, clusteringScore(data, k))).
//      foreach(println)

    (160 to 200 by 10)/*.par*/.map(k => (k, clusteringScore2(data, k))).
      toList.foreach(println)


    data.unpersist()

  }

  def teststandardscaler(rawData:RDD[String]): Unit ={
    val data = rawData.map { line =>
      val buffer = line.split(',').toBuffer
      buffer.remove(1, 3)
      buffer.remove(buffer.length - 1)
      Vectors.dense(buffer.map(_.toDouble).toArray)
    }


    import org.apache.spark.mllib.feature.StandardScaler
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(data)
    val scaledData = data.map(lp => scaler.transform(lp))
    print("standardScaler")
    //scaledData.take(10).foreach(println)

    (120 to 150 by 10).map(k =>
      (k, clusteringScore2(scaledData, k))).toList.foreach(println)


  }
  // Clustering, Take 2

  def buildNormalizationFunction(data: RDD[Vector]): (Vector => Vector) = {
    val dataAsArray = data.map(_.toArray)
    val numCols = dataAsArray.first().length
    val n = dataAsArray.count()
    val sums = dataAsArray.reduce(
      (a, b) => a.zip(b).map(t => t._1 + t._2))
    val sumSquares = dataAsArray.aggregate(
      new Array[Double](numCols)
    )(
      (a, b) => a.zip(b).map(t => t._1 + t._2 * t._2),
      (a, b) => a.zip(b).map(t => t._1 + t._2)
    )
    val stdevs = sumSquares.zip(sums).map {
      case (sumSq, sum) => math.sqrt(n * sumSq - sum * sum) / n
    }
    val means = sums.map(_ / n)

    (datum: Vector) => {
      val normalizedArray = (datum.toArray, means, stdevs).zipped.map(
        (value, mean, stdev) =>
          if (stdev <= 0)  (value - mean) else  (value - mean) / stdev
      )
      Vectors.dense(normalizedArray)
    }
  }

  def clusteringTake2(rawData: RDD[String]): Unit = {
    val data = rawData.map { line =>
      val buffer = line.split(',').toBuffer
      buffer.remove(1, 3)
      buffer.remove(buffer.length - 1)
      Vectors.dense(buffer.map(_.toDouble).toArray)
    }

    val normalizedData = data.map(buildNormalizationFunction(data)).cache()
    print("buildNormalizationFunction")
    //normalizedData.take(10).foreach(println)
//
    (120 to 150 by 10).map(k =>
      (k, clusteringScore2(normalizedData, k))).toList.foreach(println)

    normalizedData.unpersist()
  }

  // Clustering, Take 3

  def buildCategoricalAndLabelFunction(rawData: RDD[String]): (String => (String,Vector)) = {
    val splitData = rawData.map(_.split(','))
    val protocols = splitData.map(_(1)).distinct().collect().zipWithIndex.toMap
    val services = splitData.map(_(2)).distinct().collect().zipWithIndex.toMap
    val tcpStates = splitData.map(_(3)).distinct().collect().zipWithIndex.toMap
    (line: String) => {
      val buffer = line.split(',').toBuffer
      val protocol = buffer.remove(1)
      val service = buffer.remove(1)
      val tcpState = buffer.remove(1)
      val label = buffer.remove(buffer.length - 1)
      val vector = buffer.map(_.toDouble)

      val newProtocolFeatures = new Array[Double](protocols.size)
      newProtocolFeatures(protocols(protocol)) = 1.0
      val newServiceFeatures = new Array[Double](services.size)
      newServiceFeatures(services(service)) = 1.0
      val newTcpStateFeatures = new Array[Double](tcpStates.size)
      newTcpStateFeatures(tcpStates(tcpState)) = 1.0

      vector.insertAll(1, newTcpStateFeatures)
      vector.insertAll(1, newServiceFeatures)
      vector.insertAll(1, newProtocolFeatures)

      (label, Vectors.dense(vector.toArray))
    }
  }

  def clusteringTake3(rawData: RDD[String]): Unit = {
    val parseFunction = buildCategoricalAndLabelFunction(rawData)
    val data = rawData.map(parseFunction).values
    data.take(10).foreach(println)
//    val normalizedData = data.map(buildNormalizationFunction(data)).cache()
//
//    (80 to 160 by 10).map(k =>
//      (k, clusteringScore2(normalizedData, k))).toList.foreach(println)
//
//    normalizedData.unpersist()
  }




  // Detect anomalies

  def buildAnomalyDetector( sc :SparkContext,
                            data: RDD[Vector],
                            normalizeFunction: (Vector => Vector)): (Vector => Boolean) = {
    val normalizedData = data.map(normalizeFunction)
    normalizedData.cache()

    val kmeans = new KMeans()
    kmeans.setK(150)
    //kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)
    val model = kmeans.run(normalizedData)
    model.save(sc,"/home/xyx/Desktop/model/")
    normalizedData.unpersist()

    val distances = normalizedData.map(datum => distToCentroid(datum, model))
    val threshold = distances.top(100).last
    print(threshold)
    (datum: Vector) => distToCentroid(normalizeFunction(datum), model) > threshold
  }

  def anomalies(rawData: RDD[String],sc:SparkContext) = {
    val parseFunction = buildCategoricalAndLabelFunction(rawData)
    val originalAndData = rawData.map(line => (line, parseFunction(line)._2))
    val data = originalAndData.values
    val normalizeFunction = buildNormalizationFunction(data)
    val anomalyDetector = buildAnomalyDetector(sc,data, normalizeFunction)
    val anomalies = originalAndData.filter {
      case (original, datum) => anomalyDetector(datum)
    }.keys
    anomalies.take(10).foreach(println)
  }
  def buildLatestModel(rawData: RDD[String],sc:SparkContext): Unit ={
    val parseFunction = buildCategoricalAndLabelFunction(rawData)
    val data = rawData.map(parseFunction).values
    val normalizedData = data.map(buildNormalizationFunction(data)).cache()

    val kmeans = new KMeans()
    kmeans.setK(150)
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)
    val model = kmeans.run(normalizedData)
    val score = normalizedData.map(datum => distToCentroid(datum, model)).mean()
    print(score)

    normalizedData.unpersist()
    model.save(sc,"/home/xyx/Desktop/model/")

  }


}