import org.apache.spark.ml.classification.{ClassificationModel, Classifier}
import org.apache.spark.ml.linalg.{DenseMatrix, Vectors, Vector => SVector}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.types.DoubleType
// Simple Bernoulli Naive Bayes classifier - no sanity checks for brevity
// Example only - not for production use.
class SimpleNaiveBayes(val uid: String)
  extends Classifier[SVector, SimpleNaiveBayes, SimpleNaiveBayesModel] {

  def this() = this(Identifiable.randomUID("simple-naive-bayes"))

  override def train(ds: Dataset[_]): SimpleNaiveBayesModel = {
    import ds.sparkSession.implicits._
    ds.cache()
    // Note: you can use getNumClasses & extractLabeledPoints to get an RDD instead
    // Using the RDD approach is common when integrating with legacy machine
    // learning code or iterative algorithms which can create large query plans.
    // Compute the number of documents
    val numDocs = ds.count
    // Get the number of classes.
    // Note this estimator assumes they start at 0 and go to numClasses
    val numClasses = getNumClasses(ds)
    // Get the number of features by peaking at the first row
    val numFeatures: Integer = ds.select(col($(featuresCol))).head
      .get(0).asInstanceOf[SVector].size
    // Determine the number of records for each class
    val groupedByLabel = ds.select(col($(labelCol)).as[Double]).groupByKey(x => x)
    val classCounts = groupedByLabel.agg(count("*").as[Long])
      .sort(col("value")).collect().toMap
    // Select the labels and features so we can more easily map over them.
    // Note: we do this as a DataFrame using the untyped API because the Vector
    // UDT is no longer public.
    val df = ds.select(col($(labelCol)).cast(DoubleType), col($(featuresCol)))
    // Figure out the non-zero frequency of each feature for each label and
    // output label index pairs using a case class to make it easier to work with.
    val labelCounts: Dataset[LabeledToken] = df.flatMap {
      case Row(label: Double, features: SVector) =>
        features.toArray.zip(Stream from 1)
          .filter{vIdx => vIdx._2 == 1.0}
          .map{case (v, idx) => LabeledToken(label, idx)}
    }
    // Use the typed Dataset aggregation API to count the number of non-zero
    // features for each label-feature index.
    val aggregatedCounts: Array[((Double, Integer), Long)] = labelCounts
      .groupByKey(x => (x.label, x.index))
      .agg(count("*").as[Long]).collect()

    val theta = Array.fill(numClasses)(new Array[Double](numFeatures))

    // Compute the denominator for the general priors
    val piLogDenom = math.log(numDocs + numClasses)
    // Compute the priors for each class
    val pi = classCounts.map{case(_, cc) =>
      math.log(cc.toDouble) - piLogDenom }.toArray

    // For each label/feature update the probabilities
    aggregatedCounts.foreach{case ((label, featureIndex), count) =>
      // log of number of documents for this label + 2.0 (smoothing)
      val thetaLogDenom = math.log(
        classCounts.get(label).map(_.toDouble).getOrElse(0.0) + 2.0)
      theta(label.toInt)(featureIndex) = math.log(count + 1.0) - thetaLogDenom
    }
    // Unpersist now that we are done computing everything
    ds.unpersist()
    // Construct a model
    new SimpleNaiveBayesModel(uid, numClasses, numFeatures, Vectors.dense(pi),
      new DenseMatrix(numClasses, theta(0).length, theta.flatten, true))
  }

  override def copy(extra: ParamMap) = {
    defaultCopy(extra)
  }
}

// Simplified Naive Bayes Model
case class SimpleNaiveBayesModel(
                                  override val uid: String,
                                  override val numClasses: Int,
                                  override val numFeatures: Int,
                                  val pi: SVector,
                                  val theta: DenseMatrix) extends
  ClassificationModel[SVector, SimpleNaiveBayesModel] {

  override def copy(extra: ParamMap) = {
    defaultCopy(extra)
  }

  // We have to do some tricks here because we are using Spark's
  // Vector/DenseMatrix calculations - but for your own model don't feel
  // limited to Spark's native ones.
  val negThetaArray = theta.values.map(v => math.log(1.0 - math.exp(v)))
  val negTheta = new DenseMatrix(numClasses, numFeatures, negThetaArray, true)
  val thetaMinusNegThetaArray = theta.values.zip(negThetaArray)
    .map{case (v, nv) => v - nv}
  val thetaMinusNegTheta = new DenseMatrix(
    numClasses, numFeatures, thetaMinusNegThetaArray, true)
  val onesVec = Vectors.dense(Array.fill(theta.numCols)(1.0))
  val negThetaSum: Array[Double] = negTheta.multiply(onesVec).toArray

  // Here is the prediciton functionality you need to implement - for
  // ClassificationModels transform automatically wraps this.
  // If you might benefit from broadcasting your model or other optimizations you
  // can override transform and place your desired logic there.
  def predictRaw(features: Vector): Vector = {
    // Toy implementation - use BLAS or similar instead
    // the summing of the three vectors but the functionality isn't exposed.
    Vectors.dense(thetaMinusNegTheta.multiply(features).toArray.zip(pi.toArray)
      .map{case (x, y) => x + y}.zip(negThetaSum).map{case (x, y) => x + y}
    )
  }
}