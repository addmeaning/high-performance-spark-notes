import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}


val tokenizer = new Tokenizer()
tokenizer.setInputCol("name")
tokenizer.setOutputCol("tokenized_name")
val tokenizedData = tokenizer.transform(df)
val hashingTF = new HashingTF()


