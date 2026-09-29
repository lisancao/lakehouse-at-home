// Experiment d (Scala): JVM transformWithState in Real-Time Mode (SPARK-57228).
//
// Per-key running total of `value`, emitted as JSON with the produced_at of the
// newest input row so latency_consumer.py can measure it like the others.
//
//   $SPARK_HOME/bin/spark-shell --master spark://localhost:7177 \
//     --jars <kafka connector jars> -i 04_tws_scala.scala
//
// In RTM, handleInputRows is called once per input row (one-row iterator), so
// the processor must not assume it sees all of a key's rows in one call.

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{
  OutputMode, StatefulProcessor, TTLConfig, TimeMode, TimerValues, Trigger, ValueState}
import spark.implicits._

val bootstrap = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
val batch = sys.env.getOrElse("RTM_BATCH_DURATION", "5 minutes")
val ckpt = "/tmp/rtm43-checkpoints/d_tws_scala"
org.apache.commons.io.FileUtils.deleteQuietly(new java.io.File(ckpt))
spark.conf.set("spark.sql.shuffle.partitions", "16")
spark.sparkContext.setLogLevel("WARN")

class RunningTotal extends StatefulProcessor[String, (String, Int, Long), (String, String)] {
  @transient private var total: ValueState[Long] = _
  @transient private var calls: ValueState[Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    total = getHandle.getValueState[Long]("total", Encoders.scalaLong, TTLConfig.NONE)
    calls = getHandle.getValueState[Long]("calls", Encoders.scalaLong, TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      rows: Iterator[(String, Int, Long)],
      timerValues: TimerValues): Iterator[(String, String)] = {
    val batchRows = rows.toSeq
    val t = (if (total.exists()) total.get() else 0L) + batchRows.map(_._2.toLong).sum
    val c = (if (calls.exists()) calls.get() else 0L) + 1
    total.update(t)
    calls.update(c)
    val producedAt = batchRows.map(_._3).max
    Iterator.single((key,
      s"""{"key":"$key","total":$t,"calls":$c,"rows_in_call":${batchRows.size},""" +
      s""""produced_at":$producedAt}"""))
  }
}

val schema = "event_id STRING, event_type STRING, key STRING, value INT, produced_at LONG"
val events = spark.readStream.format("kafka")
  .option("kafka.bootstrap.servers", bootstrap)
  .option("subscribe", "rtm-input")
  .option("startingOffsets", "latest")
  .load()
  .select(from_json($"value".cast("string"), schema, Map.empty[String, String]).as("e"))
  .select($"e.key", $"e.value", $"e.produced_at")
  .as[(String, Int, Long)]

val totals = events
  .groupByKey(_._1)
  .transformWithState(new RunningTotal, TimeMode.None(), OutputMode.Update())
  .toDF("key", "value")

val q = totals.writeStream
  .queryName("d_tws_scala")
  .format("kafka")
  .option("kafka.bootstrap.servers", bootstrap)
  .option("topic", "rtm-tws")
  .option("checkpointLocation", ckpt)
  .outputMode("update")
  .trigger(Trigger.RealTime(batch))
  .start()

println(s"[d-scala] transformWithState RTM running, batch duration $batch")
q.awaitTermination()
