package com.knight.streaming

import com.typesafe.config.{ConfigFactory, Config}

import kafka.serializer.StringDecoder
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{InputDStream, DStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Try
import com.github.nscala_time.time.{Imports => nscalaTime}


case class Record(storeId: Int = 0, minSalePrice: Double = 0.0,
                  minListPrice: Double = 0.0,
                  title: String = "invalid",
                  currencyType: String = "nil",
                  timestamp: Long = 0L,
                  availability: String = "av",
                  priceType: String = "notype",
                  countryCode: String = "cc"
                   )

object StreamParser {
  val appName = getClass.getSimpleName.dropRight(1)

  val receiverTypeParam = appName + ".receiver_type"

  // kafka params
  val topicsParam = appName + ".topics"
  val brokersParam = appName + ".brokers"
  val offsetParam = appName + ".offset"

  val notifierTypeParam = appName + ".notifier_type"
  val notifierParams = appName + ".notifier_params"
  val rulesParam = appName + ".rules"
  // Streaming params
  val batchIntervalParam = appName + ".batch_interval"

  val msgPerPartitionRateParam = appName + ".max_msg_rate_per_partition"
  val ttlParam = appName + ".ttl_meta"
  val logger = LoggerFactory.getLogger(getClass)

  // spark params
  val coresParam = "spark.cores.max"
  // Zookeeper configurations
  val ZK = ".zookeeper"
  val ZKServersParam = appName + ZK + ".servers"
  val ZKConnTimeoutParam = appName + ZK + ".connection_timeout"
  val ZKSessTimeoutParam = appName + ZK + ".session_timeout"
  val ZKOffsetsPath = "/consumers/%s/offsets/%s/%s"

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Must specify the path to config file ")
      println("Usage: progname <path to config file> ")
      return
    }
    val url = args(0)
    logger.info("Starting " + appName)
    println("Got the path as %s".format(url))

    nscalaTime.DateTimeZone.setDefault(nscalaTime.DateTimeZone.UTC)
    val source = scala.io.Source.fromFile(url)
    val lines = try source.mkString finally source.close()
    val config = ConfigFactory.parseString(lines)
    val ssc = createStreamingContext(config)
    val isValid = validate(ssc, config)

    if (isValid == true) {
      createDStreams(ssc, config)
      ssc.start()
      ssc.awaitTermination()
    }
    else {
      println(s"Not a valid configuration : $isValid")
    }
  }

  def createStreamingContext(config: Config): StreamingContext = {
    println(s"Creating Streaming context for $appName")
    val maxRatePerPartition = config.getString(msgPerPartitionRateParam)
    val cleanTimer = config.getString(ttlParam)

    val conf = new SparkConf(true).setAppName(appName)
      .set("spark.streaming.unpersist", "true")
      .set("spark.cleaner.ttl", cleanTimer)
      .set("spark.streaming.kafka.maxRatePerPartition", maxRatePerPartition)

    val ssc = new StreamingContext(conf, Seconds(config.getInt(batchIntervalParam)))
    ssc
  }

  def stringValidator(): (Any) => (Boolean, String) = {
    (s: Any) => {
      val k = s.asInstanceOf[String]
      if (k.isEmpty) {
        (false, "string is empty")
      } else {
        (true, "")
      }
    }
  }

  def intValidator(config: Config): (Any) => (Boolean, String) = {
    (k: Any) => {
      val i = k.asInstanceOf[BigInt]
      var result = (true, "")
      val minValue = Try(config.getInt("min")).getOrElse(0)
      val maxValue = Try(config.getInt("max")).getOrElse(Int.MaxValue)
      if (i < minValue) {
        result = (result._1 & false, result._2 + "lower than min value")
      }
      if (i > maxValue) {
        result = (result._1 & false, result._2 + "greater than max value")
      }
      result
    }
  }

  def DoubleValidator(config: Config): (Any) => (Boolean, String) = {
    (k: Any) => {
      val d = k.asInstanceOf[Double]
      var result = (true, "")
      val minValue = Try(config.getDouble("min")).getOrElse(0.0)
      val maxValue = Try(config.getDouble("max")).getOrElse(Double.MaxValue)

      if (d < minValue) {
        result = (result._1 & false, result._2 + "lower than min value")
      }
      if (d > maxValue) {
        result = (result._1 & false, result._2 + "greater than max value")
      }
      result
    }
  }

  def getHandler(datatype: String, config: Config): (Any) => (Boolean, String) = {
    val handler = datatype.toLowerCase() match {
      case "string" => stringValidator()
      case "int" => intValidator(config)
      case "double" => DoubleValidator(config)
    }
    handler
  }

  def applyRules(rulesConf: Config): (Map[String, Any]) => (Boolean, String) = {
    (message: Map[String, Any]) => {
      val rulesValues: Set[String] = {
        rulesConf.entrySet().toList.map(x => x.getKey).map(_.split("\\.")(0)).toSet
      }
      var msg = (true, "")
      for (key <- rulesValues) {
        val handler = getHandler(rulesConf.getString(key + ".type"),
          rulesConf.getConfig(key))
        val value: Option[Any] = message.get(key.toLowerCase())
//        println(s"$value for $key")

        val res: (Boolean, String) = if (value != None) {
          handler(value.get)
        } else {
          (true, "")
        }

        if (res._1 == false)
          msg = (msg._1 & res._1, key + " is" + msg._2 + res._2 + " :" + value.get + "\n")
      }
      msg
    }
  }

  def getJsonParser(notifier: Notifier): (String) => Map[String, Any] = {
    (jsonString: String) => try {

      //      def getCCParams(cc: AnyRef) =
      //        (Map[String, Any]() /: cc.getClass.getDeclaredFields) { (a, f) =>
      //          f.setAccessible(true)
      //          a + (f.getName.toLowerCase() -> f.get(cc))
      //        }

      implicit val formats = DefaultFormats
      val message = parse(jsonString).values.asInstanceOf[Map[String, Any]].toList.map(x => (x._1.toLowerCase, x._2))
        .toMap
      message
    }
    catch {
      case ex: Exception => {
        ex.printStackTrace()
        // send notification based on exception
        notifier.notify(s"invalid msg given $jsonString")
        Map("" -> "")
      }
    }
  }

  def createDStreams(ssc: StreamingContext, config: Config): Any = {
    val statsStream = getKafkaStream(config, ssc)
    val notifierType = config.getString(notifierTypeParam)
    val notifierConfig = config.getConfig(notifierParams)
    val rulesConfig = config.getConfig(rulesParam)
    statsStream.foreachRDD {
      rdd =>
        if (rdd.toLocalIterator.nonEmpty) {
          rdd.foreachPartition { partitionOfRecords =>
            val notifier = NotificationFactory(notifierType, notifierConfig)
            val parserFunc = getJsonParser(notifier)
            val rulesNotifier = applyRules(rulesConfig)
            partitionOfRecords.foreach {
              record =>
                try {
                  val msg = parserFunc(record)
                  val notificationMsg = rulesNotifier(msg)
                  if (notificationMsg._1 == false) {
                    notifier.notify(notificationMsg._2 + msg)
                  }
//                  println(msg)
                }
                catch {
                  case ex: Exception =>
                    ex.printStackTrace()
                    println(record)
                }
            }
            notifier.close()
          }
        }
      //    statsStream.print()
    }
  }

  def validate(ssc: StreamingContext, config: Config): Boolean = {

    val validationResult = for {
      isValid <- Try(config.getInt(batchIntervalParam))
      isValid <- Try(config.getString(msgPerPartitionRateParam))
      isValid <- Try(config.getString(ttlParam))
      isValid <- Try(config.getString(ZKServersParam))
      isValid <- Try(config.getString(ZKConnTimeoutParam))
      isValid <- Try(config.getString(ZKSessTimeoutParam))
    } yield {
        isValid.toString
      }

    val result = validationResult.map(x => true).getOrElse(false)
    logger.info(s"Validation result - $result")
    result
  }

  /**
   * This returns a function which will do sum of tx and rx
   *
   * @return a function
   */
  def getAggregateTxRxFunction(): ((Long, Long), (Long, Long)) => (Long, Long) = {
    val aggregateTxRxFunc: ((Long, Long), (Long, Long)) => (Long, Long) = {
      case ((tx1, rx1), (tx2, rx2)) => {
        (tx1 + tx2, rx1 + rx2)
      }
    }
    aggregateTxRxFunc
  }


  def getKafkaStream(config: Config, ssc: StreamingContext): DStream[String] = {

    println("Using kafka Direct Stream")
    val topicsSet = config.getStringList(topicsParam).toSet
    val brokers = config.getString(brokersParam)
    val offset = Try(config.getString(offsetParam)).getOrElse("largest")
    println(s"reading from offset")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
      "auto.offset.reset" -> offset)
    println(s"kafkaparams $kafkaParams")

    val msgStream = KafkaUtils.createDirectStream[String, String,
      StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    //    val populateZKPartition = getPopulateZKParitionOffsetFunction(config)
    //     This is called for every batch interval
    //    populateZKPartition(msgStream)

    msgStream.map(_._2)
  }

  def getPopulateZKParitionOffsetFunction(config: Config): (InputDStream[(String, String)]) => Unit = {
    // Made the following as local variables or else while serialization the closure will happen for the entire class
    // and it will throw object not serializable error.
    // Also turned this in to function to avoid serialization issue.
    val zkServers = config.getString(ZKServersParam)
    val zkSessionTimeOut = config.getInt(ZKSessTimeoutParam)
    val zkConnectionTimeOut = config.getInt(ZKConnTimeoutParam)
    val app = appName
    val zkOffsetsPath = ZKOffsetsPath

    (msgStream: InputDStream[(String, String)]) => {
      var offsetRanges = Array[OffsetRange]()
      msgStream.transform { rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }.foreachRDD { rdd =>
        val zkClient = new ZkClient(zkServers, zkSessionTimeOut, zkConnectionTimeOut, kafka.utils.ZKStringSerializer)
        try {
          for (offset <- offsetRanges) {
            kafka.utils.ZkUtils.updatePersistentPath(zkClient,
              zkOffsetsPath.format(app, offset.topic, offset.partition),
              offset.untilOffset.toString)
          }
        } catch {
          case ex: Exception => {
            ex.printStackTrace()
          }
        }
        finally {
          try {
            zkClient.close()
          }
          catch {
            case ex: Exception => {
              ex.printStackTrace()
            }
          }
        }
      }
    }
  }
}
