package com.knight.streaming

import java.util.Properties
import com.typesafe.config.Config
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

/**
 * Created by vdinakaran on 20/8/16.
 */
object NotificationFactory {

  def apply(notifierType: String, config: Config): Notifier = {
    notifierType match {
     case "kafka" => new kafkaNotifier(config)
     case "print" => new printNotifier(config)
      case _ => new printNotifier(config)
    }
  }
}

abstract class Notifier {
  def notify(msg: String): Unit

  def close(): Unit
}

class kafkaNotifier(config: Config) extends Notifier {
  val BROKERS = "metadata.broker.list"
  val SERIALIZER_CLASS = "serializer.class"
  val PRODUCER_TYPE = "producer.type"
  val producerType = "async"
  val notificationTopic = config.getString("topic")
  val producerBrokers = config.getString("brokers")
  val props = new Properties()
  props.put(BROKERS, producerBrokers)
  props.put(SERIALIZER_CLASS, "kafka.serializer.StringEncoder")
  props.put(PRODUCER_TYPE, "async")
  val pConfig = new ProducerConfig(props)
  val producer = new Producer[String, String](pConfig)

  override def notify(msg: String) = {
    producer.send(new KeyedMessage[String, String](notificationTopic, msg))
  }

  override def close() = {
    producer.close()
  }
}


class printNotifier(config: Config) extends Notifier {
  override def notify(msg: String) = {
    println(msg)
  }

  override def close() = {
  }
}