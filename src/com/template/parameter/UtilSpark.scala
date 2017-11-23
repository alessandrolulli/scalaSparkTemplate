package com.template.parameter

import org.apache.spark.{SparkConf, SparkContext}

class UtilSpark {
  def getSparkContext(parameter: Parameter): SparkContext = {


    val conf = new SparkConf()
      .setMaster(parameter.sparkMaster.value)
      .setAppName(parameter.appName.value)
      .set("spark.executor.memory", parameter.sparkExecutorMemory.value)
      //      .set("spark.driver.memory", property.sparkExecutorMemory)
      .set("spark.storage.blockManagerSlaveTimeoutMs", parameter.sparkBlockManagerSlaveTimeoutMs.value)
      .set("spark.shuffle.manager", parameter.sparkShuffleManager.value)
      .set("spark.shuffle.consolidateFiles", parameter.sparkShuffleConsolidateFiles.value)
      .set("spark.io.compression.codec", parameter.sparkCompressionCodec.value)
      //      .set("spark.akka.frameSize", property.sparkAkkaFrameSize)
      .set("spark.rpc.message.maxSize", parameter.sparkAkkaFrameSize.value)
      //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.maxResultSize", parameter.sparkDriverMaxResultSize.value)
      .set("spark.core.connection.ack.wait.timeout", 600.toString)
      .set("spark.driver.maxResultSize", 0.toString)
    //				.set("spark.task.cpus", "8")
    //	.setJars(Array(property.jarPath)
    //)

    if (parameter.sparkCoresMax.value > 0) {
      conf.set("spark.cores.max", parameter.sparkCoresMax.value.toString)
      val executorCore = parameter.sparkCoresMax.value / parameter.sparkExecutorInstances.value
      conf.set("spark.executor.cores", executorCore.toString)
    }
    if (parameter.sparkExecutorInstances.value > 0)
      conf.set("spark.executor.instances", parameter.sparkExecutorInstances.toString)

    //conf.registerKryoClasses(Array(classOf[scala.collection.mutable.HashMap[_, _]]))
    val spark = new SparkContext(conf)

    spark
  }
}
