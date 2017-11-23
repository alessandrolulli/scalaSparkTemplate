/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.template.parameter

import scala.io.Source

class Parameter extends Enumeration {

  val appName = Value("appName", "EMPTY")

  val sparkMaster = Value("spark.master", "local[2]")
  val sparkExecutorMemory = Value("spark.executor.memory", "1024m")
  val sparkBlockManagerSlaveTimeoutMs = Value("spark.storage.blockManagerSlaveTimeoutMs", "500000")
  val sparkShuffleManager = Value("spark.shuffle.manager", "SORT")
  val sparkShuffleConsolidateFiles = Value("spark.shuffle.consolidateFiles", "false")
  val sparkCompressionCodec = Value("spark.io.compression.codec", "lz4")
  val sparkAkkaFrameSize = Value("spark.rpc.message.maxSize", "100")
  val sparkDriverMaxResultSize = Value("spark.driver.maxResultSize", "1g")
  val sparkExecutorInstances = Value("spark.executor.instances", 1)
  val sparkCoresMax = Value("spark.cores.max", 2)

  /**
    * Add any number of parameters you want.
    * Currently are supported parameters of type: String, Int, Array[Int], Double, Boolean
    *
    * Syntax to create a new parameter
    * val parameterName = Value("parameterName", defaultValue)
    * the type of your parameter will be equal to the type of the defaultValue inserted
    */

  class ValueParameterBase[T](name: String, val defaultValue: T, private var currentValue: T) extends Val(nextId, name) {

    println(outputString())

    def get = currentValue
    def value = get
    def is(value: T) = currentValue = value

    private def outputString() : String = {
      val typeString = defaultValue.getClass.getSimpleName
      val defaultValueString = defaultValue match {
        case t : Array[_] => t.mkString(",")
        case t => t.toString
      }
      val currentValueString = currentValue match {
        case t : Array[_] => t.mkString(",")
        case t => t.toString
      }

      "PARAMETER ("+name+")["+typeString+"] {"+defaultValueString+"},{"+currentValueString+"}"
    }
  }

  class ValueParameterString(name: String, defaultValue: String, currentValue: String) extends ValueParameterBase[String](name, defaultValue, currentValue)

  class ValueParameterInt(name: String, defaultValue: Int, currentValue: Int) extends ValueParameterBase[Int](name, defaultValue, currentValue)

  class ValueParameterArrayInt(name: String, defaultValue: Array[Int], currentValue: Array[Int]) extends ValueParameterBase[Array[Int]](name, defaultValue, currentValue)

  class ValueParameterDouble(name: String, defaultValue: Double, currentValue: Double) extends ValueParameterBase[Double](name, defaultValue, currentValue)

  class ValueParameterBoolean(name: String, defaultValue: Boolean, currentValue: Boolean) extends ValueParameterBase[Boolean](name, defaultValue, currentValue)

  protected final def Value(name: String, defaultValue: String): ValueParameterString = new ValueParameterString(name, defaultValue, defaultValue)

  protected final def Value(name: String, defaultValue: String, currentValue: String): ValueParameterString = new ValueParameterString(name, defaultValue, currentValue)

  protected final def Value(name: String, defaultValue: Int): ValueParameterInt = new ValueParameterInt(name, defaultValue, defaultValue)

  protected final def Value(name: String, defaultValue: Int, currentValue: Int): ValueParameterInt = new ValueParameterInt(name, defaultValue, currentValue)

  protected final def Value(name: String, defaultValue: Array[Int]): ValueParameterArrayInt = new ValueParameterArrayInt(name, defaultValue, defaultValue)

  protected final def Value(name: String, defaultValue: Array[Int], currentValue: Array[Int]): ValueParameterArrayInt = new ValueParameterArrayInt(name, defaultValue, currentValue)

  protected final def Value(name: String, defaultValue: Double): ValueParameterDouble = new ValueParameterDouble(name, defaultValue, defaultValue)

  protected final def Value(name: String, defaultValue: Double, currentValue: Double): ValueParameterDouble = new ValueParameterDouble(name, defaultValue, currentValue)

  protected final def Value(name: String, defaultValue: Boolean): ValueParameterBoolean = new ValueParameterBoolean(name, defaultValue, defaultValue)

  protected final def Value(name: String, defaultValue: Boolean, currentValue: Boolean): ValueParameterBoolean = new ValueParameterBoolean(name, defaultValue, currentValue)

  def loadProperties(filename: String = "data/config"): Unit = {
    for (line <- Source.fromFile(filename).getLines) {
      try {
        val splitted = line.split(" ")

        if (splitted.length >= 2) {
          withName(splitted(0)) match {
            case v: ValueParameterString => v.is(splitted(1))
            case v: ValueParameterInt => v.is(splitted(1).toInt)
            case v: ValueParameterArrayInt => v.is(splitted(1).split(",").map(_.toInt))
            case v: ValueParameterDouble => v.is(splitted(1).toDouble)
            case v: ValueParameterBoolean => v.is(splitted(1).toBoolean)
            case _ => throw new ClassNotFoundException()
          }
        }
      } catch {
        case e: Exception => println("PARAMETER - IMPOSSIBLE TO LOAD " + line)
      }
    }
  }

  def loadFromParametersFile() = {
    val filename = "data/parameters.data"
    for (line <- Source.fromFile(filename).getLines) {
      try {
        val splitted = line.split(" ")

        if (splitted.length == 3) {
          splitted(0) match {
            case "string" => Value(splitted(1), splitted(2))
            case "int" => Value(splitted(1), splitted(2).toInt)
            case "arrayInt" => Value(splitted(1), splitted(2).split(",").map(_.toInt))
            case "double" => Value(splitted(1), splitted(2).toDouble)
            case "boolean" => Value(splitted(1), splitted(2).toBoolean)
            case _ => Value(splitted(1), splitted(2))
          }
        }
      } catch {
        case e: Exception => println("PARAMETER - IMPOSSIBLE TO CREATE PARAMETER " + line)
      }
    }
  }
}

