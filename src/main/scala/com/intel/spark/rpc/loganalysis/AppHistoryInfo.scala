package com.intel.spark.rpc.loganalysis

import java.util
import java.util.function.Consumer

import com.fasterxml.jackson.databind.ObjectMapper
import com.sun.jersey.api.client.{Client, ClientResponse}

class AppHistoryInfo(val historyServerUrl: String, val appId: String) {

  private val client = Client.create()

  private val appUrlPrefix = s"$historyServerUrl/api/v1/applications/$appId"

  //================basic info==================

  private var basicInfoStr: String = _

  private var appName: String = _

  private var startEpoch: Long = Long.MaxValue

  private var endEpoch: Long = Long.MinValue

  private var completed: Boolean = _

  private var duration: Long = _

  private var attemptCount: Int = _

  //================stage info==================

  private var stageInfoStr: String = _

  private var stageCount: Int = _

  private var inputSize: Long = _

  //================job info==================

  private var jobInfoStr: String = _

  private var jobCount: Int = _

  private var taskCount: Long = _

  def getBasicInfo() = {
    if(appName == null) {
      val resUrl = appUrlPrefix
      val webResource = client.resource(resUrl);

      val response = webResource.accept("application/json")
        .get(classOf[ClientResponse]);
      if (response.getStatus() != 200) {
        throw new RuntimeException(s"failed to get application info by url($resUrl). HTTP error code : ${response.getStatus()}");
      }
      basicInfoStr = response.getEntity(classOf[String])
      parseBasicInfo(basicInfoStr)
    }

    basicInfoStr
  }

  private def parseBasicInfo(infoJson: String): Unit ={
    val om = new ObjectMapper()
    val map = om.readValue(infoJson, classOf[java.util.Map[String, AnyRef]])
    appName = map.get("name").asInstanceOf[String]

    val attempts = map.get("attempts").asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]

    attempts.forEach(new Consumer[java.util.Map[String, AnyRef]](){
      override def accept(t: util.Map[String, AnyRef]): Unit = {
        attemptCount += 1
        val se = t.get("startTimeEpoch").asInstanceOf[Long]
        if(se < startEpoch) startEpoch = se

        if(t.containsKey("endTimeEpoch")){
          val ee = t.get("endTimeEpoch").asInstanceOf[Long]
          if(ee > endEpoch) endEpoch = ee
        }

        val c = t.get("completed").asInstanceOf[Boolean]
        if(c) completed = true
      }
    })

    duration = endEpoch - startEpoch
  }

  def getStageInfo(): String ={
    if(stageInfoStr == null){
      val resUrl = s"$appUrlPrefix/stages"
      val webResource = client.resource(resUrl);

      val response = webResource.accept("application/json")
        .get(classOf[ClientResponse]);
      if (response.getStatus() != 200) {
        throw new RuntimeException(s"failed to get stages info by url($resUrl). HTTP error code : ${response.getStatus()}");
      }
      stageInfoStr = response.getEntity(classOf[String])
      parseStageInfo(stageInfoStr)
    }
    stageInfoStr
  }

  private def parseStageInfo(infoJson: String): Unit ={
    val om = new ObjectMapper()
    val stages = om.readValue(infoJson, classOf[java.util.List[java.util.Map[String, AnyRef]]])

    stages.forEach(new Consumer[java.util.Map[String, AnyRef]](){
      override def accept(t: util.Map[String, AnyRef]): Unit = {
        stageCount += 1
        val input = t.get("inputBytes")
        if(input.isInstanceOf[Int]){
          inputSize += input.asInstanceOf[Int]
        }else{
          inputSize += input.asInstanceOf[Long]
        }
      }
    })
  }

  def getJobInfo(): String ={
    if(jobInfoStr == null){
      val resUrl = s"$appUrlPrefix/jobs"
      val webResource = client.resource(resUrl);

      val response = webResource.accept("application/json")
        .get(classOf[ClientResponse]);
      if (response.getStatus() != 200) {
        throw new RuntimeException(s"failed to get jobs info by url($resUrl). HTTP error code : ${response.getStatus()}");
      }
      jobInfoStr = response.getEntity(classOf[String])
      parseJobInfo(jobInfoStr)
    }
    jobInfoStr
  }

  private def parseJobInfo(infoJson: String): Unit ={
    val om = new ObjectMapper()
    val stages = om.readValue(infoJson, classOf[java.util.List[java.util.Map[String, AnyRef]]])

    stages.forEach(new Consumer[java.util.Map[String, AnyRef]](){
      override def accept(t: util.Map[String, AnyRef]): Unit = {
        jobCount += 1
        val num = t.get("numTasks")
        if(num.isInstanceOf[Int]){
          taskCount += num.asInstanceOf[Int]
        }else{
          taskCount += num.asInstanceOf[Long]
        }
      }
    })
  }

  def getDuration = duration
  def getStartEpoch = startEpoch
  def getEndEpoch = endEpoch
  def isCompleted = completed
  def getAppName = appName
  def getAttempts = attemptCount
  def getStageCount = stageCount
  def getInputSize = inputSize
  def getJobCount = jobCount
  def getTaskCount = taskCount

}

object AppHistoryInfo{
  def apply(historyServerUrl: String, appId: String) = new AppHistoryInfo(historyServerUrl, appId)
}
