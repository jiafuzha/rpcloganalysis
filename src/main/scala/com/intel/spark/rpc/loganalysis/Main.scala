package com.intel.spark.rpc.loganalysis

import java.io.{File, FilenameFilter, InputStream}
import java.nio.file.Paths
import java.util.Properties
import java.util.function.BiConsumer

object Main extends App{

  val NAME_DRIVER = "driver"
  val NAME_MASTER = "master"

  val NAME_ENV_CLUSTER = "cluster"
  val NAME_ENV_SINGLE_NODE= "single-node"

  val NAME_CONFIGURATION_FILE="config.properties"

  val NAME_CFG_APP_ID="application-id"
  val NAME_CFG_LOG_DIR="log-dir"
  val NAME_CFG_OUTPUT_FILE="output-file"
  val NAME_CFG_HIS_SVR_URL="history-server-url"
  val NAME_CFG_ENV="env"
  val NAME_CFG_LOG_DATE_FORMAT="log-date-format"

  def loadConfig() = {
    val url = this.getClass.getClassLoader.getResource(NAME_CONFIGURATION_FILE)
    if(url == null){
      throw new Exception("cannot find config.properties")
    }else{
      println("loading configurations from "+url.toString)
    }
    var is: InputStream = null
    try{
      is = this.getClass.getClassLoader.getResourceAsStream(NAME_CONFIGURATION_FILE)
      val properties = new Properties()
      properties.load(is)
      properties
    }finally{
      is.close()
    }
  }

  def isBlankStr(str: String): Boolean ={
    str == null || str.trim.isEmpty
  }

  def removeSlash(url: String) = {
    var u = url
    while(u.endsWith("/") || u.endsWith("\\")){
      u = u.substring(0, u.length-1)
    }
    u
  }

  def validateLogDir(config: Properties): File ={
    val logPathStr = config.getProperty(NAME_CFG_LOG_DIR)
    assert(!isBlankStr(logPathStr), "need $NAME_CFG_LOG_DIR")
    val logPath = Paths.get(logPathStr)
    val logDir = logPath.toFile
    assert(logDir.exists() && logDir.isDirectory, s"$logPathStr does not exist or is not a directory")

    val logFiles = logDir.list(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = {
        name.endsWith(".log")
      }
    })
    assert(!logFiles.isEmpty, s"no log files (*.log) found under $logPathStr")
    logDir
  }

  def validateAppOnHistoryServer(config: Properties): AppHistoryInfo ={
    var historyServerUrl = config.getProperty(NAME_CFG_HIS_SVR_URL)
    assert(!isBlankStr(historyServerUrl), "need $NAME_CFG_HIS_SVR_URL")
    historyServerUrl = removeSlash(historyServerUrl)

    def checkHistoryServer(historyServerUrl: String, appId: String) = {
      val appInfo = AppHistoryInfo(historyServerUrl, appId)
      appInfo.getBasicInfo()
      appInfo
    }
    checkHistoryServer(historyServerUrl, appId)
  }

  val config = loadConfig()

  println("configurations are, ")
  config.forEach(new BiConsumer[AnyRef, AnyRef] {
    override def accept(t: AnyRef, u: AnyRef): Unit = {
      println(s"${t.asInstanceOf[String]}=${u.asInstanceOf[String]}")
    }
  })

  //log dir
  val logDir = validateLogDir(config)

  //env
  val env = config.getProperty(NAME_CFG_ENV)
  assert(!(isBlankStr(env) || !(NAME_ENV_CLUSTER == env || NAME_ENV_SINGLE_NODE == env)),  s"$NAME_CFG_ENV needs to be either cluster<$NAME_ENV_CLUSTER> or single node<$NAME_ENV_SINGLE_NODE>")

  //app id
  val appId = config.getProperty(NAME_CFG_APP_ID)
  assert(!isBlankStr(appId), "need $NAME_CFG_APP_ID")

  //app info
  val appInfo = validateAppOnHistoryServer(config)

  // start to analyze
  val hstIpMaps = collection.mutable.Map[String, Address]()

  //analyzing
  val analyzer = Analyzer(appInfo, logDir, hstIpMaps, "cluster" == env)
  analyzer.analyze()
}