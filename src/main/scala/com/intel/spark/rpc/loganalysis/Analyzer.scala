package com.intel.spark.rpc.loganalysis

import java.io.{File, FileFilter, FileWriter}
import java.net.{InetAddress, UnknownHostException}
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Comparator, TimeZone}

import com.intel.spark.rpc.loganalysis.Main.{analyzer, appId, appInfo, logDir}
import org.apache.commons.io.FileUtils

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * analyze kinds of logs under logDir, including collecting, resolving and categorizing
  *
  * @param appInfo
  *                object for getting app's info from history server, like start time, end time, duration...  *
  * @param logDir
  *               directory where log files put
  * @param hstIpMaps
  *                  host name to address(ip:port) map
  * @param cluster
  *                is in cluster env? if true, only use IP to distinguish sites. otherwise, port is needed.
  */
class Analyzer(val appInfo: AppHistoryInfo, val logDir: File, val hstIpMaps: collection.mutable.Map[String, Address], val cluster: Boolean) {

  private val trafficMap = collection.mutable.Map[String,ArrayBuffer[Traffic]]()

  private val ipHostMap = collection.mutable.Map[String, String]()

  private val port2SiteMap = collection.mutable.Map[Int, String]()

  private val hostIpMap = collection.mutable.Map[String, String]()

  private val unresolvableWorker = collection.mutable.Set[String]()

  private val sites = collection.mutable.Set[String]()

  private val ipPort2SiteMap = collection.mutable.Map[String,String]()

  private val calendar = Calendar.getInstance()

  private var startEpoch: Long = _

  private var endEpoch: Long = _

  private val executors = collection.mutable.Set[(String, Int, Int)]()

  private val logFileFilter = new FileFilter {
    override def accept(file: File): Boolean = {
      if (file.getName.endsWith(".log") && file.length() > 0) true else false
    }
  }

  def getAppInfo = {
    appInfo.getBasicInfo
    println(s"Application name: ${appInfo.getAppName}")
    if(!appInfo.isCompleted){
      throw new Exception(s"${appInfo.getAppName} is still running")
    }
//    calendar.setTimeZone(TimeZone.getTimeZone("GMT"))

    calendar.setTimeInMillis(appInfo.getStartEpoch)
    calendar.set(Calendar.MILLISECOND, 0)
    val startDate = calendar.getTime
    calendar.setTimeInMillis(appInfo.getEndEpoch)
    val endDate = calendar.getTime
    println(s"app started from ${Analyzer.DF_LOG.format(startDate)} and ended at ${Analyzer.DF_LOG.format(endDate)}")
    startEpoch = startDate.getTime
    endEpoch = endDate.getTime
    println(s"app duration is ${appInfo.getDuration}")
    //input size
    appInfo.getStageInfo
    println(s"total input size is ${appInfo.getInputSize}")
    //task count
    appInfo.getJobInfo()
    println(s"total number of tasks is ${appInfo.getTaskCount}")
  }

  def analyze() ={
    getAppInfo

    val logFiles = logDir.listFiles(logFileFilter)

    if(logFiles != null && logFiles.size > 0){
      collectInfo(logFiles)
      val appSummary = compute
      output(appSummary)
    }else{
      throw new Exception(s"no valid log files found under ${logDir.getAbsolutePath}")
    }
  }

  private def output(appSummary: AppSummary)={
    val outputPath = Main.config.getProperty(Main.NAME_CFG_OUTPUT_FILE)
    var outputFile: File = null
    if(!Main.isBlankStr(outputPath)){
      outputFile = new File(outputPath)
      if(!outputFile.exists()){
        println(s"file $outputPath doesn't exist. output to ${logDir.getAbsolutePath}/output")
        outputFile = null
      }
    }
    val excelOutput = new ExcelOutput(logDir, appSummary, outputFile)
    excelOutput.output()
  }

  private def compute={
    val inputSize = appInfo.getInputSize
    val executors = analyzer.getExecutors
    val taskCnt = appInfo.getTaskCount
    val appDuration = appInfo.getDuration

    val siteMap = collection.mutable.Map[String, (Stat, Stat)]()

    val shuffleMap = collection.mutable.Map[String, (Stat, Stat)]()

    def computeStats(traffics: ArrayBuffer[Traffic], rpc: Boolean): (Stat, Stat) ={
      var rpcCount = 0
      var minSize = Long.MaxValue
      var maxSize = Long.MinValue
      var totalSize = 0.toLong

      var rpcCount2 = 0
      var minSize2 = Long.MaxValue
      var maxSize2 = Long.MinValue
      var totalSize2 = 0.toLong

      traffics.foreach(t =>{
        if(rpc && (!(t.logType == Shuffle || t.logType == Broadcast))){
          compute(t)
        }else if((!rpc) && (t.logType == Shuffle || t.logType == Broadcast)){
          compute(t)
        }
      })

      def compute(t: Traffic): Unit ={
        if(t.isLocal()){
          rpcCount2 += 1
          if (minSize2 > t.size) {
            minSize2 = t.size
          }
          if (maxSize2 < t.size) {
            maxSize2 = t.size
          }
          totalSize2 += t.size
        }else {
          rpcCount += 1
          if (minSize > t.size) {
            minSize = t.size
          }
          if (maxSize < t.size) {
            maxSize = t.size
          }
          totalSize += t.size
        }
      }
      if(rpcCount == 0){
        maxSize = 0
        minSize = 0
      }
      if(rpcCount2 == 0){
        maxSize2 = 0
        minSize2 = 0
      }
      (new Stat(rpcCount, maxSize, minSize, totalSize), new Stat(rpcCount2, maxSize2, minSize2, totalSize2))
    }

    for((site, traffics) <- analyzer.getAllTraffics){
      val rpcStats = computeStats(traffics, true)
      val shuffleStats = computeStats(traffics, false)
      siteMap += (site -> rpcStats)
      shuffleMap += (site -> shuffleStats)
    }

    new AppSummary(appId, appInfo.getAppName, inputSize,
      executors.toSeq, taskCnt, appDuration, siteMap.toMap, shuffleMap.toMap)
  }

  private def collectInfo(logFiles: Array[File]): Unit ={
    val truncateStr = Main.config.getProperty("truncate-log-by-time")
    val truncate = truncateStr != null && "true".equalsIgnoreCase(truncateStr)
    val truncLogFiles = if(truncate) truncateLogFiles(logDir, logFiles) else logFiles

    sortLogFiles(truncLogFiles) // make <site>-endpointref.log at head to get valid site-ip:port map
    truncLogFiles.foreach(f => analyzeEach(f))

    fillSite
  }

  private def inRange(line: String, startEpoch: Long, endEpoch: Long): Boolean = {
    var in = false
    Analyzer.PATTERN_LOG_DATE.findFirstMatchIn(line).foreach(m => {
      val time = Analyzer.DF_LOG.parse(m.group(0)).getTime
      in = time <= endEpoch && time >= startEpoch
    })
    in
  }

  private def truncateLogFiles(logDir: File, files: Array[File]): Array[File] ={
    val truncDir = new File(logDir, "trunc")
    if(truncDir.exists()) FileUtils.deleteDirectory(truncDir)
    truncDir.mkdir()
    files.foreach(f => {
      val newFile = new File(truncDir, f.getName)
      var writer: FileWriter = null
      try{
        writer = new FileWriter(newFile)
        Source.fromFile(f).getLines().foreach(line => {
          if(inRange(line, startEpoch, endEpoch)){
            writer.write(line)
            writer.write("\n")
          }
        })
      }finally{
        if(writer != null) writer.close()
      }
    })
    println(s"logs are truncated and put to ${truncDir.getAbsolutePath}")
    truncDir.listFiles(logFileFilter)
  }

  private def fillSite = {
    for((site, traffics) <- getAllTraffics){
      traffics.foreach(t => {
        if(t.sourceAddress.isSiteNameSet()){
          ipPort2SiteMap += (t.sourceAddress.getAddressName -> t.sourceAddress.getSiteName())
        }
        if(t.targetAddress.isSiteNameSet()){
          ipPort2SiteMap += (t.targetAddress.getAddressName -> t.targetAddress.getSiteName())
        }
      })
    }

    for((site, traffics) <- getAllTraffics){
      traffics.foreach(t => {
        if(!t.targetAddress.isSiteNameSet()) {
          var targetSite = ipPort2SiteMap.getOrElse(t.targetAddress.getAddressName, "unknown")
          if (targetSite == "unknown") {
//            if (!cluster) {
//              targetSite = port2SiteMap.getOrElse(t.targetAddress.getPort(), "unknown")
//            }else{
              targetSite = ipHostMap.getOrElse(t.targetAddress.ip, "unknown")
//            }
          }
          t.targetAddress.setSiteName(targetSite)
        }
      })
    }
  }

  def getAllTraffics = trafficMap

  def getExecutors = executors

  /**
    * sort log files to make sure dependencies resolved first
    * <site>-endpointref > <site>-executor-endpointref > master-* > driver-* > rest of log files
    * @param files
    */
  private def sortLogFiles(files: Array[File]): Unit ={
    util.Arrays.sort(files, new Comparator[File](){

      def getMappedValue(f: File):Int = {
        val fields = f.getName.split(Analyzer.LOG_FILE_NAME_SEPARATOR)
        if(fields.length < 2){
          throw new Exception(s"file name of ${f.getAbsolutePath} should be " +
            s"<site>${Analyzer.LOG_FILE_NAME_SEPARATOR}(${Analyzer.LOG_FILE_NAME_SEPARATOR}executor)${Analyzer.LOG_FILE_NAME_SEPARATOR}<logtype>.log")
        }

        if(fields.length == 2 && "endpointref.log" == fields(1)){//<site>-endpointref.log
          -1
        }else if("endpointref.log" == fields(fields.length-1)){//<site>-executor-endpointref.log
          0
        }else if("master" == fields(0)){
          1
        }else if("driver" == fields(0)){
          2
        }else{
          100
        }
      }

      override def compare(o1: File, o2: File): Int = {
        val v1 = getMappedValue(o1);
        val v2 = getMappedValue(o2);
        v1 - v2
      }
    })
  }

  /**
    * map 127.0.*.* to site's own ip
    *
    * @param siteAddr
    * @param host
    * @return
    */
  private def resolveIPAgainstSite(siteAddr: Address, host: String): String = {
    //take care of 127.0.0.1 and 127.0.1.1
    if(host == "127.0.0.1" || host == "127.0.1.1" || host == "localhost"){
      siteAddr.ip
    }else {
      resolveIp(host)
    }
  }

  /**
    * resolve site's ip. port will be set later
    * @param site
    * @param ignoreUnknownHost
    * @return
    */
  private def toAddressFromSite(site: String, ignoreUnknownHost: Boolean): Address = {
    sites += site
    site match{
      case Main.NAME_MASTER | Main.NAME_DRIVER => hstIpMaps.getOrElse(site, null)
      case _ => {
                  try {
                    hstIpMaps.getOrElseUpdate(site, Address("", site, resolveIp(site))) //name will be updated when set port
                  }catch{
                    case e: UnknownHostException => {
                      if (ignoreUnknownHost) {
                        unresolvableWorker += site
                        println(s"unresolvable worker($site), ignoring")
                        null
                      }else{
                        throw e
                      }
                    }
                    case _ @ e => throw e
                  }
                }
    }
  }

  /**
    * resolve site's address after parsing the first log of its endpointref
    * set port only if its ip has been resolved already
    *
    * @param site
    * @param hostAndPort
    */
  private def addSiteAddress(site: String, hostAndPort: String): Unit = {
    if(!hstIpMaps.contains(site)){
      val rst = Analyzer.validateIpPort(hostAndPort)
      if(rst._1){//already is IP
        if(site == Main.NAME_MASTER || site == Main.NAME_DRIVER){//add map for host name
          hstIpMaps += (site -> Address(hostAndPort, site, rst._2, rst._3))
          hstIpMaps += (hostAndPort -> Address(hostAndPort, site, rst._2, rst._3))
        }else{
          hstIpMaps += (site -> Address(hostAndPort, site, rst._2, rst._3))
          if(unresolvableWorker.contains(site)){
            hstIpMaps += (hostAndPort -> Address(hostAndPort, site, rst._2, rst._3))
          }
        }
      }else{
        val ip = resolveIp(rst._2)
        val name = s"$ip:${rst._3}"
        if(site == Main.NAME_MASTER || site == Main.NAME_DRIVER) {
          hstIpMaps += (site -> Address(name, site, ip, rst._3))
          hstIpMaps += (name -> Address(name, site, ip, rst._3))
        }else{
          hstIpMaps += (site -> Address(name, site, ip, rst._3))
          if(unresolvableWorker.contains(site)){
            hstIpMaps += (name -> Address(name, site, rst._2, rst._3))
          }
        }
      }
    }else{
      val address = hstIpMaps(site)
      if(!address.isPortSet()){
        address.setPort(hostAndPort.split(":")(1).toInt)
      }
    }
  }

  private def analyzeLogEndpointref(site: String, file: File, executor: Boolean): Unit ={
    val array = trafficMap.getOrElseUpdate(site, new ArrayBuffer[Traffic]())
    var siteAddr = toAddressFromSite(site, true)
    var addressResolved = siteAddr != null && siteAddr.isPortSet()

    Source.fromFile(file).getLines().foreach(line => {
      val matcher = Analyzer.PATTERN_ENDPOINTREF.findFirstMatchIn(line)
      matcher.foreach(m => {
        //get length of message
        var len = 0
        val ss = Analyzer.PATTERN_BUFFEN_LEN.replaceSomeIn(line, (m) => {
          len = m.group(1).toInt
          Some("")
        })
        len += ss.getBytes.length //approximate length
        //resolve address
        if(!addressResolved){
          addSiteAddress(site, m.group(1))
          siteAddr = toAddressFromSite(site, false)//retry to get ip after addSiteAddress
          addressResolved = true
        }
        //search RegisterBlockManager from executor log
        if(executor){
          val bmMatcher = Analyzer.PATTERN_BLOCKMANAGER.findFirstMatchIn(ss)
          bmMatcher.foreach(m =>{
            val ip = resolveIPAgainstSite(siteAddr, m.group(1))
            val port = m.group(2)
            ipPort2SiteMap += (s"$ip:$port" -> site)
          })
        }
        //search executor configurations from master
        if(site == Main.NAME_MASTER){
          val exeMatcher = Analyzer.PATTERN_EXECUTORADDED.findFirstMatchIn(ss)
          exeMatcher.foreach(m =>{
            val id = m.group(1)
            val worker = m.group(2)
            val cores = m.group(3)
            val memory = m.group(4)
            executors.add((s"$worker-$id", cores.toInt, memory.toInt))
          })
        }
        //traffics
        val endPointName = m.group(2)
        if("spark-client" != endPointName){//likely non-local traffic, could be local traffic after resolving ip:port
          val hostAddr = m.group(3)
          val ipPortStr = hostAddr.substring(hostAddr.indexOf("@")+1)
          val ipPort = ipPortStr.split(":")

          val targetIp = resolveIPAgainstSite(siteAddr, ipPort(0))
          val name = s"$targetIp:${ipPort(1)}"
          array += Traffic(siteAddr, Address(name, "", targetIp, ipPort(1).toInt), len, EndpointRef, executor, true)
        }else if("Executor" == m.group(3)){//driver RPC to executors
          array += Traffic(siteAddr, Address("executors", "executors", "0.0.0.0", 111111111), len, EndpointRef, executor, true)
        }else{
          array += Traffic(siteAddr, siteAddr, len, EndpointRef, executor, true)
        }
      })
    })
  }

  private def analyzeLogBroadcast(site: String, file: File, executor: Boolean): Unit ={
    val array = trafficMap.getOrElseUpdate(site, new ArrayBuffer[Traffic]())
    val siteAddr = toAddressFromSite(site, true)

    Source.fromFile(file).getLines().foreach(line => {
      val matcherRead = Analyzer.PATTERN_BROADCAST_READ.findFirstMatchIn(line)
      matcherRead.foreach(m => {
        val targetIp = resolveIPAgainstSite(siteAddr, m.group(3))
        val name = s"$targetIp:${m.group(4)}"
        array += Traffic(siteAddr, Address(name, "", targetIp, m.group(4).toInt), m.group(2).toInt, Broadcast, executor, false)
      })
      val matcherWrite = Analyzer.PATTERN_BROADCAST_WRITE.findFirstMatchIn(line)
      matcherWrite.foreach(m => {
        val targetIp = resolveIPAgainstSite(siteAddr, m.group(3))
        val name = s"$targetIp:${m.group(4)}"
        array += Traffic(siteAddr, Address(name, "", targetIp, m.group(4).toInt), m.group(2).toInt, Broadcast, executor, true)
      })
    })
  }

  private def analyzeLogShuffle(site: String, file: File, executor: Boolean): Unit ={
    val array = trafficMap.getOrElseUpdate(site, new ArrayBuffer[Traffic]())
    var siteAddr = toAddressFromSite(site, false)
    Source.fromFile(file).getLines().foreach(line => {
      val matcher = Analyzer.PATTERN_SHUFFLE.findFirstMatchIn(line)
      matcher.foreach(m => {
        val host = m.group(2)
        val port = m.group(3)
        val targetIp = resolveIPAgainstSite(siteAddr, host)
        val name = s"$targetIp:$port"
        array += Traffic(siteAddr, Address(name, "", targetIp, port.toInt), m.group(1).toInt, Shuffle, executor, false)
      })
    })
  }

  private def analyzeLogReply(site: String, file: File, executor: Boolean): Unit ={
    val array = trafficMap.getOrElseUpdate(site, new ArrayBuffer[Traffic]())
    var siteAddr = toAddressFromSite(site, false)
    Source.fromFile(file).getLines().foreach(line => {
      val matcher = Analyzer.PATTERN_REPLY.findFirstMatchIn(line)
      matcher.foreach(m => {
        val host = m.group(1)
        val port = m.group(2)
        val targetIp = resolveIPAgainstSite(siteAddr, host)
        val name = s"$targetIp:$port"
        array += Traffic(siteAddr, Address(name, "", targetIp, port.toInt), m.group(3).getBytes.length, Reply, executor, true)
      })
    })
  }

  private def analyzeLogClientport(site: String, file: File, executor: Boolean): Unit ={
      Source.fromFile(file).getLines().foreach(line => {
        val matcher = Analyzer.PATTERN_CLIENTPORT.findFirstMatchIn(line)
        matcher.foreach(m => {
          val port = m.group(1).split(":")(1)
          port2SiteMap += (port.toInt -> site)
        })
      })
  }

  private def analyzeLogInbox(site: String, file: File, executor: Boolean): Unit ={
    println(s"ignoring inbox log ${file.getAbsolutePath}")
  }

  private def analyzeMaster(file: File): Unit = {
    val fields = file.getName.split(Analyzer.LOG_FILE_NAME_SEPARATOR)
    assert(fields.size==2, s"bad master file name. should be master${Analyzer.LOG_FILE_NAME_SEPARATOR}<log type>.log")
    analyzeCommon(fields(0), file, fields(1), false)
  }

  private def analyzeDriver(file: File): Unit = {
    val fields = file.getName.split(Analyzer.LOG_FILE_NAME_SEPARATOR)
    assert(fields.size==2, s"bad driver file name. should be driver${Analyzer.LOG_FILE_NAME_SEPARATOR}<log type>.log")
    analyzeCommon(fields(0), file, fields(1), false)
  }

  private def analyzeWorker(file: File): Unit = {
    val fields = file.getName.split(Analyzer.LOG_FILE_NAME_SEPARATOR)
    assert(fields.size>=2 && fields.size<=3, s"bad worker file name, should be <site>${Analyzer.LOG_FILE_NAME_SEPARATOR}(${Analyzer.LOG_FILE_NAME_SEPARATOR}executor)${Analyzer.LOG_FILE_NAME_SEPARATOR}<logtype>.log")
    analyzeCommon(fields(0), file, fields(fields.length-1), fields.length==3)
  }

  private def analyzeCommon(site: String, file: File, logTypeStr: String, executor: Boolean): Unit ={
    val alyzFunc = logTypeStr match {
      case "broadcast.log"      => analyzeLogBroadcast _
      case "endpointref.log"    => analyzeLogEndpointref _
      case "shuffle.log"        => analyzeLogShuffle _
      case "reply.log"          => analyzeLogReply _
      case "inbox.log"          => analyzeLogInbox _
      case "clientport.log"     => analyzeLogClientport _
      case logType: String  => throw new Exception(s"unsupported log type: $logType")
    }
    alyzFunc(site, file, executor)
  }

  private def analyzeEach(file: File): Unit ={
    val fields = file.getName.split(Analyzer.LOG_FILE_NAME_SEPARATOR)
    fields(0) match {
      case "master" => analyzeMaster(file)
      case "driver" => analyzeDriver(file)
      case _        => if(Analyzer.isIp(fields(0))) {throw new Exception(s"need hostname as log file prefix, not IP, ${fields(0)}")}
                       analyzeWorker(file)
    }
  }

  private def resolveIp(hostname: String)={
    if(Analyzer.isIp(hostname)){
      hostname
    }else {
      hostIpMap.getOrElseUpdate(hostname, {
        val ip = InetAddress.getAllByName(hostname)
        val rst = ip.find(i => !(i.getHostAddress.equals("127.0.0.1") || i.getHostAddress.equals("127.0.1.1")))
        if (rst == None) {
          throw new Exception(s"cannot resolve IP for $hostname")
        } else {
          rst.get.getHostAddress
        }
      })
    }
  }

  private def resolveHostname(ip: String)={
    if(!Analyzer.isIp(ip)){
      throw new Exception(s"$ip is not a valid ip")
    }
    ipHostMap.getOrElseUpdate(ip, {InetAddress.getByName(ip).getHostName})
  }

  private def totalTraffic(): ((Long, Long), (Long, Long), (Long, Long)) ={
    var totalTraffic = 0L
    var totalTrafficBytes = 0L
    var totalRPC = 0L
    var totalRPCBytes = 0L
    var totalBrdShu = 0L
    var totalBrdShuBytes = 0L

    for((site, traffics) <- analyzer.getAllTraffics){
      println(s"=======================$site=====================")
      traffics.foreach(t => {
        println(s"    $t")
        totalTraffic += 1
        totalTrafficBytes += t.size

        if(!(t.logType == Broadcast || t.logType == Shuffle)){
          totalRPC += 1
          totalRPCBytes += t.size
        }else{
          totalBrdShu += 1
          totalBrdShuBytes += t.size
        }
      })
    }

    println(s"=======================executors configuration=====================")
    for((id, cores, memory) <- analyzer.getExecutors){
      println(s"$id(cores: $cores, memory: ${memory}M)")
    }

    println(s"=======================summary=====================")
    println(s"total Traffic: $totalTraffic")
    println(s"total bytes in Traffic: $totalTrafficBytes")
    println(s"total RPC: $totalTraffic")
    println(s"total bytes in RPC: $totalTrafficBytes")
    println(s"total Shuffle&Broadcast: $totalBrdShu")
    println(s"total bytes in Shuffle&Broadcast: $totalBrdShuBytes")

    ((totalTraffic, totalTrafficBytes), (totalRPC, totalRPCBytes), (totalBrdShu, totalBrdShuBytes))
  }

  private def printToDriverTrafficNbr(analyzer: Analyzer): Map[String, (Long, Long)]={

    var totalRPC = 0
    var totalBytes = 0L
    for((_, traffics) <- analyzer.getAllTraffics){
      traffics.foreach(t => {
        if (t.targetAddress.getSiteName() == "driver" && (!t.isLocal()) && (!(t.logType == Shuffle || t.logType == Broadcast))) {
          totalRPC += 1
          totalBytes += t.size
        }
      })
    }

    var totalOutRPC = 0
    var totalOutBytes = 0L
    analyzer.getAllTraffics("driver").foreach(t => {
      if ((!t.isLocal()) && (!(t.logType == Shuffle || t.logType == Broadcast))) {
        totalOutRPC += 1
        totalOutBytes += t.size
      }
    })

    println(s"Total remote RPC to driver: count($totalRPC), size($totalBytes)")
    println(s"Total remote RPC from driver: count($totalOutRPC), size($totalOutBytes)")
    null
  }
}

object Analyzer{

  def apply(appInfo: AppHistoryInfo, logDir: File, hstIpMaps: collection.mutable.Map[String, Address], cluster: Boolean)
    = new Analyzer(appInfo, logDir, hstIpMaps, cluster)

  val DF_LOG = new SimpleDateFormat(Main.config.getProperty("log-date-format"))
  val timeZone = Main.config.getProperty("timezone")
  if(timeZone != null && timeZone.length>0)
    DF_LOG.setTimeZone(TimeZone.getTimeZone(timeZone))

  val LOG_FILE_NAME_SEPARATOR = if(System.getenv("FILE_NAME_SEPARATOR") != null) System.getenv("FILE_NAME_SEPARATOR") else "-"

  val PATTERN_BROADCAST_READ = """\^\^read blockId\(([^\)]+)\) of length\((\d+)\) from address\(BlockManagerId\(\d+, ([^,]+), (\d+), [^\)]+\)\)""".r
  val PATTERN_BROADCAST_WRITE = """\^\^write blockId\(([^\)]+)\) of length\((\d+)\) bytes to address\(BlockManagerId\(\d+, ([^,]+), (\d+), [^\)]+\)\)""".r

  val PATTERN_ENDPOINTREF = """RequestMessage\(([^,]+), NettyRpcEndpointRef\(([^:]+)://([^\)]+)\), (.+)\)""".r
  val PATTERN_BUFFEN_LEN = """buffer length\((\d+)\)""".r

  val PATTERN_SHUFFLE = """\^\^read length\((\d+)\) from address\(([^:]+):(\d+)\)""".r

  val PATTERN_REPLY = """reply to ([^:]+):(\d+) \[with\] (.+)""".r

  val PATTERN_HOST_AND_PORT = """([^:]+):(\d{4,5})""".r
  val PATTERN_HOST_AND_OPTIONAL_PORT = """([^:]+)(:\d{4,5})?""".r
  val PATTERN_IP = """\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}""".r

  val PATTERN_CLIENTPORT = """\^\^local address is (.+)""".r

  val PATTERN_BLOCKMANAGER = """RegisterBlockManager\(BlockManagerId\([^,]+, ([^,]+), (\d{4,5}), [^\)]+\)""".r

  val PATTERN_EXECUTORADDED = """ExecutorAdded\((\d+),[^,]+,([^,]+),(\d+),(\d+)\)""".r

  val PATTERN_LOG_DATE = buildLogDatePatternFromDateFormat(Main.config.getProperty("log-date-format"))

  def buildLogDatePatternFromDateFormat(dateFormat: String) = {
    val regex = dateFormat.replaceAll("[y|M|d|H|m|s|S]+", """\\d{1,4}""")
    println(s"regex of log date is $regex")
    ("^"+regex).r
  }

  def validateIpPort(str: String)={
    var host: String = ""
    var port: Int = 0
    var isValid: Boolean = false
    PATTERN_HOST_AND_PORT.findFirstMatchIn(str).foreach(m =>{
      host = m.group(1)
      port = m.group(2).toInt
      PATTERN_IP.findFirstMatchIn(host).foreach(m => {
        isValid = true
      })
    })
    (isValid, host, port)
  }

  def validateIpWithPortOptional(str: String)={
    var host: String = ""
    var port: String = ""
    var isValid: Boolean = false
    PATTERN_HOST_AND_OPTIONAL_PORT.findFirstMatchIn(str).foreach(m =>{
      host = m.group(1)
      if(m.groupCount>1) {
        port = m.group(2)
      }
      PATTERN_IP.findFirstMatchIn(host).foreach(m => {
        isValid = true
      })
    })
    (isValid, host, port)
  }

  def isIp(host: String): Boolean ={
    host.matches(PATTERN_IP.regex)
  }
}
