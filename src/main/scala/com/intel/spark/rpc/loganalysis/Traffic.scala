package com.intel.spark.rpc.loganalysis

/**
  *
  * @param name
  *             could be empty string. but final value is ip:port
  * @param siteName
  *                   site name where this Address is from
  * @param ip
  *           ip part of Address
  * @param port
  *             port part of address
  */
class Address(private var name: String, private var siteName: String, val ip: String, private var port: Int){

  assert(Analyzer.isIp(ip), s"not valid ip, $ip")

  def this(name: String, siteName: String, ip: String){
    this(name, siteName, ip, 0)
  }

  def isPortSet(): Boolean ={
    port > 0
  }

  def getPort(): Int ={
    port
  }

  /**
    * port may be get later from log file.
    * set full name of this Address when port is set.
    * @param p
    */
  def setPort(p: Int): Unit ={
    port = p
    name = s"$ip:$port"
  }

  def isSiteNameSet()={
    siteName != ""
  }

  def getSiteName()={
    siteName
  }

  /**
    * site name may be resolved later after enough info is collected
    * @param siteName
    */
  def setSiteName(siteName: String): Unit ={
    this.siteName = siteName
  }

  def getAddressName = name

  override def toString: String = name
}

object Address{
  def apply(name: String, siteName: String, ip: String, port: Int) = new Address(name, siteName, ip, port)
  def apply(name: String, siteName: String, ip: String) = new Address(name, siteName, ip)
  def apply(name: String, ip: String) = new Address(name, name, ip)
}

/**
  *
  * @param sourceAddress
  *                      source address
  * @param targetAddress
  *                      target address
  * @param size
  *             approximate size of traffic, not actual size of sent bytes
  * @param logType
  *                type of log
  * @param executor
  *                 is traffic from executor?
  * @param out
  *            is traffic out or in?
  */
class Traffic(val sourceAddress: Address, val targetAddress: Address, val size: Int, val logType: LogType, val executor: Boolean, val out: Boolean) {
  def isLocal(): Boolean = {
    if(Traffic.cluster){
      sourceAddress.ip == targetAddress.ip
    }else {
      sourceAddress.ip == targetAddress.ip && sourceAddress.getPort == targetAddress.getPort
    }
  }


  override def toString: String = {
    s"${sourceAddress.toString} ${if(out) "sends to" else "reads from"} ${targetAddress.toString}(${targetAddress.getSiteName()}) $size bytes. $logType"
  }

}

object Traffic{
  val cluster = Main.NAME_ENV_CLUSTER == Main.config.getProperty(Main.NAME_CFG_ENV)

  def apply(sourceAddress: Address, targetAddress: Address, size: Int, logType: LogType, executor: Boolean, out: Boolean): Traffic = {
    val traffic = new Traffic(sourceAddress, targetAddress, size, logType, executor, out)
    traffic
  }
}

abstract class LogType
case object EndpointRef extends LogType
case object Broadcast extends LogType
case object Shuffle extends LogType
case object Reply extends LogType
case object ClientPort extends LogType