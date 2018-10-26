package com.intel.spark.rpc.loganalysis

import java.io.File
import java.net.InetAddress

object Test extends App{

  val line = "RequestMessage(null, NettyRpcEndpointRef(spark://CoarseGrainedScheduler@jiafu-dev:38706), 1,9,FINISHED,buffer length(1545))"
//  val patBroadcastRead = """\^\^read blockId\(([^\)]+)\) of length\((\d+)\) from address\(BlockManagerId\(\d+, ([^,]+), (\d+), [^\)]+\)\)""".r
//  val patBroadcastWrite = """\^\^write blockId\(([^\)]+)\) of length\((\d+)\) bytes to address\(BlockManagerId\(\d+, ([^,]+), (\d+), [^\)]+\)\)""".r
//  val patEndpointref = """RequestMessage\(([^,]+), NettyRpcEndpointRef\(([^:]+)://([^\)]+)\), (.+)\)""".r
//  val matcher = patEndpointref.findFirstMatchIn(line)
//  matcher.foreach(m => {
//    println(m.groupCount)
//    for(i <- 0 to m.groupCount){
//      println(s"$i: ${m.group(i)}")
//    }
//  })

//  val pat = """buffer length\((\d+)\)""".r
//  val ss = pat.replaceSomeIn(line, (m) => {
//    println(m.groupCount)
//    println(m.group(1))
//    Some("")
//  })
//
//  println(ss)

//  val patHostPort = """([^:]+):(\d{4,5})""".r
//  val patIp = """\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}""".r
//  val line2 = "jiafu-dev:38706"
//  val line3 = "10.239.10.104:38706"
//  patHostPort.findFirstMatchIn(line3).foreach(m =>{
//    println(m.group(1))
//    println(m.group(2))
//    patIp.findFirstMatchIn(m.group(1)).foreach(m => {
//      println(m.group(0))
//    })
//  })
// val PATTERN_HOST_AND_OPTIONAL_PORT = """([^:]+)(:\d{4,5})?""".r
//  val line3 = "10.239.10.104"
//  PATTERN_HOST_AND_OPTIONAL_PORT.findFirstMatchIn(line3).foreach(m =>{
//      println(m.group(1))
//    if(m.groupCount>1) {
//      println(m.group(2))
//    }
//
//    })

//  val ip = InetAddress.getAllByName("worker")
//  println(ip)
//  ip.foreach(i => println(i.getHostAddress))
//  println(ip.find(i => !(i.getHostAddress.equals("127.0.0.1") || i.getHostAddress.equals("127.0.1.1"))))

//  val name = InetAddress.getAllByName("10.239.10.104")
//  name.foreach(n => println(n.getHostName))


//  val ip = "192.345.3.4"
//  println(ip.matches(Analyzer.PATTERN_IP.regex))
//  val f = new File("abc/aa.log")
//  println(f.getName)

//  val PATTERN_REPLY = """reply to ([^:]+):(\d+) \[with\] (.+)""".r
//  val line4 = "18/10/18 17:58:17 DEBUG RemoteNettyRpcCallContext: reply to 10.239.10.104:50476 [with] true"
//  PATTERN_REPLY.findFirstMatchIn(line4).foreach(m => {
//    for(i <- 0 to m.groupCount){
//      println(m.group(i))
//    }
//  })

  println("Master@10.239.10.104".indexOf("@"))
}
