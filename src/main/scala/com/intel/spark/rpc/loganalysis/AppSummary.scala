package com.intel.spark.rpc.loganalysis

class AppSummary(val appId: String, val appName: String, val inputSize: Long, val executors: Seq[(String, Int, Int)], val taskCount: Long,
                 val appDuration: Long, val sitesRpc: Map[String, (Stat, Stat)], val sitesShuffle: Map[String, (Stat, Stat)]
                ) {

}

class Stat(val rpcCount: Long, val msgSizeMax: Long, val msgSizeMin: Long, val totalMsgSize: Long)
