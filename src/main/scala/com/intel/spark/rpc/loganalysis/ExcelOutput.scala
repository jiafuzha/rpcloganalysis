package com.intel.spark.rpc.loganalysis

import java.io.{File, FileInputStream, FileOutputStream, InputStream}

import org.apache.poi.ss.usermodel.{BorderStyle, CellStyle, CellType, Row}
import org.apache.poi.ss.util.CellRangeAddress
import org.apache.poi.xssf.usermodel.{XSSFSheet, XSSFWorkbook}

class ExcelOutput(val logDir: File, val appSummary: AppSummary, val existingFile: File) {
  private val outputDir = new File(logDir, "output")
  private val defaultOutputFile = new File(outputDir, "analysis.xlsx")

  private var rownum = 0

  private def createHead(sheet: XSSFSheet, workbook: XSSFWorkbook) = {
    val heads = Seq("Input Data Size (byte)", "Configurations", "Total Task Number", "Duration (ms)", "Total Traffic on Master", "Total Traffic on Driver",
    "Total Traffic on Workers", "Traffic per Second on Master", "Traffic per Second on Driver", "Traffic per Second on Workers", "Traffic Size on Master(Min/Avg/Max)",
    "Traffic Size on Driver(Min/Avg/Max)", "Traffic Size on Workers(Min/Avg/Max)")
    rownum += 1
    val row = sheet.createRow(rownum)
    var colIndex = 0
    val style = workbook.createCellStyle();
    style.setBorderTop(BorderStyle.THICK) // double lines border
    style.setBorderBottom(BorderStyle.THICK) // single line border
    val font = workbook.createFont()
    font.setBold(true)
    style.setFont(font)

    heads.foreach(h => {
      colIndex += 1
      val cell = row.createCell(colIndex)
      cell.setCellValue(h)
      cell.setCellStyle(style)
    })

    for(i <- 1 to colIndex){
      sheet.autoSizeColumn(i)
    }
  }

  private def calcRpcMaM(stat: Stat) = {
    var avg = if(stat.rpcCount==0) 0.0 else stat.totalMsgSize.toDouble/stat.rpcCount
    avg = BigDecimal(avg).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble
    (stat.msgSizeMin, avg, stat.msgSizeMax)
  }

  private def calcRpcRate(stat: Stat) = {
    val rate = stat.rpcCount.toDouble*1000/appSummary.appDuration
    BigDecimal(rate).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  private def createCell(row: Row, colIndex: Int, value: String, style: CellStyle, cellType: CellType=CellType.STRING): Unit ={
    val cell = row.createCell(colIndex)
    cell.setCellType(cellType)
    cell.setCellValue(value)
    cell.setCellStyle(style)
  }

  private def writeTrafficSize(rows: Array[Row], index: Int, style: CellStyle): Unit ={
    writeTotalCommon(rows, index, style, (stat: Stat) => {calcRpcMaM(stat).toString.replaceAll(",", "/")}, (stat: Stat) => {calcRpcMaM(stat).toString}.replaceAll(",", "/"))
  }

  private def writeTrafficRate(rows: Array[Row], index: Int, style: CellStyle): Int ={
    writeTotalCommon(rows, index, style, (stat: Stat) => {calcRpcRate(stat).toString}, (stat: Stat) => {calcRpcRate(stat).toString})
  }

  private def writeTotalTraffic(rows: Array[Row], index: Int, style: CellStyle): Int ={
    writeTotalCommon(rows, index, style, (stat: Stat) => {stat.rpcCount.toString}, (stat: Stat) => {stat.rpcCount.toString})
  }

  private def writeTotalCommon(rows: Array[Row], index: Int, style: CellStyle, calcFunc: Stat=>String,
                       workerCalcFunc: Stat=>String): Int ={
    var colIndex = index

    def createCells(name: String): Unit ={
      colIndex += 1
      createCell(rows(0), colIndex, "RPC remote: "+calcFunc(appSummary.sitesRpc(name)._1), style)
      createCell(rows(1), colIndex, "RPC local: "+calcFunc(appSummary.sitesRpc(name)._2), style)
      createCell(rows(2), colIndex, "shuffle remote: "+calcFunc(appSummary.sitesShuffle(name)._1), style)
      createCell(rows(3), colIndex, "shuffle local: "+calcFunc(appSummary.sitesShuffle(name)._2), style)
    }

    createCells(Main.NAME_MASTER)
    createCells(Main.NAME_DRIVER)

    def collectWorkerInfo(map: Map[String, (Stat, Stat)]): (String, String) ={
      val sbRpcRemoteTotal = new StringBuilder()
      val sbRpcLocalTotal = new StringBuilder()

      map.foreach((e) => {
        val site = e._1
        val rpcRemote = e._2._1
        val rpcLocal = e._2._2
        if(!(site==Main.NAME_DRIVER || site==Main.NAME_MASTER)){
          sbRpcRemoteTotal.append(site).append("(").append(workerCalcFunc(rpcRemote)).append(")\r\n")
          sbRpcLocalTotal.append(site).append("(").append(workerCalcFunc(rpcLocal)).append(")\r\n")
        }
      })
      sbRpcRemoteTotal.setLength(sbRpcRemoteTotal.length-1)
      sbRpcLocalTotal.setLength(sbRpcLocalTotal.length-1)

      (sbRpcRemoteTotal.toString(), sbRpcLocalTotal.toString())
    }

    val workerRpcs = collectWorkerInfo(appSummary.sitesRpc)
    val workerShuffles = collectWorkerInfo(appSummary.sitesShuffle)

    colIndex += 1
    createCell(rows(0), colIndex, "RPC remote:\n"+workerRpcs._1, style)
    createCell(rows(1), colIndex, "RPC local:\n"+workerRpcs._2, style)
    createCell(rows(2), colIndex, "shuffle remote:\n"+workerShuffles._1, style)
    createCell(rows(3), colIndex, "shuffle local:\n"+workerShuffles._2, style)

    colIndex
  }

  private def writeGeneralInfo(sheet: XSSFSheet, rows: Array[Row], style: CellStyle): Int ={
    val row = rows(0)
    var colIndex = 0
    val startRow = rownum - 3

    createCell(row, colIndex, s"${appSummary.appName}(${appSummary.appId})", style)
    sheet.addMergedRegion(new CellRangeAddress(startRow, rownum, colIndex, colIndex))

    colIndex += 1
    createCell(row, colIndex, appSummary.inputSize.toString, style, CellType.NUMERIC)
    sheet.addMergedRegion(new CellRangeAddress(startRow, rownum, colIndex, colIndex))

    colIndex += 1
    createCell(row, colIndex, appSummary.executors.mkString("\r\n"), style)
    sheet.addMergedRegion(new CellRangeAddress(startRow, rownum, colIndex, colIndex))

    colIndex += 1
    createCell(row, colIndex, appSummary.taskCount.toString, style, CellType.NUMERIC)
    sheet.addMergedRegion(new CellRangeAddress(startRow, rownum, colIndex, colIndex))

    colIndex += 1
    createCell(row, colIndex, appSummary.appDuration.toString, style, CellType.NUMERIC)
    sheet.addMergedRegion(new CellRangeAddress(startRow, rownum, colIndex, colIndex))

    colIndex
  }

  private def createRows(sheet: XSSFSheet, workbook: XSSFWorkbook, style: CellStyle): Array[Row] ={
    rownum += 1
    val row1 = sheet.createRow(rownum)
    row1.setRowStyle(style)

    rownum += 1
    val row2 = sheet.createRow(rownum)
    row2.setRowStyle(style)

    rownum += 1
    val row3 = sheet.createRow(rownum)
    row3.setRowStyle(style)

    rownum += 1
    val row4 = sheet.createRow(rownum)
    row4.setRowStyle(style)

    Array(row1, row2, row3, row4)
  }

  def output() = {
    if(existingFile != null && existingFile.exists() && existingFile.isFile){
      println("output to existing file. "+existingFile.getAbsolutePath)
      outputToExistingExcel(existingFile)
    }else{
      println("output to new file. "+defaultOutputFile.getAbsolutePath)
      outputToNewExcel(defaultOutputFile)
    }
  }

  private def outputToNewExcel(outputFile: File): Unit ={
    if(!outputDir.exists() && (!outputDir.mkdir())){
      throw new Exception(s"cannot create directory ${outputDir.getAbsolutePath}")
    }
    if(outputFile.exists() && (!outputFile.delete())){
      throw new Exception(s"cannot delete ${outputFile.getAbsolutePath}")
    }

    val workbook = new XSSFWorkbook()
    val sheet = workbook.createSheet("sheet0")
    createHead(sheet, workbook)

    writeStats(workbook, sheet, outputFile)
  }

  private def outputToExistingExcel(outputFile: File): Unit ={
    val workbook = readFile(outputFile)
    val sheet = workbook.getSheetAt(0)
    rownum = sheet.getLastRowNum

    writeStats(workbook, sheet, outputFile)
  }

  private def writeStats(workbook: XSSFWorkbook, sheet: XSSFSheet, outputFile: File): Unit ={
    val style = workbook.createCellStyle
    style.setWrapText(true)

    val rows = createRows(sheet, workbook, style)

    var colIndex = writeGeneralInfo(sheet, rows, style)

    colIndex = writeTotalTraffic(rows, colIndex, style)
    colIndex = writeTrafficRate(rows, colIndex, style)
    writeTrafficSize(rows, colIndex, style)

    write(workbook, outputFile)
  }

  private def readFile(file: File): XSSFWorkbook ={
    var is: InputStream = null
    try{
      is = new FileInputStream(file)
      new XSSFWorkbook(is)
    }finally{
      if(is != null) is.close()
    }
  }

  private def write(workbook: XSSFWorkbook, outputFile: File): Unit ={
    var fos: FileOutputStream = null
    try{
      fos = new FileOutputStream(outputFile)
      workbook.write(fos)
    }finally {
      fos.close()
      workbook.close()
    }
  }

}

object ExcelOutput extends App {
  val workbook = new XSSFWorkbook()
  val sheet = workbook.createSheet("work1")
  val row = sheet.createRow(1)
  val cell = row.createCell(1)

  cell.setCellType(CellType.NUMERIC)
  cell.setCellValue(10.5)

  var fos: FileOutputStream = null
  try{
    fos = new FileOutputStream("test.xls")
    workbook.write(fos)
  }finally {
    fos.close()
    workbook.close()
  }
}
