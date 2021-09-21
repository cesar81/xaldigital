package com.xaldigital.challenge

import org.apache.spark.sql.functions.{col,length}
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import java.util.Properties

object Sample {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  val log = Logger.getLogger("SampleXaldigital")
  
  def main(args: Array[String]){    
    log.info("Inicio del Programa")
    
    val spark = getSessionLocal()
    val ruta = "C:/Users/cesar/Downloads/Xal Digital/Sample.csv"
            
    log.info("Lectura en Disco local del archivo Sample.csv")
    val dfSampleCSV = readCsvLocal(spark, ruta)
    //log.info("Esquema del CSV Sample: ")
    //dfSampleCSV.printSchema()  
    
    log.info("Valida longitud = 2 y Solo letras: " +  validaLongitud2SoloLetras(dfSampleCSV, "state"))    
    
    overwriteTablePostgresqlLocal(dfSampleCSV, "xaldigital", "sample")
    spark.stop
    log.info("Fin del Programa")
    
  }
  
  def validaLongitud2SoloLetras(df: DataFrame, columna:String): Boolean = {
    log.info("Inicio validaLongitud2SoloLetras")
    var valido = false
    log.info("Numero de registros col=state con longitud diferente a 2: "+ df.filter(length(col(columna)) =!= 2).count())
    log.info("Numero de registros col=state con solo letras: "+ df.filter(col(columna).rlike("^[a-zA-Z]*$")).count())
    log.info("Numero de registros col=state no son solo letras: "+ df.filter(col(columna).rlike("[^a-zA-Z]")).count())
    
    if (df.filter(length(col(columna)) =!= 2).count() == 0 &&
        df.filter(col(columna).rlike("[^a-zA-Z]")).count() == 0 ){
      valido = true
    }
    log.info("Fin validaLongitud2SoloLetras")
    return valido
  }  
  
  def overwriteTablePostgresqlLocal(df: DataFrame, nombreSchema: String, nombreTabla: String){    
    log.info("Inicio overwriteTablePostgresqlLocal")
    
    val connectionProperties = new Properties()
    connectionProperties.put("user", "postgres")
    connectionProperties.put("password", "bigdata")     
    
    df.write.mode("overwrite").jdbc("jdbc:postgresql://localhost:5432/" 
                + nombreSchema, nombreTabla, connectionProperties)
    log.info("Fin overwriteTablePostgresqlLocal")
  }
  
  /** Funcion para obtener el SparkSession. */   
  def getSessionLocal(): SparkSession = {
    val spark = SparkSession.builder().appName("Spark Example")
                      .config("spark.master","local[*]")
                      .getOrCreate()
    return spark
  }
  
  /** Funcion para consultar una tabla en Postgresql Local
   *
   *  @author Cesar Reyes.
   *  @param spark instancia de SparkSesion.
   *  @param nombreSchema El nombre del esquema o base de datos donde se encuentra la tabla a consultar.
   *  @param nombreTabla El nombre de la tabla a consultar.
   */
  def readTablePostgresqlLocal(spark: SparkSession, nombreSchema: String, nombreTabla: String): DataFrame = {
    val dfTabla = spark.read
                    .option("url","jdbc:postgresql://localhost:5432/" + nombreSchema)
                    .option("driver", "org.postgresql.Driver")
                    .option("dbtable", nombreTabla)
                    .option("user", "postgres")
                    .option("password", "bigdata")
                    .format("jdbc").load
    return dfTabla
  }
  
  def readCsvLocal(spark: SparkSession, ruta: String): DataFrame = {
    val dfTabla = spark.read.option("header",true).option("inferSchema", true).csv("file:///"+ ruta)     
    return dfTabla
  }  
}