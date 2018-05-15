import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileUtil

object ReportsConversion {

  def wrk = "file:///C:/temp/"
  //spark context
  val conf = new SparkConf().setAppName("ReportsConversion").setMaster("local")
  val sc = new SparkContext(conf)
  //hadoop context
  val hdpconf =  new Configuration()
  val fs = FileSystem.getLocal(hdpconf)

  def main(args: Array[String]) : Unit = {
    val docrootClient = new Docroot(wrk + "client/docroot")
    val docrootClientKernel = new Docroot(wrk + "/productclientversion/docroot.f6")
    println(docrootClient)
    println(docrootClientKernel)
    docrootClient.extractAll()
    docrootClientKernel.extractAll()
    sc.stop()
  }

  class Docroot(val docroot: String)  extends java.io.Serializable {
    def reports() : String = {docroot + "/Reports"}
    def screens() : String = {docroot + "/xml"}
    def screenswms() : String = {screens() + "/wms"}
    def screensltt() : String = {screens() + "/ltt"}
    override def toString: String = docroot + "\n" + screens() + "\n" + screenswms() + "\n" + screensltt()
    def reportsExtract() = extract(reports(), "<expression>", docroot + "/reportsextraction")
    def screensWmsExtract() = extract(screenswms(), "<report ", docroot + "/screenswmsextraction")
    def screensLttExtract() = extract(screensltt(), "<report ", docroot + "/screenslttextraction")
    def extractAll() =
    {
      reportsExtract()
      screensWmsExtract()
      screensLttExtract()
    }
    def extract(path:String, value:String, dest:String) =
    {
      val filedest = dest + "/result.txt"
      val filedesttemp =  dest + "/resulttemp.txt"
      fs.delete(new Path (filedest), true)
      fs.delete(new Path (filedesttemp), true)
      (
        sc.parallelize(Seq("<tag>")) ++
        sc.wholeTextFiles(path)
          .flatMap(
            file =>
            file._2
              .split("\\r\\n|\\n|\\r")
              .iterator
              .zipWithIndex
              .map( x => (file._1,x._1,x._2)))
          .filter(line => line._2.toLowerCase().contains(value))
          .map( line => line._2 + "<!-- file " + line._1 + " linenumber " + (line._3 + 1) + " -->"  ) ++
        sc.parallelize(Seq("</tag>"))
      ).repartition(1)
      .saveAsTextFile(filedesttemp)
      FileUtil.copyMerge(fs, new Path(filedesttemp), fs, new Path (filedest), false, hdpconf, null)
      fs.delete(new Path(filedesttemp), true)
    }
  }
}
