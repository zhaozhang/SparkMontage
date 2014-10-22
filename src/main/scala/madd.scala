import org.eso.fits._
import java.io._
import scala.io.Source

object madd {
  def main(args: Array[String]) {
    class Fits(val path:String, val naxis: Array[Int], val crpix: Array[Double], val ncol: Int, val matrix: Array[Array[Float]]){
      def getPath = path
      def getNaxis = naxis
      def getCrpix = crpix
      def getNcol = ncol
      def getMatrix = matrix      
    }
    
    def readfits(path: String): Fits = {
      val file = new FitsFile(path)
      val hdu:FitsHDUnit = file.getHDUnit(0)
      val hdr = hdu.getHeader()
      
      val dm: FitsMatrix = hdu.getData().asInstanceOf[FitsMatrix]
      val naxis = dm.getNaxis()
      val crpix = dm.getCrpix()
      val ncol = naxis(0)
      val nval = dm.getNoValues()
      val nrow = nval/ncol

      var matrix = Array.ofDim[Float](nrow, ncol)
      (0 until nrow) map (i => dm.getFloatValues(i*ncol, ncol, matrix(i)))
      
      val f = new Fits(path, naxis, crpix, ncol, matrix)
      f
    }
    
    def processMeta(fitslist: Array[Fits]):Map[String, Map[String, Int]] = {
      val crpix1l = ((0 until fitslist.length) map (i => fitslist(i).getCrpix(1))).toList
      val crpix1max = crpix1l.max
      
      val crpix0l = ((0 until fitslist.length) map (i => fitslist(i).getCrpix(0))).toList
      val crpix0max = crpix0l.max
      
      val start = ((0 until fitslist.length) map (i => (crpix1max - fitslist(i).getCrpix(1) + 1).toInt)).toList
      val end = ((0 until fitslist.length) map (i => (start(i) + fitslist(i).getNaxis(1) - 1).toInt)).toList
      val offset = ((0 until fitslist.length) map (i => (crpix0max - fitslist(i).getCrpix(0)).toInt)).toList
      
      var map = Map[String, Map[String, Int]]()
      (0 until fitslist.length) map (i => map += (i.toString -> Map("start"->start(i), "end"->end(i), "offset"->offset(i))))
      map
      
    }
    
    def add(fitslist: Array[Fits], map: Map[String, Map[String, Int]], tcol: Int, trow: Int): Array[Array[Float]] = {
      val matrix = Array.ofDim[Float](trow, tcol)
      for {i<-(0 until trow)
        j<-(0 until tcol)
      }matrix(i)(j)=Float.NaN
           
      
      val l = (1 to trow) map (i => (0 until fitslist.length) filter (x => i-map(x.toString)("start") >= 0 && i-map(x.toString)("end")<0)) 
      val lmap = (1 to trow) zip l
     
      lmap map { 
        case (row, flist) => {          
          var array2d = Array.ofDim[Float](flist.length, tcol)
          for{i<-(0 until flist.length)
        	  j<-(0 until tcol)
          }array2d(i)(j)=Float.NaN
      	         
          for ((x, i) <- flist zipWithIndex)  {
          	var offset = map(x.toString)("offset")
          	var start = map(x.toString)("start")
          	Array.copy(fitslist(x).matrix(row-start), 0, array2d(i), offset, fitslist(x).ncol) 
          }
          
          def avgColumn(c: Int): Float = {
            var sum: Float = 0
            var count: Int = 0
            for (r <- (0 until flist.length)){
              if(!java.lang.Float.isNaN(array2d(r)(c))){
                sum = sum + array2d(r)(c)
                count = count +1
              }
            }
            if (count > 0){
              sum/count
            }else{
              Float.NaN
            }    
          }
          
          var array = (for (c <- (0 until tcol))
        	  yield avgColumn(c)).toArray
          
          Array.copy(array, 0, matrix(row-1), 0, tcol)
        }
      }
      matrix
    }
    
    def createFits(template: Template, matrix: Array[Array[Float]], path: String) {
      val tcol = template.tcol
      val trow = template.trow
      val naxis = Array(tcol, trow)
      val nax = tcol * trow
      var data = new Array[Float](nax)
      for (i<-0 until nax) data(i) = Float.NaN
      
      for (i<-0 until trow){
        Array.copy(matrix(i), 0, data, i*tcol, tcol)
      }
     
      var hdu: FitsHDUnit = null
      var mtx: FitsMatrix = new FitsMatrix(Fits.DOUBLE, naxis)
      mtx.setFloatValues(0, data)
      
      mtx.setCrpix(template.crpix)
      mtx.setCrval(template.crval)
      mtx.setCdelt(template.cdelt)
      
      var hdr: FitsHeader = mtx.getHeader()
      hdr.addKeyword(new FitsKeyword("", ""))
      var fitsWCS = new FitsWCS(hdr)

      hdr.addKeyword(new FitsKeyword("CTYPE1", "= "+template.ctype(0)))
      hdr.addKeyword(new FitsKeyword("CTYPE2", "= "+template.ctype(1)))
      
      hdu = new FitsHDUnit(hdr, mtx)
      
      var file: FitsFile = new FitsFile()
      file.addHDUnit(hdu)
      file.writeFile(path)
    }
    
    
    def findmax(fitslist: Array[Fits]) {
      var max: Float = 0
      for (f<-fitslist) {
        for(i<- 0 until f.matrix.length) {
          for(j<- 0 until f.ncol){
            if (f.matrix(i)(j) > max)
              max = f.matrix(i)(j)
          }
        }
      }
      println("max: "+max)
    }
    
    class Template(val path: String){
      var kmap:Map[String, String] = Map()
      for(line <- Source.fromFile(path).getLines()){
        var words = (line.split('=') map (x=>x.trim))
        if (words.length > 1)
          kmap+=(words(0)->words(1))
      }
      def tcol = kmap("NAXIS1").toInt
      def trow = kmap("NAXIS2").toInt
      def crpix:Array[Double] = Array(kmap("CRPIX1").toFloat, kmap("CRPIX2").toFloat)
      def crval:Array[Double] = Array(kmap("CRVAL1").toFloat, kmap("CRVAL2").toFloat)
      def cdelt:Array[Double] = Array(kmap("CDELT1").toFloat, kmap("CDELT2").toFloat)
      def ctype:Array[String] = Array(kmap("CTYPE1"), kmap("CTYPE2"))
    }
    
    //Main program entrance
    val flist = new File("/Users/zhaozhang/projects/Montage/m101/corrdir/").listFiles.filter(_.getName.endsWith(".fits"))
    
    val fitslist = (for(i<-0 until flist.length) yield readfits(flist(i).toString)).toArray
    
    val map = processMeta(fitslist)
    
    val template = new Template("/Users/zhaozhang/projects/Montage/m101/template.hdr")
        
    val matrix = add(fitslist, map, template.tcol, template.trow)
    
    createFits(template, matrix, "final.fits")
  }
}
