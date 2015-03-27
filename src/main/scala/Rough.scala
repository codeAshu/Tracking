import com.useready.tracking.recommendation.Period

/**
 * Created by Ashu on 27-03-2015.
 */


/**
 * rough
 */
object Rough {

  def main(args: Array[String]) {
    val s = "2110"
    val l = s.toList
    val j = l(0).toString.toLong
    println("j=>"+j)
    val x = l.slice(2,l.size).foldLeft("")((b,a)=>b.toString+a.toString).replaceAll("^\"|\"$", "").toInt

//   val x= "".replaceAll("^\"|\"$", "").toInt
    println(x)


   // println(Period.ValueSet.)




  }

  }
