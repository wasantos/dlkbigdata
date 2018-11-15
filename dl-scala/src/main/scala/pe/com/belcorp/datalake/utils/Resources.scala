package pe.com.belcorp.datalake.utils

import java.util.Scanner

/**
  * Utility object for dealing with resources
  */
object Resources {

  /**
    * Loads a resource file as a String
    *
    * @param path the path where the file is located
    * @return
    */
  def load(path: String): String = {
    val stream = getClass.getClassLoader.getResourceAsStream(path)
    new Scanner(stream, "UTF-8").useDelimiter("\\A").next()
  }

}
