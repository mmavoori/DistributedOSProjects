import java.io.File
import java.io.FileWriter
import java.io.PrintWriter

object actorLog {
 
  
  def using[A <: {def close(): Unit}, B](param: A)(f: A => B): B =
		  try { f(param) } finally { param.close() }

  def writeToFile(fileName:String, data:String) =
		  using (new FileWriter(fileName)) {
	  		fileWriter => fileWriter.write(data)
  }

  
  def appendToFile(fileName:String, textData:String) =
		  using (new FileWriter(fileName, true)){
	  		fileWriter => using (new PrintWriter(fileWriter)) {
	  			printWriter => 
	  			  {
	  			    println()
	  			    printWriter.println(textData)
	  			  }
	  		}
  }

/*
  
   
  def writeToFile(p: String, s: String) {
    val pw = new java.io.PrintWriter(new File(p))
    try pw.write(s)
    finally pw.close()
  }
  * */
  
}