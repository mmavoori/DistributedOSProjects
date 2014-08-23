import akka.actor.Actor
import akka.actor.Props
import akka.actor.ActorSystem
import akka.actor.ActorRef
import scala.util.Random
import scala.collection.mutable.ListBuffer
import sun.security.util.Length
import java.io.File
import java.io.FileWriter
import java.io.PrintWriter
import java.util.Date



case object gossip
case class msg_start(id:Int, ref_list:List[ActorRef], neighbors_List:List[Int])
case object Stop
case class check(id:Int)




object Project4_Bonus extends App {
  
  val logger = new ConsoleLogger()
  val system=ActorSystem("main")
  if(args.length==3)
  {
  val reference=system.actorOf(Props(new Master(args(0).toInt,args(1).toString,args(2).toString)),"master")
  }
  else
  {
  logger.function1()
  }
  }


class Master(nodes: Int, topo: String, alg: String) extends Actor
{
  val logger_1 = new ConsoleLogger()
  logger_1.function2()
  var no_of_nodes:Int=nodes
  var topology:String=topo
  var algorithm:String=alg
  var nodes_ref:List[ActorRef] = Nil
  var converge_nodes:List[Int]=Nil
  var numNodesRoot:Int=0
  var flag2:Int=0
  var conv_id:Int=0
  var START_TIME=0L
  var failed_node : Int = 0
  
  if (topology.equalsIgnoreCase("2DGrid"))
  {
    if (math.sqrt(no_of_nodes.toDouble) % 1 == 0)
    {
      
      numNodesRoot =  math.sqrt(no_of_nodes.toDouble).toInt
      logger_1.function3(no_of_nodes) 
    }
    else
    {
          logger_1.function4(no_of_nodes)
          var newNumNodes : Int = no_of_nodes
          while (math.sqrt(newNumNodes.toDouble) % 1 != 0)
          {
           newNumNodes = newNumNodes + 1 
          }
          no_of_nodes = newNumNodes
          logger_1.function5(no_of_nodes)
          numNodesRoot = math.sqrt(no_of_nodes.toDouble).toInt
          
    }
   }
  for(i <- 0 to no_of_nodes-1)
  {
    nodes_ref ::= context.actorOf(Props(new Node))
  } 
  if (topology.equalsIgnoreCase("2DGrid"))
   {
     for (i <-0 to no_of_nodes-1)
     {
       var neighbors_list:List[Int] = Nil
       if (!(i >= (no_of_nodes - numNodesRoot)))
       {
         neighbors_list ::= i + numNodesRoot
       }
       if (!(i < numNodesRoot))
       {
        neighbors_list ::= i - numNodesRoot 
       }
       if (!(i % numNodesRoot ==0))
       {
         neighbors_list ::= i - 1
       }
       if (!((i + 1) % numNodesRoot ==0))
       {
         neighbors_list ::= i + 1
       }
       logger_1.function6(i)
       nodes_ref(i) ! msg_start(i, nodes_ref, neighbors_list)
     }
   }
  else if (topology.equalsIgnoreCase("full")) 
  {	  
          for(i<- 0 to no_of_nodes-1)
          {            
             var neighbors_list:List[Int] = Nil
            for(j<- 0 to no_of_nodes-1)
            {
              if(j!=i)
              {
                neighbors_list ::= j
              }
            }
            logger_1.function6(i)
            nodes_ref(i) ! msg_start(i, nodes_ref, neighbors_list)
          } 
  }
  else
  {
    logger_1.function7()
    println("Program will not run, please check logs")
    context.system.shutdown
  }
//failure node code
   var random_node=Random.nextInt(nodes_ref.length)
   failed_node=random_node
   logger_1.function8(failed_node)
   context.stop(nodes_ref(failed_node))
   
   //END
   
   if(algorithm.equalsIgnoreCase("gossip"))
   {
     START_TIME=System.currentTimeMillis
     println("--------START_TIME--------" +START_TIME)
     var random_node=Random.nextInt(nodes_ref.length)
     while(nodes_ref(random_node) == nodes_ref(failed_node))
     {
     random_node=Random.nextInt(nodes_ref.length)
     
     }
     logger_1.function9(random_node)
     if(nodes_ref(random_node) == nodes_ref(failed_node) )
     {
       logger_1.function10()
       context.system.shutdown
     }
     nodes_ref(random_node) ! gossip
   } 
   else
   {
     logger_1.function11()
     println("Program will not run, please check logs")
     context.system.shutdown
   }
   
   
  def receive ={  
  case check(id:Int) =>
    {
      logger_1.function12()
      conv_id=id
      if(!(converge_nodes.contains(conv_id)))
         {
        logger_1.function13(conv_id)
        converge_nodes ::= conv_id
          
         }
        if(flag2==0)
        {
          if((converge_nodes.length.toDouble/no_of_nodes)>=0.90)
          {
                   flag2=1
         logger_1.function14()     
         println("-----------ALGORITHM CONVERGED----------")
    	 context.system.shutdown
    	 var END_TIME=System.currentTimeMillis
    	 println("-----END_TIME-----=" +END_TIME)
         println("TIME IN MILLISECONDS="+(END_TIME-START_TIME))
         
        }
          else if((no_of_nodes - converge_nodes.length.toDouble) == 1)
          {
            flag2=1
            logger_1.function15()
            context.system.shutdown
    	    var END_TIME=System.currentTimeMillis
    	    println("-----END_TIME-----=" +END_TIME)
    	    println("TIME IN MILLISECONDS="+(END_TIME-START_TIME))
    	    
          }
      }
    }
 // }
  }
}
class Node() extends Actor
{
  //properties of code class
  val logger_2 = new ConsoleLogger()
  //logger_2.function16(node_id)
  var node_id:Int=0
  var nodes_ref:List[ActorRef] = Nil
  var neighbors:List[Int] = Nil
  var counter:Int=0
  var s : Double = 0
    var w : Double = 1
    var oldSWValue : Double = 0
    var newSWValue : Double = 0
    var random_node : Int = 0
    var noOfRounds :Int = 0
    var noOfTimes : Int = 0
    var i : Int = 0    
    var flag : Int = 0
    var date=new Date;
   
  
  def receive = 
  {
    case msg_start(id:Int, ref_list:List[ActorRef], neighbors_List:List[Int]) =>
      {
       node_id=id
       nodes_ref=ref_list
       neighbors=neighbors_List
       //s = id
       logger_2.function16(id)
       
      }
    case `gossip` =>
      {
        
        if(counter<10)
        { 
         context.parent ! check(node_id)
         counter=counter+1    
         logger_2.function18(node_id, counter)
         for(i <- 0 to 1)
          {
        var random_node=Random.nextInt(neighbors.length)
        logger_2.function19(random_node, node_id)
        nodes_ref(neighbors(random_node)) ! gossip  
        
          }
        }
                 
      }  
   }
}
 trait Logger
 {
   var date=new Date;
   var fname:String=null
  def time() : String =
  {
    var newtime:String = null
    newtime= date.getMonth()+"/"+date.getDate()+"\t\t"+date.getHours()+":"+date.getMinutes()+":"+date.getSeconds()
    return newtime
  }
  def function1() : Unit = 
  {
    actorLog.writeToFile("./Logs/Boss.txt","DATESTAMP \t TIMESTAMP \t MESSAGE \n")
    actorLog.appendToFile("./Logs/Boss.txt",time()+"\t ERROR!!! : proper number of arguments not provided \n")
    println("Program will not run, please check logs")
  }
  def function2() : Unit = 
  {
  actorLog.writeToFile("./Logs/Boss.txt","DATESTAMP \t TIMESTAMP \t MESSAGE \t\t\t\t\t\t\tNODE SELECTED \n")
  actorLog.appendToFile("./Logs/Boss.txt","\t\t\t\t"+time()+"\t\t Boss Actor started \n")
  }
  def function3(no_of_nodes : Int) : Unit = 
  {
    actorLog.appendToFile("./Logs/Boss.txt",time()+"\t\tNumber of nodes entered is a perfect square : "+no_of_nodes)
  }
  def function4(no_of_nodes : Int) : Unit = 
  {
    actorLog.appendToFile("./Logs/Boss.txt",time()+"\t\tNumber of nodes entered is not a perfect square : "+no_of_nodes)
  }
  def function5(no_of_nodes : Int) : Unit =
  {
    actorLog.appendToFile("./Logs/Boss.txt",time()+"\t\tThe incremented number of nodes : "+no_of_nodes)
  }
  def function6(i : Int) : Unit =
  {
    actorLog.appendToFile("./Logs/Boss.txt",time()+"\t\tBoss Actor has sent the worker "+i+" its initialization information"+"\t\t\t "+i)
  }
  def function7() : Unit = 
  {
    actorLog.appendToFile("./Logs/Boss.txt",time()+"\t\t ERROR!!! : proper arguments not provided, check the argument provided for topology \n") 
  }
  def function8(failed_node : Int) : Unit = 
  {
    actorLog.appendToFile("./Logs/Boss.txt",time()+"\t\tBoss Actor has chosen the worker "+failed_node+" randomly as the Failure node."+"\t\t\t "+failed_node)
  }
  def function9(random_node : Int) : Unit = 
  {
    actorLog.appendToFile("./Logs/Boss.txt",time()+"\t\tBoss Actor has chosen the worker "+random_node+" randomly to start Gossip algorithm"+"\t\t "+random_node)
  }
  def function10() : Unit = 
  {
    actorLog.appendToFile("./Logs/Boss.txt",time()+"\t\tERROR!!! Actor selection by Boss for initiating gossip algorithm is wrong")
  }
  def function11() : Unit = 
  {
    actorLog.appendToFile("./Logs/Boss.txt",time()+"\t\t ERROR!!! : proper arguments not provided, check the argument provided for algorithm \n") 
  }
  def function12() : Unit = 
  {
    actorLog.appendToFile("./Logs/Boss.txt",time()+"\t\tchecking the convergence status")
  }
  def function13(conv_id : Int) : Unit = 
  {
    actorLog.appendToFile("./Logs/Boss.txt",time()+"\t\tNode"+conv_id+" is added to converge nodes list"+"\t\t\t\t\t\t "+conv_id)
  }
  def function14() : Unit = 
  {
    actorLog.appendToFile("./Logs/Boss.txt",time()+"\t\t***System is converged now***")
    actorLog.appendToFile("./Logs/Boss.txt",time()+"\t\tExiting from Boss Actor")     
  }
  def function15() : Unit = 
  {
    actorLog.appendToFile("./Logs/Boss.txt",time()+"\t\tNo more nodes can be added into the converge nodes list")
  }
  def function16(node_id : Int) : Unit = 
  {
    this.fname="./Logs/node"+node_id+" log.txt"
    var fp= new File(fname)
    actorLog.writeToFile(fname,"DATESTAMP \t TIMESTAMP \t\t MESSAGE \t\t\t\t\tSOURCE NODE \tDESTINATION NODE \n")
    actorLog.appendToFile(fname,"\t\t"+time()+"\t\tLog created for node "+node_id+"\n")
  }
  def function17(id : Int) : Unit = 
  {
    this.fname="./Logs/node"+id+" log.txt"
    var fp= new File(fname)
  }
  def function18(node_id : Int, counter : Int) : Unit = 
  {
    actorLog.appendToFile(fname,time()+"\t\t Gossip being performed for Node "+node_id+" & count for this node at present ="+counter)
  }
  def function19(random_node : Int, node_id : Int) : Unit = 
  {
    actorLog.appendToFile(fname,time()+"\t\t Gossip being called for Node "+random_node+" from node "+node_id+"\t\t\t\t"+node_id+"\t\t"+random_node)
  }
 }
 
 class ConsoleLogger extends Logger {
            println("Extending Logger trait")
    }

 
 
