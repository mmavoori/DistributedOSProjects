import scala.math._
import scala.util._
import scala.collection.mutable._
import scala.actors.Actor
import scala.actors.Actor._


case object Stop

class Peer(var Ref_array: Array[Peer], r_table : Array[Array[String]], Leaf: Array[String] ,Neighbour: Array[String], Actor_id: String, var avg:Avg_Hops,var gridDim:Int) extends Actor {
  var obj = new Pastry_op
  var hop_counter: Int = 0 
  def act() {
   loop {
    react {
        case ("Msg", result_key: String, hop_counter: Int,num:Int) =>
          {
            var flag: Boolean = false
            var p: Int = 0
            if(!result_key.equals(Actor_id)) 
            {
            while (p < 4) {
              if (result_key.equals(Leaf(p)))
              {
                var Send_to: Int = obj.Integer_base_ten(Leaf(p))
                flag = true
                Ref_array(Send_to) ! ("Terminate", hop_counter + 1,num)
              }
              p=p+1
            }
               	}
               else
               {
                self ! ("Terminate", hop_counter,num)
                flag=true
             }
               
            if (!flag) {
              var l: Int = result_key.zip(Actor_id).takeWhile(Function.tupled(_ == _)).map(_._1).mkString.length()
              var d1:Char = result_key.apply(l)//check
              var dl:Int=d1.toString().toInt 
              if (r_table(l)(dl) == "NULL") {
                 var fwd: String = ""
                fwd = obj.rare(l, result_key, Leaf, r_table, Neighbour, Actor_id,gridDim)
                var Send_to: Int = obj.Integer_base_ten(fwd)
                Ref_array(Send_to) ! ("Msg", result_key, num,hop_counter + 1)
              } else {
                var Send_to: Int = obj.Integer_base_ten(r_table(l)(dl))
                Ref_array(Send_to) ! ("Msg", result_key,num,hop_counter + 1) //also Key
              }
            }
          }
        case ("Terminate", hop_counter: Int,num:Int) =>
          {
            avg!hop_counter
          }
        case ("Stop")=>
          exit()
      }
    }
  }
}

class Avg_Hops(temp:Int,reqs:Int) extends Actor {
 
  var avg:Double=0
  def act() {

    loop {

      react {
  case (n:Int)=>
    {
      avg=avg+n
    }
  case ("Stop")=>
    println("Average :"+avg/(temp*reqs))
    exit()
}
    }
 }
}
class Pastry_op {
  def Integer_base_ten(nodeID: String): Int = {
    var it: Long = nodeID.toLong
    var a: Int = 0
    var p: Long = 0
    var nat: Int = 0;
    var ct: Int = 0;
    while (it > 3) {
      p = it % 10;
      nat = (nat + p * pow(3, ct)).toInt
      it = it / 10;
      ct = ct + 1
    }
    nat = (nat + it * pow(3, ct)).toInt
    nat
  }

  def rare(l: Int, key: String, Leaf_rare: Array[String], r_table_rare: Array[Array[String]], Neighbour: Array[String], Actor_id: String,gridDim:Int): String = {
    var d: Int = 0
    var dmin: Int = 1000
    var cl: Int = 0
    var Send_to: String = ""
    var flag:Boolean=false   
    for (u <- 0 until 4) {
      cl = length_common(Leaf_rare(u), key)
      if (cl >= l)
      {
        d = proxydist(Leaf_rare(u), key,gridDim)
        flag = true
      }
        if (d < dmin && flag==true) {
        dmin = d
        Send_to = Leaf_rare(u)
      }
    }
    for (u <- 0 until 13) {
      for (v <- 0 until 3) {
        cl = length_common(r_table_rare(u)(v), key)
        if (cl >= l)
        { 
          d = proxydist(r_table_rare(u)(v), key,gridDim)
        if (d < dmin) {
          dmin = d
          Send_to = r_table_rare(u)(v)
        }
        }

      }
    }
    for (u <- 0 until 4) {
      cl = length_common(Neighbour(u), key)
      if (cl >= l)
        d = proxydist(Neighbour(u), key,gridDim)
      if (d < dmin) {
        dmin = d
        Send_to = Neighbour(u)
      }
    }
    Send_to
  }

  def length_common(k1: String, k2: String): Int = {
    k1.zip(k2).takeWhile(Function.tupled(_ == _)).map(_._1).mkString.length()
  }

  def proxydist(k1: String, k2: String,gridDim:Int): Int = {
    
    if(k1!="NULL"&&k2!="NULL")
    {
    var n1: Int = Integer_base_ten(k1)
    var n2: Int = Integer_base_ten(k2)
    var distance=(  abs(n1 % gridDim - n2 % gridDim) + abs(n1 / gridDim - n2 / gridDim))
  distance
    }
    else
     50000
  }
}

object Project3 extends Application {

  override def main(args: Array[String]) {

    val MaxRequests:Int=args(1).toInt
    val na:Int = args(0).toInt 
    val temp = pow(round(sqrt(na)), 2).toInt
    val numBits=round(log(temp)/log(3)).toInt
    var gridDim: Int = sqrt(temp).toInt
    var k: Int = 0
    var arr = Array.ofDim[String](gridDim, gridDim)
    for (i <- 0 until gridDim) {
      for (j <- 0 until gridDim) {
        arr(i)(j) = Unique_ID(k);
        k = k + 1;
      }
    }
    var PeerArray: Array[Peer] = new Array[Peer](temp)
    var avg: Avg_Hops= new Avg_Hops(temp,MaxRequests)
    for (i <- 0 to temp-1) {
      var r_table = Array.ofDim[String](13, 3)
      for (u <- 0 until 13) {
        for (v <- 0 until 3) {
        	
          r_table(u)(v) = RoutingTable(arr, u, v, i,gridDim)

        }

      }
      var Leaf = Array.ofDim[String](4)
      var it: Int = i - 2
      var ut: Int = 0
      while (ut < 4) {
        if (it != i) {
          Leaf(ut) = Unique_ID(it)
         ut = ut + 1
        }
        it = it + 1
      }
      var NeighbourTable  = Array.ofDim[String](4)
      var bt: Int = i % gridDim
      var at: Int = i / gridDim
      if (at - 1 >= 0)
        NeighbourTable (0) = arr(at - 1)(bt)
      else
        NeighbourTable (0) = "NULL"
      if (at + 1 < gridDim-1)
        NeighbourTable (1) = arr(at + 1)(bt)
      else
        NeighbourTable (1) = "NULL"
      if (bt - 1 >= 0)
        NeighbourTable (2) = arr(at)(bt - 1)
      else
        NeighbourTable (2) = "NULL"
      if (bt + 1 < gridDim-1)
        NeighbourTable (3) = arr(at)(bt + 1)
      else
        NeighbourTable (3) = "NULL"

var unid:String=""
unid=Unique_ID(i)
      PeerArray(i) = new Peer(null, r_table, Leaf , NeighbourTable, unid,avg,gridDim)
      
    }
    for (zi <- 0 to temp-1) 
    {
      PeerArray(zi).Ref_array = PeerArray
      PeerArray(zi).start()		  
    }
    avg.start()
var numinstances=1
while(numinstances<=MaxRequests)
{
  for(test<-0 until temp) 
   {
    var ct=System.currentTimeMillis()/1000
    var keybaseten=0
    
    do{
    keybaseten=Random.nextInt(temp)
    }while(keybaseten==test)
    while(System.currentTimeMillis()/1000-ct!=1)
    {}
    PeerArray(test) ! ("Msg",Unique_ID(keybaseten),numinstances,0)
  }
numinstances=numinstances+1;
}
var exitFlag:Int=0
do 
{
  exitFlag=0
  for(test<-0 until temp)
  {
    if(PeerArray(test).getState==Actor.State.Suspended)
      exitFlag=exitFlag+1
  }
}while(exitFlag<temp-2)
  for(i<-0 until temp)
  {
    PeerArray(i)!"Stop"
  }

avg!"Stop"

  }
  
  def RoutingTable(arr: Array[Array[String]], u: Int, v: Int, i: Int,Sqrt_t:Int): String = {

    {
    var r_table: String = ""
    var uid: String = Unique_ID(i)
    var matchstring: String = uid.dropRight(13 - u)
    matchstring = matchstring.concat(v.toString())
    var Distance_metric: Int = 0;
    var d: Int = 10000;
    if (!uid.startsWith(matchstring)) {

      for (a <- 0 until Sqrt_t) {
        for (b <- 0 until Sqrt_t) {

          if (arr(a)(b).startsWith(matchstring)) {
            Distance_metric = abs(i % Sqrt_t - b) + abs(i / Sqrt_t - a)

            if (Distance_metric < d) {
              r_table = arr(a)(b)
              d = Distance_metric
            }
          }
        }
      }
      if (r_table.length() != 0)
        r_table
      else
        "NULL"
    } else
      "NULL"
  }
  }
  
  
  def Unique_ID(i: Int): String = {
    var q = i;
    var p: Int = 0;
    var str = ""
    while (q >= 3) {
      p = q % 3;
      str = str.concat(p.toString())
      q = q / 3;
    }
    str = str.concat(q.toString())
    str = str.reverse
    var len: Int = str.length()
    var l: Int = 1;
    var str1: String = ""
    while (l <= (13 - len)) {
      str1 = str1.concat("0")
      l = l + 1;
    }
    str1 = str1.concat(str)
    str1
  }
}