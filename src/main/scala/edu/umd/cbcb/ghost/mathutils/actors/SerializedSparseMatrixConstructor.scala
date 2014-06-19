import org.apache.commons.math.linear._

//import scala.actors.Actor
import akka.actor.Actor

case class AddEntry( i: Int, j: Int, e: Double )
// Tells the actor that they are done
case class Stop()

class SerializedSparseSymmetricMatrixConstructor[MatT <: RealMatrix]( val m: MatT ) extends Actor { 
  

  def receive = { 
	case AddEntry(i, j, e) => { 
	  m.setEntry(i,j,e)
	  m.setEntry(j,i,e)
	}
	case Stop() => { 
	  context.stop(self)
	}
    case _ => {
        if(done) {
            context.stop(self)
        }
    }
  }
  
  def done() = { true }

}

