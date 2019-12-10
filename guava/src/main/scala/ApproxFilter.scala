import java.io.{InputStream, ObjectInputStream, ObjectOutputStream, OutputStream}

import com.google.common.hash.Funnel
import com.google.common.{hash => g}
import org.apache.beam.sdk.coders.{AtomicCoder, Coder, VarIntCoder}
import org.apache.beam.sdk.util.CoderUtils

import scala.reflect.ClassTag

abstract class ApproxFilter[T] extends Serializable {
  type Param

  // TODO: overload constructors with default expectedInsertions, fpp, etc.
  // Sub-classes must implement the following constructor
  // def this(xs: Iterable[T], expectedInsertions: Long, fpp: Double, param: Param)

  def mightContain(value: T): Boolean
  val elementCount: Long
  val expectedFpp: Double

  protected def setField(name: String, value: Any): Unit = {
    val f = getClass.getDeclaredField(name)
    f.setAccessible(true)
    f.set(this, value)
  }
}

object ApproxFilter {
  def decode[T, F[_] <: ApproxFilter[_]](in: InputStream): F[T] =
    new ApproxFilterCoder[T, F]().decode(in)
}

class ApproxFilterCoder[T, F[_] <: ApproxFilter[_]] extends AtomicCoder[F[T]] {
  override def encode(value: F[T], outStream: OutputStream): Unit =
    new ObjectOutputStream(outStream).writeObject(value)

  override def decode(inStream: InputStream): F[T] =
    new ObjectInputStream(inStream).readObject().asInstanceOf[F[T]]
}

////////////////////////////////////////////////////////////
// BloomFilter
////////////////////////////////////////////////////////////

class BloomFilter[T](elems: Iterable[T], expectedInsertions: Long, fpp: Double)(
  implicit val funnel: BloomFilter[T]#Param)
  extends ApproxFilter[T] {
  override type Param = g.Funnel[T]

  val internal: g.BloomFilter[T] = {
    val bf = g.BloomFilter.create(funnel, expectedInsertions, fpp)
    elems.foreach(bf.put)
    bf
  }

  override def mightContain(value: T): Boolean = internal.mightContain(value)
  override val elementCount: Long = internal.approximateElementCount()
  override val expectedFpp: Double = internal.expectedFpp()

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeObject(funnel)
    internal.writeTo(out)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    val funnel = in.readObject().asInstanceOf[g.Funnel[T]]
    val internal = g.BloomFilter.readFrom(in, funnel)
    setField("funnel", funnel)
    setField("internal", internal)
  }
}

////////////////////////////////////////////////////////////
// SetFilter
////////////////////////////////////////////////////////////

class SetFilter[T](elems: Iterable[T])(implicit val coder: SetFilter[T]#Param)
  extends ApproxFilter[T] {
  override type Param = Coder[T]

  val set: Set[T] = elems.toSet

  override def mightContain(value: T): Boolean = set(value)
  override val elementCount: Long = set.size
  override val expectedFpp: Double = 1.0

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeObject(coder)
    out.writeInt(set.size)
    set.foreach(coder.encode(_, out))
  }

  private def readObject(in: ObjectInputStream): Unit = {
    val coder = in.readObject().asInstanceOf[Coder[T]]
    val builder = Set.newBuilder[T]
    (1 to in.readInt()).foreach(_ => builder += coder.decode(in))
    setField("coder", coder)
    setField("set", builder.result())
  }
}

////////////////////////////////////////////////////////////

object Test {

  private def test[F[Int] <: ApproxFilter[Int]](f: F[Int]): Unit = {
    val copy = CoderUtils.clone(new ApproxFilterCoder[Int, F](), f)
    require((1 to 100).forall(copy.mightContain))
  }

  def main(args: Array[String]): Unit = {
    implicit val intFunnel: Funnel[Int] = g.Funnels.integerFunnel().asInstanceOf[g.Funnel[Int]]
    implicit val intCoder: Coder[Int] = VarIntCoder.of().asInstanceOf[Coder[Int]]

    val bf = new BloomFilter[Int](1 to 100, 1000, 0.01)
    val sf = new SetFilter[Int](1 to 100)

    test(bf)
    test(sf)
  }
}
