import java.io.{InputStream, ObjectInputStream, ObjectOutputStream, OutputStream}

import com.google.common.hash.Funnel
import com.google.common.{hash => g}
import org.apache.beam.sdk.coders.{AtomicCoder, Coder, VarIntCoder}
import org.apache.beam.sdk.util.CoderUtils

import scala.reflect.ClassTag

abstract class ApproxFilter[T] extends Serializable {
  type Param <: AnyRef

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
  def build[F[_] <: ApproxFilter[_]](implicit ct: ClassTag[F[_]]): Builder[F] =
    new Builder[F](ct)

  class Builder[F[_] <: ApproxFilter[_]](ct: ClassTag[F[_]]) {
    private val constructor = ct.runtimeClass.getConstructors.find { c =>
      val cp = c.getParameters
      cp.length == 4 &&
        classOf[Iterable[_]].isAssignableFrom(cp(0).getType) &&
        classOf[Long].isAssignableFrom(cp(1).getType) &&
        classOf[Double].isAssignableFrom(cp(2).getType)
      // FIXME: figure out why this is false
      // param.getClass.isAssignableFrom(cp(3).getType)
    } match {
      case Some(c) => c
      case None => throw new IllegalStateException(
        s"Missing constructor ${ct.runtimeClass.getSimpleName}(Iterable[T], Long, Double, Param)")
    }

    def from[T](xs: Iterable[T],
                expectedInsertions: Long,
                fpp: Double)(implicit param: F[T]#Param): F[T] =
      constructor
        .newInstance(xs, expectedInsertions: java.lang.Long, fpp: java.lang.Double, param)
        .asInstanceOf[F[T]]
  }
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

class BloomFilter[T](elems: Iterable[T], expectedInsertions: Long, fpp: Double,
                     val funnel: g.Funnel[T])
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

class SetFilter[T](elems: Iterable[T], expectedInsertions: Long, fpp: Double, val coder: Coder[T])
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
  def test[F[Int] <: ApproxFilter[Int]](implicit param: F[Int]#Param,
                                        ct: ClassTag[F[_]]): Unit = {
    val xs = (1 to 100)
    val af = ApproxFilter.build[F].from(xs, 1000, 0.01)
    require(xs.forall(af.mightContain))

    val afCoder = new ApproxFilterCoder[Int, F]()
    val copy = CoderUtils.clone(afCoder, af)

    require(xs.forall(copy.mightContain))
  }

  def main(args: Array[String]): Unit = {
    implicit val intFunnel: Funnel[Int] = g.Funnels.integerFunnel().asInstanceOf[g.Funnel[Int]]
    implicit val intCoder: Coder[Int] = VarIntCoder.of().asInstanceOf[Coder[Int]]

    test[BloomFilter]
    test[SetFilter]
  }
}
