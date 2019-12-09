import java.io.{InputStream, ObjectInputStream, ObjectOutputStream, OutputStream}

import com.google.common.{hash => g}
import org.apache.beam.sdk.coders.{AtomicCoder, Coder, StringUtf8Coder, VarIntCoder}
import org.apache.beam.sdk.util.CoderUtils

import scala.reflect.ClassTag

abstract class ApproxFilter[T] {
  type Param <: AnyRef

  // Sub-classes must implement the following constructors
  // def this(in: InputStream)
  // def this(xs: Iterable[T], numElems: Long, fpp: Double, param: Param)

  def mightContain(value: T): Boolean
  val elementCount: Long
  val expectedFpp: Double

  def writeTo(out: OutputStream): Unit
}

object ApproxFilter {
  def build[T, F[_] <: ApproxFilter[_]](xs: Iterable[T],
                                        numElems: Long,
                                        fpp: Double)(implicit param: F[T]#Param,
                                                     ct: ClassTag[F[T]]): F[T] = {
    ct.runtimeClass.getConstructors.find { c =>
      val cp = c.getParameters
      cp.length == 4 &&
        classOf[Iterable[_]].isAssignableFrom(cp(0).getType) &&
        classOf[Long].isAssignableFrom(cp(1).getType) &&
        classOf[Double].isAssignableFrom(cp(2).getType) &&
        cp(3).getName == "param"
        // FIXME: figure out why this is false
        // param.getClass.isAssignableFrom(cp(3).getType)
    } match {
      case Some(c) => c
        .newInstance(xs, numElems: java.lang.Long, fpp: java.lang.Double, param)
        .asInstanceOf[F[T]]
      case None => throw new IllegalStateException(
        s"Missing constructor ${ct.runtimeClass.getSimpleName}(Iterable[T], Long, Double, Param)")
    }
  }
}
private object ApproxFilterUtils {
  def readObject[T](in: InputStream): T = new ObjectInputStream(in).readObject().asInstanceOf[T]
  def writeObject[T](out: OutputStream, obj: T): Unit = new ObjectOutputStream(out).writeObject(obj)
}

class ApproxFilterCoder[T, F[_] <: ApproxFilter[_]] extends AtomicCoder[F[T]] {
  private val stringCoder = StringUtf8Coder.of()

  override def encode(value: F[T], outStream: OutputStream): Unit = {
    stringCoder.encode(value.getClass.getName, outStream)
    value.writeTo(outStream)
  }

  override def decode(inStream: InputStream): F[T] =
    Class.forName(stringCoder.decode(inStream))
      .getConstructor(classOf[InputStream])
      .newInstance(inStream)
      .asInstanceOf[F[T]]
}

////////////////////////////////////////////////////////////
// BloomFilter
////////////////////////////////////////////////////////////

class BloomFilter[T](val internal: g.BloomFilter[T],
                     val funnel: g.Funnel[T]) extends ApproxFilter[T] {
  override type Param = g.Funnel[T]
  def this(numElems: Long, fpp: Double, param: g.Funnel[T]) =
    this(g.BloomFilter.create[T](param, numElems, fpp), param)

  def this(in: InputStream, funnel: g.Funnel[T]) =
    this(g.BloomFilter.readFrom[T](in, funnel), funnel)

  def this(in: InputStream) = this(in, ApproxFilterUtils.readObject[g.Funnel[T]](in))

  def this(xs: Iterable[T], numElems: Long, fpp: Double, param: g.Funnel[T]) =
    this(BloomFilter.build(xs, numElems, fpp, param), param)

  override def mightContain(value: T): Boolean = internal.mightContain(value)
  override val elementCount: Long = internal.approximateElementCount()
  override val expectedFpp: Double = internal.expectedFpp()

  override def writeTo(out: OutputStream): Unit = {
    ApproxFilterUtils.writeObject(out, funnel)
    internal.writeTo(out)
  }
}

private object BloomFilter {
  def build[T](xs: Iterable[T], numElems: Long, fpp: Double, funnel: g.Funnel[T]): g.BloomFilter[T] = {
    val bf = g.BloomFilter.create(funnel, numElems, fpp)
    xs.foreach(bf.put)
    bf
  }
}

////////////////////////////////////////////////////////////
// SetFilter
////////////////////////////////////////////////////////////

class SetFilter[T](val set: Set[T], val coder: Coder[T]) extends ApproxFilter[T] {
  override type Param = Coder[T]

  def this(in: InputStream, coder: Coder[T]) =
    this(SetFilter.readSet(in, coder), coder)

  def this(in: InputStream) = this(in, ApproxFilterUtils.readObject[Coder[T]](in))

  def this(xs: Iterable[T], numElems: Long, fpp: Double, param: Coder[T]) =
    this(xs.toSet, param)

  override def mightContain(value: T): Boolean = set(value)
  override val elementCount: Long = set.size
  override val expectedFpp: Double = 1.0

  override def writeTo(out: OutputStream): Unit = {
    ApproxFilterUtils.writeObject(out, coder)
    SetFilter.writeSet(out, coder, set)
  }
}

private object SetFilter {
  private val intCoder = VarIntCoder.of()

  def readSet[T](in: InputStream, coder: Coder[T]): Set[T] = {
    val size = intCoder.decode(in)
    var i = 0
    val builder = Set.newBuilder[T]
    while (i < size) {
      builder += coder.decode(in)
      i += 1
    }
    builder.result()
  }

  def writeSet[T](out: OutputStream, coder: Coder[T], set: Set[T]): Unit = {
    intCoder.encode(set.size, out)
    set.foreach(coder.encode(_, out))
  }
}

////////////////////////////////////////////////////////////

object Test {
  import magnolify.guava.auto._

  def test[F[Int] <: ApproxFilter[Int]](implicit param: F[Int]#Param,
                                        ct: ClassTag[F[Int]]): Unit = {
    val xs = (1 to 100)
    val af = ApproxFilter.build[Int, F](xs, 1000, 0.01)
    require(xs.forall(af.mightContain))

    val afCoder = new ApproxFilterCoder[Int, F]()
    val copy = CoderUtils.clone(afCoder, af)

    require(xs.forall(copy.mightContain))
  }

  def main(args: Array[String]): Unit = {
    implicit val intFunnel = g.Funnels.integerFunnel().asInstanceOf[g.Funnel[Int]]
    implicit val intCoder = VarIntCoder.of().asInstanceOf[Coder[Int]]

    test[BloomFilter]
    test[SetFilter]
  }
}
