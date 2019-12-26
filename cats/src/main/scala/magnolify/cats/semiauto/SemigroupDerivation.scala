/*
 * Copyright 2019 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package magnolify.cats.semiauto

import cats.Semigroup
import magnolia._

import scala.annotation.implicitNotFound
import scala.language.experimental.macros

object SemigroupDerivation {
  type Typeclass[T] = Semigroup[T]

  def combine[T](caseClass: CaseClass[Typeclass, T]): Typeclass[T] =
    if (SemigroupMethods.shouldCombineAllOption(caseClass)) {
      new Semigroup[T] {
        private val combineImpl = SemigroupMethods.combine(caseClass)
        override def combine(x: T, y: T): T = combineImpl(x, y)

        private val combineAllOptionImpl = SemigroupMethods.combineAllOption(caseClass)
        override def combineAllOption(as: IterableOnce[T]): Option[T] = combineAllOptionImpl(as)
      }
    } else {
      Semigroup.instance(SemigroupMethods.combine(caseClass))
    }

  @implicitNotFound("Cannot derive Semigroup for sealed trait")
  private sealed trait Dispatchable[T]
  def dispatch[T: Dispatchable](sealedTrait: SealedTrait[Typeclass, T]): Typeclass[T] = ???

  implicit def apply[T]: Typeclass[T] = macro Magnolia.gen[T]
}

private object SemigroupMethods {
  def combine[T, Typeclass[T] <: Semigroup[T]](caseClass: CaseClass[Typeclass, T]): (T, T) => T =
    (x, y) =>
      caseClass.construct { p =>
        p.typeclass.combine(p.dereference(x), p.dereference(y))
      }

  def shouldCombineAllOption[T, Typeclass[T] <: Semigroup[T]](
    caseClass: CaseClass[Typeclass, T]): Boolean =
      caseClass.parameters.forall { p =>
        val impl = p.typeclass.getClass.getMethod("combineAllOption", classOf[IterableOnce[T]])
        val base = classOf[Semigroup[T]].getClass.getMethod("combineAllOption", classOf[IterableOnce[T]])
        impl != base
      }

  def combineAllOption[T, Typeclass[T] <: Semigroup[T]](
    caseClass: CaseClass[Typeclass, T]): IterableOnce[T] => Option[T] = (xs: IterableOnce[T]) => {
    val buffer = Array.fill[List[Any]](caseClass.parameters.length)(Nil)
    xs.foreach { x =>
      var i = 0
      caseClass.parameters.iterator.foreach { p =>
        buffer(i) = p.dereference(x) :: buffer(i)
        i += 1
      }
    }

    if (buffer.isEmpty || buffer.head.isEmpty) {
      None
    } else {
      val result = new Array[Any](caseClass.parameters.length)
      var i = 0
      caseClass.parameters.iterator.foreach { p =>
        result(i) = p.typeclass.combineAllOption(buffer(i).asInstanceOf[List[p.PType]]).get
        i += 1
      }
      Some(caseClass.rawConstruct(result))
    }
  }
}
