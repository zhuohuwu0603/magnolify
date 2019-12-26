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

import cats.Hash
import magnolia._
import magnolify.shims.MurmurHash3Compat

import scala.language.experimental.macros
import scala.util.hashing.MurmurHash3

object HashDerivation {
  type Typeclass[T] = Hash[T]

  def combine[T](caseClass: CaseClass[Typeclass, T]): Typeclass[T] = new Hash[T] {
    override def hash(x: T): Int =
      if (caseClass.parameters.isEmpty) {
        caseClass.typeName.short.hashCode
      } else {
        val seed = MurmurHash3Compat.seed(caseClass.typeName.short.hashCode)
        val h = caseClass.parameters.foldLeft(seed) { (h, p) =>
          MurmurHash3.mix(h, p.typeclass.hash(p.dereference(x)))
        }
        MurmurHash3.finalizeHash(h, caseClass.parameters.size)
      }

    private val eqvImpl = EqMethods.combine(caseClass)

    override def eqv(x: T, y: T): Boolean = eqvImpl(x, y)
  }

  def dispatch[T](sealedTrait: SealedTrait[Typeclass, T]): Typeclass[T] = new Hash[T] {
    override def hash(x: T): Int = sealedTrait.dispatch(x) { sub =>
      sub.typeclass.hash(sub.cast(x))
    }

    private val eqvImpl = EqMethods.dispatch(sealedTrait)

    override def eqv(x: T, y: T): Boolean = eqvImpl(x, y)
  }

  implicit def apply[T]: Typeclass[T] = macro Magnolia.gen[T]
}
