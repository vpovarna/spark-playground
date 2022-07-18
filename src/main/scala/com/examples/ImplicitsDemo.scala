package com.examples

import scala.language.implicitConversions

object ImplicitsDemo extends App {

  // Auto-injected by the compiler
  def methodWithImplicitArgument(implicit x: Int) = x + 37
  implicit val implicitInt: Int = 63

  println(methodWithImplicitArgument) // will print 100

  // Implicit methods

  case class Person(name: String) {
    def greet: Unit = println(s"Hello, $name")
  }

  implicit def fromStringToPerson(name: String): Person = Person(name)

  "Bob".greet

  // Implicit class
  implicit class Dog(name: String) {
    def bark: Unit = println("Bark!")
  }

  "Lassie".bark // automatically convert the string into Dog and call the bark method

  /**
   * The compiler looks for implicits into:
   *   - Local scope
   *   - Imported scope
   *   - companion object of the types involved in the method call
   */
}
