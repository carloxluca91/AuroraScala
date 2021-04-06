package it.luca.aurora.core

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import scala.reflect.runtime.universe.{TypeTag, typeOf}

package object utils {

  def className[T](implicit typeTag: TypeTag[T]): String = typeOf[T].typeSymbol.name.toString

  def now(): Timestamp = new Timestamp(System.currentTimeMillis())

  def toDate(timestamp: Timestamp): Date = new Date(timestamp.getTime)

  def toDate(timestamp: Timestamp, pattern: String): String = new SimpleDateFormat(pattern).format(toDate(timestamp))
}
