package io.hydrolix.connectors

import scala.collection.mutable

import org.slf4j.LoggerFactory

import io.hydrolix.connectors.expr._
import io.hydrolix.connectors.types.{ConcreteType, ValueType}

/**
 * Extractor values that match expressions of the form Comparison(GetField(fieldName), op, Literal(value))
 * (or the reverse) and returns (op, value) if it matches
 */
sealed trait FieldCompareLiteral[T] {
  def unapply(expr: Expr[Boolean]): Option[(String, ComparisonOp, T)]
}

object FieldCompareLiteral {
  private val logger = LoggerFactory.getLogger(getClass)

  private val cache = mutable.HashMap[(WhichField, ConcreteType), RealFCL[_]]()

  sealed trait WhichField {
    def matches(name: String): Boolean
  }
  case object AnyField extends WhichField {
    override def matches(name: String): Boolean = true
  }

  // TODO case-insensitive maybe?
  case class SpecificField(expected: String) extends WhichField {
    override def matches(name: String): Boolean = name == expected
  }

  /** An FCL that never matches anything */
  private object NoOpFCL extends FieldCompareLiteral[Nothing] {
    override def unapply(expr: Expr[Boolean]): Option[(String, ComparisonOp, Nothing)] = None
  }

  private final class RealFCL[T] private[FieldCompareLiteral](desiredField: WhichField, typ: ConcreteType) extends FieldCompareLiteral[T] {
    override def unapply(expr: Expr[Boolean]): Option[(String, ComparisonOp, T)] = {
      expr match {
        case Comparison(GetField(field, fieldType), op, Literal(literalValue, literalType)) if desiredField.matches(field) =>
          tryMatch(field, typ, fieldType, op, literalValue, literalType)

        case Comparison(Literal(literalValue, literalType), op, GetField(field, fieldType)) if desiredField.matches(field) =>
          // In case of reverse comparisons!
          tryMatch(field, typ, fieldType, op.negate(), literalValue, literalType)

        case _ => None
      }
    }

    private def tryMatch(fieldName: String, expectedType: ValueType, fieldType: ValueType, op: ComparisonOp, literalValue: Any, literalType: ValueType): Option[(String, ComparisonOp, T)] = {
      if (fieldType == expectedType && literalType == expectedType) {
        // field and literal are same type, just cast the value

        Some(fieldName, op, literalValue.asInstanceOf[T])
      } else {
        val eFieldCast = fieldType.tryCast(expectedType, literalValue)
        val eLiteralCast = literalType.tryCast(expectedType, literalValue)

        (eFieldCast, eLiteralCast) match {
          case (Some(fieldCast), Some(literalCast)) if fieldCast == literalCast =>
            // Both types successfully cast to the same value
            // TODO this might be too restrictive
            Some(fieldName, op, fieldCast.asInstanceOf[T])

          case (Some(fieldCast), Some(literalCast)) =>
            logger.warn(s"Field type $fieldType and literal type $literalType both claim to be able to cast to $expectedType but they got different values (field: $fieldCast; literal: $literalCast)!")
            None
          case (None, Some(_)) =>
            logger.warn(s"Field type $fieldType can't cast to $expectedType")
            None
          case (Some(_), None) =>
            logger.warn(s"Literal type $literalType can't cast to $expectedType")
            None
          case (None, None) =>
            logger.info(s"Neither field type $fieldType nor literal type $literalType can cast to $expectedType")
            None
        }
      }
    }
  }

  /**
   * @param mField optional so callers don't need a conditional FCL; they just get a no-op one when it's None
   */
  def apply[T](mField: Option[WhichField], typ: ConcreteType): FieldCompareLiteral[T] = {
    (mField match {
      case None => NoOpFCL
      case Some(mField) =>
        cache.getOrElse((mField, typ), {
          new RealFCL[T](mField, typ).also { it =>
            cache.update((mField, typ), it)
          }
        })
    }).asInstanceOf[FieldCompareLiteral[T]]
  }
}
