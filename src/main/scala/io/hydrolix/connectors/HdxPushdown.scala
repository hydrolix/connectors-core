/*
 * Copyright (c) 2023 Hydrolix Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.hydrolix.connectors

import java.time.Instant
import scala.math.Ordered.orderingToOrdered

import com.google.common.{collect => gc}
import org.slf4j.LoggerFactory

import io.hydrolix.connectors.FieldCompareLiteral.{AnyField, SpecificField}
import io.hydrolix.connectors.expr._
import io.hydrolix.connectors.types._

//noinspection ScalaWeakerAccess,ScalaUnusedSymbol
object HdxPushdown {
  private val log = LoggerFactory.getLogger(getClass)
  import ComparisonOp._

  /**
   * Note, [[Comparison.unapply]] assumes `shardOps` is a subset of `timeOps`; fix that if this assumption
   * is ever falsified!
   */
  private val timeOps = Set(LT, LE, GT, GE, EQ, NE)
  private val shardOps = Set(EQ, NE)
  private val hdxOps = Map(
    LT -> LT.getSymbol,
    LE -> LE.getSymbol,
    GT -> GT.getSymbol,
    GE -> GE.getSymbol,
    EQ -> EQ.getSymbol,
    NE -> "!="
  )

  private val hdxSimpleTypes: Set[ValueType] = Set(
    BooleanType,
    StringType,
    Int8Type,
    Int16Type,
    Int32Type,
    Int64Type,
    UInt8Type,
    UInt32Type,
    UInt64Type,
    Float32Type,
    Float64Type,
  )

  /**
   * Tests whether a given predicate should be pushable for the given timestamp and shard key field names. Note that we
   * have no concept of a unique key, so we can never return 1 here. However, `prunePartition` should be able to
   * authoritatively prune particular partitions, since it will have their specific min/max timestamps and shard keys.
   *
   * Note that at this never returns `1` at present, because the lower-level HDXReader only does block-level filtering
   * at best; every block of 8k rows that ''contains at least one'' matching row still needs to be scanned.
   *
   * You might be tempted to opine that `foo != 1234` could be pushed down as a type-1 predicate, but when there are no
   * matching rows, HDXReader will return an empty result set anyway, so whatever benefit we might get will be very
   * limited--presumably Spark, Trino, etc. already process empty results very quickly. ;)
   *
   * @param primaryField   name of the timestamp ("primary key") field for this table
   * @param mShardKeyField name of the shard key field for this table
   * @param predicate      predicate to test for pushability
   * @param cols           the Hydrolix column metadata
   * @return (see [[https://spark.apache.org/docs/3.3.2/api/java/org/apache/spark/sql/connector/read/SupportsPushDownV2Filters.html SupportsPushDownV2Filters]])
   *          - 1 if this predicate doesn't need to be evaluated again after scanning
   *          - 2 if this predicate still needs to be evaluated again after scanning
   *          - 3 if this predicate is not pushable
   */
  def pushable(primaryField: String,
             mShardKeyField: Option[String],
                  predicate: Expr[Boolean],
                       cols: Map[String, HdxColumnInfo])
                           : Int =
  {
    val primaryCompare = FieldCompareLiteral[Instant](Some(SpecificField(primaryField)), TimestampType.Millis)
    val shardKeyCompare = FieldCompareLiteral[String](mShardKeyField.map(SpecificField.apply), StringType)
    val anyStringCompare = FieldCompareLiteral[String](Some(AnyField), StringType)

    predicate match {
      case primaryCompare(_, op, _) if timeOps.contains(op) =>
        // Comparison between the primary key field and a timestamp literal:
        // 2 because FilterInterpreter doesn't look at the primary key field
        2

      case In(GetField(`primaryField`, TimestampType(_)), ArrayLiteral(_, _, _)) =>
        // primaryField IN (timestamp literals):
        // 2 because FilterInterpreter doesn't look at the primary key field
        2

      case shardKeyCompare(_, op, _) if shardOps.contains(op) =>
        // shardKeyField == string or shardKeyField <> string
        // Note: this is a 2 even when shardKeyField == string because in the rare case of a hash collision we still
        // need to compare the raw strings.
        2

      case In(GetField(field, StringType), ArrayLiteral(_, _, _)) if mShardKeyField.contains(field) =>
        // shardKeyField IN (string literals)
        2

      case anyStringCompare(f, op, _) if hdxOps.contains(op) =>
        // field:string op 'literal'
        val hcol = cols.getOrElse(f, sys.error(s"No HdxColumnInfo for $f"))
        if (hcol.indexed == 2) {
          // This field is indexed in all partitions, but we can't return 1 because `turbine_cmd` only does block-level
          // filtering, i.e. it will return a block of 8k rows when that block contains _any_ matching value(s).
          2
        } else {
          2
        }
      case Comparison(GetField(_, _), op, Literal(_, typ)) if hdxOps.contains(op) && hdxSimpleTypes.contains(typ) =>
        // field: !string op literal
        // TODO this currently disqualifies non-string comparisons due to an implementation restriction in turbine_cmd
        log.info("Implementation restriction: simple comparisons against non-string fields can't be pushed down yet")
        3
      case Not(expr) =>
        // child is pushable
        pushable(primaryField, mShardKeyField, expr, cols)
      case And(left, right) =>
        // max of childrens' pushability
        List(
          pushable(primaryField, mShardKeyField, left, cols),
          pushable(primaryField, mShardKeyField, right, cols)
        ).max
      case Or(left, right) =>
        // max of childrens' pushability
        List(
          pushable(primaryField, mShardKeyField, left, cols),
          pushable(primaryField, mShardKeyField, right, cols)
        ).max
      case _: Literal[_] => 1 // TODO is this right?
      case _ =>
        // Something else; it should be logged by the caller as non-pushable
        3
    }
  }

  def prunablePredicate(primaryField: String,
                      mShardKeyField: Option[String],
                           predicate: Expr[Boolean],
                       stillValidCnf: Boolean)
                                    : Option[Expr[Boolean]] =
  {
    val primaryCompare = FieldCompareLiteral[Instant](Some(SpecificField(primaryField)), TimestampType.Millis)
    val shardKeyCompare = FieldCompareLiteral[String](mShardKeyField.map(SpecificField.apply), StringType)

    predicate match {
      // Top-level simple comparisons against timestamp are always prunable regardless of CNF position
      case cmp @ primaryCompare(_, op, _) if timeOps.contains(op) => Some(cmp)

      // Top-level simple comparisons against shard key are always prunable regardless of CNF position
      case cmp @ shardKeyCompare(_, op, _) if shardOps.contains(op) => Some(cmp)

      case and @ And(left, right) =>
        val mlp = prunablePredicate(primaryField, mShardKeyField, left, stillValidCnf)
        val mrp = prunablePredicate(primaryField, mShardKeyField, right, stillValidCnf)

        // TODO explain this
        (mlp, mrp) match {
          case (Some(lp), Some(rp))              => Some(and) // AND is always prunable if both legs are prunable
          case (Some(lp), None) if stillValidCnf => Some(lp)  // Half-prunable AND is prunable when in a CNF position
          case (None, Some(rp)) if stillValidCnf => Some(rp)  // Half-prunable AND is prunable when in a CNF position
          case _ => None
        }

      // TODO explain this
      case or @ Or(left, right) =>
        val mlp = prunablePredicate(primaryField, mShardKeyField, left, stillValidCnf)
        val mrp = prunablePredicate(primaryField, mShardKeyField, right, stillValidCnf)

        // Prunable if both children are prunable
        if (mlp.isDefined && mrp.isDefined) {
          Some(or)
        } else {
          None
        }

      case not @ Not(child) =>
        // Prunable if child is prunable; child will no longer be in CNF position
        prunablePredicate(primaryField, mShardKeyField, child, stillValidCnf = false).map(Not.apply)

      case in @ In(GetField(`primaryField`, TimestampType(_)), ArrayLiteral(_, ArrayType(TimestampType(_), _), _)) =>
        // TODO IN/EQ on timestamps are unlikely
        if (stillValidCnf) Some(in) else None

      case in @ In(GetField(fld, StringType), ArrayLiteral(_, ArrayType(StringType, _), _)) if mShardKeyField.contains(fld) =>
        if (stillValidCnf) Some(in) else None

      case _ => None
    }
  }

  /**
   * Evaluate the min/max timestamps and shard key of a single partition against a single predicate
   * to determine if the partition CAN be pruned, i.e. doesn't need to be scanned.
   *
   * TODO partition min/max are only at second resolution, figure out whether anything needs to change here!
   *
   * @param primaryField      the name of the primary timestamp field for this partition
   * @param mShardKeyField    the name of the shard key field for this partition
   * @param predicate         the predicate to evaluate
   * @param partitionMin      the minimum timestamp for data in this partition
   * @param partitionMax      the maximum timestamp for data in this partition
   * @param partitionShardKey the shard key of this partition. Not optional because `42bc986dc5eec4d3` (`wyhash("")`)
   *                          appears when there's no shard key.
   * @return `true` if this partition should be pruned (i.e. NOT scanned) according to `predicate`, or
   *         `false` if it MUST be scanned
   */
  def includePartition(primaryField: String,
                     mShardKeyField: Option[String],
                          predicate: Expr[Boolean],
                       partitionMin: Instant,
                       partitionMax: Instant,
                  partitionShardKey: String)
                                   : Boolean =
  {
    val primaryCompare = FieldCompareLiteral[Instant](Some(SpecificField(primaryField)), TimestampType.Millis)
    val shardKeyCompare = FieldCompareLiteral[String](mShardKeyField.map(SpecificField.apply), StringType)
    val partitionRange = gc.Range.closed(partitionMin, partitionMax)

    predicate match {
      // Top-level simple comparisons against timestamp are always CNF-valid
      case primaryCompare(_, EQ, timestamp) =>
        // [pk = time] => scan if time IN partition bounds
        timestamp >= partitionMin && timestamp <= partitionMax

      case primaryCompare(_, NE, timestamp) =>
        // pk != time => scan if time NOT IN partition bounds
        timestamp < partitionMin || timestamp > partitionMax

      case primaryCompare(_, GE, timestamp) =>
        // pk >= time => scan if time >= partitionMin
        timestamp <= partitionMax

      case primaryCompare(_, GT, timestamp) =>
        timestamp < partitionMin

      case primaryCompare(_, LE, timestamp) =>
        timestamp >= partitionMax

      case primaryCompare(_, LT, timestamp) =>
        timestamp > partitionMax

      // Top-level simple comparisons against shard key are always CNF-valid
      case shardKeyCompare(_, EQ, shardKey) => partitionShardKey == WyHash(shardKey)
      case shardKeyCompare(_, NE, shardKey) => partitionShardKey != WyHash(shardKey)

      case In(f@GetField(`primaryField`, tt @ TimestampType(_)), ArrayLiteral(ts, ArrayType(TimestampType(_), _), _)) =>
        // [`timeField` IN (<timestampLiterals>)]
        val timeLiterals = ts.asInstanceOf[Seq[Instant]]

        // Scan if any of the timestamp literals is between partitionMin & partitionMax
        timeLiterals.exists { inst =>
          inst >= partitionMin && inst <= partitionMax
        }

      case In(gf@GetField(f, StringType), ArrayLiteral(ss, ArrayType(StringType, _), _)) if mShardKeyField.contains(f) =>
        // [`shardKeyField` IN (<stringLiterals>)]
        val hashes = ss.asInstanceOf[Seq[String]].map(WyHash(_)).toSet

        hashes.contains(partitionShardKey)

      case And(primaryCompare(_, op1, time1), primaryCompare(_, op2, time2)) =>
        val timeRange = (op1, op2) match {
          case (GE, LE) => Some(gc.Range.closed(time1, time2))
          case (GT, LT) => Some(gc.Range.open(time1, time2))
          case (GE, LT) => Some(gc.Range.closedOpen(time1, time2))
          case (GT, LE) => Some(gc.Range.openClosed(time1, time2))
          case (LE, GE) => Some(gc.Range.closed(time2, time1))
          case (LT, GT) => Some(gc.Range.open(time2, time1))
          case (LT, GE) => Some(gc.Range.closedOpen(time2, time1))
          case (LE, GT) => Some(gc.Range.openClosed(time2, time1))
          case _ => None
        }

        timeRange.exists(_.isConnected(partitionRange))

      case Or(left, right) =>
        val leftScan = includePartition(primaryField, mShardKeyField, left, partitionMin, partitionMax, partitionShardKey)
        val rightScan = includePartition(primaryField, mShardKeyField, right, partitionMin, partitionMax, partitionShardKey)

        leftScan || rightScan // TODO ???

      case Not(child) =>
        !includePartition(primaryField, mShardKeyField, child, partitionMin, partitionMax, partitionShardKey)

      // TODO add more valid cases!
      // TODO add more valid cases!
      // TODO add more valid cases!
      // TODO add more valid cases!

      case _ =>
        true
    }
  }

  private def quote(s: String): String = {
    val in = if (s.contains("'")) s.replace("'", "''") else s
    s"'$in'"
  }

  /**
   * Try to render a predicate into something that will hopefully be acceptable to FilterExprParser
   *
   * @return Some(s) if the predicate was rendered successfully; None if not
   */
  def renderHdxFilterExpr(expr: Expr[Boolean],
               primaryKeyField: String,
                          cols: Map[String, HdxColumnInfo])
                              : Option[String] =
  {
    expr match {
      case Comparison(GetField(field, typ), op, Literal(value, _)) if timeOps.contains(op) && hdxSimpleTypes.contains(typ) =>
        val hcol = cols.getOrElse(field, sys.error(s"No HdxColumnInfo for $field"))
        val hdxOp = hdxOps.getOrElse(op, sys.error(s"No hydrolix operator for $op"))

        if (hcol.indexed == 2) {
          if (hcol.hdxType.`type` == HdxValueType.String) {
            // This field is indexed in all partitions, make it so
            Some(s""""$field" $hdxOp ${quote(value.toString)}""")
          } else {
            log.warn(s"TODO $field isn't a string, we can't pushdown (yet?)")
            None
          }
        } else {
          None
        }

      case Comparison(GetField(field, TimestampType(_)), op, TimestampLiteral(lit)) if timeOps.contains(op) =>
        if (field == primaryKeyField) {
          // FilterInterpreter specifically doesn't try to use the primary key field
          None
        } else {
          // Note, at this point we don't care if it's the partition min/max timestamp; any timestamp will do
          val hdxOp = hdxOps.getOrElse(op, sys.error(s"No hydrolix operator for $op"))
          val hcol = cols.getOrElse(field, sys.error(s"No HdxColumnInfo for $field"))

          val long = if (hcol.hdxType.`type` == HdxValueType.DateTime64) lit.toEpochMilli else lit.getEpochSecond

          if (hcol.indexed == 2) {
            Some(s""""$field" $hdxOp '$long'""")
          } else {
            None
          }
        }

      case And(left, right) =>
        for {
          l <- renderHdxFilterExpr(left, primaryKeyField, cols)
          r <- renderHdxFilterExpr(right, primaryKeyField, cols)
        } yield {
          s"($l AND $r)"
        }

      case Or(left, right) =>
        for {
          l <- renderHdxFilterExpr(left, primaryKeyField, cols)
          r <- renderHdxFilterExpr(right, primaryKeyField, cols)
        } yield {
          s"($l OR $r)"
        }

      case Not(Equal(l, r)) =>
        // Spark turns `foo <> bar` into `NOT (foo = bar)`, put it back so FilterInterpreter likes it better
        // ... the Comparison test above will catch the NotEqual
        renderHdxFilterExpr(NotEqual(l, r), primaryKeyField, cols)

      case Not(other) =>
        renderHdxFilterExpr(other, primaryKeyField, cols)
          .map(res => s"NOT ($res)")

      case _ =>
        None
    }
  }

  /**
   * Given an `aggregation` expression, if it's pushable, return a StructField with a suggested name for the
   * expression, along with its value type.
   */
  def pushableAgg[T : Numeric](aggregation: AggregateFun[T], primaryKeyField: String): Option[StructField] = {
    aggregation match {
      case CountStar => Some(StructField("COUNT(*)", Int64Type))
      case Min(GetField(`primaryKeyField`, ttype @ TimestampType(_))) => Some(StructField(s"MIN($primaryKeyField)", ttype, nullable = true))
      case Max(GetField(`primaryKeyField`, ttype @ TimestampType(_))) => Some(StructField(s"MAX($primaryKeyField)", ttype, nullable = true))
      case _ => None
    }
  }

  def planPartitions(info: HdxConnectionInfo,
                     jdbc: HdxJdbcSession,
                    table: HdxTable,
                     cols: StructType,
              pushedPreds: List[Expr[Boolean]])
                         : List[HdxPartitionScanPlan] =
  {
    val hdxCols = table.hdxCols
      .filter {
        case (name, _) => cols.fields.exists(_.name == name)
      }

    // Get min & max timestamps for partition pruning -- the +1/-1 for GT/LT takes care of millisecond/second
    // granularity mismatches between query bounds and catalog bounds
    val minTimestamp = pushedPreds.collectFirst {
      case GreaterEqual(GetField(table.primaryKeyField, TimestampType(_)), TimestampLiteral(inst)) => inst
      case GreaterThan(GetField(table.primaryKeyField, TimestampType(_)), TimestampLiteral(inst)) => inst.plusMillis(1)
    }

    val maxTimestamp = pushedPreds.collectFirst {
      case LessEqual(GetField(table.primaryKeyField, TimestampType(_)), TimestampLiteral(inst)) => inst
      case LessThan(GetField(table.primaryKeyField, TimestampType(_)), TimestampLiteral(inst)) => inst.minusMillis(1)
    }

    // TODO consider other kinds of timestamp predicates like EQ, NE, IN(...)
    val parts = jdbc.collectPartitions(table.ident.head, table.ident(1), minTimestamp, maxTimestamp)

    parts.zipWithIndex.flatMap { case (dbPartition, i) =>
      doPlan(table, info.partitionPrefix, cols, pushedPreds, hdxCols, dbPartition, i)
    }
  }

  def doPlan(table: HdxTable,
   partitionPrefix: Option[String],
              cols: StructType,
       pushedPreds: List[Expr[Boolean]],
           hdxCols: Map[String, HdxColumnInfo],
       dbPartition: HdxDbPartition,
                 i: Int)
                  : Option[HdxPartitionScanPlan] =
  {
    val min = dbPartition.minTimestamp
    val max = dbPartition.maxTimestamp
    val sk = dbPartition.shardKey
    val pkType = cols.byName(table.primaryKeyField).`type`.asInstanceOf[TimestampType]

    // pushedPreds is implicitly an AND here
    val pushResults = pushedPreds.map(includePartition(table.primaryKeyField, table.shardKeyField, _, min, max, sk))
    if (pushedPreds.nonEmpty && !pushResults.contains(true)) {
      // At least one pushed predicate said we could skip this partition
      log.debug(s"Skipping partition ${i + 1}: $dbPartition")
      None
    } else {
      log.debug(s"Scanning partition ${i + 1}: $dbPartition. Per-predicate results: ${pushedPreds.zip(pushResults).mkString("\n  ", "\n  ", "\n")}")
      // Either nothing was pushed, or at least one predicate didn't want to prune this partition; scan it

      val (path, storageId) = dbPartition.storageId match {
        case Some(id) if dbPartition.partition.startsWith(id.toString + "/") =>
          log.debug(s"storage_id = ${dbPartition.storageId}, partition = ${dbPartition.partition}")
          // Remove storage ID prefix if present; it's not there physically
          ("db/hdx/" + dbPartition.partition.drop(id.toString.length + 1), id)
        case _ =>
          // No storage ID from catalog or not present in the path, assume the prefix is there
          val defaults = table.storages.filter(_._2.isDefault)
          if (defaults.isEmpty) {
            if (table.storages.isEmpty) {
              // Note: this won't be empty if the storage settings override is used
              sys.error(s"No storage found for partition ${dbPartition.partition}")
            } else {
              val firstId = table.storages.head._1
              log.warn(s"Partition ${dbPartition.partition} had no `storage_id`, and cluster has no default storage; using the first (#$firstId)")
              (partitionPrefix.getOrElse("") + dbPartition.partition, firstId)
            }
          } else {
            val firstDefault = defaults.head._1
            if (defaults.size > 1) {
              log.warn(s"Partition ${dbPartition.partition} had no `storage_id`, and cluster has multiple default storages; using the first (#$firstDefault)")
            }
            (partitionPrefix.getOrElse("") + dbPartition.partition, firstDefault)
          }
      }

      Some(
        HdxPartitionScanPlan(
          table.ident.head,
          table.ident(1),
          table.primaryKeyField,
          storageId,
          path,
          cols,
          pushedPreds,
          hdxCols
        )
      )
    }
  }
}
