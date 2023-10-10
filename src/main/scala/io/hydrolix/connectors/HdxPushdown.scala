package io.hydrolix.connectors

import java.time.Instant

import org.slf4j.LoggerFactory

import io.hydrolix.connectors.types._
import io.hydrolix.connectors.expr._

/**
 * TODO:
 *  - see if multi-part names will ever show up (e.g. in a join?); that would break [[GetField]] but hopefully only
 *    in a way that would allow fewer pushdown opportunities rather than incorrect results.
 */
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
   * @param primaryKeyField name of the timestamp ("primary key") field for this table
   * @param mShardKeyField  name of the shard key field for this table
   * @param predicate       predicate to test for pushability
   * @param cols            the Hydrolix column metadata
   * @return (see [[https://spark.apache.org/docs/3.3.2/api/java/org/apache/spark/sql/connector/read/SupportsPushDownV2Filters.html SupportsPushDownV2Filters]])
   *          - 1 if this predicate doesn't need to be evaluated again after scanning
   *          - 2 if this predicate still needs to be evaluated again after scanning
   *          - 3 if this predicate is not pushable
   */
  def pushable(primaryKeyField: String,
                mShardKeyField: Option[String],
                     predicate: Expr[Boolean],
                          cols: Map[String, HdxColumnInfo])
                              : Int =
  {
    predicate match {
      case Comparison(GetField(`primaryKeyField`, TimestampType(_)), op, TimestampLiteral(_)) if timeOps.contains(op) =>
        // Comparison between the primary key field and a timestamp literal:
        // 2 because FilterInterpreter doesn't look at the primary key field
        2
      case In(GetField(`primaryKeyField`, TimestampType(_)), ArrayLiteral(_, TimestampType(_), _)) =>
        // primaryKeyField IN (timestamp literals):
        // 2 because FilterInterpreter doesn't look at the primary key field
        2
      case Comparison(GetField(field, StringType), op, StringLiteral(_)) if mShardKeyField.contains(field) && shardOps.contains(op) =>
        // shardKeyField == string or shardKeyField <> string
        // Note: this is a 2 even when shardKeyField == string because in the rare case of a hash collision we still
        // need to compare the raw strings.
        2
      case In(GetField(field, StringType), ArrayLiteral(_, StringType, _)) if mShardKeyField.contains(field) =>
        // shardKeyField IN (string literals)
        2
      case Comparison(GetField(f, StringType), op, StringLiteral(_)) if hdxOps.contains(op) =>
        // field:string op 'literal'
        val hcol = cols.getOrElse(f, sys.error(s"No HdxColumnInfo for $f"))
        if (hcol.indexed == 2) {
          // This field is indexed in all partitions, but we can't return 1 because `turbine_cmd` only does block-level
          // filtering, i.e. it will return a block of 8k rows when that block contains _any_ matching value(s).
          2
        } else {
          2
        }
      case Comparison(GetField(f, _), op, Literal(_, typ)) if hdxOps.contains(op) && hdxSimpleTypes.contains(typ) =>
        // field: !string op literal
        // TODO this currently disqualifies non-string comparisons due to an implementation restriction in turbine_cmd
        log.info("Implementation restriction: simple comparisons against non-string fields can't be pushed down yet")
        3
      case Not(expr) =>
        // child is pushable
        pushable(primaryKeyField, mShardKeyField, expr, cols)
      case And(children) =>
        // max of childrens' pushability
        val kps = children.map { child =>
          pushable(primaryKeyField, mShardKeyField, child, cols)
        }
        kps.max
      case Or(children) =>
        // max of childrens' pushability
        val kps = children.map { child =>
          pushable(primaryKeyField, mShardKeyField, child, cols)
        }
        kps.max
      case _ =>
        // Something else; it should be logged by the caller as non-pushable
        3
    }
  }

  /**
   * Evaluate the min/max timestamps and shard key of a single partition against a single predicate
   * to determine if the partition CAN be pruned, i.e. doesn't need to be scanned.
   *
   * TODO partition min/max are only at second resolution, figure out whether anything needs to change here!
   *
   * @param primaryKeyField   the name of the timestamp field for this partition
   * @param mShardKeyField    the name of the shard key field for this partition
   * @param predicate         the predicate to evaluate
   * @param partitionMin      the minimum timestamp for data in this partition
   * @param partitionMax      the maximum timestamp for data in this partition
   * @param partitionShardKey the shard key of this partition. Not optional because `42bc986dc5eec4d3` (`wyhash("")`)
   *                          appears when there's no shard key.
   * @return `true` if this partition should be pruned (i.e. NOT scanned) according to `predicate`, or
   *         `false` if it MUST be scanned
   */
  def prunePartition(primaryKeyField: String,
                      mShardKeyField: Option[String],
                           predicate: Expr[Boolean],
                        partitionMin: Instant,
                        partitionMax: Instant,
                   partitionShardKey: String)
                                    : Boolean =
  {
    predicate match {
      case Comparison(GetField(`primaryKeyField`, TimestampType(_)), op, TimestampLiteral(timestamp)) if timeOps.contains(op) =>
        // [`timestampField` <op> <timestampLiteral>], where op ∈ `timeOps`

        op match {
          case EQ => // prune if timestamp IS OUTSIDE partition min/max
            timestamp.compareTo(partitionMin) < 0 || timestamp.compareTo(partitionMax) > 0

          case NE => // prune if timestamp IS INSIDE partition min/max
            timestamp.compareTo(partitionMin) >= 0 && timestamp.compareTo(partitionMax) <= 0

          case GE | GT => // prune if timestamp IS AFTER partition max
            // TODO seriously consider whether > and >= should be treated the same given mismatched time grain
            timestamp.compareTo(partitionMax) >= 0

          case LE | LT => // prune if timestamp IS BEFORE partition min
            // TODO seriously consider whether < and <= should be treated the same given mismatched time grain
            timestamp.compareTo(partitionMin) <= 0

          case _ =>
            // Shouldn't happen because the pattern guard already checked in timeOps
            sys.error(s"Unsupported comparison operator for timestamp: $op")
        }

      case Comparison(GetField(field, StringType), op, StringLiteral(shardKey)) if mShardKeyField.contains(field) && shardOps.contains(op) =>
        // [`shardKeyField` <op> <stringLiteral>], where op ∈ `shardOps`
        // TODO do we need to care about 42bc986dc5eec4d3 here?
        val hashed = WyHash(shardKey)

        op match {
          case EQ =>
            // Shard key must match partition's; prune if NOT EQUAL
            partitionShardKey != hashed
          case NE =>
            // Shard key must NOT match partition's; prune if EQUAL
            partitionShardKey == hashed
          case _ =>
            // Shouldn't happen because the pattern guard already checked in shardOps
            sys.error(s"Unsupported comparison operator for shard key: $op")
        }

      case In(f@GetField(`primaryKeyField`, TimestampType(_)), ArrayLiteral(ts, TimestampType(_), _)) =>
        // [`timeField` IN (<timestampLiterals>)]
        val comparisons = ts.map { t =>
          Equal(f, TimestampLiteral(t.asInstanceOf[Instant]))
        }
        val results = comparisons.map(prunePartition(primaryKeyField, mShardKeyField, _, partitionMin, partitionMax, partitionShardKey))
        // This partition can be pruned if _every_ literal IS NOT within this partition's time bounds
        !results.contains(false)

      case In(gf@GetField(f, StringType), ArrayLiteral(ss, StringType, _)) if mShardKeyField.contains(f) =>
        // [`shardKeyField` IN (<stringLiterals>)]
        val comparisons = ss.map(s => Equal(gf, StringLiteral(s.asInstanceOf[String])))
        val results = comparisons.map(prunePartition(primaryKeyField, mShardKeyField, _, partitionMin, partitionMax, partitionShardKey))
        // This partition can be pruned if _every_ literal IS NOT this partition's shard key
        // TODO do we need care about hash collisions here? It might depend on whether op is EQ or NE
        !results.contains(false)

      case And(children) =>
        val kps = children.map { child =>
          prunePartition(primaryKeyField, mShardKeyField, child, partitionMin, partitionMax, partitionShardKey)
        }

        kps.contains(true) // TODO!!

      case Or(children) =>
        val kps = children.map { child =>
          prunePartition(primaryKeyField, mShardKeyField, child, partitionMin, partitionMax, partitionShardKey)
        }

        !kps.contains(false) // TODO!!

      case Not(expr) =>
        val pruneChild = prunePartition(primaryKeyField, mShardKeyField, expr, partitionMin, partitionMax, partitionShardKey)
        !pruneChild // TODO!!

      case _ =>
        false
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

      case And(children) =>
        val results = children.map(kid => renderHdxFilterExpr(kid, primaryKeyField, cols))
        if (results.contains(None)) None else Some(results.flatten.mkString("(", " AND ", ")"))

      case Or(children) =>
        val results = children.map(kid => renderHdxFilterExpr(kid, primaryKeyField, cols))
        if (results.contains(None)) None else Some(results.flatten.mkString("(", " OR ", ")"))

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

    // TODO we have `pushedPreds`, we can make this query a lot more selective (carefully!)
    val parts = jdbc.collectPartitions(table.ident.head, table.ident(1))

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

    // pushedPreds is implicitly an AND here
    val pushResults = pushedPreds.map(prunePartition(table.primaryKeyField, table.shardKeyField, _, min, max, sk))
    if (pushedPreds.nonEmpty && pushResults.contains(true)) {
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
