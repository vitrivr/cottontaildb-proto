package org.vitrivr.cottontail.client.language.dql

import org.vitrivr.cottontail.client.language.basics.Direction
import org.vitrivr.cottontail.client.language.basics.Distances
import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.extensions.*
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
@Suppress("UNCHECKED_CAST")
class Query(entity: String? = null): LanguageFeature() {
    /** Internal [CottontailGrpc.Query.Builder]. */
    val builder = CottontailGrpc.QueryMessage.newBuilder()

    init {
        if (entity != null) {
            this.builder.queryBuilder.setFrom(CottontailGrpc.From.newBuilder().setScan(CottontailGrpc.Scan.newBuilder().setEntity(entity.parseEntity())))
        }
    }

    /**
     * Sets the transaction ID for this [Query].
     *
     * @param txId The new transaction ID.
     */
    override fun txId(txId: Long): Query {
        this.builder.metadataBuilder.transactionId = txId
        return this
    }

    /**
     * Sets the query ID for this [Query].
     *
     * @param queryId The new query ID.
     */
    override fun queryId(queryId: String): Query {
        this.builder.metadataBuilder.queryId = queryId
        return this
    }

    /**
     * Adds a SELECT projection to this [Query]. Call this method repeatedly to add multiple projections.

     * Calling this method on a [Query] with a projection other than SELECT, will reset the previous projection.
     *
     * @param column The name of the column to select.
     * @param alias The column alias. This is optional.
     * @return [Query]
     */
    fun select(column: String, alias: String? = null): Query {
        val builder = this.builder.queryBuilder.projectionBuilder
        if (builder.op != CottontailGrpc.Projection.ProjectionOperation.SELECT) {
            builder.clearElements()
            builder.op = CottontailGrpc.Projection.ProjectionOperation.SELECT
        }
        val element = builder.addElementsBuilder()
        element.column = column.parseColumn()
        if (alias != null) {
            element.alias = alias.parseColumn()
        }
        return this
    }

    /**
     * Adds a SELECT DISTINCT projection to this [Query]. Call this method repeatedly to add multiple projections.
     *
     * Calling this method on a [Query] with a projection other than SELECT DISTINCT, will reset the previous projection.
     *
     * @param column The name of the column to select.
     * @param alias The column alias. This is optional.
     * @return [Query]
     */
    fun distinct(column: String, alias: String? = null): Query {
        val builder = this.builder.queryBuilder.projectionBuilder
        if (builder.op != CottontailGrpc.Projection.ProjectionOperation.SELECT_DISTINCT) {
            builder.clearElements()
            builder.op = CottontailGrpc.Projection.ProjectionOperation.SELECT_DISTINCT
        }
        val element = builder.addElementsBuilder()
        element.column = column.parseColumn()
        if (alias != null) {
            element.alias = alias.parseColumn()
        }
        return this
    }

    /**
     * Adds a SELECT COUNT projection to this [Query].
     *
     * Calling this method resets the PROJECTION part of the query.
     *
     * @return [Query]
     */
    fun count(): Query {
        val builder = this.builder.queryBuilder.projectionBuilder
        if (builder.op != CottontailGrpc.Projection.ProjectionOperation.COUNT) {
            builder.clearElements()
            builder.op = CottontailGrpc.Projection.ProjectionOperation.COUNT
        }
        builder.addElements(CottontailGrpc.Projection.ProjectionElement.newBuilder().setColumn("*".parseColumn()))
        return this
    }

    /**
     * Adds a SELECT EXISTS projection to this [Query].
     *
     * Calling this method resets the PROJECTION part of the query.
     *
     * @return [Query]
     */
    fun exists(): Query {
        val builder = this.builder.queryBuilder.projectionBuilder
        if (builder.op != CottontailGrpc.Projection.ProjectionOperation.EXISTS) {
            builder.clearElements()
            builder.op = CottontailGrpc.Projection.ProjectionOperation.EXISTS
        }
        builder.addElements(CottontailGrpc.Projection.ProjectionElement.newBuilder().setColumn("*".parseColumn()))
        return this
    }

    /**
     * Adds a FROM-clause with a SCAN to this [Query]
     *
     * Calling this method resets the FROM part of the query.
     *
     * @param entity The entity to SCAN.
     * @return This [Query]
     */
    fun from(entity: String): Query {
        this.builder.queryBuilder.setFrom(CottontailGrpc.From.newBuilder().setScan(CottontailGrpc.Scan.newBuilder().setEntity(entity.parseEntity())))
        return this
    }

    /**
     * Adds a FROM-clause with a SAMPLE to this [Query]
     *
     * Calling this method resets the FROM part of the query.
     *
     * @param entity The entity to SAMPLE.
     * @param seed The random number generator seed for SAMPLE
     * @return This [Query]
     */
    fun sample(entity: String, seed: Long = System.currentTimeMillis()): Query {
        this.builder.queryBuilder.setFrom(CottontailGrpc.From.newBuilder().setSample(CottontailGrpc.Sample.newBuilder().setEntity(entity.parseEntity()).setSeed(seed)))
        return this
    }

    /**
     * Adds a FROM-clause with a SUB SELECT to this [Query]
     *
     * @param query The [Query] to SUB SELECT from.
     * @return This [Query]
     */
    fun from(query: Query): Query {
        require(query != this) { "SUB-SELECT query cannot specify itself."}
        this.builder.queryBuilder.setFrom(CottontailGrpc.From.newBuilder().setSubSelect(query.builder.queryBuilder))
        return this
    }

    /**
     * Adds a WHERE-clause to this [Query].
     *
     * @param predicate The [Predicate] that specifies the conditions that need to be met.
     * @return This [Query]
     */
    fun where(predicate: Predicate): Query {
        val builder = this.builder.queryBuilder.whereBuilder
        when (predicate) {
            is Atomic -> builder.setAtomic(predicate.toPredicate())
            is And -> builder.setCompound(predicate.toPredicate())
            is Or -> builder.setCompound(predicate.toPredicate())
        }
        return this
    }

    /**
     * Adds a kNN-clause to this [Query] and returns it.
     *
     * Calling this method has side-effects on various aspects of the [Query] (i.e., PROJECTION, ORDER and LIMIT).
     * Most importantly, this function is not idempotent, i.e., calling it multiple times changes the structure of the
     * query, e.g., by adding multiple distance functions. Use [clear] to be on the safe side.
     *
     * @param column The column to apply the kNN to
     * @param k The k parameter in the kNN
     * @param distance The distance metric to use.
     * @param query Query vector to use.
     * @param weight Weight vector to use; this is not supported anymore!
     * @return This [Query]
     */
    @Deprecated("Deprecated since version 0.13.0; use nns() function instead!", replaceWith = ReplaceWith("nns"))
    fun knn(column: String, k: Int, distance: String, query: Any, weight: Any? = null): Query {
        if (weight != null)throw UnsupportedOperationException("Weighted NNS is no longer supported by Cottontail DB. Use weighted distance function with respective arguments instead.")

        /* Calculate distance. */
        distance(column, query, Distances.valueOf(distance.uppercase()),"distance")

        /* Update ORDER BY clause. */
        this.builder.queryBuilder.orderBuilder.addComponents(CottontailGrpc.Order.Component.newBuilder().setColumn(
            CottontailGrpc.ColumnName.newBuilder().setName("distance")
        ).setDirection(Direction.ASC.toGrpc()))

        /* Update LIMIT clause. */
        this.builder.queryBuilder.limit = k.toLong()

        return this
    }

    /**
     * Transforms this [Query] to a Neighbor Search (NS) query and returns it.
     *
     * Calling this method has side-effects on various aspects of the [Query] (i.e., PROJECTION, ORDER and LIMIT).
     * Most importantly, this function is not idempotent, i.e., calling it multiple times changes the structure of the
     * query, e.g., by adding multiple distance functions. Use [clear] to be on the safe side.
     *
     * @param probingColumn The column to perform NNS on. Type must be compatible with choice of distance function.
     * @param query Query value to use. Type must be compatible with choice of distance function.
     * @param distance The distance function to use. Function argument must be compatible with column type.
     * @param name The name of the column that holds the calculated distance value.
     * @return This [Query]
     */
    fun distance(probingColumn: String, query: Any, distance: Distances, name: String): Query {
        /* Parse necessary functions. */
        val distanceColumn = name.parseColumn()
        val distanceFunction = CottontailGrpc.Function.newBuilder()
            .setName(distance.toGrpc())
            .addArguments(CottontailGrpc.Expression.newBuilder().setColumn(probingColumn.parseColumn()))
            .addArguments(CottontailGrpc.Expression.newBuilder().setLiteral(CottontailGrpc.Literal.newBuilder().setVectorData(query.convertAnyToVector())))

        /* Update projection: Add distance column + alias. */
        this.builder.queryBuilder.projectionBuilder.addElements(CottontailGrpc.Projection.ProjectionElement.newBuilder().setAlias(distanceColumn).setFunction(distanceFunction))

        /* Update LIMIT clause. */
        return this
    }

    /**
     * Transforms this [Query] to a Farthest Neighbor Search (FNS) query and returns it.
     *
     * Calling this method has side-effects on various aspects of the [Query] (i.e., PROJECTION, ORDER and LIMIT).
     * Most importantly, this function is not idempotent, i.e., calling it multiple times changes the structure of the
     * query, e.g., by adding multiple distance functions. Use [clear] to be on the safe side.
     *
     * @param probingColumn The column to perform fulltext search on.
     * @param query Query [String] value to use. Type must be compatible with choice of distance function.
     * @param name The name of the column that holds the calculated distance value.
     * @return This [Query]
     */
    fun fulltext(probingColumn: String, query: String, name: String): Query {
        val scoreColumn = name.parseColumn()
        val fulltextFunction = CottontailGrpc.Function.newBuilder()
            .setName(CottontailGrpc.FunctionName.newBuilder().setName("fulltext"))
            .addArguments(CottontailGrpc.Expression.newBuilder().setColumn(probingColumn.parseColumn()))
            .addArguments(CottontailGrpc.Expression.newBuilder().setLiteral(CottontailGrpc.Literal.newBuilder().setStringData(query)))

        /* Update projection: Add distance column + alias. */
        this.builder.queryBuilder.projectionBuilder.addElements(CottontailGrpc.Projection.ProjectionElement.newBuilder().setAlias(scoreColumn).setFunction(fulltextFunction))

        return this
    }

    /**
     * Adds a ORDER BY-clause to this [Query] and returns it
     *
     * @param column The column to order by
     * @param direction The sort [Direction]
     * @return This [Query]
     */
    fun order(column: String, direction: Direction): Query {
        val builder = this.builder.queryBuilder.orderBuilder
        val cBuilder = builder.addComponentsBuilder()
        cBuilder.column = column.parseColumn()
        cBuilder.direction = direction.toGrpc()
        return this
    }

    /**
     * Adds a SKIP-clause in the Cottontail DB query language.
     *
     * @param skip The number of results to skip.
     * @return This [Query]
     */
    fun skip(skip: Long): Query {
        this.builder.queryBuilder.skip = skip
        return this
    }

    /**
     * Adds a LIMIT-clause in the Cottontail DB query language.
     *
     * @param limit The number of results to return at maximum.
     * @return This [Query]
     */
    fun limit(limit: Long): Query {
        this.builder.queryBuilder.limit = limit
        return this
    }

    /**
     * Clears this [Query] making it a green slate object that can be used to build a [Query]
     *
     * @return This [Query]
     */
    fun clear(): Query {
        this.builder.queryBuilder.clear()
        return this
    }

    /**
     * Tries to convert [Any] to a [CottontailGrpc.Vector].
     *
     * Only works for compatible types, otherwise throws an [IllegalStateException]
     *
     * @return [CottontailGrpc.Vector]
     */
    private fun Any.convertAnyToVector(): CottontailGrpc.Vector = when (this) {
        is Array<*> -> {
            require(this[0] is Number) { "Only arrays of numbers can be converted to vector literals." }
            (this as Array<Number>).toVector()
        }
        is BooleanArray -> this.toVector()
        is IntArray -> this.toVector()
        is LongArray -> this.toVector()
        is FloatArray -> this.toVector()
        is DoubleArray -> this.toVector()
        else -> throw IllegalStateException("Conversion of ${this.javaClass.simpleName} to vector element is not supported.")
    }
}