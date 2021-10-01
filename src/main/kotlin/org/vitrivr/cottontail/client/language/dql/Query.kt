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
        this.builder.txIdBuilder.value = txId
        return this
    }

    /**
     * Sets the query ID for this [Query].
     *
     * @param queryId The new query ID.
     */
    override fun queryId(queryId: String): Query {
        this.builder.txIdBuilder.queryId = queryId
        return this
    }

    /**
     * Adds a SELECT projection to this [Query].
     *
     * @param fields The names of the columns to return.
     * @return [Query]
     */
    fun select(vararg fields: String): Query {
        this.builder.queryBuilder.clearProjection()
        val builder = this.builder.queryBuilder.projectionBuilder
        builder.op = CottontailGrpc.Projection.ProjectionOperation.SELECT
        for (field in fields) {
            builder.addElements(CottontailGrpc.Projection.ProjectionElement.newBuilder().setColumn(field.parseColumn()))
        }
        return this
    }

    /**
     * Adds a SELECT projection to this [Query].
     *
     * @param fields The names of the columns to return and their alias (null, if no alias is set).
     * @param clear If set, the existing projection will be cleared.
     * @return [Query]
     */
    fun select(vararg fields: Pair<String,String?>, clear: Boolean = false): Query {
        val builder = this.builder.queryBuilder.projectionBuilder
        builder.op = CottontailGrpc.Projection.ProjectionOperation.SELECT
        if (clear) builder.clearElements()
        for (field in fields) {
            val c = CottontailGrpc.Projection.ProjectionElement.newBuilder().setColumn(field.first.parseColumn())
            if (field.second != null) {
                c.alias = field.second!!.parseColumn()
            }
            builder.addElements(c)
        }
        return this
    }

    /**
     * Adds a SELECT projection to this [Query].
     *
     * Calling this method resets the PROJECTION part of the query.
     *
     * @param fields The names of the columns to return.
     * @param clear If set, the existing projection will be cleared.
     * @return [Query]
     */
    fun distinct(vararg fields: Pair<String,String?>, clear: Boolean = false): Query {
        val builder = this.builder.queryBuilder.projectionBuilder
        builder.op = CottontailGrpc.Projection.ProjectionOperation.SELECT_DISTINCT
        if (clear) builder.clearElements()
        for (field in fields) {
            val c = CottontailGrpc.Projection.ProjectionElement.newBuilder().setColumn(field.first.parseColumn())
            if (field.second != null) {
                c.alias = field.second!!.parseColumn()
            }
            builder.addElements(c)
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
        this.builder.queryBuilder.clearProjection()
        val builder = this.builder.queryBuilder.projectionBuilder
        builder.op = CottontailGrpc.Projection.ProjectionOperation.COUNT
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
        this.builder.queryBuilder.clearProjection()
        val builder = this.builder.queryBuilder.projectionBuilder
        builder.op = CottontailGrpc.Projection.ProjectionOperation.EXISTS
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
        return neighborSearch(column, query, Distances.valueOf(distance.uppercase()),"distance", k.toLong())
    }

    /**
     * Transforms this [Query] to a Neighbor Search (NS) query and returns it.
     *
     * Calling this method has side-effects on various aspects of the [Query] (i.e., PROJECTION, ORDER and LIMIT).
     * Most importantly, this function is not idempotent, i.e., calling it multiple times changes the structure of the
     * query, e.g., by adding multiple distance functions. Use [clear] to be on the safe side.
     *
     * @param column The column to perform NNS on. Type must be compatible with choice of distance function.
     * @param query Query value to use. Type must be compatible with choice of distance function.
     * @param distance The distance function to use. Function argument must be compatible with column type.
     * @param name The name of the column that holds the calculated distance value.
     * @param k The number of entries to return. It is highly recommended using a reasonable number here, since otherwise, Cottontail DB may run out of memory.
     * @param direction The desired sort direction. [Direction.ASC] for NNS and [Direction.DESC] for FNS.
     * @return This [Query]
     */
    fun neighborSearch(column: String, query: Any, distance: Distances, name: String, k: Long, direction: Direction = Direction.ASC): Query {
        /* Parse necessary functions. */
        val distanceColumn = name.parseColumn()
        val distanceFunction = CottontailGrpc.Function.newBuilder()
            .setName(distance.toGrpc())
            .addArguments(CottontailGrpc.Expression.newBuilder().setColumn(column.parseColumn()))
            .addArguments(CottontailGrpc.Expression.newBuilder().setLiteral(CottontailGrpc.Literal.newBuilder().setVectorData(query.convertAnyToVector())))

        /* Update projection: Add distance column + alias. */
        this.builder.queryBuilder.projectionBuilder.addElements(CottontailGrpc.Projection.ProjectionElement.newBuilder().setAlias(distanceColumn).setFunction(distanceFunction))

        /* Update ORDER BY clause. */
        this.builder.queryBuilder.orderBuilder.addComponents(CottontailGrpc.Order.Component.newBuilder().setColumn(distanceColumn).setDirection(direction.toGrpc()))

        /* Update LIMIT clause. */
        this.builder.queryBuilder.limit = k
        return this
    }

    /**
     * Adds a ORDER BY-clause to this [Query] and returns it
     *
     * @param clauses ORDER BY clauses in the form of <column> <order>
     * @param clear If set, the existing ORDER BY-clause will be cleared.
     * @return This [Query]
     */
    fun order(vararg clauses: Pair<String,String>, clear: Boolean = false): Query {
        val builder = this.builder.queryBuilder.orderBuilder
        if (clear) builder.clearComponents()
        for (c in clauses) {
            val cBuilder = builder.addComponentsBuilder()
            cBuilder.column = c.first.parseColumn()
            cBuilder.direction = CottontailGrpc.Order.Direction.valueOf(c.second.uppercase())
        }
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