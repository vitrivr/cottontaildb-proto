package org.vitrivr.cottontail.client.language

import org.vitrivr.cottontail.client.language.extensions.*
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Query(entity: String? = null) {
    /** Internal [CottontailGrpc.Query.Builder]. */
    val builder = CottontailGrpc.Query.newBuilder()

    init {
        if (entity != null) {
            this.builder.setFrom(CottontailGrpc.From.newBuilder().setScan(CottontailGrpc.Scan.newBuilder().setEntity(entity.parseEntity())))
        }
    }

    /**
     * Adds a SELECT projection to this [Query].
     *
     * @param fields The names of the columns to return.
     * @return [Query]
     */
    fun select(vararg fields: String): Query {
        this.builder.clearProjection()
        val builder = this.builder.projectionBuilder
        builder.op = CottontailGrpc.Projection.ProjectionOperation.SELECT
        for (field in fields) {
            builder.addColumns(CottontailGrpc.Projection.ProjectionElement.newBuilder().setColumn(field.parseColumn()))
        }
        return this
    }

    /**
     * Adds a SELECT projection to this [Query].
     *
     * @param fields The names of the columns to return.
     * @return [Query]
     */
    fun distinct(vararg fields: String): Query {
        this.builder.clearProjection()
        val builder = this.builder.projectionBuilder
        builder.op = CottontailGrpc.Projection.ProjectionOperation.SELECT_DISTINCT
        for (field in fields) {
            builder.addColumns(CottontailGrpc.Projection.ProjectionElement.newBuilder().setColumn(field.parseColumn()))
        }
        return this
    }

    /**
     * Adds a SELECT COUNT projection to this [Query].
     *
     * @return [Query]
     */
    fun count(): Query {
        this.builder.clearProjection()
        val builder = this.builder.projectionBuilder
        builder.op = CottontailGrpc.Projection.ProjectionOperation.COUNT
        builder.addColumns(CottontailGrpc.Projection.ProjectionElement.newBuilder().setColumn("*".parseColumn()))
        return this
    }

    /**
     * Adds a SELECT EXISTS projection to this [Query].
     *
     * @return [Query]
     */
    fun exists(): Query {
        this.builder.clearProjection()
        val builder = this.builder.projectionBuilder
        builder.op = CottontailGrpc.Projection.ProjectionOperation.EXISTS
        builder.addColumns(CottontailGrpc.Projection.ProjectionElement.newBuilder().setColumn("*".parseColumn()))
        return this
    }

    /**
     * Adds a FROM-clause with a SCAN to this [Query]
     *
     * @param entity The entity to SCAN.
     * @return This [Query]
     */
    fun from(entity: String): Query {
        this.builder.clearFrom()
        this.builder.setFrom(CottontailGrpc.From.newBuilder().setScan(CottontailGrpc.Scan.newBuilder().setEntity(entity.parseEntity())))
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
        this.builder.clearFrom()
        this.builder.setFrom(CottontailGrpc.From.newBuilder().setSubSelect(query.builder))
        return this
    }

    /**
     * Adds a WHERE-clause to this [Query].
     *
     * @param predicate The [Predicate] that specifies the conditions that need to be met.
     * @return This [Query]
     */
    fun where(predicate: Predicate): Query {
        this.builder.clearWhere()
        val builder = this.builder.whereBuilder
        when (predicate) {
            is Atomic -> builder.setAtomic(predicate.toPredicate())
            is And -> builder.setCompound(predicate.toPredicate())
            is Or -> builder.setCompound(predicate.toPredicate())
        }
        return this
    }

    /**
     * Adds a kNN-clause to this [Query] and returns it
     *
     * @param column The column to apply the kNN to
     * @param k The k parameter in the kNN
     * @param distance The distance metric to use.
     * @param queries List of query vectors to use (one required).
     */
    fun knn(column: String, k: Int, distance: String, vararg queries: Any): Query {
        this.builder.clearWhere()
        val builder = this.builder.knnBuilder
        builder.attribute = column.parseColumn()
        builder.k = k
        builder.distance = CottontailGrpc.Knn.Distance.valueOf(distance.toUpperCase())
        queries.forEach { builder.addQuery(it.toVector()) }
        return this
    }

    /**
     * Adds a kNN-clause to this [Query] and returns it
     *
     * @param column The column to apply the kNN to.
     * @param k The k parameter in the kNN
     * @param distance The distance metric to use.
     * @param queries List of query vectors to use (one required).
     * @param weights List of weight vectors to use (one required).
     */
    fun knn(column: String, k: Int, distance: String, queries: List<Any> = emptyList(), weights: List<Any> = emptyList()): Query {
        require(queries.size == weights.size) { "Equal number of query and weight vectors are expected"}
        this.builder.clearWhere()
        val builder = this.builder.knnBuilder
        builder.attribute = column.parseColumn()
        builder.k = k
        builder.distance = CottontailGrpc.Knn.Distance.valueOf(distance.toUpperCase())
        queries.forEach { builder.addQuery(it.toVector()) }
        weights.forEach { builder.addWeights(it.toVector()) }
        return this
    }

    /**
     * Adds a ORDER BY-clause to this [Query] and returns it
     *
     * @param clauses ORDER BY clauses in the form of <column> <order>
     */
    fun order(vararg clauses: Pair<String,String>): Query {
        this.builder.clearOrder()
        val builder = this.builder.orderBuilder
        for (c in clauses) {
            val cBuilder = builder.addComponentsBuilder()
            cBuilder.column = c.first.parseColumn()
            cBuilder.direction = CottontailGrpc.Order.Direction.valueOf(c.second.toUpperCase())
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
        this.builder.skip = skip
        return this
    }

    /**
     * Adds a LIMIT-clause in the Cottontail DB query language.
     *
     * @param limit The number of results to return at maximum.
     * @return This [Query]
     */
    fun limit(limit: Long): Query {
        this.builder.limit = limit
        return this
    }
}