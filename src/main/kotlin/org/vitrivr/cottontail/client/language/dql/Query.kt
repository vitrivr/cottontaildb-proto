package org.vitrivr.cottontail.client.language.dql

import org.vitrivr.cottontail.client.language.extensions.*
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 1.0.2
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
     * @param fields The names of the columns to return and their alias (null, if no alias is set).
     * @return [Query]
     */
    fun select(vararg fields: Pair<String,String?>): Query {
        this.builder.clearProjection()
        val builder = this.builder.projectionBuilder
        builder.op = CottontailGrpc.Projection.ProjectionOperation.SELECT
        for (field in fields) {
            val c = CottontailGrpc.Projection.ProjectionElement.newBuilder().setColumn(field.first.parseColumn())
            if (field.second != null) {
                c.alias = field.second!!.parseColumn()
            }
            builder.addColumns(c)
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