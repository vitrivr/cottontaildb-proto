package org.vitrivr.cottontail.client.language.dml

import org.vitrivr.cottontail.client.language.extensions.*
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * An UPDATE query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class Update(entity: String? = null) {
    /** Internal [CottontailGrpc.DeleteMessage.Builder]. */
    val builder = CottontailGrpc.UpdateMessage.newBuilder()

    init {
        if (entity != null) {
            this.builder.setFrom(CottontailGrpc.From.newBuilder().setScan(CottontailGrpc.Scan.newBuilder().setEntity(entity.parseEntity())))
        }
    }

    /**
     * Adds a FROM-clause to this [Update].
     *
     * @param entity The name of the entity to  [Update].
     * @return This [Update]
     */
    fun from(entity: String): Update {
        this.builder.clearFrom()
        this.builder.setFrom(
            CottontailGrpc.From.newBuilder().setScan(CottontailGrpc.Scan.newBuilder().setEntity(entity.parseEntity())))
        return this
    }

    /**
     * Adds a WHERE-clause to this [Update].
     *
     * @param predicate The [Predicate] that specifies the conditions that need to be met for an [Update].
     * @return This [Update]
     */
    fun where(predicate: Predicate): Update {
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
     * Adds value assignments this [Update]
     *
     * @param assignments The value assignments for the [Update]
     * @return This [Update]
     */
    fun values(vararg assignments: Pair<String,Any?>): Update {
        this.builder.clearUpdates()
        for (assignment in assignments) {
            this.builder.addUpdates(
                CottontailGrpc.UpdateMessage.UpdateElement.newBuilder()
                .setColumn(assignment.first.parseColumn())
                .setValue(CottontailGrpc.Expression.newBuilder().setLiteral(assignment.second?.convert() ?: CottontailGrpc.Literal.newBuilder().setNullData(CottontailGrpc.Null.newBuilder()).build()))
            )
        }
        return this
    }

    /**
     * Converts an [Any] to a [CottontailGrpc.Literal]
     *
     * @return [CottontailGrpc.Literal]
     */
    private fun Any.convert(): CottontailGrpc.Literal = when(this) {
        is Array<*> -> (this as Array<Number>).toLiteral()
        is BooleanArray -> this.toLiteral()
        is IntArray -> this.toLiteral()
        is LongArray -> this.toLiteral()
        is FloatArray -> this.toLiteral()
        is DoubleArray -> this.toLiteral()
        is Boolean -> this.toLiteral()
        is Byte -> this.toLiteral()
        is Short -> this.toLiteral()
        is Int -> this.toLiteral()
        is Long -> this.toLiteral()
        is Float -> this.toLiteral()
        is Double -> this.toLiteral()
        is String -> this.toLiteral()
        else -> throw IllegalStateException("Conversion of ${this.javaClass.simpleName} to literal is not supported.")
    }
}