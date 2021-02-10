package org.vitrivr.cottontail.client.language

import org.vitrivr.cottontail.client.language.extensions.parseColumn
import org.vitrivr.cottontail.client.language.extensions.parseEntity
import org.vitrivr.cottontail.client.language.extensions.toLiteral
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A INSERT query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Insert(entity: String? = null) {
    /** Internal [CottontailGrpc.DeleteMessage.Builder]. */
    val builder = CottontailGrpc.InsertMessage.newBuilder()

    init {
        if (entity != null) {
            this.builder.setFrom(CottontailGrpc.From.newBuilder().setScan(CottontailGrpc.Scan.newBuilder().setEntity(entity.parseEntity())))
        }
    }

    /**
     * Adds a FROM-clause to this [Insert].
     *
     * @param entity The name of the entity to [Insert] to.
     * @return This [Insert]
     */
    fun into(entity: String): Insert {
        this.builder.clearFrom()
        this.builder.setFrom(
            CottontailGrpc.From.newBuilder().setScan(CottontailGrpc.Scan.newBuilder().setEntity(entity.parseEntity())))
        return this
    }

    /**
     * Adds value assignments this [Insert]
     *
     * @param assignments The value assignments for the [Insert]
     * @return This [Update]
     */
    fun values(vararg assignments: Pair<String,Any?>): Insert {
        this.builder.clearInserts()
        for (assignment in assignments) {
            this.builder.addInserts(
                CottontailGrpc.InsertMessage.InsertElement.newBuilder()
                    .setColumn(assignment.first.parseColumn())
                    .setValue(assignment.second?.toLiteral() ?: CottontailGrpc.Literal.newBuilder().setNullData(CottontailGrpc.Null.newBuilder()).build())
            )
        }
        return this
    }
}