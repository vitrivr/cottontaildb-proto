package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.extensions.parseEntity
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A CREATE ENTITY query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class CreateEntity(name: String) {
    /** Internal [CottontailGrpc.CreateEntityMessage.Builder]. */
    val builder = CottontailGrpc.CreateEntityMessage.newBuilder()

    init {
        this.builder.definitionBuilder.entity = name.parseEntity()
    }

    /**
     * Adds a column to this [CreateEntity].
     *
     * @param name The name of the column.
     * @param type The [CottontailGrpc.Type] of the column.
     * @param length The length of the column (>= 1 for vector columns)
     * @param nullable Flag indicating whether column should be nullable.
     * @return this [CreateEntity]
     */
    fun column(name: String, type: CottontailGrpc.Type, length: Int = 0, nullable: Boolean = false): CreateEntity {
        val addBuilder = builder.definitionBuilder.addColumnsBuilder()
        addBuilder.name = name
        addBuilder.type = type
        addBuilder.length = length
        addBuilder.nullable = nullable
        return this
    }

    /**
     * Adds a column to this [CreateEntity].
     *
     * @param name The name of the column.
     * @param type The [CottontailGrpc.Type] of the column (as string).
     * @param length The length of the column (>= 1 for vector columns)
     * @param nullable Flag indicating whether column should be nullable.
     * @return this [CreateEntity]
     */
    fun column(name: String, type: String, length: Int = 0, nullable: Boolean = false)
        = this.column(name, CottontailGrpc.Type.valueOf(type.toUpperCase()), length, nullable)
}