package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.extensions.parseEntity
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A OPTIMIZE ENTITY query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class OptimizeEntity(name: String) {
    /** Internal [CottontailGrpc.OptimizeEntityMessage.Builder]. */
    val builder = CottontailGrpc.OptimizeEntityMessage.newBuilder()

    init {
        builder.entity = name.parseEntity()
    }
}