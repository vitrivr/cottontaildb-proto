package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.extensions.parseEntity
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A DROP ENTITY query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class DropEntity(name: String) {
    /** Internal [CottontailGrpc.DeleteMessage.Builder]. */
    val builder = CottontailGrpc.DropEntityMessage.newBuilder()

    init {
        builder.entity = name.parseEntity()
    }
}