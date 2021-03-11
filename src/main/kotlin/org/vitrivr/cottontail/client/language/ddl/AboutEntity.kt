package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.extensions.parseEntity
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A message to retrieves information about an entity.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class AboutEntity(name: String) {
    /** Internal [CottontailGrpc.ListEntityMessage.Builder]. */
    val builder = CottontailGrpc.EntityDetailsMessage.newBuilder()
    init {
        this.builder.entity = name.parseEntity()
    }
}