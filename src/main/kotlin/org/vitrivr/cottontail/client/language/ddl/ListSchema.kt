package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A message to list all schemas.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class ListSchema {
    /** Internal [CottontailGrpc.ListSchemaMessage.Builder]. */
    val builder = CottontailGrpc.ListSchemaMessage.newBuilder()
}