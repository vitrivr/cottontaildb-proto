package org.vitrivr.cottontail.client.language.extensions

import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * Parses a [String] into an [CottontailGrpc.EntityName]
 *
 * @return [CottontailGrpc.EntityName]
 */
fun String.parseEntity(): CottontailGrpc.EntityName {
    val split = this.toLowerCase().split('.')
    return when (split.size) {
        1 -> CottontailGrpc.EntityName.newBuilder().setName(split[0]).build()
        2 -> CottontailGrpc.EntityName.newBuilder().setName(split[1]).setSchema(CottontailGrpc.SchemaName.newBuilder().setName(split[0])).build()
        else -> throw IllegalStateException("Cottontail DB entity names can consist of at most three components separated by a dot: <schema>.<entity>")
    }
}

/**
 * Parses a [String] into an [CottontailGrpc.ColumnName]
 *
 * @return [CottontailGrpc.ColumnName]
 */
fun String.parseColumn(): CottontailGrpc.ColumnName {
    val split = this.toLowerCase().split('.')
    return when (split.size) {
        1 -> CottontailGrpc.ColumnName.newBuilder().setName(split[0]).build()
        2 -> CottontailGrpc.ColumnName.newBuilder().setName(split[1]).setEntity(CottontailGrpc.EntityName.newBuilder().setName(split[0])).build()
        3 -> CottontailGrpc.ColumnName.newBuilder().setName(split[2]).setEntity(CottontailGrpc.EntityName.newBuilder().setName(split[1]).setSchema(CottontailGrpc.SchemaName.newBuilder().setName(split[0]))).build()
        else -> throw IllegalStateException("Cottontail DB column names can consist of at most three components separated by a dot: <schema>.<entity>.<column>")
    }
}

/**
 *
 */
fun String.parseOperator(): CottontailGrpc.ComparisonOperator = when(this) {
    "=" -> CottontailGrpc.ComparisonOperator.EQUAL
    "==" -> CottontailGrpc.ComparisonOperator.EQUAL
    ">" -> CottontailGrpc.ComparisonOperator.GREATER
    "<" -> CottontailGrpc.ComparisonOperator.LEQUAL
    ">=" -> CottontailGrpc.ComparisonOperator.GEQUAL
    "<=" -> CottontailGrpc.ComparisonOperator.LEQUAL
    "IS NULL" -> CottontailGrpc.ComparisonOperator.ISNULL
    "IS NOT NULL" -> CottontailGrpc.ComparisonOperator.ISNOTNULL
    else -> CottontailGrpc.ComparisonOperator.valueOf(this)
}