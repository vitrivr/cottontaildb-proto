package org.vitrivr.cottontail.client.language.extensions

import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * Parses a [String] into an [CottontailGrpc.EntityName]
 *
 * @return [CottontailGrpc.EntityName]
 */
fun String.parseSchema(): CottontailGrpc.SchemaName {
    val split = this.toLowerCase().split('.')
    return when (split.size) {
        1 -> CottontailGrpc.SchemaName.newBuilder().setName(split[0]).build()
        2 -> CottontailGrpc.SchemaName.newBuilder().setName(split[1]).build()
        else -> throw IllegalStateException("Cottontail DB schema names can consist of at most two components separated by a dot: [warren.]<schema>")
    }
}

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
        3 -> CottontailGrpc.EntityName.newBuilder().setName(split[2]).setSchema(CottontailGrpc.SchemaName.newBuilder().setName(split[1])).build()
        else -> throw IllegalStateException("Cottontail DB entity names can consist of at most three components separated by a dot: [warren.]<schema>.<entity>")
    }
}

/**
 * Parses a [String] into an [CottontailGrpc.IndexName]
 *
 * @return [CottontailGrpc.IndexName]
 */
fun String.parseIndex(): CottontailGrpc.IndexName {
    val split = this.toLowerCase().split('.')
    return when (split.size) {
        1 -> CottontailGrpc.IndexName.newBuilder().setName(split[0]).build()
        2 -> CottontailGrpc.IndexName.newBuilder().setName(split[1]).setEntity(CottontailGrpc.EntityName.newBuilder().setName(split[0])).build()
        3 -> CottontailGrpc.IndexName.newBuilder().setName(split[2]).setEntity(CottontailGrpc.EntityName.newBuilder().setName(split[1]).setSchema(CottontailGrpc.SchemaName.newBuilder().setName(split[0]))).build()
        4 -> CottontailGrpc.IndexName.newBuilder().setName(split[3]).setEntity(CottontailGrpc.EntityName.newBuilder().setName(split[2]).setSchema(CottontailGrpc.SchemaName.newBuilder().setName(split[1]))).build()
        else -> throw IllegalStateException("Cottontail DB index names can consist of at most three components separated by a dot: [warren.]<schema>.<entity>.<index>")
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
        4 -> CottontailGrpc.ColumnName.newBuilder().setName(split[3]).setEntity(CottontailGrpc.EntityName.newBuilder().setName(split[2]).setSchema(CottontailGrpc.SchemaName.newBuilder().setName(split[1]))).build()
        else -> throw IllegalStateException("Cottontail DB column names can consist of at most three components separated by a dot: [warren.]<schema>.<entity>.<column>")
    }
}

/**
 * Parses a [String] into an [CottontailGrpc.AtomicBooleanPredicate.Builder]
 *
 * @return [CottontailGrpc.ColumnName]
 */
fun String.parseOperator(): CottontailGrpc.ComparisonOperator = when(val value = this.toUpperCase()) {
    "=" -> CottontailGrpc.ComparisonOperator.EQUAL
    "==" -> CottontailGrpc.ComparisonOperator.EQUAL
    "!=" -> CottontailGrpc.ComparisonOperator.EQUAL
    "!==" -> CottontailGrpc.ComparisonOperator.EQUAL
    ">" -> CottontailGrpc.ComparisonOperator.GREATER
    "<" -> CottontailGrpc.ComparisonOperator.LESS
    ">=" -> CottontailGrpc.ComparisonOperator.GEQUAL
    "<=" -> CottontailGrpc.ComparisonOperator.LEQUAL
    "NOT IN" -> CottontailGrpc.ComparisonOperator.IN
    "NOT LIKE" -> CottontailGrpc.ComparisonOperator.LIKE
    "NOT MATCH" -> CottontailGrpc.ComparisonOperator.MATCH
    "IS NULL" -> CottontailGrpc.ComparisonOperator.ISNULL
    "IS NOT NULL" -> CottontailGrpc.ComparisonOperator.ISNULL
    else -> CottontailGrpc.ComparisonOperator.valueOf(value)
}

/**
 * Parses a [String] into an [Boolean] indicating whether it is a NOT or not.
 *
 * @return [Boolean]
 */
fun String.parseNot(): Boolean = when(this.toUpperCase()) {
    "!=", "!==", "NOT IN", "NOT LIKE", "NOT MATCH", "IS NOT NULL" -> true
    else -> false
}