package org.vitrivr.cottontail.client.language.extensions

import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * Converts an [Array] of [Number]s to a [CottontailGrpc.Literal]
 *
 * @return [CottontailGrpc.Literal]
 */
fun Array<Number>.toLiteral(): CottontailGrpc.Literal
        = CottontailGrpc.Literal.newBuilder().setVectorData(this.toVector()).build()
/**
 * Converts a [BooleanArray] to a [CottontailGrpc.Literal]
 *
 * @return [CottontailGrpc.Literal]
 */
fun BooleanArray.toLiteral(): CottontailGrpc.Literal
        = CottontailGrpc.Literal.newBuilder().setVectorData(this.toVector()).build()
/**
 * Converts a [IntArray] to a [CottontailGrpc.Literal]
 *
 * @return [CottontailGrpc.Literal]
 */
fun IntArray.toLiteral(): CottontailGrpc.Literal
        = CottontailGrpc.Literal.newBuilder().setVectorData(this.toVector()).build()
/**
 * Converts a [LongArray] to a [CottontailGrpc.Literal]
 *
 * @return [CottontailGrpc.Literal]
 */
fun LongArray.toLiteral(): CottontailGrpc.Literal
        = CottontailGrpc.Literal.newBuilder().setVectorData(this.toVector()).build()
/**
 * Converts a [FloatArray] to a [CottontailGrpc.Literal]
 *
 * @return [CottontailGrpc.Literal]
 */
fun FloatArray.toLiteral(): CottontailGrpc.Literal
        = CottontailGrpc.Literal.newBuilder().setVectorData(this.toVector()).build()
/**
 * Converts a [DoubleArray] to a [CottontailGrpc.Literal]
 *
 * @return [CottontailGrpc.Literal]
 */
fun DoubleArray.toLiteral(): CottontailGrpc.Literal
        = CottontailGrpc.Literal.newBuilder().setVectorData(this.toVector()).build()
/**
 * Converts a [Boolean] to a [CottontailGrpc.Literal]
 *
 * @return [CottontailGrpc.Literal]
 */
fun Boolean.toLiteral() = CottontailGrpc.Literal.newBuilder().setBooleanData(this).build()

/**
 * Converts a [Byte] to a [CottontailGrpc.Literal]
 *
 * @return [CottontailGrpc.Literal]
 */
fun Byte.toLiteral() = CottontailGrpc.Literal.newBuilder().setIntData(this.toInt()).build()

/**
 * Converts a [Short] to a [CottontailGrpc.Literal]
 *
 * @return [CottontailGrpc.Literal]
 */
fun Short.toLiteral() = CottontailGrpc.Literal.newBuilder().setIntData(this.toInt()).build()

/**
 * Converts a [Int] to a [CottontailGrpc.Literal]
 *
 * @return [CottontailGrpc.Literal]
 */
fun Int.toLiteral() = CottontailGrpc.Literal.newBuilder().setIntData(this).build()

/**
 * Converts a [Long] to a [CottontailGrpc.Literal]
 *
 * @return [CottontailGrpc.Literal]
 */
fun Long.toLiteral() = CottontailGrpc.Literal.newBuilder().setLongData(this).build()

/**
 * Converts a [Float] to a [CottontailGrpc.Literal]
 *
 * @return [CottontailGrpc.Literal]
 */
fun Float.toLiteral() = CottontailGrpc.Literal.newBuilder().setFloatData(this).build()

/**
 * Converts a [Double] to a [CottontailGrpc.Literal]
 *
 * @return [CottontailGrpc.Literal]
 */
fun Double.toLiteral() = CottontailGrpc.Literal.newBuilder().setDoubleData(this).build()

/**
 * Converts a [String] to a [CottontailGrpc.Literal]
 *
 * @return [CottontailGrpc.Literal]
 */
fun String.toLiteral() = CottontailGrpc.Literal.newBuilder().setStringData(this).build()
