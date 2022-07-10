package org.vitrivr.cottontail.client.language.extensions

import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * Converts an [Any] to a [CottontailGrpc.Literal].
 *
 * This is an internal function, since it can mess with the Java type system.
 *
 * @return [CottontailGrpc.Literal]
 */
@Suppress("UNCHECKED_CAST")
internal fun Any.toGrpc(): CottontailGrpc.Literal = when(this) {
    is BooleanArray -> this.toGrpc()
    is IntArray -> this.toGrpc()
    is LongArray -> this.toGrpc()
    is FloatArray -> this.toGrpc()
    is DoubleArray -> this.toGrpc()
    is Boolean -> this.toGrpc()
    is Byte -> this.toGrpc()
    is Short -> this.toGrpc()
    is Int -> this.toGrpc()
    is Long -> this.toGrpc()
    is Float -> this.toGrpc()
    is Double -> this.toGrpc()
    is String -> this.toGrpc()
    is CottontailGrpc.Literal -> this
    else -> throw IllegalStateException("Conversion of ${this.javaClass.simpleName} to literal is not supported.")
}

/**
 * Converts a [BooleanArray] to a [CottontailGrpc.Literal]
 *
 * @return [CottontailGrpc.Literal]
 */
fun BooleanArray.toGrpc(): CottontailGrpc.Literal
        = CottontailGrpc.Literal.newBuilder().setVectorData(this.toVector()).build()
/**
 * Converts a [IntArray] to a [CottontailGrpc.Literal]
 *
 * @return [CottontailGrpc.Literal]
 */
fun IntArray.toGrpc(): CottontailGrpc.Literal
        = CottontailGrpc.Literal.newBuilder().setVectorData(this.toVector()).build()
/**
 * Converts a [LongArray] to a [CottontailGrpc.Literal]
 *
 * @return [CottontailGrpc.Literal]
 */
fun LongArray.toGrpc(): CottontailGrpc.Literal
        = CottontailGrpc.Literal.newBuilder().setVectorData(this.toVector()).build()
/**
 * Converts a [FloatArray] to a [CottontailGrpc.Literal]
 *
 * @return [CottontailGrpc.Literal]
 */
fun FloatArray.toGrpc(): CottontailGrpc.Literal
        = CottontailGrpc.Literal.newBuilder().setVectorData(this.toVector()).build()
/**
 * Converts a [DoubleArray] to a [CottontailGrpc.Literal]
 *
 * @return [CottontailGrpc.Literal]
 */
fun DoubleArray.toGrpc(): CottontailGrpc.Literal
        = CottontailGrpc.Literal.newBuilder().setVectorData(this.toVector()).build()
/**
 * Converts a [Boolean] to a [CottontailGrpc.Literal]
 *
 * @return [CottontailGrpc.Literal]
 */
fun Boolean.toGrpc() = CottontailGrpc.Literal.newBuilder().setBooleanData(this).build()

/**
 * Converts a [Byte] to a [CottontailGrpc.Literal]
 *
 * @return [CottontailGrpc.Literal]
 */
fun Byte.toGrpc() = CottontailGrpc.Literal.newBuilder().setIntData(this.toInt()).build()

/**
 * Converts a [Short] to a [CottontailGrpc.Literal]
 *
 * @return [CottontailGrpc.Literal]
 */
fun Short.toGrpc() = CottontailGrpc.Literal.newBuilder().setIntData(this.toInt()).build()

/**
 * Converts a [Int] to a [CottontailGrpc.Literal]
 *
 * @return [CottontailGrpc.Literal]
 */
fun Int.toGrpc() = CottontailGrpc.Literal.newBuilder().setIntData(this).build()

/**
 * Converts a [Long] to a [CottontailGrpc.Literal]
 *
 * @return [CottontailGrpc.Literal]
 */
fun Long.toGrpc() = CottontailGrpc.Literal.newBuilder().setLongData(this).build()

/**
 * Converts a [Float] to a [CottontailGrpc.Literal]
 *
 * @return [CottontailGrpc.Literal]
 */
fun Float.toGrpc() = CottontailGrpc.Literal.newBuilder().setFloatData(this).build()

/**
 * Converts a [Double] to a [CottontailGrpc.Literal]
 *
 * @return [CottontailGrpc.Literal]
 */
fun Double.toGrpc() = CottontailGrpc.Literal.newBuilder().setDoubleData(this).build()

/**
 * Converts a [String] to a [CottontailGrpc.Literal]
 *
 * @return [CottontailGrpc.Literal]
 */
fun String.toGrpc() = CottontailGrpc.Literal.newBuilder().setStringData(this).build()
