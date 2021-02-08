package org.vitrivr.cottontail.client.language.extensions

import org.vitrivr.cottontail.grpc.CottontailGrpc


/**
 * Tries to convert [Any] to a [CottontailGrpc.Vector].
 *
 * Only works for compatible types, otherwise throws an [IllegalStateException]
 *
 * @return [CottontailGrpc.Vector]
 */
fun Any.toVector(): CottontailGrpc.Vector = when (this) {
    is BooleanArray -> this.toVector()
    is IntArray -> this.toVector()
    is LongArray -> this.toVector()
    is FloatArray -> this.toVector()
    is DoubleArray -> this.toVector()
    is Array<*> -> (this as Array<Number>).toVector()
    else -> throw IllegalStateException("Conversion of ${this.javaClass.simpleName} to vector element is not supported.")
}

/**
 * Converts an [Array] of [Number]s to a [CottontailGrpc.Vector].
 *
 * @return [CottontailGrpc.Vector]
 */
fun Array<Number>.toVector(): CottontailGrpc.Vector = when (this[0]){
    is Byte -> CottontailGrpc.Vector.newBuilder().setIntVector(CottontailGrpc.IntVector.newBuilder().addAllVector(this.map { it.toInt() })).build()
    is Short -> CottontailGrpc.Vector.newBuilder().setIntVector(CottontailGrpc.IntVector.newBuilder().addAllVector(this.map { it.toInt() })).build()
    is Int -> CottontailGrpc.Vector.newBuilder().setIntVector(CottontailGrpc.IntVector.newBuilder().addAllVector(this.map { it.toInt() })).build()
    is Long -> CottontailGrpc.Vector.newBuilder().setLongVector(CottontailGrpc.LongVector.newBuilder().addAllVector(this.map { it.toLong() })).build()
    is Float -> CottontailGrpc.Vector.newBuilder().setFloatVector(CottontailGrpc.FloatVector.newBuilder().addAllVector(this.map { it.toFloat() })).build()
    is Double -> CottontailGrpc.Vector.newBuilder().setDoubleVector(CottontailGrpc.DoubleVector.newBuilder().addAllVector(this.map { it.toDouble() })).build()
    else -> throw IllegalStateException("Conversion of ${this[0].javaClass.simpleName} to vector element is not supported.")
}

/**
 * Converts a [BooleanArray] to a [CottontailGrpc.Vector]
 *
 * @return [CottontailGrpc.Vector]
 */
fun BooleanArray.toVector(): CottontailGrpc.Vector
    = CottontailGrpc.Vector.newBuilder().setBoolVector(CottontailGrpc.BoolVector.newBuilder().addAllVector(this.asIterable())).build()
/**
 * Converts a [IntArray] to a [CottontailGrpc.Vector]
 *
 * @return [CottontailGrpc.Vector]
 */
fun IntArray.toVector(): CottontailGrpc.Vector
    = CottontailGrpc.Vector.newBuilder().setIntVector(CottontailGrpc.IntVector.newBuilder().addAllVector(this.asIterable())).build()

/**
 * Converts a [LongArray] to a [CottontailGrpc.Vector]
 *
 * @return [CottontailGrpc.Vector]
 */
fun LongArray.toVector(): CottontailGrpc.Vector
    = CottontailGrpc.Vector.newBuilder().setLongVector(CottontailGrpc.LongVector.newBuilder().addAllVector(this.asIterable())).build()

/**
 * Converts a [FloatArray] to a [CottontailGrpc.Vector]
 *
 * @return [CottontailGrpc.Vector]
 */
fun FloatArray.toVector(): CottontailGrpc.Vector
    = CottontailGrpc.Vector.newBuilder().setFloatVector(CottontailGrpc.FloatVector.newBuilder().addAllVector(this.asIterable())).build()

/**
 * Converts a [DoubleArray] to a [CottontailGrpc.Vector]
 *
 * @return [CottontailGrpc.Vector]
 */
fun DoubleArray.toVector(): CottontailGrpc.Vector
    = CottontailGrpc.Vector.newBuilder().setDoubleVector(CottontailGrpc.DoubleVector.newBuilder().addAllVector(this.asIterable())).build()