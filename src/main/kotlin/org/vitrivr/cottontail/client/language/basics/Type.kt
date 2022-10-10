package org.vitrivr.cottontail.client.language.basics

import org.vitrivr.cottontail.grpc.CottontailGrpc
import java.time.Instant
import java.util.*

/**
 * The (column) [Type] supported by Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
enum class Type(val grpc: CottontailGrpc.Type) {
    BOOLEAN(CottontailGrpc.Type.BOOLEAN),
    BYTE(CottontailGrpc.Type.BYTE),
    SHORT(CottontailGrpc.Type.SHORT),
    INTEGER(CottontailGrpc.Type.INTEGER),
    LONG(CottontailGrpc.Type.LONG),
    FLOAT(CottontailGrpc.Type.FLOAT),
    DOUBLE(CottontailGrpc.Type.DOUBLE),
    DATE(CottontailGrpc.Type.DATE),
    STRING(CottontailGrpc.Type.STRING),
    COMPLEX32(CottontailGrpc.Type.COMPLEX32),
    COMPLEX64(CottontailGrpc.Type.COMPLEX64),
    DOUBLE_VECTOR(CottontailGrpc.Type.DOUBLE_VEC),
    FLOAT_VECTOR(CottontailGrpc.Type.FLOAT_VEC),
    LONG_VECTOR(CottontailGrpc.Type.LONG_VEC),
    INTEGER_VECTOR(CottontailGrpc.Type.INT_VEC),
    BOOLEAN_VECTOR(CottontailGrpc.Type.BOOL_VEC),
    COMPLEX32_VECTOR(CottontailGrpc.Type.COMPLEX32_VEC),
    COMPLEX64_VECTOR(CottontailGrpc.Type.COMPLEX64_VEC),
    BYTESTRING(CottontailGrpc.Type.BYTESTRING),
    UNDEFINED(CottontailGrpc.Type.UNRECOGNIZED);

    companion object {
        /**
         * Converts a [CottontailGrpc.Type] to a [Type].
         *
         * @param grpcType The [CottontailGrpc.Type] to convert.
         * @return [Type]
         */
        fun of(grpcType: CottontailGrpc.Type) = when (grpcType) {
            CottontailGrpc.Type.BOOLEAN -> BOOLEAN
            CottontailGrpc.Type.BYTE -> BYTE
            CottontailGrpc.Type.SHORT -> SHORT
            CottontailGrpc.Type.INTEGER -> INTEGER
            CottontailGrpc.Type.LONG -> LONG
            CottontailGrpc.Type.FLOAT -> FLOAT
            CottontailGrpc.Type.DOUBLE -> DOUBLE
            CottontailGrpc.Type.DATE -> DATE
            CottontailGrpc.Type.STRING -> STRING
            CottontailGrpc.Type.COMPLEX32 -> COMPLEX32
            CottontailGrpc.Type.COMPLEX64 -> COMPLEX64
            CottontailGrpc.Type.DOUBLE_VEC -> DOUBLE_VECTOR
            CottontailGrpc.Type.FLOAT_VEC -> FLOAT_VECTOR
            CottontailGrpc.Type.LONG_VEC -> LONG_VECTOR
            CottontailGrpc.Type.INT_VEC -> INTEGER_VECTOR
            CottontailGrpc.Type.BOOL_VEC -> BOOLEAN_VECTOR
            CottontailGrpc.Type.COMPLEX32_VEC -> COMPLEX32_VECTOR
            CottontailGrpc.Type.COMPLEX64_VEC -> COMPLEX64_VECTOR
            CottontailGrpc.Type.BYTESTRING -> BYTESTRING
            CottontailGrpc.Type.UNRECOGNIZED -> UNDEFINED
        }
    }

    /**
     * Converts the given value to a [String] representation.
     *
     * @param value The [Any] value (can be null).
     * @param maxLength The maximum length (for vector elements).
     * @return String
     */
    fun toString(value: Any?, maxLength: Int = 4): String = when(value) {
        BOOLEAN -> (value as Boolean?)?.toString()
        BYTE ->  (value as Byte?)?.toString()
        SHORT ->  (value as Short?)?.toString()
        INTEGER -> (value as Int?)?.toString()
        LONG ->  (value as Long?)?.toString()
        FLOAT ->  (value as Float?)?.toString()
        DOUBLE ->  (value as Double?)?.toString()
        DATE ->  (value as Long?)?.let { Date.from(Instant.ofEpochMilli(it)) }?.toString()
        STRING ->  (value as String?)?.toString()
        COMPLEX32 -> (value as Pair<*,*>?)?.let { "${it.first} + i${it.second}" }
        COMPLEX64 -> (value as Pair<*,*>?)?.let { "${it.first} + i${it.second}" }
        DOUBLE_VECTOR -> (value as DoubleArray?)?.let { this.vectorToString(it.toList(), maxLength) }
        FLOAT_VECTOR -> (value as FloatArray?)?.let { this.vectorToString(it.toList(), maxLength) }
        LONG_VECTOR -> (value as LongArray?)?.let { this.vectorToString(it.toList(), maxLength) }
        INTEGER_VECTOR -> (value as IntArray?)?.let { this.vectorToString(it.toList(), maxLength) }
        BOOLEAN_VECTOR -> (value as BooleanArray?)?.let { this.vectorToString(it.toList(), maxLength) }
        COMPLEX32_VECTOR -> (value as Array<Pair<*,*>>?)?.let { complexToString(it.toList(), maxLength) }
        COMPLEX64_VECTOR -> (value as Array<Pair<*,*>>?)?.let { complexToString(it.toList(), maxLength) }
        BYTESTRING -> "~~BINARY~~"
        else -> "~~N/A~~"
    } ?: "~~NULL~~"

    /**
     * Concatenates a vector (list) into a [String]
     *
     * @param vector The [List] to concatenate.
     * @param max The maximum number of elements to include.
     */
    private fun vectorToString(vector: List<*>, max: Int = 4) = if (vector.size > max) {
        "[${vector.take(max - 1).joinToString(", ")}.., ${vector.last()}]"
    } else {
        "[${vector.joinToString(", ")}]"
    }

    /**
     * Concatenates a vector (list) into a [String]
     *
     * @param vector The [List] to concatenate.
     * @param max The maximum number of elements to include.
     */
    private fun complexToString(vector: List<Pair<*,*>>, max: Int = 4) = if (vector.size > max) {
        "[${vector.take(max - 1).joinToString(", ") { "${it.first} + i${it.second}" }}.., ${vector.last().first} + i${vector.last().second}]"
    } else {
        "[${vector.joinToString(", ") { "${it.first} + i${it.second}" }}]"
    }

}