package org.vitrivr.cottontail.client.language.basics

import org.vitrivr.cottontail.grpc.CottontailGrpc

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
    BLOB(CottontailGrpc.Type.BLOB),
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
            CottontailGrpc.Type.BLOB -> BLOB
            CottontailGrpc.Type.UNRECOGNIZED -> UNDEFINED
        }
    }
}