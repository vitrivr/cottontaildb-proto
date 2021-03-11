package org.vitrivr.cottontail.client.language.basics

import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * The (column) [Type] supported by Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
enum class Type(val grpc: CottontailGrpc.Type) {
    BOOLEAN(CottontailGrpc.Type.BOOLEAN),
    BYTE(CottontailGrpc.Type.BYTE),
    SHORT(CottontailGrpc.Type.SHORT),
    INTEGER(CottontailGrpc.Type.INTEGER),
    LONG(CottontailGrpc.Type.LONG),
    FLOAT(CottontailGrpc.Type.FLOAT),
    DOUBLE(CottontailGrpc.Type.DOUBLE),
    STRING(CottontailGrpc.Type.STRING),
    COMPLEX32(CottontailGrpc.Type.COMPLEX32),
    COMPLEX64(CottontailGrpc.Type.COMPLEX64),
    DOUBLE_VECTOR(CottontailGrpc.Type.DOUBLE_VEC),
    FLOAT_VECTOR(CottontailGrpc.Type.FLOAT_VEC),
    LONG_VECTOR(CottontailGrpc.Type.LONG_VEC),
    INT_VECTOR(CottontailGrpc.Type.INT_VEC),
    BOOL_VECTOR(CottontailGrpc.Type.BOOL_VEC),
    COMPLEX32_VECTOR(CottontailGrpc.Type.COMPLEX32_VEC),
    COMPLEX64_VECTOR(CottontailGrpc.Type.COMPLEX64_VEC);
}