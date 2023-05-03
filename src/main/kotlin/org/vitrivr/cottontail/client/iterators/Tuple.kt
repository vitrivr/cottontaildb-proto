package org.vitrivr.cottontail.client.iterators

import org.vitrivr.cottontail.client.iterators.values.Complex32
import org.vitrivr.cottontail.client.iterators.values.Complex64
import org.vitrivr.cottontail.client.language.basics.Type
import org.vitrivr.cottontail.grpc.CottontailGrpc
import java.util.*

/**
 * A [Tuple] as returned by the [TupleIterator].
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
abstract class Tuple(val raw: CottontailGrpc.QueryResponseMessage.Tuple) {
    /** Internal list of values. */
    private val values: Array<Any?> = Array(raw.dataCount) { it ->
        val data = raw.dataList[it]
        when (data.dataCase) {
            CottontailGrpc.Literal.DataCase.BOOLEANDATA -> data.booleanData
            CottontailGrpc.Literal.DataCase.INTDATA -> data.intData
            CottontailGrpc.Literal.DataCase.LONGDATA -> data.longData
            CottontailGrpc.Literal.DataCase.FLOATDATA -> data.floatData
            CottontailGrpc.Literal.DataCase.DOUBLEDATA -> data.doubleData
            CottontailGrpc.Literal.DataCase.DATEDATA -> Date(data.dateData.utcTimestamp)
            CottontailGrpc.Literal.DataCase.STRINGDATA -> data.stringData
            CottontailGrpc.Literal.DataCase.COMPLEX32DATA -> Complex32(data.complex32Data.real, data.complex32Data.imaginary)
            CottontailGrpc.Literal.DataCase.COMPLEX64DATA -> Complex64(data.complex64Data.real, data.complex64Data.imaginary)
            CottontailGrpc.Literal.DataCase.VECTORDATA -> {
                val vector = data.vectorData
                when (vector.vectorDataCase) {
                    CottontailGrpc.Vector.VectorDataCase.FLOATVECTOR -> FloatArray(vector.floatVector.vectorCount) { vector.floatVector.getVector(it) }
                    CottontailGrpc.Vector.VectorDataCase.DOUBLEVECTOR -> DoubleArray(vector.doubleVector.vectorCount) { vector.doubleVector.getVector(it) }
                    CottontailGrpc.Vector.VectorDataCase.INTVECTOR -> IntArray(vector.intVector.vectorCount) { vector.intVector.getVector(it) }
                    CottontailGrpc.Vector.VectorDataCase.LONGVECTOR -> LongArray(vector.longVector .vectorCount) { vector.longVector.getVector(it) }
                    CottontailGrpc.Vector.VectorDataCase.BOOLVECTOR -> BooleanArray(vector.boolVector.vectorCount) { vector.boolVector.getVector(it) }
                    CottontailGrpc.Vector.VectorDataCase.COMPLEX32VECTOR -> Array(vector.complex32Vector.vectorCount) { Complex32(vector.complex32Vector.getVector(it).real, vector.complex32Vector.getVector(it).imaginary)}
                    CottontailGrpc.Vector.VectorDataCase.COMPLEX64VECTOR -> Array(vector.complex64Vector.vectorCount) { Complex64(vector.complex64Vector.getVector(it).real, vector.complex64Vector.getVector(it).imaginary)}
                    else  -> UnsupportedOperationException("Vector data of type ${vector.vectorDataCase} is not supported by TupleIterator.")
                }
            }
            CottontailGrpc.Literal.DataCase.BYTESTRINGDATA -> data.byteStringData.toByteArray()
            CottontailGrpc.Literal.DataCase.DATA_NOT_SET -> null
            else -> UnsupportedOperationException("Data of type ${data.dataCase} is not supported by TupleIterator.")
        }
    }
    abstract fun nameForIndex(index: Int): String
    abstract fun simpleNameForIndex(index: Int): String
    abstract fun indexForName(name: String): Int
    abstract fun type(name: String): Type
    abstract fun type(index: Int): Type
    fun size() = this.values.size
    operator fun get(name: String) = this.values[indexForName(name)]
    operator fun get(index: Int): Any? = this.values[index]
    fun asBoolean(index: Int): Boolean? = this.values[index] as? Boolean
    fun asByte(index: Int): Byte? = this.values[index] as? Byte
    fun asShort(index: Int): Short? = this.values[index] as? Short
    fun asInt(index: Int): Int? = this.values[index] as? Int
    fun asLong(index: Int): Long? = this.values[index] as? Long
    fun asFloat(index: Int): Float? = this.values[index] as? Float
    fun asDouble(index: Int): Double? = this.values[index] as? Double
    fun asBooleanVector(index: Int): BooleanArray? = this.values[index] as? BooleanArray
    fun asIntVector(index: Int): IntArray? = this.values[index] as? IntArray
    fun asLongVector(index: Int): LongArray? = this.values[index] as? LongArray
    fun asFloatVector(index: Int): FloatArray? = this.values[index] as? FloatArray
    fun asDoubleVector(index: Int): DoubleArray? = this.values[index] as? DoubleArray
    fun asString(index: Int): String? = this.values[index] as? String
    fun asDate(index: Int): Date? = this.values[index] as? Date
    fun asByteString(index: Int): ByteArray? = this.values[index] as? ByteArray
    fun asComplex32(index: Int): Complex32? = this.values[index] as? Complex32
    fun asComplex64(index: Int): Complex64? = this.values[index] as? Complex64
    fun asComplex32Vector(index: Int): Array<Complex32>? = this.values[index] as? Array<Complex32>
    fun asComplex64Vector(index: Int): Array<Complex64>? = this.values[index] as? Array<Complex64>
    fun asBoolean(name: String): Boolean? = this.asBoolean(indexForName(name))
    fun asByte(name: String): Byte? = this.asByte(indexForName(name))
    fun asShort(name: String): Short? = this.asShort(indexForName(name))
    fun asInt(name: String): Int? = this.asInt(indexForName(name))
    fun asLong(name: String): Long? = this.asLong(indexForName(name))
    fun asFloat(name: String): Float? = this.asFloat(indexForName(name))
    fun asDouble(name: String): Double? = this.asDouble(indexForName(name))
    fun asBooleanVector(name: String): BooleanArray? = this.asBooleanVector(indexForName(name))
    fun asIntVector(name: String): IntArray?  = this.asIntVector(indexForName(name))
    fun asLongVector(name: String): LongArray? = this.asLongVector(indexForName(name))
    fun asFloatVector(name: String): FloatArray? = this.asFloatVector(indexForName(name))
    fun asDoubleVector(name: String): DoubleArray? = this.asDoubleVector(indexForName(name))
    fun asString(name: String): String? = this.asString(indexForName(name))
    fun asDate(name: String): Date? = this.asDate(indexForName(name))
    fun asByteString(name: String): ByteArray?  = this.asByteString(indexForName(name))
    fun asComplex32(name: String): Complex32?  = this.asComplex32(indexForName(name))
    fun asComplex64(name: String): Complex64?  = this.asComplex64(indexForName(name))
    override fun toString(): String = this.values.joinToString(", ") { it?.toString() ?: "<null>" }
}