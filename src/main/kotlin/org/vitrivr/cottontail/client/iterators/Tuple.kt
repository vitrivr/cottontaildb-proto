package org.vitrivr.cottontail.client.iterators

import org.vitrivr.cottontail.client.language.basics.Type
import org.vitrivr.cottontail.grpc.CottontailGrpc
import java.util.*

/**
 * A [Tuple] as returned by the [TupleIterator].
 *
 * @author Ralph Gasser
 * @version 1.2.0
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
            CottontailGrpc.Literal.DataCase.COMPLEX32DATA -> data.complex32Data.real to data.complex32Data.imaginary
            CottontailGrpc.Literal.DataCase.COMPLEX64DATA -> data.complex64Data.real to data.complex64Data.imaginary
            CottontailGrpc.Literal.DataCase.VECTORDATA -> {
                val vector = data.vectorData
                when (vector.vectorDataCase) {
                    CottontailGrpc.Vector.VectorDataCase.FLOATVECTOR -> FloatArray(vector.floatVector.vectorCount) { vector.floatVector.getVector(it) }
                    CottontailGrpc.Vector.VectorDataCase.DOUBLEVECTOR -> DoubleArray(vector.doubleVector.vectorCount) { vector.doubleVector.getVector(it) }
                    CottontailGrpc.Vector.VectorDataCase.INTVECTOR -> IntArray(vector.intVector.vectorCount) { vector.intVector.getVector(it) }
                    CottontailGrpc.Vector.VectorDataCase.LONGVECTOR -> LongArray(vector.longVector .vectorCount) { vector.longVector.getVector(it) }
                    CottontailGrpc.Vector.VectorDataCase.BOOLVECTOR -> BooleanArray(vector.boolVector.vectorCount) { vector.boolVector.getVector(it) }
                    CottontailGrpc.Vector.VectorDataCase.COMPLEX32VECTOR -> Array(vector.complex32Vector.vectorCount) { vector.complex32Vector.getVector(it).real to  vector.complex32Vector.getVector(it).imaginary}
                    CottontailGrpc.Vector.VectorDataCase.COMPLEX64VECTOR -> Array(vector.complex64Vector.vectorCount) { vector.complex64Vector.getVector(it).real to  vector.complex64Vector.getVector(it).imaginary}
                    else  -> UnsupportedOperationException("Vector data of type ${vector.vectorDataCase} is not supported by TupleIterator.")
                }
            }
            CottontailGrpc.Literal.DataCase.BYTESTRINGDATA -> data.byteStringData.toByteArray()
            CottontailGrpc.Literal.DataCase.DATA_NOT_SET -> null
            else -> UnsupportedOperationException("Data of type ${data.dataCase} is not supported by TupleIterator.")
        }
    }

    abstract fun nameForIndex(index: Int): String
    abstract fun indexForName(name: String): Int

    abstract fun type(name: String): Type
    abstract fun type(index: Int): Type

    fun size() = this.values.size

    operator fun get(name: String) = this.values[indexForName(name)]
    operator fun get(index: Int): Any? = this.values[index]

    fun asBoolean(index: Int): Boolean? {
        val value = this.values[index]
        return if (value is Boolean) { value } else { null }
    }
    abstract fun asBoolean(name: String): Boolean?

    fun asByte(index: Int): Byte? {
        val value = this.values[index]
        return if (value is Byte) { value } else { null }
    }
    abstract fun asByte(name: String): Byte?

    fun asShort(index: Int): Short? {
        val value = this.values[index]
        return if (value is Short) { value } else { null }
    }
    abstract fun asShort(name: String): Short?

    fun asInt(index: Int): Int? {
        val value = this.values[index]
        return if (value is Int) { value } else { null }
    }
    abstract fun asInt(name: String): Int?

    fun asLong(index: Int): Long? {
        val value = this.values[index]
        return if (value is Long) { value } else { null }
    }
    abstract fun asLong(name: String): Long?

    fun asFloat(index: Int): Float? {
        val value = this.values[index]
        return if (value is Float) { value } else { null }
    }
    abstract fun asFloat(name: String): Float?

    fun asDouble(index: Int): Double? {
        val value = this.values[index]
        return if (value is Double) { value } else { null }
    }
    abstract fun asDouble(name: String): Double?

    fun asBooleanVector(index: Int): BooleanArray? {
        val value = this.values[index]
        return if (value is BooleanArray) { value } else { null }
    }
    abstract fun asBooleanVector(name: String): BooleanArray?

    fun asIntVector(index: Int): IntArray? {
        val value = this.values[index]
        return if (value is IntArray) { value } else { null }
    }
    abstract fun asIntVector(name: String): IntArray?

    fun asLongVector(index: Int): LongArray? {
        val value = this.values[index]
        return if (value is LongArray) { value } else { null }
    }
    abstract fun asLongVector(name: String): LongArray?

    fun asFloatVector(index: Int): FloatArray? {
        val value = this.values[index]
        return if (value is FloatArray) { value } else { null }
    }
    abstract fun asFloatVector(name: String): FloatArray?

    fun asDoubleVector(index: Int): DoubleArray? {
        val value = this.values[index]
        return if (value is DoubleArray) { value } else { null }
    }
    abstract fun asDoubleVector(name: String): DoubleArray?

    fun asString(index: Int): String? {
        val value = this.values[index]
        return if (value is String) { value } else { null }
    }
    abstract fun asString(name: String): String?

    fun asDate(index: Int): Date? {
        val value = this.values[index]
        return if (value is Date) { value } else { null }
    }
    abstract fun asDate(name: String): Date?

    fun asByteString(index: Int): ByteArray? {
        val value = this.values[index]
        return if (value is ByteArray) { value } else { null }
    }
    abstract fun asByteString(name: String): ByteArray?

    override fun toString(): String = this.values.joinToString(", ") { it?.toString() ?: "<null>" }
}