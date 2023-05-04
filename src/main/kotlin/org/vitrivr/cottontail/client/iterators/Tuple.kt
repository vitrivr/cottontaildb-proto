package org.vitrivr.cottontail.client.iterators

import org.vitrivr.cottontail.core.toValue
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A [Tuple] as returned by the [TupleIterator].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
abstract class Tuple(val raw: CottontailGrpc.QueryResponseMessage.Tuple) {
    /** Internal list of values. */
    private val values: Array<Value?> = Array(raw.dataCount) { raw.dataList[it].toValue() }

    abstract fun nameForIndex(index: Int): String
    abstract fun simpleNameForIndex(index: Int): String
    abstract fun indexForName(name: String): Int
    abstract fun type(name: String): Types<*>
    abstract fun type(index: Int): Types<*>
    fun size() = this.values.size
    operator fun get(name: String): Value? = this.values[indexForName(name)]
    operator fun get(index: Int): Value? = this.values[index]
    fun asBoolean(index: Int): BooleanValue? = this.values[index] as? BooleanValue
    fun asByte(index: Int): ByteValue? = this.values[index] as? ByteValue
    fun asShort(index: Int): ShortValue? = this.values[index] as? ShortValue
    fun asInt(index: Int): IntValue? = this.values[index] as? IntValue
    fun asLong(index: Int): LongValue? = this.values[index] as? LongValue
    fun asFloat(index: Int): FloatValue? = this.values[index] as? FloatValue
    fun asDouble(index: Int): DoubleValue? = this.values[index] as? DoubleValue
    fun asBooleanVector(index: Int): BooleanVectorValue? = this.values[index] as? BooleanVectorValue
    fun asIntVector(index: Int): IntVectorValue? = this.values[index] as? IntVectorValue
    fun asLongVector(index: Int): LongVectorValue? = this.values[index] as? LongVectorValue
    fun asFloatVector(index: Int): FloatVectorValue? = this.values[index] as? FloatVectorValue
    fun asDoubleVector(index: Int): DoubleVectorValue? = this.values[index] as? DoubleVectorValue
    fun asString(index: Int): StringValue? = this.values[index] as? StringValue
    fun asDate(index: Int): DateValue? = this.values[index] as? DateValue
    fun asByteString(index: Int): ByteStringValue? = this.values[index] as? ByteStringValue
    fun asComplex32(index: Int): Complex32Value? = this.values[index] as? Complex32Value
    fun asComplex64(index: Int): Complex64Value? = this.values[index] as? Complex64Value
    fun asComplex32Vector(index: Int): Complex32VectorValue? = this.values[index] as? Complex32VectorValue
    fun asComplex64Vector(index: Int): Complex64VectorValue? = this.values[index] as? Complex64VectorValue
    fun asBoolean(name: String): BooleanValue? = this.asBoolean(indexForName(name))
    fun asByte(name: String): ByteValue? = this.asByte(indexForName(name))
    fun asShort(name: String): ShortValue? = this.asShort(indexForName(name))
    fun asInt(name: String): IntValue? = this.asInt(indexForName(name))
    fun asLong(name: String): LongValue? = this.asLong(indexForName(name))
    fun asFloat(name: String): FloatValue? = this.asFloat(indexForName(name))
    fun asDouble(name: String): DoubleValue? = this.asDouble(indexForName(name))
    fun asBooleanVector(name: String): BooleanVectorValue? = this.asBooleanVector(indexForName(name))
    fun asIntVector(name: String): IntVectorValue? = this.asIntVector(indexForName(name))
    fun asLongVector(name: String): LongVectorValue? = this.asLongVector(indexForName(name))
    fun asFloatVector(name: String): FloatVectorValue? = this.asFloatVector(indexForName(name))
    fun asDoubleVector(name: String): DoubleVectorValue? = this.asDoubleVector(indexForName(name))
    fun asString(name: String): StringValue? = this.asString(indexForName(name))
    fun asDate(name: String): DateValue? = this.asDate(indexForName(name))
    fun asByteString(name: String): ByteStringValue?  = this.asByteString(indexForName(name))
    fun asComplex32(name: String): Complex32Value?  = this.asComplex32(indexForName(name))
    fun asComplex64(name: String): Complex64Value?  = this.asComplex64(indexForName(name))
    override fun toString(): String = this.values.joinToString(", ") { it?.toString() ?: "<null>" }
}