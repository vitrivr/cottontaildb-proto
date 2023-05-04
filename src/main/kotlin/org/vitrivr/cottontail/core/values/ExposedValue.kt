package org.vitrivr.cottontail.core.values

import org.vitrivr.cottontail.core.values.types.Types

/**
 * A [Value] that is part of Cottontail DBs public interface.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
sealed interface ExposedValue: Value {
    /** Size of this [Value]. */
    val logicalSize: Int

    /** The [Types] of this [Value]. */
    val type: Types<*>
}