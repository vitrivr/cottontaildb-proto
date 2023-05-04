package org.vitrivr.cottontail.client.language.basics.predicate

import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.client.language.basics.expression.Expression

/**
 * A IS NULL operator, which can be used as [Predicate] to evaluate if an [Expression] is null.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@Serializable
data class IsNull(val expression: Expression)