package org.vitrivr.cottontail.client

import com.google.protobuf.Empty
import io.grpc.Context
import io.grpc.ManagedChannel
import io.grpc.StatusRuntimeException
import org.vitrivr.cottontail.client.iterators.TupleIterator
import org.vitrivr.cottontail.client.iterators.TupleIteratorImpl
import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.ddl.*
import org.vitrivr.cottontail.client.language.dml.BatchInsert
import org.vitrivr.cottontail.client.language.dml.Delete
import org.vitrivr.cottontail.client.language.dml.Insert
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.client.language.dml.Update
import org.vitrivr.cottontail.grpc.*
import kotlin.coroutines.cancellation.CancellationException

/**
 * A simple Cottontail DB client for querying, data management and data definition. Can work with [LanguageFeature]s
 * and classical [CottontailGrpc] messages.
 *
 * As opposed to the pure gRPC implementation, the [SimpleClient] offers some advanced functionality such
 * as a more convenient [TupleIterator], cancelable queries and auto commit of 'simple' queries without
 * explicit transaction context.
 *
 * The [SimpleClient] wraps a [ManagedChannel]. It remains to the caller, to setup and close that [ManagedChannel].
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
class SimpleClient(private val channel: ManagedChannel): AutoCloseable {

    /** [Context.CancellableContext] that can be used to cancel all queries that are currently being executed by this [SimpleClient]. */
    private val context = Context.current().withCancellation()

    /**
     * Begins a new transaction through this [SimpleClient].
     *
     * @return The ID of the newly begun transaction.
     */
    fun begin(): Long = TXNGrpc.newBlockingStub(this.channel).begin(Empty.getDefaultInstance()).transactionId

    /**
     * Commits a transaction through this [SimpleClient].
     *
     * @param txId The transaction ID to commit.
     */
    fun commit(txId: Long) = this.context.run {
        val tx = CottontailGrpc.Metadata.newBuilder().setTransactionId(txId).build()
        TXNGrpc.newBlockingStub(this.channel).commit(tx)
    }

    /**
     * Rolls back a transaction through this [SimpleClient].
     *
     * @param txId The transaction ID to rollback.
     */
    fun rollback(txId: Long) = this.context.run {
        val tx = CottontailGrpc.Metadata.newBuilder().setTransactionId(txId).build()
        TXNGrpc.newBlockingStub(this.channel).rollback(tx)
    }

    /**
     * Kills and rolls back a transaction through this [SimpleClient].
     *
     * @param txId The transaction ID to kill and rollback.
     */
    fun kill(txId: Long) = this.context.run {
        val tx = CottontailGrpc.Metadata.newBuilder().setTransactionId(txId).build()
        TXNGrpc.newBlockingStub(this.channel).kill(tx)
    }

    /**
     * Executes [CottontailGrpc.Query] through this [SimpleClient]
     *
     * @param message [CottontailGrpc.Query] to execute.
     * @return An [Iterator] iof [CottontailGrpc.QueryResponseMessage]
     */
    fun query(message: CottontailGrpc.QueryMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DQLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.query(message), inner) { iterator, success ->
                if (message.metadata.transactionId <= 0L) {
                    if (success) {
                        this.commit(iterator.transactionId)
                    } else {
                        this.rollback(iterator.transactionId)
                    }
                }
            }
        }
    }

    /**
     * Executes [Query] through this [SimpleClient]
     *
     * @param q [Query] to execute.
     * @return [TupleIterator] of the result.
     */
    fun query(q: Query): TupleIterator = this.query(q.builder.build())

    /**
     * Executes [CottontailGrpc.BatchedQueryMessage] through this [SimpleClient]
     *
     * @param message [CottontailGrpc.Query] to execute.
     * @return An [Iterator] iof [CottontailGrpc.QueryResponseMessage]
     */
    fun batchedQuery(message: CottontailGrpc.BatchedQueryMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DQLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.batchQuery(message), inner) { iterator, success ->
                if (message.metadata.transactionId <= 0L) {
                    if (success) {
                        this.commit(iterator.transactionId)
                    } else {
                        this.rollback(iterator.transactionId)
                    }
                }
            }
        }
    }

    /**
     * Explains [CottontailGrpc.Query] through this [SimpleClient]
     *
     * @param message [CottontailGrpc.Query] to executed.
     * @return An [Iterator] iof [CottontailGrpc.QueryResponseMessage]
     */
    fun explain(message: CottontailGrpc.QueryMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DQLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.explain(message), inner) { iterator, success ->
                if (message.metadata.transactionId <= 0L) {
                    if (success) {
                        this.commit(iterator.transactionId)
                    } else {
                        this.rollback(iterator.transactionId)
                    }
                }
            }
        }
    }

    /**
     * Executes [Query] through this [SimpleClient]
     *
     * @param q [Query] to execute.
     * @return [TupleIterator] of the result.
     */
    fun explain(q: Query): TupleIterator = this.explain(q.builder.build())

    /**
     * Executes this [CottontailGrpc.InsertMessage] through this [SimpleClient]
     *
     * @param message [CottontailGrpc.InsertMessage] to execute.
     * @return [TupleIterator] containing the query response.
     */
    fun insert(message: CottontailGrpc.InsertMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DMLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.insert(message), inner) { iterator, success ->
                if (message.metadata.transactionId <= 0L) {
                    if (success) {
                        this.commit(iterator.transactionId)
                    } else {
                        this.rollback(iterator.transactionId)
                    }
                }
            }
        }
    }

    /**
     * Executes this [Insert] through this [SimpleClient]
     *
     * @return [TupleIterator] containing the query response.
     */
    fun insert(query: Insert): TupleIterator = this.insert(query.builder.build())

    /**
     * Executes this [CottontailGrpc.BatchInsertMessage] through this [SimpleClient]
     *
     * @param message [CottontailGrpc.BatchInsertMessage] to execute.
     * @return [TupleIterator] containing the query response.
     */
    fun insert(message: CottontailGrpc.BatchInsertMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DMLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.insertBatch(message), inner) { iterator, success ->
                if (message.metadata.transactionId <= 0L) {
                    if (success) {
                        this.commit(iterator.transactionId)
                    } else {
                        this.rollback(iterator.transactionId)
                    }
                }
            }
        }
    }

    /**
     * Executes this [BatchInsert] through this [SimpleClient]
     *
     * @param query [BatchInsert] to execute.
     * @return [TupleIterator] containing the query response.
     */
    fun insert(query: BatchInsert): TupleIterator = this.insert(query.builder.build())

    /**
     * Executes this [CottontailGrpc.UpdateMessage] through this [SimpleClient]
     *
     * @param message [CottontailGrpc.UpdateMessage] to execute.
     * @return [TupleIterator] containing the query response.
     */
    fun update(message: CottontailGrpc.UpdateMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DMLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.update(message), inner){ iterator, success ->
                if (message.metadata.transactionId <= 0L) {
                    if (success) {
                        this.commit(iterator.transactionId)
                    } else {
                        this.rollback(iterator.transactionId)
                    }
                }
            }
        }
    }

    /**
     * Executes this [Update] through this [SimpleClient]
     *
     * @param query [Update] to execute.
     * @return [TupleIterator] containing the query response.
     */
    fun update(query: Update): TupleIterator = this.update(query.builder.build())


    /**
     * Explains [CottontailGrpc.DeleteMessage] through this [SimpleClient]
     *
     * @param message [CottontailGrpc.DeleteMessage] to execute.
     * @return [TupleIterator] containing the query response.
     */
    fun delete(message: CottontailGrpc.DeleteMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DMLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.delete(message), inner) { iterator, success ->
                if (message.metadata.transactionId <= 0L) {
                    if (success) {
                        this.commit(iterator.transactionId)
                    } else {
                        this.rollback(iterator.transactionId)
                    }
                }
            }
        }
    }

    /**
     * Executes this [Delete] through this [SimpleClient]
     *
     * @param query [Delete] to execute.
     * @return [TupleIterator] containing the query response.
     */
    fun delete(query: Delete): TupleIterator = this.delete(query.builder.build())

    /**
     * Creates a new schema through this [SimpleClient].
     *
     * @param message [CottontailGrpc.CreateSchemaMessage] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun create(message: CottontailGrpc.CreateSchemaMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DDLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.createSchema(message), inner) { iterator, success ->
                if (message.metadata.transactionId <= 0L) {
                    if (success) {
                        this.commit(iterator.transactionId)
                    } else {
                        this.rollback(iterator.transactionId)
                    }
                }
            }
        }
    }

    /**
     * Creates a new schema through this [SimpleClient].
     *
     * @param message [CreateSchema] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun create(message: CreateSchema): TupleIterator = this.create(message.builder.build())

    /**
     * Creates a new entity through this [SimpleClient].
     *
     * @param message [CottontailGrpc.CreateEntityMessage] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun create(message: CottontailGrpc.CreateEntityMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DDLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.createEntity(message), inner) { iterator, success ->
                if (message.metadata.transactionId <= 0L) {
                    if (success) {
                        this.commit(iterator.transactionId)
                    } else {
                        this.rollback(iterator.transactionId)
                    }
                }
            }
        }
    }

    /**
     * Creates a new entity through this [SimpleClient].
     *
     * @param message [CreateEntity] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun create(message: CreateEntity): TupleIterator = this.create(message.builder.build())

    /**
     * Creates a new index through this [SimpleClient].
     *
     * @param message [CottontailGrpc.CreateIndexMessage] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun create(message: CottontailGrpc.CreateIndexMessage): TupleIterator = this.context.call {
        val stub = DDLGrpc.newBlockingStub(this.channel)
        val inner = Context.current().withCancellation()
        inner.call {
            TupleIteratorImpl(stub.createIndex(message), inner) { iterator, success ->
                if (message.metadata.transactionId <= 0L) {
                    if (success) {
                        this.commit(iterator.transactionId)
                    } else {
                        this.rollback(iterator.transactionId)
                    }
                }
            }
        }
    }

    /**
     * Creates a new index through this [SimpleClient].
     *
     * @param message [CreateIndex] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun create(message: CreateIndex): TupleIterator = this.create(message.builder.build())

    /**
     * Drops a schema through this [SimpleClient].
     *
     * @param message [CottontailGrpc.DropSchemaMessage] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun drop(message: CottontailGrpc.DropSchemaMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DDLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.dropSchema(message), inner) { iterator, success ->
                if (message.metadata.transactionId <= 0L) {
                    if (success) {
                        this.commit(iterator.transactionId)
                    } else {
                        this.rollback(iterator.transactionId)
                    }
                }
            }
        }
    }

    /**
     * Drops a schema through this [SimpleClient].
     *
     * @param message [CreateIndex] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun drop(message: DropSchema): TupleIterator = this.drop(message.builder.build())

    /**
     * Drops an entity through this [SimpleClient].
     *
     * @param message [CottontailGrpc.DropEntityMessage] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun drop(message: CottontailGrpc.DropEntityMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DDLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.dropEntity(message), inner) { iterator, success ->
                if (message.metadata.transactionId <= 0L) {
                    if (success) {
                        this.commit(iterator.transactionId)
                    } else {
                        this.rollback(iterator.transactionId)
                    }
                }
            }
        }
    }

    /**
     * Drops an entity through this [SimpleClient].
     *
     * @param message [DropEntity] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun drop(message: DropEntity): TupleIterator = this.drop(message.builder.build())

    /**
     * Drops an index through this [SimpleClient].
     *
     * @param message [CottontailGrpc.DropIndexMessage] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun drop(message: CottontailGrpc.DropIndexMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DDLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.dropIndex(message), inner) { iterator, success ->
                if (message.metadata.transactionId <= 0L) {
                    if (success) {
                        this.commit(iterator.transactionId)
                    } else {
                        this.rollback(iterator.transactionId)
                    }
                }
            }
        }
    }

    /**
     * Drops an index through this [SimpleClient].
     *
     * @param message [DropIndex] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun drop(message: DropIndex): TupleIterator = this.drop(message.builder.build())

    /**
     * Lists all schemas through this [SimpleClient].
     *
     * @param message [CottontailGrpc.ListSchemaMessage] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun list(message: CottontailGrpc.ListSchemaMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DDLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.listSchemas(message), inner) { iterator, success ->
                if (message.metadata.transactionId <= 0L) {
                    if (success) {
                        this.commit(iterator.transactionId)
                    } else {
                        this.rollback(iterator.transactionId)
                    }
                }
            }
        }
    }

    /**
     * Lists all schemas through this [SimpleClient].
     *
     * @param message [ListSchemas] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun list(message: ListSchemas): TupleIterator = this.list(message.builder.build())

    /**
     * Lists all entities in a schema through this [SimpleClient].
     *
     * @param message [CottontailGrpc.ListEntityMessage] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun list(message: CottontailGrpc.ListEntityMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DDLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.listEntities(message), inner) { iterator, success ->
                if (message.metadata.transactionId <= 0L) {
                    if (success) {
                        this.commit(iterator.transactionId)
                    } else {
                        this.rollback(iterator.transactionId)
                    }
                }
            }
        }
    }

    /**
     * Lists all entities in a schema through this [SimpleClient].
     *
     * @param message [ListEntities] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun list(message: ListEntities): TupleIterator = this.list(message.builder.build())

    /**
     * Lists detailed information about an entity through this [SimpleClient].
     *
     * @param message [CottontailGrpc.EntityDetailsMessage] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun about(message: CottontailGrpc.EntityDetailsMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DDLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.entityDetails(message), inner) { iterator, success ->
                if (message.metadata.transactionId <= 0L) {
                    if (success) {
                        this.commit(iterator.transactionId)
                    } else {
                        this.rollback(iterator.transactionId)
                    }
                }
            }
        }
    }

    /**
     * Lists detailed information about an entity through this [SimpleClient].
     *
     * @param message [AboutEntity] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun about(message: AboutEntity): TupleIterator = this.about(message.builder.build())

    /**
     * Truncates the given entity through this [SimpleClient].
     *
     * @param message [TruncateEntity] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun truncate(message: TruncateEntity): TupleIterator = this.truncate(message.builder.build())

    /**
     * Truncates the given entity through this [SimpleClient].
     *
     * @param message [CottontailGrpc.OptimizeEntityMessage] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun truncate(message: CottontailGrpc.TruncateEntityMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DDLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.truncateEntity(message), inner) { iterator, success ->
                if (message.metadata.transactionId <= 0L) {
                    if (success) {
                        this.commit(iterator.transactionId)
                    } else {
                        this.rollback(iterator.transactionId)
                    }
                }
            }
        }
    }

    /**
     * Optimizes an entity through this [SimpleClient].
     *
     * @param message [CottontailGrpc.OptimizeEntityMessage] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun optimize(message: CottontailGrpc.OptimizeEntityMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DDLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.optimizeEntity(message), inner) { iterator, success ->
                if (message.metadata.transactionId <= 0L) {
                    if (success) {
                        this.commit(iterator.transactionId)
                    } else {
                        this.rollback(iterator.transactionId)
                    }
                }
            }
        }
    }

    /**
     * Optimizes an entity through this [SimpleClient].
     *
     * @param message [OptimizeEntity] to execute.
     * @return [TupleIterator]
     */
    fun optimize(message: OptimizeEntity): TupleIterator = this.optimize(message.builder.build())

    /**
     * Pings this Cottontail DB instance. The method returns true on success and false otherwise.
     *
     * @return true on success, false otherwise.
     */
    fun ping(): Boolean = try {
        DQLGrpc.newBlockingStub(this.channel).ping(Empty.getDefaultInstance())
        true
    } catch (e: StatusRuntimeException) {
        false
    }

    /**
     * Closes this [SimpleClient].
     */
    override fun close() {
        if (!this.context.isCancelled) {
            this.context.cancel(CancellationException("Cottontail DB client was closed by the user."))
        }
    }
}