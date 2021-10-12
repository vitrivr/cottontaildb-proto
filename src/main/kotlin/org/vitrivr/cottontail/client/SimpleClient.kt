package org.vitrivr.cottontail.client

import com.google.protobuf.Empty
import io.grpc.Context
import io.grpc.ManagedChannel
import io.grpc.StatusRuntimeException
import org.vitrivr.cottontail.client.iterators.TupleIterator
import org.vitrivr.cottontail.client.iterators.TupleIteratorImpl
import org.vitrivr.cottontail.client.language.ddl.*
import org.vitrivr.cottontail.client.language.dml.BatchInsert
import org.vitrivr.cottontail.client.language.dml.Delete
import org.vitrivr.cottontail.client.language.dml.Insert
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.client.language.dml.Update
import org.vitrivr.cottontail.grpc.*

/**
 * A simple Cottontail DB client for querying and data management.
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
class SimpleClient(private val channel: ManagedChannel): AutoCloseable {

    /** Internal flag used to track closed state of this [SimpleClient]. */
    @Volatile
    private var closed: Boolean = false

    /** Context that can be used to cancel all queries that are currently being executed by this [SimpleClient]. */
    private val clientContext = Context.CancellableContext.current().withCancellation()

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
    fun commit(txId: Long) = this.clientContext.run {
        val tx = CottontailGrpc.Metadata.newBuilder().setTransactionId(txId).build()
        TXNGrpc.newBlockingStub(this.channel).commit(tx)
    }

    /**
     * Rolls back a transaction through this [SimpleClient].
     *
     * @param txId The transaction ID to rollback.
     */
    fun rollback(txId: Long) = this.clientContext.run {
        val tx = CottontailGrpc.Metadata.newBuilder().setTransactionId(txId).build()
        TXNGrpc.newBlockingStub(this.channel).rollback(tx)
    }

    /**
     * Kills and rolls back a transaction through this [SimpleClient].
     *
     * @param txId The transaction ID to kill and rollback.
     */
    fun kill(txId: Long) = this.clientContext.run {
        val tx = CottontailGrpc.Metadata.newBuilder().setTransactionId(txId).build()
        TXNGrpc.newBlockingStub(this.channel).kill(tx)
    }

    /**
     * Executes [CottontailGrpc.Query] through this [SimpleClient]
     *
     * @param query [CottontailGrpc.Query] to execute.
     * @return An [Iterator] iof [CottontailGrpc.QueryResponseMessage]
     */
    fun query(query: CottontailGrpc.QueryMessage): TupleIterator = this.clientContext.call {
        val stub = DQLGrpc.newBlockingStub(this.channel)
        TupleIteratorImpl(stub.query(query))
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
     * @param query [CottontailGrpc.Query] to execute.
     * @return An [Iterator] iof [CottontailGrpc.QueryResponseMessage]
     */
    fun batchedQuery(query: CottontailGrpc.BatchedQueryMessage): TupleIterator = this.clientContext.call {
        val stub = DQLGrpc.newBlockingStub(this.channel)
        TupleIteratorImpl(stub.batchQuery(query))
    }

    /**
     * Explains [CottontailGrpc.Query] through this [SimpleClient]
     *
     * @param query [CottontailGrpc.Query] to executed.
     * @return An [Iterator] iof [CottontailGrpc.QueryResponseMessage]
     */
    fun explain(query: CottontailGrpc.QueryMessage): TupleIterator = this.clientContext.call {
        val stub = DQLGrpc.newBlockingStub(this.channel)
        TupleIteratorImpl(stub.explain(query))
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
     * @param query [CottontailGrpc.InsertMessage] to execute.
     * @return [TupleIterator] containing the query response.
     */
    fun insert(query: CottontailGrpc.InsertMessage): TupleIterator = this.clientContext.call {
        val stub = DMLGrpc.newBlockingStub(this.channel)
        TupleIteratorImpl(stub.insert(query))
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
     * @param query [CottontailGrpc.BatchInsertMessage] to execute.
     * @return [TupleIterator] containing the query response.
     */
    fun insert(query: CottontailGrpc.BatchInsertMessage): TupleIterator = this.clientContext.call {
        val stub = DMLGrpc.newBlockingStub(this.channel)
        TupleIteratorImpl(stub.insertBatch(query))
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
     * @param query [CottontailGrpc.UpdateMessage] to execute.
     * @return [TupleIterator] containing the query response.
     */
    fun update(query: CottontailGrpc.UpdateMessage): TupleIterator = this.clientContext.call {
        val stub = DMLGrpc.newBlockingStub(this.channel)
        TupleIteratorImpl(stub.update(query))
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
     * @param query [CottontailGrpc.DeleteMessage] to execute.
     * @return [TupleIterator] containing the query response.
     */
    fun delete(query: CottontailGrpc.DeleteMessage): TupleIterator = this.clientContext.call {
        val stub = DMLGrpc.newBlockingStub(this.channel)
        TupleIteratorImpl(stub.delete(query))
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
    fun create(message: CottontailGrpc.CreateSchemaMessage): TupleIterator = this.clientContext.call {
        val stub = DDLGrpc.newBlockingStub(this.channel)
        TupleIteratorImpl(stub.createSchema(message))
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
    fun create(message: CottontailGrpc.CreateEntityMessage): TupleIterator = this.clientContext.call {
        val stub = DDLGrpc.newBlockingStub(this.channel)
        TupleIteratorImpl(stub.createEntity(message))
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
    fun create(message: CottontailGrpc.CreateIndexMessage): TupleIterator = this.clientContext.call {
        val stub = DDLGrpc.newBlockingStub(this.channel)
        TupleIteratorImpl(stub.createIndex(message))
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
    fun drop(message: CottontailGrpc.DropSchemaMessage): TupleIterator = this.clientContext.call {
        val stub = DDLGrpc.newBlockingStub(this.channel)
        TupleIteratorImpl(stub.dropSchema(message))
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
    fun drop(message: CottontailGrpc.DropEntityMessage): TupleIterator = this.clientContext.call {
        val stub = DDLGrpc.newBlockingStub(this.channel)
        TupleIteratorImpl(stub.dropEntity(message))
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
    fun drop(message: CottontailGrpc.DropIndexMessage): TupleIterator = this.clientContext.call {
        val stub = DDLGrpc.newBlockingStub(this.channel)
        TupleIteratorImpl(stub.dropIndex(message))
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
    fun list(message: CottontailGrpc.ListSchemaMessage): TupleIterator = this.clientContext.call {
        val stub = DDLGrpc.newBlockingStub(this.channel)
        TupleIteratorImpl(stub.listSchemas(message))
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
    fun list(message: CottontailGrpc.ListEntityMessage): TupleIterator = this.clientContext.call {
        val stub = DDLGrpc.newBlockingStub(this.channel)
        TupleIteratorImpl(stub.listEntities(message))
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
    fun about(message: CottontailGrpc.EntityDetailsMessage): TupleIterator = this.clientContext.call {
        val stub = DDLGrpc.newBlockingStub(this.channel)
        TupleIteratorImpl(stub.entityDetails(message))
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
    fun truncate(message: CottontailGrpc.TruncateEntityMessage): TupleIterator = this.clientContext.call {
        val stub = DDLGrpc.newBlockingStub(this.channel)
        TupleIteratorImpl(stub.truncateEntity(message))
    }

    /**
     * Optimizes an entity through this [SimpleClient].
     *
     * @param message [CottontailGrpc.OptimizeEntityMessage] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun optimize(message: CottontailGrpc.OptimizeEntityMessage): TupleIterator = this.clientContext.call {
        val stub = DDLGrpc.newBlockingStub(this.channel)
        TupleIteratorImpl(stub.optimizeEntity(message))
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
        if (!this.clientContext.isCancelled) {
            this.clientContext.close()
        }
    }
}