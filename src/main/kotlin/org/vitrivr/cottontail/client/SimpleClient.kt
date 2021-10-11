package org.vitrivr.cottontail.client

import com.google.protobuf.Empty
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.StatusRuntimeException
import org.vitrivr.cottontail.client.iterators.TupleIterator
import org.vitrivr.cottontail.client.iterators.SynchronousTupleIterator
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
 * @version 2.0.0
 */
class SimpleClient(private val channel: ManagedChannel) {

    /** Endpoint used for executing a query through Cottontail DB. */
    private val dql by lazy { DQLGrpc.newBlockingStub(this.channel)  }

    /** Endpoint used for managing data Cottontail DB. */
    private val dml by lazy { DMLGrpc.newBlockingStub(this.channel)  }

    /** Endpoint used for managing data Cottontail DB. */
    private val ddl by lazy { DDLGrpc.newBlockingStub(this.channel)  }

    /** Endpoint used for transaction management through Cottontail DB. */
    private val tx by lazy { TXNGrpc.newBlockingStub(this.channel)  }

    /**
     * Constructor to create a Cottontail DB client with a new [ManagedChannel].
     *
     * @param host Host IP address of the [SimpleClient]
     * @param port The port of the [SimpleClient]
     */
    constructor(host: String, port: Int): this(ManagedChannelBuilder.forAddress(host, port).usePlaintext().build())

    /**
     * Begins a new transaction through this [SimpleClient].
     *
     * @return The ID of the newly begun transaction.
     */
    fun begin(): Long = this.tx.begin(Empty.getDefaultInstance()).value

    /**
     * Commits a transaction through this [SimpleClient].
     *
     * @param txId The transaction ID to commit.
     */
    fun commit(txId: Long) {
        val tx = CottontailGrpc.TransactionId.newBuilder().setValue(txId).build()
        this.tx.commit(tx)
    }

    /**
     * Rolls back a transaction through this [SimpleClient].
     *
     * @param txId The transaction ID to rollback.
     */
    fun rollback(txId: Long) {
        val tx = CottontailGrpc.TransactionId.newBuilder().setValue(txId).build()
        this.tx.rollback(tx)
    }

    /**
     * Executes [CottontailGrpc.Query] through this [SimpleClient]
     *
     * @param query [CottontailGrpc.Query] to execute.
     * @return An [Iterator] iof [CottontailGrpc.QueryResponseMessage]
     */
    fun query(query: CottontailGrpc.QueryMessage): TupleIterator = SynchronousTupleIterator(this.dql.query(query))

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
    fun batchedQuery(query: CottontailGrpc.BatchedQueryMessage): TupleIterator = SynchronousTupleIterator(this.dql.batchQuery(query))

    /**
     * Explains [CottontailGrpc.Query] through this [SimpleClient]
     *
     * @param query [CottontailGrpc.Query] to executed.
     * @return An [Iterator] iof [CottontailGrpc.QueryResponseMessage]
     */
    fun explain(query: CottontailGrpc.QueryMessage): TupleIterator = SynchronousTupleIterator(this.dql.explain(query))

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
    fun insert(query: CottontailGrpc.InsertMessage): TupleIterator = SynchronousTupleIterator(this.dml.insert(query))

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
    fun insert(query: CottontailGrpc.BatchInsertMessage): TupleIterator = SynchronousTupleIterator(this.dml.insertBatch(query))

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
    fun update(query: CottontailGrpc.UpdateMessage): TupleIterator = SynchronousTupleIterator(this.dml.update(query))

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
    fun delete(query: CottontailGrpc.DeleteMessage): TupleIterator = SynchronousTupleIterator(this.dml.delete(query))

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
    fun create(message: CottontailGrpc.CreateSchemaMessage): TupleIterator = SynchronousTupleIterator(this.ddl.createSchema(message))

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
    fun create(message: CottontailGrpc.CreateEntityMessage): TupleIterator = SynchronousTupleIterator(this.ddl.createEntity(message))

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
    fun create(message: CottontailGrpc.CreateIndexMessage): TupleIterator = SynchronousTupleIterator(this.ddl.createIndex(message))

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
    fun drop(message: CottontailGrpc.DropSchemaMessage): TupleIterator = SynchronousTupleIterator(this.ddl.dropSchema(message))

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
    fun drop(message: CottontailGrpc.DropEntityMessage): TupleIterator = SynchronousTupleIterator(this.ddl.dropEntity(message))

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
    fun drop(message: CottontailGrpc.DropIndexMessage): TupleIterator = SynchronousTupleIterator(this.ddl.dropIndex(message))

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
    fun list(message: CottontailGrpc.ListSchemaMessage): TupleIterator = SynchronousTupleIterator(this.ddl.listSchemas(message))

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
    fun list(message: CottontailGrpc.ListEntityMessage): TupleIterator = SynchronousTupleIterator(this.ddl.listEntities(message))

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
    fun about(message: CottontailGrpc.EntityDetailsMessage): TupleIterator = SynchronousTupleIterator(this.ddl.entityDetails(message))

    /**
     * Lists detailed information about an entity through this [SimpleClient].
     *
     * @param message [AboutEntity] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun about(message: AboutEntity): TupleIterator = this.about(message.builder.build())

    /**
     * Optimizes an entity through this [SimpleClient].
     *
     * @param message [CottontailGrpc.OptimizeEntityMessage] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun optimize(message: CottontailGrpc.OptimizeEntityMessage): TupleIterator = SynchronousTupleIterator(this.ddl.optimizeEntity(message))

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
}