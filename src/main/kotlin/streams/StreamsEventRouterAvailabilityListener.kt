package streams

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.kernel.availability.AvailabilityListener
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.internal.LogService
import streams.config.StreamsConfig
import streams.utils.SystemUtils

class StreamsEventRouterAvailabilityListener(private val db: GraphDatabaseAPI,
                                             private val databaseManagementService: DatabaseManagementService,
                                             private val configuration: StreamsConfig,
                                             private val log: LogService) : AvailabilityListener {
    private val streamsLog = log.getUserLog(StreamsEventRouterAvailabilityListener::class.java)
    private var streamsConstraintsService: StreamsConstraintsService? = null
    private var txHandler: StreamsTransactionEventHandler? = null
    private var streamHandler: PersistentActionEventRouter? = null

    private val mutex = Mutex()


    private fun registerTransactionEventHandler() = runBlocking {
        mutex.withLock {
            configuration.loadStreamsConfiguration()
            streamsLog.info("Initialising the Streams Source module")
            if (streamsConstraintsService == null) {
                streamsConstraintsService = StreamsConstraintsService(db)
            }
            streamHandler = PersistentActionEventRouter(log)
            txHandler = StreamsTransactionEventHandler(streamHandler!!, streamsConstraintsService!!)
            streamHandler!!.start()
            streamsConstraintsService!!.start()
            databaseManagementService.registerTransactionEventListener(db.databaseName(), txHandler)
            streamsLog.info("Streams Source transaction handler initialised")
        }
    }

    private fun unregisterTransactionEventHandler() = runBlocking {
        mutex.withLock {
            SystemUtils.ignoreExceptions({
                streamHandler?.stop()
                databaseManagementService.unregisterTransactionEventListener(db.databaseName(), txHandler)
            }, UninitializedPropertyAccessException::class.java, IllegalStateException::class.java)
        }
    }

    override fun available() {
        val whenAvailable = {
            registerTransactionEventHandler()
        }
        val systemDbWaitTimeout = configuration.getSystemDbWaitTimeout()
        val whenNotAvailable = {
            streamsLog.info("""
                                |Cannot start Streams Source module because database ${SystemUtils.SYSTEM_DATABASE_NAME} 
                                |is not available after $systemDbWaitTimeout ms
                            """.trimMargin())
        }
        // wait for the system db became available
        SystemUtils.executeWhenSystemDbIsAvailable(databaseManagementService, configuration, whenAvailable, whenNotAvailable)
    }

    override fun unavailable() {
        unregisterTransactionEventHandler()
    }
}