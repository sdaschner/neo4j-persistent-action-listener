package streams.utils

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.neo4j.dbms.api.DatabaseManagementService
import streams.config.StreamsConfig
import streams.extensions.getSystemDb

object SystemUtils {

    @JvmStatic val SYSTEM_DATABASE_NAME = "system"

    fun <T> ignoreExceptions(action: () -> T, vararg toIgnore: Class<out Throwable>): T? {
        return try {
            action()
        } catch (e: Throwable) {
            if (toIgnore.isEmpty()) {
                return null
            }
            return when (e::class.java) {
                in toIgnore -> null
                else -> throw e
            }
        }
    }

    fun executeWhenSystemDbIsAvailable(databaseManagementService: DatabaseManagementService,
                                       configuration: StreamsConfig,
                                       actionIfAvailable: () -> Unit,
                                       actionIfNotAvailable: (() -> Unit)?) {
        val systemDb = databaseManagementService.getSystemDb()
        val systemDbWaitTimeout = configuration.getSystemDbWaitTimeout()
        GlobalScope.launch(Dispatchers.IO) {
            if (systemDb.isAvailable(systemDbWaitTimeout)) {
                actionIfAvailable()
            } else if (actionIfNotAvailable != null) {
                actionIfNotAvailable()
            }
        }
    }

}