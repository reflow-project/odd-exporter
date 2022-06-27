package io.piveau.consus

import io.vertx.core.Future
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.auth.authentication.TokenCredentials
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.predicate.ResponsePredicate
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import io.vertx.pgclient.PgPool
import io.vertx.sqlclient.SqlClient
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import kotlin.coroutines.CoroutineContext

class SupersetVerticle : CoroutineVerticle() {

    val log = LoggerFactory.getLogger(javaClass)

    lateinit var sqlClient: SqlClient
    lateinit var client: WebClient

    private var tableNamePrefix = ""

    override suspend fun start() {

        log.info("Starting SupersetVerticle")
        vertx.eventBus().consumer<JsonObject>(ADDRESS) {
            launch(vertx.dispatcher() as CoroutineContext) {
                handleMessage(it)
            }
        }


        sqlClient = PgPool.pool(vertx, config.getString("postgresqlAddress", ""))
        client = WebClient.create(vertx)



        tableNamePrefix = config.getString("prefix", tableNamePrefix)
        log.info("Started SupersetVerticle")
    }

    private suspend fun handleMessage(message: Message<JsonObject>) {

        val tableInfo = message.body()
        val tableName = "$tableNamePrefix${tableInfo.getString("tableName")}"
        val replaceTable = tableInfo.getBoolean("replaceTable")
        val tableSchema = tableInfo.getJsonObject("tableSchema")
        val tableRows = tableInfo.getJsonArray("tableRows")
        val tableKeys = tableInfo.getJsonObject("tableKeys")



        val tuples = try {
            parseTableRows(tableRows, tableSchema)
        } catch (e: Exception) {
            message.fail(500, "Parsing and converting table info: ${e.message}")
            return
        }



        try {

            if(replaceTable){
                sqlClient.query(tableName.dropTableQuery()).execute().await()
            }
            sqlClient.query(tableName.createTableQuery(tableSchema.map, tableKeys)).execute().await()

            sqlClient
                .preparedQuery(tableName.insertIntoQuery(tableSchema.fieldNames(), tableKeys))
                .executeBatch(tuples)
                .onSuccess{
                    message.reply(JsonObject().put("status", "inserted into database"))
                    return@onSuccess
                }
                .onFailure { message.fail(500, "Export failure ${it.message}") }
            
        } catch (e: Exception) {
            message.fail(500, "PostgreSQL failure: ${e.message}")
        }
    }


    companion object {
        const val ADDRESS: String = "io.piveau.pipe.consus.superset.queue"
    }

}

private fun String.createTableQuery(columns: Map<String, Any>, keys: JsonObject): String {
    val buffer = StringBuffer("CREATE TABLE IF NOT EXISTS \"$this\" (")
    buffer.append(columns.entries.map { "\"${it.key}\" ${it.value}" }.joinToString(","))
    if(keys.containsKey("primary")){
        buffer.append("${keys.getString("primary")}")
    }
    if(keys.containsKey("foreign")){
        buffer.append("${keys.getString("foreign")}")
    }
    return buffer.append(");").toString()
}

private fun String.dropTableQuery(): String {
    val buffer = StringBuffer("DROP TABLE IF EXISTS \"$this\"")
    return buffer.append(";").toString()
}

private fun String.insertIntoQuery(columns: Set<String>, keys:JsonObject): String {
    if(keys.containsKey("primary")){
        return "INSERT INTO \"$this\" VALUES (${columns.mapIndexed { index, _ -> "$${index + 1}" }.joinToString(",")}) ON CONFLICT(id) DO UPDATE SET ${columns.mapIndexed{index, name -> "$name=$${index+1}"}.joinToString(",")};"
    }
    else{
        return "INSERT INTO \"$this\" VALUES (${columns.mapIndexed { index, _ -> "$${index + 1}" }.joinToString(",")});"
    }
    }
