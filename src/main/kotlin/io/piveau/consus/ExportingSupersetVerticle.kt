package io.piveau.consus

import io.piveau.pipe.PipeContext
import io.vertx.config.ConfigRetriever
import io.vertx.core.DeploymentOptions
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.WebClient
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import java.io.ByteArrayInputStream
import kotlin.coroutines.CoroutineContext

class ExportingSupersetVerticle : CoroutineVerticle() {
    val log = LoggerFactory.getLogger(javaClass)

    override suspend fun start() {
        vertx.eventBus().consumer<PipeContext>(PIPE_ADDRESS) {
            launch(vertx.dispatcher() as CoroutineContext) {
                handlePipe(it)
            }
        }

        val config = ConfigRetriever.create(vertx).config.await()
        val client = WebClient.create(vertx)


        val supersetConfig = config.getJsonObject("PIVEAU_SUPERSET_CONFIG", JsonObject())
        vertx.deployVerticle(
            SupersetVerticle::class.java,
            DeploymentOptions().setConfig(supersetConfig).setWorker(true).setMaxWorkerExecuteTime(3000000)
        )

    }


    private fun handlePipe(message: Message<PipeContext>): Unit = with(message.body()) {
        loadData(stringData, dataInfo.getString("tableName"), dataInfo.getBoolean("replaceTable"), mimeType)
            .onSuccess { reply -> log.info("Dataset ${reply}: {}", dataInfo.getString("tableName")) }
            .onFailure { cause ->
                log.error(
                    "Import failure: {}",
                    dataInfo.getString("tableName"),
                    cause
                )
            }
        
    }

    private fun loadData(data: String, name: String, replaceTable: Boolean, mimeType: String?): Future<String> {
        val promise = Promise.promise<String>()
        try {
            val tableInfo = parseTransormedData(data)
            tableInfo
            .put("tableName", name)
            .put("replaceTable", replaceTable)

            vertx.eventBus().request<JsonObject>(SupersetVerticle.ADDRESS, tableInfo)
                .onSuccess {
                    log.info("Successfully imported: {}", name)
                    promise.complete(it.body().getString("status"))
                }
                .onFailure {
                    log.error("Failed to import: {}", name, it)
                    promise.fail(it)
                }


        } catch (e: Exception) {
            promise.fail(e)
        }
        return promise.future()
    }





    companion object {
        const val PIPE_ADDRESS = "io.piveau.pipe.consus.export.superset.pipe"
        const val UPLOAD_ADDRESS = "io.piveau.pipe.consus.export.superset.upload"
    }
}
