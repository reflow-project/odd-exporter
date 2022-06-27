package io.piveau

import io.piveau.consus.ExportingSupersetVerticle
import io.piveau.pipe.connector.PipeConnector
import io.vertx.core.DeploymentOptions
import io.vertx.core.Launcher
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.ext.web.handler.CorsHandler
import io.vertx.ext.web.handler.StaticHandler
import io.vertx.ext.web.openapi.RouterBuilder
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.FlowPreview
import org.slf4j.LoggerFactory


class MainVerticle : CoroutineVerticle() {

    val log = LoggerFactory.getLogger(javaClass)

    @FlowPreview
    override suspend fun start() {
        vertx.deployVerticle(ExportingSupersetVerticle::class.java, DeploymentOptions().setWorker(true).setMaxWorkerExecuteTime(3000000)).await()
        val connector = PipeConnector.create(vertx, DeploymentOptions()).await()
        connector.publishTo(ExportingSupersetVerticle.PIPE_ADDRESS)

    }

fun main(args: Array<String>) {
    Launcher.executeCommand("run", *(args.plus(MainVerticle::class.java.name)))
}

}