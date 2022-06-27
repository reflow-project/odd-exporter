package io.piveau.consus

import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import org.junit.jupiter.api.*
import org.junit.jupiter.api.extension.ExtendWith

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension::class)
class SupersetTest {

    @BeforeEach
    fun setup(vertx: Vertx, testContext: VertxTestContext) {
        vertx.deployVerticle(
            SupersetVerticle(), DeploymentOptions()
                .setWorker(true)
                .setConfig(
                    JsonObject()
                        .put("postgresqlAddress", "postgresql://superset:superset@localhost:20000/superset")
                ))
            .onSuccess { testContext.completeNow() }
            .onFailure { testContext.failNow(it) }
    }
}
