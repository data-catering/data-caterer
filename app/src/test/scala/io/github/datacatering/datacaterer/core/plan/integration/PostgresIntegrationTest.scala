package io.github.datacatering.datacaterer.core.plan.integration

//class PostgresIntegrationTest extends AnyFunSuite with TestContainerForAll {
//
//  override val containerDef = GenericContainer.Def(
//    "postgres:14.5",
//    env = Map("POSTGRES_USER" -> "postgres", "POSTGRES_PASSWORD" -> "postgres"),
//    exposedPorts = Seq(5432),
//    classpathResourceMapping = Seq(FileSystemBind("sample/sql/postgres/customer.sql", "/docker-entrypoint-initdb.d/customer.sql", BindMode.READ_ONLY))
//  )
//
//  ignore("Postgres: Can generate and validate") {
//    withContainers { postgresContainer =>
//
//    }
//  }
//
//}
