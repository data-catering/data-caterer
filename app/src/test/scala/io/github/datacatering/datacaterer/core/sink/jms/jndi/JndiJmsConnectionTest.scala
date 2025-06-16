package io.github.datacatering.datacaterer.core.sink.jms.jndi

import org.scalatest.funsuite.AnyFunSuiteLike

import javax.naming.Context

class JndiJmsConnectionTest extends AnyFunSuiteLike {

  test("Can get all required connection properties for JMS") {
    val connectionConfig = Map(
      "initialContextFactory" -> "com.solacesystems.jndi.SolJNDIInitialContextFactory",
      "connectionFactory" -> "/jms/cf/default",
      "url" -> "smf://localhost:55555",
      "vpnName" -> "default",
      "user" -> "admin",
      "password" -> "admin"
    )

    val res = new JndiJmsConnection(connectionConfig).getConnectionProperties
    assertResult(5)(res.size())
    assert(res.containsKey(Context.INITIAL_CONTEXT_FACTORY))
    assertResult(connectionConfig("initialContextFactory"))(res.getProperty(Context.INITIAL_CONTEXT_FACTORY))
    assert(res.containsKey(Context.SECURITY_PRINCIPAL))
    assertResult(connectionConfig("user") + "@" + connectionConfig("vpnName"))(res.getProperty(Context.SECURITY_PRINCIPAL))
    assert(res.containsKey(Context.SECURITY_CREDENTIALS))
    assertResult(connectionConfig("password"))(res.getProperty(Context.SECURITY_CREDENTIALS))
    assert(res.containsKey(Context.PROVIDER_URL))
    assertResult(connectionConfig("url"))(res.getProperty(Context.PROVIDER_URL))
  }
}
