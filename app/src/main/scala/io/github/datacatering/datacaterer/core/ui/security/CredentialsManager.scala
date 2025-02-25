package io.github.datacatering.datacaterer.core.ui.security

import io.github.datacatering.datacaterer.core.ui.model.CredentialsRequest

trait CredentialsManager {

  def validateCredentials(credentialsRequest: CredentialsRequest): String
}
