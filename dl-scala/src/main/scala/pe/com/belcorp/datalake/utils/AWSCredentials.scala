package pe.com.belcorp.datalake.utils

import com.amazonaws.auth.{AWSSessionCredentials, InstanceProfileCredentialsProvider}

case class AWSCredentials(token: String, accessKey: String, secretKey: String)

object AWSCredentials {
  def fetch(): AWSCredentials = {
    val provider = InstanceProfileCredentialsProvider.getInstance()
    val credentials = provider.getCredentials.asInstanceOf[AWSSessionCredentials]

    AWSCredentials(
      credentials.getSessionToken, credentials.getAWSAccessKeyId, credentials.getAWSSecretKey)
  }
}
