package pe.com.belcorp.datalake.utils.monitoring

import java.util.UUID

import com.amazonaws.services.lambda.AWSLambdaClientBuilder
import com.amazonaws.services.lambda.model.{InvocationType, InvokeRequest}
import pe.com.belcorp.datalake.utils.Params
import play.api.libs.json.Json

/**
  * Callback lambda with status update for the job
  */
object Callback {
  def report(params: Params, status: String, sentinel: Option[String] = None): Unit = {
    if (params.lambdaCallback.isDefined) {
      val lambda = AWSLambdaClientBuilder.defaultClient()
      val payload = Json.obj(
        "Step" -> params.stepId(),
        "Status" -> status,
        "Task" -> params.asTaskJson,
        "Sentinel" -> sentinel
      ).toString()

      lambda.invoke(new InvokeRequest()
        .withFunctionName(params.lambdaCallback())
        .withInvocationType(InvocationType.Event)
        .withPayload(payload))
    }
  }

  def reportOn(params: Params)(f: => Any): Unit = {
    val sentinel = Some(UUID.randomUUID().toString)
    
    try {
      f
      report(params, "COMPLETED", sentinel)
    } catch {
      case e: Exception =>
        report(params, "FAILED", sentinel)
        throw e
    }
  }
}
