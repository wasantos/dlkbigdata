package pe.com.belcorp.datalake.main.old

import pe.com.belcorp.datalake.analytics.Model
import pe.com.belcorp.datalake.analytics.dwh.DWHModel
import pe.com.belcorp.datalake.utils.monitoring.Callback.reportOn
import pe.com.belcorp.datalake.utils.monitoring.Task
import pe.com.belcorp.datalake.utils.{DB, Lock, Params, SparkUtils}

object LoadModel {
  type Factory = (DB, Params) => Model

  val modelsAvailable: Map[String, Factory] = Map(
    "dwh" -> ((db, params) => new DWHModel(db, params))
  )

  def main(args: Array[String]): Unit = {
    val params = new Params(args)
    val manager = new DB(params.jdbc())

    // Not used, but avoids shutdown errors
    val spark = SparkUtils.getSparkSession()

    Lock.init(params.lockTable())

    try {
      reportOn(params) {
        Task.init(params.system.getOrElse("all"), params)
        Task.ifMonitoring { t => t.success("UPDATING_FUNCTIONAL") }

        if (params.processAll()) {
          for (model <- modelsAvailable.values.map(_ (manager, params))) {
            executeModel(model, params)
          }
        } else {
          val model = modelsAvailable(params.model())(manager, params)

          executeModel(model, params)
        }

        Task.ifMonitoring { t => t.success("FUNCTIONAL_UPDATED") }
      }
    } catch {
      case e: Exception =>
        Task.ifMonitoring { t =>
          t.failure("FUNCTIONAL_FAILED", e)
        }

        throw e
    } finally {
      spark.stop()
      manager.close()
    }
  }

  def executeModel(model: Model, params: Params): Unit = {
    val lockName = s"functionalCreateTable: ${model.getClass.getCanonicalName}"

    // Create operation should be isolated
    Lock.acquire(lockName) { _ =>
      if (params.create()) model.create()
    }

    // Update can be unlocked
    if (params.update()) model.update()
  }
}
