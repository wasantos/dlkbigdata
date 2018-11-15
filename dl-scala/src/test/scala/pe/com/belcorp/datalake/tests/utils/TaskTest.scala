package pe.com.belcorp.datalake.tests.utils

import java.time.{Clock, Instant, ZoneId}

import org.scalatest.FunSuite
import pe.com.belcorp.datalake.utils.monitoring.Task.Event
import play.api.libs.json.Json
import pe.com.belcorp.datalake.utils.Goodies.withClock
import pe.com.belcorp.datalake.utils.Params
import pe.com.belcorp.datalake.utils.monitoring.Task

class TaskTest extends FunSuite {
  test("Event#toJson") {
    val instant = Instant.now()
    val clock = Clock.fixed(instant, ZoneId.of("UTC"))
    val timestamp = Json.toJson(instant).toString()
    val params = new Params(baseParams)

    withClock(clock, { _ =>
      val expected = Json.parse(
        s"""
           |{
           |  "id": "abcdef",
           |  "status": "STARTED",
           |  "ok": true,
           |  "system": "sicc",
           |  "partitions": {
           |    "country": "pe",
           |    "year": "2018",
           |    "month": "05",
           |    "day": "05",
           |    "secs": "1234"
           |  },
           |  "timestamp": $timestamp,
           |  "payload": {
           |    "lines": 1,
           |    "errors": 2
           |  }
           |}
         """.stripMargin)

      val actual = Json.parse(
        Event("abcdef", "STARTED", "sicc", params.partitioningSpecification, ok = true,
          Json.obj("lines" -> 1, "errors" -> 2)
        ).toJson)

      assert(expected == actual)
    })
  }

  test("Task#id without executionId") {
    val instant = Instant.now()
    val clock = Clock.fixed(instant, ZoneId.of("UTC"))
    val millis = instant.toEpochMilli.toString

    withClock(clock, { _ =>
      val params = new Params(baseParams)
      val task = new Task("sicc", params)

      assert(task.id ==
        s"sicc:pt_country=pe/pt_year=2018/pt_month=05/pt_day=05/pt_secs=1234:$millis")
    })
  }

  test("Task#id with executionId") {
    val params = new Params(baseParams ++ Seq("--execution-id", "56789"))
    val task = new Task("sicc", params)

    assert(task.id ==
      s"sicc:pt_country=pe/pt_year=2018/pt_month=05/pt_day=05/pt_secs=1234:56789")
  }

  private val baseParams = Seq(
    "--country", "pe", "--year", "2018",
    "--month", "05", "--day", "05", "--secs", "1234"
  )
}
