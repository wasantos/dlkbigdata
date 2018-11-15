package pe.com.belcorp.datalake.utils

import java.sql.Connection

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.jooq.impl.DSL
import org.jooq.{DSLContext, SQLDialect}

class DB(jdbcUrl: String) {
  // Force loading of JDBC driver
  Class.forName("com.amazon.redshift.jdbc.Driver")

  private val connectionPool: HikariDataSource = initConnectionPool(jdbcUrl)

  type Action[T] = Connection => T

  def checkout[T](action: Action[T]): T = {
    val conn = connectionPool.getConnection

    try {
      action(conn)
    } finally {
      conn.close()
    }
  }

  def transaction[T](action: Action[T]): T = {
    checkout { conn =>
      DB.transaction(conn)(action)
    }
  }

  def close(): Unit = connectionPool.close()

  private def initConnectionPool(jdbcUrl: String): HikariDataSource = {
    val config = new HikariConfig()
    config.setJdbcUrl(jdbcUrl)

    new HikariDataSource(config)
  }
}

object DB {
  def dsl(conn: Connection): DSLContext = DSL.using(conn, SQLDialect.POSTGRES)

  def transaction[T](conn: Connection)(action: Connection => T): T = {
    val oldValue = conn.getAutoCommit
    conn.setAutoCommit(false)

    try {
      val returnValue = action(conn)
      conn.commit()
      returnValue
    } catch {
      case e: Throwable => conn.rollback(); throw e
    } finally {
      conn.setAutoCommit(oldValue)
    }
  }

  def convertInlinedParameters(sql: String, params: Map[String, String]): String = {
    if(params.isEmpty) return sql

    val regex = """\?(\w+)""".r

    regex.replaceAllIn(sql, matchData => {
      params.getOrElse(matchData.group(1), 'none) match {
        case 'none => matchData.group(0)
        case s: String => s
      }
    })
  }

  def convertNamedParameters(sql: String, params: Map[String, Any]): (String, Array[AnyRef]) = {
    if(params.isEmpty) {
      return (sql, Array.empty[AnyRef])
    }

    val regex = """\:(\w+)""".r
    val names = regex.findAllIn(sql).matchData.map(_.group(1)).toList

    val newSql = regex.replaceAllIn(sql, matchData => {
      params.getOrElse(matchData.group(1), 'none) match {
        case 'none => matchData.group(0)
        case s: Seq[_] => (1 to s.length).map(_ => "?").mkString(", ")
        case _ => "?"
      }
    })

    val values = names.flatMap {
      params.getOrElse(_, 'none) match {
        case 'none => Seq.empty
        case s: Seq[_] => s
        case o: Any => Seq(o)
      }
    }.map(_.asInstanceOf[AnyRef]).toArray

    (newSql, values)
  }

  implicit class ExtendedDSLContext(val dsl: DSLContext) extends AnyVal {
    def executeNamed(sql: String,
                     bindings: Map[String, Any] = Map.empty,
                     inlined: Map[String, String] = Map.empty): Int = {
      val withInlined = convertInlinedParameters(sql, inlined)
      val (statement, params) = convertNamedParameters(withInlined, bindings)
      dsl.execute(statement, params: _*)
    }
  }

}
