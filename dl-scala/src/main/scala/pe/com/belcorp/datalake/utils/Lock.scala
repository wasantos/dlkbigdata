package pe.com.belcorp.datalake.utils

import com.amazonaws.services.dynamodbv2._

/**
  * Wraps DynamoDB lock client for easier use
  */
class Lock(lockTable: String) {
  private val dynamodb = AmazonDynamoDBClientBuilder.defaultClient()
  private val locker = new AmazonDynamoDBLockClient(
    AmazonDynamoDBLockClientOptions.builder(dynamodb, lockTable).build())

  def acquire[T](lockName: String)(f: LockItem => T): T = {
    val lock = locker.acquireLock(AcquireLockOptions.builder(lockName).build())

    try {
      f(lock)
    } finally {
      locker.releaseLock(lock)
    }
  }
}

object Lock {
  private var _lock: Lock = null

  def init(lockTable: String): Unit = {
    _lock = new Lock(lockTable)
  }

  def acquire[T](lockName: String)(f: LockItem => T): T =
    _lock.acquire(lockName)(f)
}
