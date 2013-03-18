import psycopg2
import psycopg2.extras
from psycopg2.extensions import (ISOLATION_LEVEL_SERIALIZABLE,
                                 TransactionRollbackError)
from contextlib import closing


class SerializedTransaction(object):
    def __init__(self, conn, max_retry=10):
        conn.set_isolation_level(ISOLATION_LEVEL_SERIALIZABLE)
        self.conn = conn
        self.max_retry = max_retry

    def execute_op(self, operation, *args, **kwargs):
        with closing(self.new_cursor()) as cur:
            for i in range(self.max_retry):
                try:
                    # attempt to execute transaction
                    final_result = operation(cur, *args, **kwargs)
                    self.conn.commit()

                    # break out of infinite loop in order to cleanup and return
                    # result on transaction success
                    break

                except TransactionRollbackError:
                    # retry transaction
                    self.conn.rollback()
                    continue

                except Exception:
                    self.conn.rollback()
                    raise
            else:
                raise InfiniteRollbackError

            return final_result

    def new_cursor(self):
        return self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def close_connection(self):
        self.conn.close()


class SerializedTransactionException(BaseException):
    def __init__(self, message):
        BaseException.__init__(self, message)


class OperationFailure(SerializedTransactionException):
    def __init__(self, message=None):
        SerializedTransactionException.__init__(self, message)


class StateFailure(SerializedTransactionException):
    def __init__(self, message=None):
        SerializedTransactionException.__init__(self, message)


class InfiniteRollbackError(SerializedTransactionException):
    def __init__(self, message=None):
        SerializedTransactionException.__init__(self, message)
