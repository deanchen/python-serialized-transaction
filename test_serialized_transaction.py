import pytest
import psycopg2
import psycopg2.extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from serialized_transaction \
    import (SerializedTransaction, InfiniteRollbackError, OperationFailure)


DATABASE = '__serialized_transaction_test_db'
USER = 'postgres'
PASSWORD = None


class TestSerializedTransaction(object):
    @classmethod
    def setup_class(cls):
        """ setup any state specific to the execution of the given module."""
        cls.management_conn = psycopg2.connect(user=USER, password=PASSWORD)
        cls.management_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = cls.management_conn.cursor()
        cur.execute('DROP DATABASE IF EXISTS ' + DATABASE)

        cur.execute('CREATE DATABASE ' + DATABASE)
        cur.close()

        cls.conn = psycopg2.connect(database=DATABASE, user=USER,
                                    password=PASSWORD)
        cls.st = SerializedTransaction(cls.conn)

    @classmethod
    def teardown_class(cls):
        conn = psycopg2.connect(user=USER, password=PASSWORD)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute('DROP DATABASE IF EXISTS ' + DATABASE)
        cur.close()
        conn.close()

    def setup_method(self, method):
        cur = self.conn.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            DROP TABLE IF EXISTS table1;
            CREATE TABLE table1 (
                id  SERIAL PRIMARY KEY,
                column1   TEXT
            );
            DROP TABLE IF EXISTS table2;
            CREATE TABLE table2 (
                id  SERIAL PRIMARY KEY
            );
            INSERT INTO table1 (column1) VALUES ('a');
            INSERT INTO table1 (column1) VALUES ('b');
        """)
        self.conn.commit()

    def test_basic_query(self):
        select_result = self.st.execute_op(_select_row_a)
        assert select_result == 'a'

    def test_basic_query_with_args(self):
        select_result = self.st.execute_op(_select_row_arg, 'a')
        assert select_result == 'a'

        select_result = self.st.execute_op(_select_row_arg, 'b')
        assert select_result == 'b'

    def test_basic_query_with_kwargs(self):
        select_result = self.st.execute_op(_select_row_arg, column1='a')
        assert select_result == 'a'

        select_result = self.st.execute_op(_select_row_arg, column1='b')
        assert select_result == 'b'

    def test_basic_operation_failure(self):
        with pytest.raises(OperationFailure):
            self.st.execute_op(_no_action)

    def test_infinite_retry(self):
        with pytest.raises(InfiniteRollbackError):
            self.st.execute_op(_raise_serialization_error_op)

        with pytest.raises(InfiniteRollbackError):
            self.st.execute_op(_raise_deadlock_op)

    def test_close_connection(self):
        self.st.close_connection()


def _select_row_a(cur):
    cur.execute("""
        SELECT * from table1 where column1 = 'a';
    """)
    return cur.fetchone()['column1']


def _select_row_arg(cur, column1):
    cur.execute("""
        SELECT * from table1 where column1 = %s;
    """, [column1])
    return cur.fetchone()['column1']


def _no_action(cur):
    cur.execute("""
        SELECT * from table1 where column1 = 'a';
    """)
    raise OperationFailure


def _raise_serialization_error_op(cur):
    cur.execute("""
        DO $$BEGIN RAISE EXCEPTION 'Serialization error'
        USING ERRCODE = 'serialization_failure'; END $$;
    """)


def _raise_deadlock_op(cur):
    cur.execute("""
        DO $$BEGIN RAISE EXCEPTION 'Deadlock error'
        USING ERRCODE = 'deadlock_detected'; END $$;
    """)
