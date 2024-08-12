from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment


def main():
    # Set up the Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    table_env = StreamTableEnvironment.create(env)

    # Define Kafka source table
    kafka_source_ddl = """
        CREATE TABLE kafka_source (
            eventType STRING,
            source STRING,
            tableId STRING,
            issueId STRING,
            issueType STRING,
            severity STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'monte-carlo-observation',
            'properties.bootstrap.servers' = 'localhost:9092',
            'format' = 'json'
        )
    """
    table_env.execute_sql(kafka_source_ddl)

    # Define PostgreSQL sink table
    postgres_sink_ddl = """
        CREATE TABLE issue_event (
            eventType STRING,
            source STRING,
            tableId STRING,
            issueId STRING,
            issueType STRING,
            severity STRING
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://localhost:5432/postgres',
            'table-name' = 'issue_event',
            'driver' = 'org.postgresql.Driver',
            'username' = 'postgres',
            'password' = 'postgres'
        )
    """
    table_env.execute_sql(postgres_sink_ddl)


    # Transfer data from Kafka to PostgreSQL
    table_env.sql_query("""
        INSERT INTO issue_event
        SELECT * FROM kafka_source
    """).execute_insert("sink").wait()

    # Execute the Flink job
    env.execute('Kafka to PostgreSQL Job')

if __name__ == '__main__':
    main()
