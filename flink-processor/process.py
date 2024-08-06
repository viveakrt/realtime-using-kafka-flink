import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def main():
    # Create streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()

    settings = EnvironmentSettings.new_instance() \
                                  .in_streaming_mode() \
                                  .use_blink_planner() \
                                  .build()

    # Create table environment
    tbl_env = StreamTableEnvironment.create(stream_execution_environment=env,
                                            environment_settings=settings)

    # Add Kafka connector dependency
    kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                             'flink-sql-connector-kafka_2.11-1.13.0.jar')

    tbl_env.get_config() \
           .get_configuration() \
           .set_string("pipeline.jars", f"file://{kafka_jar}")

    src_ddl = """
        CREATE TABLE health_issues (
            eventType VARCHAR,
            source VARCHAR,
            timestamp TIMESTAMP(3),
            tableId VARCHAR,
            issueId VARCHAR,
            issueType VARCHAR,
            severity VARCHAR,
            description VARCHAR,
            detectedBy VARCHAR,
            detectedAt TIMESTAMP(3),
            affectedColumns ARRAY<VARCHAR>,
            resolutionStatus VARCHAR,
            proctime AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'health-issues',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'health-issues-group',
            'format' = 'json'
        )
    """

    tbl_env.execute_sql(src_ddl)

    # Create and initiate loading of source Table
    tbl = tbl_env.from_path('health_issues')

    print('\nSource Schema')
    tbl.print_schema()

    sql = """
        SELECT
          eventType,
          TUMBLE_END(proctime, INTERVAL '60' SECONDS) AS window_end,
          COUNT(issueId) AS issue_count
        FROM health_issues
        GROUP BY
          TUMBLE(proctime, INTERVAL '60' SECONDS),
          eventType
    """
    issues_tbl = tbl_env.sql_query(sql)

    print('\nProcess Sink Schema')
    issues_tbl.print_schema()

    sink_ddl = """
        CREATE TABLE issue_summary (
            eventType VARCHAR,
            window_end TIMESTAMP(3),
            issue_count BIGINT
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'issue-summary',
            'properties.bootstrap.servers' = 'localhost:9092',
            'format' = 'json'
        )
    """
    tbl_env.execute_sql(sink_ddl)

    issues_tbl.execute_insert('issue_summary').wait()

    tbl_env.execute('windowed-issue-summary')

if __name__ == '__main__':
    main()
