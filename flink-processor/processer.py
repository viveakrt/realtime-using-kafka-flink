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
        CREATE TABLE sales_usd (
            eventType STRING,
            source STRING,
            timestamp TIMESTAMP(3),
            tableId STRING,
            issueId STRING,
            issueType STRING,
            severity STRING,
            description STRING,
            metadata STRUCT<
                detectedBy STRING,
                detectedAt TIMESTAMP(3),
                affectedColumns ARRAY<STRING>,
                resolutionStatus STRING
            >
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sales-usd',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'sales-usd-group',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601',
            'json.ignore-parse-errors' = 'true'
        )
    """

    tbl_env.execute_sql(src_ddl)

    # Create and initiate loading of source Table
    tbl = tbl_env.from_path('sales_usd')

    print('\nSource Schema')
    tbl.print_schema()

    sql = """
        SELECT
          event.seller_id,
          TUMBLE_END(event.timestamp, INTERVAL '60' SECONDS) AS window_end,
          CAST(SUM(event.amount_usd) * 0.85 AS DECIMAL(10, 2)) AS window_sales
        FROM (
          VALUES
            (%(seller_id)s, TIMESTAMP '%(timestamp)s', %(amount_usd)s)
        ) AS event(seller_id, timestamp, amount_usd)
        GROUP BY
          TUMBLE(event.timestamp, INTERVAL '60' SECONDS),
          event.seller_id
    """ % {
        'seller_id': 1,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'amount_usd': 100.0,
    }
    revenue_tbl = tbl_env.sql_query(sql)

    print('\nProcess Sink Schema')
    revenue_tbl.print_schema()

    ###############################################################
    # Create Kafka Sink Table
    ###############################################################
    sink_ddl = """
        CREATE TABLE sales_euros (
            seller_id VARCHAR,
            window_end TIMESTAMP(3),
            window_sales DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sales-euros',
            'properties.bootstrap.servers' = 'localhost:9092',
            'format' = 'json'
        )
    """
    tbl_env.execute_sql(sink_ddl)

    # Write time windowed aggregations to sink table
    revenue_tbl.execute_insert('sales_euros').wait()

    tbl_env.execute('windowed-sales-euros')

if __name__ == '__main__':
    main()
