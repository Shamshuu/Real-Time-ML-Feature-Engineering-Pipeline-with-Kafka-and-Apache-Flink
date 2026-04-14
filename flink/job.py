import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def run_job():
    # 1. Setup Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1) # For demo purposes
    
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    user_events_topic = os.getenv('USER_EVENTS_TOPIC', 'user-events')
    content_metadata_topic = os.getenv('CONTENT_METADATA_TOPIC', 'content-metadata')
    feature_store_topic = os.getenv('FEATURE_STORE_TOPIC', 'feature-store')

    t_env.get_config().set("pipeline.name", "Real-Time-ML-Feature-Engineering")
    t_env.get_config().set("table.exec.source.idle-timeout", "10s")

    # 2. Define Source Tables
    # user-events Source
    # We use STRING for timestamp and TO_TIMESTAMP with explicit format
    t_env.execute_sql(f"""
        CREATE TABLE user_events (
            user_id STRING,
            content_id STRING,
            event_type STRING,
            dwell_time_ms INT,
            `timestamp` STRING,
            event_time AS TO_TIMESTAMP(REPLACE(REPLACE(`timestamp`, 'Z', ''), 'T', ' '), 'yyyy-MM-dd HH:mm:ss.SSS'),
            WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{user_events_topic}',
            'properties.bootstrap.servers' = '{kafka_bootstrap}',
            'properties.group.id' = 'flink-feature-eng',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """)

    # content-metadata Source (Compacted)
    t_env.execute_sql(f"""
        CREATE TABLE content_metadata (
            content_id STRING,
            category STRING,
            creator_id STRING,
            publish_timestamp STRING,
            PRIMARY KEY (content_id) NOT ENFORCED
        ) WITH (
            'connector' = 'upsert-kafka',
            'topic' = '{content_metadata_topic}',
            'properties.bootstrap.servers' = '{kafka_bootstrap}',
            'key.format' = 'raw',
            'value.format' = 'json'
        )
    """)

    # 3. Define Sink Table
    # Note: feature_value is STRING to accommodate different types of features
    t_env.execute_sql(f"""
        CREATE TABLE feature_store (
            entity_id STRING,
            feature_name STRING,
            feature_value STRING,
            computed_at TIMESTAMP(3),
            PRIMARY KEY (entity_id, feature_name) NOT ENFORCED
        ) WITH (
            'connector' = 'upsert-kafka',
            'topic' = '{feature_store_topic}',
            'properties.bootstrap.servers' = '{kafka_bootstrap}',
            'key.format' = 'json',
            'value.format' = 'json'
        )
    """)

    # 4. Feature Computations with StatementSet
    statement_set = t_env.create_statement_set()

    # Feature 1: User click_rate (1h Tumbling Window)
    statement_set.add_insert_sql("""
        INSERT INTO feature_store
        SELECT 
            user_id as entity_id,
            'click_rate' as feature_name,
            CAST(CAST(SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS STRING) as feature_value,
            TUMBLE_END(event_time, INTERVAL '1' HOUR) as computed_at
        FROM user_events
        GROUP BY user_id, TUMBLE(event_time, INTERVAL '1' HOUR)
    """)

    # Feature 2: User avg_dwell_time (1h Tumbling Window)
    statement_set.add_insert_sql("""
        INSERT INTO feature_store
        SELECT 
            user_id as entity_id,
            'avg_dwell_time' as feature_name,
            CAST(AVG(dwell_time_ms) AS STRING) as feature_value,
            TUMBLE_END(event_time, INTERVAL '1' HOUR) as computed_at
        FROM user_events
        GROUP BY user_id, TUMBLE(event_time, INTERVAL '1' HOUR)
    """)

    # Feature 3: Content engagement_rate (15m Sliding Window, 5m Slide)
    statement_set.add_insert_sql("""
        INSERT INTO feature_store
        SELECT 
            content_id as entity_id,
            'engagement_rate' as feature_name,
            CAST(
                CASE WHEN SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) = 0 THEN 0.0
                ELSE CAST(SUM(CASE WHEN event_type IN ('like', 'share') THEN 1 ELSE 0 END) AS DOUBLE) / SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END)
                END AS STRING
            ) as feature_value,
            HOP_END(event_time, INTERVAL '5' MINUTE, INTERVAL '15' MINUTE) as computed_at
        FROM user_events
        GROUP BY content_id, HOP(event_time, INTERVAL '5' MINUTE, INTERVAL '15' MINUTE)
    """)

    # Feature 4: User category_affinity_score (Stream-Table Join, 1h Tumbling Window)
    statement_set.add_insert_sql("""
        INSERT INTO feature_store
        SELECT 
            CONCAT(u.user_id, ':', m.category) as entity_id,
            'category_affinity_count' as feature_name,
            CAST(COUNT(*) AS STRING) as feature_value,
            TUMBLE_END(u.event_time, INTERVAL '1' HOUR) as computed_at
        FROM user_events u
        JOIN content_metadata m ON u.content_id = m.content_id
        GROUP BY u.user_id, m.category, TUMBLE(u.event_time, INTERVAL '1' HOUR)
    """)

    print("Launching Flink Feature Engineering Job via StatementSet...")
    statement_set.execute().wait()

if __name__ == "__main__":
    import traceback
    try:
        run_job()
    except Exception as e:
        print("ERROR IN FLINK JOB:")
        traceback.print_exc()
        raise e
