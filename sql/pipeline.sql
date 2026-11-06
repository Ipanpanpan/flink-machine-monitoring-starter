-- Derived from the 17-page lecturer guide: Flink_Streamlit_Student_Guide.pdf.
-- Extra configuration: checkpointing makes finished CSV part files observable.
-- Run using:
-- docker compose exec jobmanager ./bin/sql-client.sh -f /opt/flink/sql/pipeline.sql

SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '5 s';
SET 'parallelism.default' = '1';
SET 'pipeline.name' = 'Machine Temperature Windows';

CREATE TABLE machine_stream (
    machine_id INT,
    temperature INT,
    event_time AS PROCTIME()
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '2',
    'fields.machine_id.min' = '1',
    'fields.machine_id.max' = '3',
    'fields.temperature.min' = '60',
    'fields.temperature.max' = '100'
);

CREATE TABLE tumbling_output (
    machine_id INT,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    avg_temperature DOUBLE
) WITH (
    'connector' = 'filesystem',
    'path' = 'file:///opt/flink/output/tumbling',
    'format' = 'csv',
    'sink.rolling-policy.rollover-interval' = '10 s',
    'sink.rolling-policy.check-interval' = '2 s'
);

CREATE TABLE sliding_output (
    machine_id INT,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    avg_temperature DOUBLE
) WITH (
    'connector' = 'filesystem',
    'path' = 'file:///opt/flink/output/sliding',
    'format' = 'csv',
    'sink.rolling-policy.rollover-interval' = '10 s',
    'sink.rolling-policy.check-interval' = '2 s'
);

EXECUTE STATEMENT SET
BEGIN
    INSERT INTO tumbling_output
    SELECT
        machine_id,
        window_start,
        window_end,
        CAST(AVG(temperature) AS DOUBLE)
    FROM TABLE(
        TUMBLE(
            TABLE machine_stream,
            DESCRIPTOR(event_time),
            INTERVAL '10' SECOND
        )
    )
    GROUP BY machine_id, window_start, window_end;

    INSERT INTO sliding_output
    SELECT
        machine_id,
        window_start,
        window_end,
        CAST(AVG(temperature) AS DOUBLE)
    FROM TABLE(
        HOP(
            TABLE machine_stream,
            DESCRIPTOR(event_time),
            INTERVAL '5' SECOND,
            INTERVAL '10' SECOND
        )
    )
    GROUP BY machine_id, window_start, window_end;
END;
