# Real-Time Feature Engineering Pipeline Analysis

## Batch vs. Streaming Divergence

When comparing the real-time features produced by Flink with a batch computation (e.g., a Pandas script running on a full day's data), several divergences can occur:

1.  **Window Boundaries**: Streaming windows are defined by event time and firing intervals (e.g., 1-hour tumbling). If a batch script uses different boundary definitions (e.g., wall-clock hour or simply grouping by `FLOOR(timestamp)`), the records included in each bucket might differ slightly, especially for events occurring at the exact second of transition.
2.  **Late Data Handling**: The streaming pipeline uses a **30-second watermark**. This means any event arriving more than 30 seconds late (relative to the simulation clock) is dropped and never included in the window aggregate. A batch script, however, sees the entire dataset at once and includes all late events in their respective historical windows. This leads to slightly higher counts/metrics in batch for windows with late data.
3.  **State Initialization**: The streaming pipeline starts computing from the moment it is launched. If it joins with a `content-metadata` topic, it only enriches events that arrive *after* the metadata has been loaded into state. A batch join (e.g., `pd.merge`) is naturally more complete as it considers the whole cross-product of available data.

**ML Implications**: Feature divergence can lead to "training-serving skew". If a model is trained on "perfect" batch data but served with "approximate" streaming features, its performance may degrade. Monitoring this divergence is critical for production ML systems.

## Late Event Handling

In this implementation, the data producer deliberately injects 5% late events with a lag between 35 and 90 seconds. 

### Watermarking Strategy
We implemented `WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(30))`. This choice of 30 seconds means:
- Events with a lag <= 30s are correctly processed and included in their assigned windows.
- Events with a lag > 30s (which accounts for most of our injected late events, as they range 35-90s) are technically "late for the watermark".

### Evidence of Handling
During execution, the Flink job logs will indicate `Dropped late event` for events where the watermark has already passed their assigned timestamp. This is visible in the Flink TaskManager logs. 

### Dashboard Observations
The Dashboard displays a "Late Events Dropped" metric. Because our producer injects events with lags up to 90s (well beyond the 30s tolerance), this counter will increase steadily, demonstrating that Flink is correctly enforcing the watermark and pruning stale data to maintain processing efficiency and deterministic output for subsequent windows.

If we were to increase the watermark tolerance to 100 seconds, the "Late Events Dropped" counter would remain at zero (assuming no network jitter), but at the cost of increasing the latency of window firing by 100 seconds.
