# Short Report Outline (suggested 3–5 pages)

## 1. Introduction and objectives
Explain real-time machine-temperature monitoring and why windowed aggregation is useful.

## 2. System architecture
Diagram: Flink DataGen -> SQL tumbling + hopping -> filesystem CSV -> Pandas -> Streamlit.
Describe Docker JobManager/TaskManager roles.

## 3. Implementation
Document installed tools, project file structure, SQL definitions, window configurations, and dashboard refresh.
Explain that temperatures and machine identifiers are simulated.

## 4. Results and evidence
Insert your own images: Flink running job, window result files, baseline dashboard, extended alerts/filter, CSV export.
Include at least one sample CSV row or table (label sample data as synthetic).

## 5. Extension description
Describe adjustable Warning/High thresholds, alerts, machine filter, delta, download.
Explain why these features help users monitor and inspect streaming trends.

## 6. Testing
A small table: Docker started; both CSV output folders populated; dashboard updates; adjustable thresholds change status; filters work; exported CSV opens. Record actual pass/fail and screenshot evidence.

## 7. Discussion and limitations
Tumbling vs sliding; tradeoff between update frequency and overlapping windows.
Limitations: DataGen simulation, processing time, rolling-file latency, no real incident notification, averages hide short spikes.

## 8. Conclusion
Summarize what was demonstrated, not untested improvements.
