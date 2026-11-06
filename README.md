# Real-Time Machine Temperature Monitoring & Alerts

Academic practical based on `Flink_Streamlit_Student_Guide.pdf` (17 pages), with minor dashboard extensions.

## Original requirements
- DataGen source: 2 records/second, machine IDs 1–3, temperatures 60–100°C.
- Tumbling: 10-second non-overlapping average by machine.
- Hopping/sliding: 10-second average, 5-second slide, by machine.
- Write results to separate filesystem CSV folders.
- Display latest averages, machine status, and temperature trend in Streamlit.

## Extensions in this starter
- Adjustable Warning and High thresholds.
- High/Warning machine count and high-temperature alert message.
- Select visible machines.
- Show latest sliding minus tumbling average (descriptive comparison only).
- Download processed sliding-window results as CSV.
- Safer CSV consumption: read only finished Flink `part-*` files.

## Prerequisites
Windows with Docker Desktop running, Python, and PowerShell.

## 1. Start the Flink cluster
From project root:

```powershell
docker version
docker compose up -d
docker compose ps
```

Open `http://localhost:8081`. Expect 1 TaskManager and 2 slots.

## 2. Submit the Flink SQL pipeline

```powershell
docker compose exec jobmanager ./bin/sql-client.sh -f /opt/flink/sql/pipeline.sql
```

The SQL script creates the DataGen source, both CSV sinks, and submits one continuous statement-set job. It also enables 5-second checkpointing so completed output files become available to readers.

Wait for the job to become **RUNNING** and for completed checkpoints. Look for finished `part-*` files in `output/tumbling` and `output/sliding` (do not read `.inprogress` files).

**Do not repeatedly run the SQL command while the old job is still RUNNING**: that creates duplicate continuous jobs and additional output files.

## 3. Start dashboard (PowerShell; no activation required)

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m streamlit run dashboard.py
```

Open `http://localhost:8501`.

## 4. Test the extensions
- Default thresholds: Warning >=80°C, High >=90°C. These classify the **sliding-window average**, not an individual sensor reading.
- Change thresholds with sidebar sliders and observe status changes.
- To trigger a High alert for the demonstration, lower the High threshold enough relative to the current observed average (and set Warning below it).
- Change the machine filter, check the comparison table and chart.
- Download the CSV and inspect its columns.

## 5. Screenshot evidence
Use the suggestions in `screenshots/README.md`. Show an actual alert using the thresholds visible in the screenshot.

## 6. Stop

```powershell
docker compose down
```

This stops the local cluster. Output CSV files remain on your host. On restart, rerun the SQL submission because this is a local demo rather than a durable deployment.

## Limitations
- Data is generated, not collected from physical sensors.
- Processing time (`PROCTIME`) is used, not device event time.
- The file-based transfer creates latency; dashboard refresh every 5 seconds does not mean end-to-end 5-second latency.
- A window average can hide individual temperature spikes.
- Output part files grow over time; this is a demo setup, not a production storage/retention solution.

## Suggested academic deliverables
1. Executable code (`docker-compose.yml`, `sql/pipeline.sql`, `dashboard.py`, `requirements.txt`).
2. Evidence screenshots (`screenshots/`).
3. A short report based on `report_outline.md`.
4. Optional 2–4-minute demonstration recording.

The original guide does **not** state an explicit lecturer grading rubric or submission manifest; confirm exact requirements with the lecturer.
