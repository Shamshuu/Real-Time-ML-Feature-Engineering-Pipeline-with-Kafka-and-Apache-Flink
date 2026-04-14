# Real-Time ML Feature Engineering Pipeline

A production-style real-time feature engineering pipeline using **Apache Kafka** and **Apache Flink (PyFlink)**.

## Architecture
- **Data Producer**: Simulates user interactions with 60x acceleration and 5% late events.
- **Kafka**:
    - `user-events`: Raw interaction stream.
    - `content-metadata`: Compacted topic for content enrichment.
    - `feature-store`: Compacted topic for final computed features.
- **Flink Job**:
    - Watermarking (30s tolerance).
    - User Features: Click rate, Avg dwell time (1h window).
    - Content Features: Engagement rate (15m window, 5m slide).
    - Cross Features: Category affinity join (1h window).
- **Dashboard**: Streamlit app for real-time observability.

## Setup & Execution

### Prerequisites
- Docker & Docker Compose
- ~4GB RAM allocated to Docker

### Running the pipeline
1. Clone the repository and navigate to the project root.
2. Build and start the services:
   ```bash
   docker-compose up --build
   ```
3. Wait for all services to report `healthy`. Initial startup may take 2-3 minutes as Flink JobManager and Kafka initialize.
4. Access the Dashboard at: [http://localhost:8501](http://localhost:8501)
5. Access the Flink Web UI at: [http://localhost:8081](http://localhost:8081)

## Environment Variables
Defined in `.env.example`. Most are pre-configured in `docker-compose.yml`.

## Reports
See [ANALYSIS.md](./ANALYSIS.md) for detailed analysis of the pipeline's behavior.