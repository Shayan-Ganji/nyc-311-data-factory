# NYC 311 Data Factory (Stage 1) - Shayan Ganji

An industrial, resilient, and idempotent data engineering factory for NYC 311 DOT service requests.

## Quickstart & Execution

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run unit tests offline (100% offline test discovery)
python -m unittest discover tests

# 3. Execute the full data factory pipeline (from raw fetch to curated analytical outputs)
python -m src.data_factory.cli --full-run
```
Outputs are deterministically written to `data/curated/` with cryptographic manifest in `data/curated/run_manifest.json`.
