# NYC 311 Data Factory (Stage 1) - Shayan Ganji

Production-grade, resilient, and deterministic data ingestion engine for NYC 311 DOT service requests.

## Quickstart & Execution

```bash
# 1. Install dependencies and package in editable mode
pip install -r requirements.txt && pip install -e .

# 2. Run offline unit tests (21 tests, 100% offline, ~0.2s)
python -m unittest discover tests -v

# 3. Execute the full end-to-end ingestion and curation pipeline
python -m data_factory.cli --full-run
```

- **Decision Reports:** [English Report](report.md) | [گزارش فارسی](report.fa.md)
- **Analytical Notebook:** [solution.ipynb](solution.ipynb)
- **Curated Artifacts & Checksums:** Stored in `data/curated/` with cryptographic manifest in `data/curated/run_manifest.json`.
