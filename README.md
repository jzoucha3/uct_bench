# jzouca_UCTBench_minimal

Minimal full-stack UCT Benchmark workspace.

Components:

1. Core pipeline
2. Demo frontend
3. Backend API
4. DuckDB persistence

Primary documentation:

- [Pipeline System Guide](/home/joey/jzouca_UCTBench_minimal/docs/PIPELINE_SYSTEM_GUIDE.md)

Quick start:

```bash
cd /home/joey/jzouca_UCTBench_minimal
source /home/joey/jzoucha_UCTBench/.venv/bin/activate
python scripts/run_pipeline_demo.py --dry-run
```

Backend:

```bash
uvicorn backend_api.main:app --reload --port 8000
```

Frontend:

```bash
cd frontend
npm install
npm run dev
```

Frontend: `http://localhost:5173`
Backend: `http://localhost:8000`
