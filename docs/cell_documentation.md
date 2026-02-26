# Case.ipynb - Cell Documentation

Quick reference for what each cell does.

---

## Cell 1: Title
Project title markdown.

---

## Cell 2: Setup & Configuration

**What it does:**
- Imports: pandas, matplotlib, os, SparkSession
- Sets paths: `CLIENTES_PATH` and `PEDIDOS_PATH` using `os.path.join()`
- Creates `spark` session with `local[*]` (all CPU cores)

**Key configs:**
- 4GB memory for driver/executor
- 200 shuffle partitions (good for 1M records)
- 10MB broadcast threshold (auto-broadcasts clients table in joins)
- Arrow enabled (fast pandas conversions)

**Variables created:**
- `spark` - use for all DataFrame operations
- `CLIENTES_PATH` - path to clients JSON (~10K records)
- `PEDIDOS_PATH` - path to orders JSON (~1M records)

---

## Next Cell: [Ready to document]

*Say "doc" when you've written your next cell*

---

**Usage**: 
- "doc" → document latest cell
- "doc [number]" → document specific cell
