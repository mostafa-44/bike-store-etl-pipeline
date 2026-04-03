# =============================================================================
# main.py — ETL Pipeline Orchestrator
# DataCraft Academy | End-to-End ETL Pipeline Project
# =============================================================================
"""
Run the complete pipeline end-to-end:

    python main.py

Pipeline stages
---------------
  1  extract.py        → raw DataFrames (DB_Tables + DeltaLake + API)
  2  quality_checks.py → cleaned data  → staging_1/
  3  transform.py      → enriched data → staging_2/
  4  data_model.py     → Star Schema   → Information_Mart/
  5  visualize.py      → BI charts     → Visualization/
"""

import time
import traceback

from extract       import run_extraction
from quality_checks import run_quality_checks
from transform     import run_transformations
from data_model    import run_data_model
from visualize     import run_visualizations


BANNER = """
╔══════════════════════════════════════════════════════════════╗
║         DataCraft Academy — Python ETL Pipeline             ║
║         End-to-End Data Engineering Project                 ║
╚══════════════════════════════════════════════════════════════╝
"""


def main():
    print(BANNER)
    pipeline_start = time.time()

    try:
        # ── Milestone 1: Extract ──────────────────────────────────────────────
        t0 = time.time()
        raw_data = run_extraction()
        print(f"  ⏱  Extract completed in {time.time()-t0:.1f}s")

        # ── Milestone 2: Quality Checks ───────────────────────────────────────
        t0 = time.time()
        clean_data = run_quality_checks(raw_data)
        print(f"  ⏱  Quality checks completed in {time.time()-t0:.1f}s")

        # ── Milestone 3: Transformations ──────────────────────────────────────
        t0 = time.time()
        transformed_data = run_transformations(clean_data)
        print(f"  ⏱  Transformations completed in {time.time()-t0:.1f}s")

        # ── Milestone 4: Data Model ───────────────────────────────────────────
        t0 = time.time()
        model = run_data_model(transformed_data)
        print(f"  ⏱  Data model built in {time.time()-t0:.1f}s")

        # ── Milestone 5: Visualizations ───────────────────────────────────────
        t0 = time.time()
        run_visualizations(model, transformed_data)
        print(f"  ⏱  Visualizations completed in {time.time()-t0:.1f}s")

    except Exception:
        print("\n❌ Pipeline failed with the following error:")
        traceback.print_exc()
        return

    total_time = time.time() - pipeline_start
    print("\n" + "="*60)
    print(f"  ✅  Pipeline completed successfully in {total_time:.1f}s")
    print("="*60)
    print("""
  Output folders
  ──────────────
  staging_1/        cleaned CSVs + quality report
  staging_2/        transformed CSVs
  Information_Mart/ Star Schema tables (fact + dims)
  Visualization/    8 BI charts (PNG)
""")


if __name__ == "__main__":
    main()
