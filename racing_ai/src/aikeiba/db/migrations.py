from __future__ import annotations

from pathlib import Path

from aikeiba.db.duckdb import DuckDb


def apply_migrations(db: DuckDb) -> None:
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
          version VARCHAR PRIMARY KEY,
          applied_at TIMESTAMP DEFAULT now()
        );
        """
    )

    migrations_dir = Path(__file__).parent / "migrations"
    migrations = sorted(migrations_dir.glob("*.sql"))
    for path in migrations:
        version = path.name
        already = db.query_df("SELECT 1 AS one FROM schema_migrations WHERE version = ?", (version,))
        if len(already) > 0:
            continue
        sql = path.read_text(encoding="utf-8")
        db.execute(sql)
        db.execute("INSERT INTO schema_migrations(version) VALUES (?)", (version,))
