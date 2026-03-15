"""
Definiciones de Dagster: Carga todos los assets de los 3 pipelines
"""

import hashlib
from pathlib import Path
from dagster import (
    Definitions,
    load_assets_from_modules,
    load_asset_checks_from_modules,
    sensor,
    RunRequest,
    DefaultSensorStatus,
    define_asset_job,
    AssetSelection,
)

from src.dagster_app.assets import renta_canarias_ia, renta_islas_ia, nivel_estudios_ia, mapa_canarias_ia

# ── Ruta de la carpeta vigilada ───────────────────────────────────────────────
CARPETA_DATOS = Path(__file__).resolve().parent.parent.parent / "data"

# ── Assets y checks ───────────────────────────────────────────────────────────
all_assets = load_assets_from_modules([renta_canarias_ia, renta_islas_ia, nivel_estudios_ia, mapa_canarias_ia])
all_checks = load_asset_checks_from_modules([renta_canarias_ia, renta_islas_ia, nivel_estudios_ia])

# ── Job que materializa todos los assets ──────────────────────────────────────
todo_el_pipeline = define_asset_job(
    name="todo_el_pipeline",
    selection=AssetSelection.all()
)

# ── Función auxiliar: hash de la carpeta /data ────────────────────────────────
def _hash_carpeta(path: Path) -> str:
    h = hashlib.md5()
    for f in sorted(path.rglob("*")):
        if f.is_file() and not f.suffix in {".pyc", ".tmp"}:
            h.update(str(f).encode())        # nombre del archivo
            h.update(str(f.stat().st_mtime).encode())  # fecha de modificación
    return h.hexdigest()

# ── Sensor: detecta cambios en /data y lanza el pipeline ─────────────────────
@sensor(
    job=todo_el_pipeline,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=30,
    description="Lanza el pipeline completo cuando cambia algún archivo en /data"
)
def sensor_cambio_datos(context):
    if not CARPETA_DATOS.exists():
        context.log.warning(f"Carpeta no encontrada: {CARPETA_DATOS}")
        return

    nuevo_hash = _hash_carpeta(CARPETA_DATOS)
    ultimo_hash = context.cursor or ""

    context.log.info(f"Hash actual: {nuevo_hash[:8]}... | Hash anterior: {ultimo_hash[:8] or 'ninguno'}...")

    if nuevo_hash != ultimo_hash:
        context.update_cursor(nuevo_hash)
        context.log.info("Cambio detectado en /data — lanzando pipeline completo")
        yield RunRequest(run_key=nuevo_hash)
    else:
        context.log.info("✓ Sin cambios en /data")

# ── Definiciones finales ──────────────────────────────────────────────────────
defs = Definitions(
    assets=all_assets,
    asset_checks=all_checks,
    jobs=[todo_el_pipeline],
    sensors=[sensor_cambio_datos],
)
