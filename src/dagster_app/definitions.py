"""
Definiciones de Dagster: Carga todos los assets de los 3 pipelines
"""

from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules

from src.dagster_app.assets import renta_canarias_ia, renta_islas_ia, nivel_estudios_ia

# Cargar assets de los 3 módulos
all_assets = load_assets_from_modules([renta_canarias_ia, renta_islas_ia, nivel_estudios_ia])
all_checks = load_asset_checks_from_modules([renta_canarias_ia, renta_islas_ia, nivel_estudios_ia])

defs = Definitions(
    assets=all_assets,
    asset_checks=all_checks,
)