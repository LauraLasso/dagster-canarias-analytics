"""
Definiciones de Dagster: Carga todos los assets de los 3 pipelines
"""

from dagster import Definitions, load_assets_from_modules

from src.dagster_app.assets import renta_canarias, renta_islas, nivel_estudios

# Cargar assets de los 3 módulos
all_assets = load_assets_from_modules([renta_canarias, renta_islas, nivel_estudios])

defs = Definitions(
    assets=all_assets,
)