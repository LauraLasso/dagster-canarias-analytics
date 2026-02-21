"""
definitions.py
Lanzar con: dagster dev -f definitions.py
"""

from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules
from src.dagster_app.assets import renta_canarias, renta_islas, nivel_estudios

defs = Definitions(
    assets=load_assets_from_modules([renta_canarias, renta_islas, nivel_estudios]),
    asset_checks=load_asset_checks_from_modules([renta_canarias, renta_islas, nivel_estudios])
)
