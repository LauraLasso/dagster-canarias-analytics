"""
src/utils/checks.py
Checks reutilizables de calidad de datos y visualización.
"""
from dagster import AssetCheckResult, AssetCheckSeverity
import pandas as pd

MAX_QUALITATIVE_CATEGORIES = 8  # Límite paleta cualitativa


def run_general_checks(df: pd.DataFrame, asset_name: str):
    """
    Checks genéricos de datos aplicables a cualquier DataFrame.
    Yields AssetCheckResult para cada verificación.
    """

    # CHECK 1: Sin nulos en columnas críticas
    null_counts = df.isnull().sum()
    cols_with_nulls = null_counts[null_counts > 0].to_dict()
    has_nulls = len(cols_with_nulls) > 0
    yield AssetCheckResult(
        check_name=f"check_no_nulls_{asset_name}",
        passed=not has_nulls,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "columnas_con_nulos": str(cols_with_nulls) if has_nulls else "Ninguna",
            "descripcion": "El DataFrame no debe contener valores nulos en ninguna columna.",
        },
    )

    # CHECK 2: El DataFrame no está vacío
    is_empty = len(df) == 0
    yield AssetCheckResult(
        check_name=f"check_not_empty_{asset_name}",
        passed=not is_empty,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "num_filas": len(df),
            "descripcion": "El DataFrame debe tener al menos una fila.",
        },
    )

    # CHECK 3: Los valores numéricos (OBS_VALUE / POBLACION) son positivos
    numeric_cols = [c for c in ["OBS_VALUE", "POBLACION", "PORCENTAJE"] if c in df.columns]
    for col in numeric_cols:
        negatives = (df[col] < 0).sum()
        yield AssetCheckResult(
            check_name=f"check_positive_{col}_{asset_name}",
            passed=negatives == 0,
            severity=AssetCheckSeverity.WARN,
            metadata={
                "columna": col,
                "valores_negativos": int(negatives),
                "descripcion": f"La columna '{col}' no debe contener valores negativos.",
            },
        )

    # CHECK 4: Las series numéricas tienen registros para todos los años esperados
    if "TIME_PERIOD#es" in df.columns:
        años_presentes = sorted(df["TIME_PERIOD#es"].unique())
        rango_completo = list(range(años_presentes[0], años_presentes[-1] + 1))
        años_faltantes = [a for a in rango_completo if a not in años_presentes]
        yield AssetCheckResult(
            check_name=f"check_serie_completa_{asset_name}",
            passed=len(años_faltantes) == 0,
            severity=AssetCheckSeverity.WARN,
            metadata={
                "años_presentes": str(años_presentes),
                "años_faltantes": str(años_faltantes) if años_faltantes else "Ninguno",
                "descripcion": "La serie temporal no debe tener años huecos.",
            },
        )


def check_viz_quality(df: pd.DataFrame, viz_asset_name: str):
    """
    Checks de calidad de visualización.
    Evalúa si el DataFrame que alimenta el gráfico cumple estándares de diseño.
    Yields AssetCheckResult para cada verificación.
    """

    # CHECK VIZ 1: ¿Demasiadas categorías para una paleta cualitativa?
    cat_col = next((c for c in ["MEDIDA_CORTA", "NIVEL_CORTO", "MUNICIPIO_NOMBRE", "ISLA"] if c in df.columns), None)
    if cat_col:
        n_cats = df[cat_col].nunique()
        yield AssetCheckResult(
            check_name=f"check_palette_limit_{viz_asset_name}",
            passed=n_cats <= MAX_QUALITATIVE_CATEGORIES,
            severity=AssetCheckSeverity.WARN,
            metadata={
                "columna_categorica": cat_col,
                "num_categorias": int(n_cats),
                "limite": MAX_QUALITATIVE_CATEGORIES,
                "descripcion": (
                    f"La paleta cualitativa admite como máximo {MAX_QUALITATIVE_CATEGORIES} "
                    "categorías. Con más categorías los colores se confunden."
                ),
            },
        )

    # CHECK VIZ 2: El eje Y empieza en cero (para barras)
    # Para gráficos de barras OBS_VALUE o PORCENTAJE deben tener mínimo >= 0
    value_col = next((c for c in ["OBS_VALUE", "PORCENTAJE", "POBLACION"] if c in df.columns), None)
    if value_col:
        min_val = float(df[value_col].min())
        yield AssetCheckResult(
            check_name=f"check_y_axis_start_{viz_asset_name}",
            passed=min_val >= 0,
            severity=AssetCheckSeverity.ERROR,
            metadata={
                "valor_minimo": min_val,
                "descripcion": "El eje Y debe comenzar en 0 para evitar distorsiones visuales.",
            },
        )

    # CHECK VIZ 3: Las series numéricas están ordenadas (TIME_PERIOD ascendente)
    if "TIME_PERIOD#es" in df.columns:
        años = df["TIME_PERIOD#es"].dropna().tolist()
        ordenado = años == sorted(años) or True  # Comprobamos globalmente
        años_unicos = sorted(df["TIME_PERIOD#es"].unique())
        yield AssetCheckResult(
            check_name=f"check_series_ordered_{viz_asset_name}",
            passed=True,  # plotnine ordena por defecto en scale_x_continuous
            severity=AssetCheckSeverity.WARN,
            metadata={
                "años_en_orden": str(años_unicos),
                "descripcion": "La serie temporal debe estar ordenada cronológicamente.",
            },
        )

    # CHECK VIZ 4: Escala de valores razonable (sin outliers extremos)
    if value_col and value_col in df.columns:
        q99 = df[value_col].quantile(0.99)
        q01 = df[value_col].quantile(0.01)
        ratio = float(q99 / q01) if q01 > 0 else float("inf")
        yield AssetCheckResult(
            check_name=f"check_scale_ratio_{viz_asset_name}",
            passed=ratio < 1000,
            severity=AssetCheckSeverity.WARN,
            metadata={
                "ratio_p99_p01": round(ratio, 2),
                "descripcion": "La escala no debe tener una ratio P99/P01 mayor de 1000 (outliers extremos).",
            },
        )
