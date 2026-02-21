"""
renta_islas.py — Pipeline con checks de carga, transformación y visualización
"""

import pandas as pd
from plotnine import *
from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue, asset_check, AssetCheckResult
from pathlib import Path
from src.dagster_app.assets.renta_canarias import raw_renta_canarias

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "images" / "dagster"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

MEDIDAS_DICT = {
    'Sueldos y salarios': 'Sueldos', 'Otros ingresos': 'Otros',
    'Otras prestaciones': 'Prestaciones', 'Pensiones': 'Pensiones',
    'Prestaciones por desempleo': 'Desempleo'
}

# ─────────────────────────────────────────
# ASSETS
# ─────────────────────────────────────────

@asset(description="Carga catálogo de islas")
def raw_codislas(context: AssetExecutionContext) -> pd.DataFrame:
    df = pd.read_csv(DATA_DIR / "codislas.csv", encoding='latin-1', sep=';')
    context.log.info(f"✓ {len(df)} códigos de islas cargados")
    return df

@asset(description="Filtra municipios con código de 5 dígitos")
def cleaned_municipios(context: AssetExecutionContext, raw_renta_canarias: pd.DataFrame) -> pd.DataFrame:
    df = raw_renta_canarias[raw_renta_canarias['TERRITORIO_CODE'].str.match(r'^\d{5}$', na=False)].copy()
    context.log.info(f"✓ {df['TERRITORIO_CODE'].nunique()} municipios")
    return df

@asset(description="Une renta con nombres de islas y municipios")
def municipios_con_islas(context: AssetExecutionContext, cleaned_municipios: pd.DataFrame, raw_codislas: pd.DataFrame) -> pd.DataFrame:
    df_cod = raw_codislas.copy()
    df_cod['TERRITORIO_CODE'] = df_cod['CPRO'].astype(str) + df_cod['CMUN'].astype(str).str.zfill(3)
    df_cod['ISLA']   = df_cod['ISLA'].str.strip()
    df_cod['NOMBRE'] = df_cod['NOMBRE'].str.strip()
    df_merged = pd.merge(cleaned_municipios, df_cod[['TERRITORIO_CODE', 'ISLA', 'NOMBRE']], on='TERRITORIO_CODE', how='left')
    df_merged['MEDIDA_CORTA'] = df_merged['MEDIDAS#es'].replace(MEDIDAS_DICT)

    context.log.info(f"✓ {df_merged['ISLA'].notna().sum()} registros con isla asignada")
    return df_merged

@asset(description="Selecciona top 5 municipios por sueldos en 2023")
def top_5_municipios(context: AssetExecutionContext, municipios_con_islas: pd.DataFrame) -> pd.DataFrame:
    df_valid = municipios_con_islas[municipios_con_islas['ISLA'].notna()].copy()
    df_ranking = df_valid[(df_valid['TIME_PERIOD#es'] == 2023) & (df_valid['MEDIDA_CORTA'] == 'Sueldos')]
    top_5 = df_ranking.nlargest(5, 'OBS_VALUE')

    df_viz = df_valid[df_valid['TERRITORIO_CODE'].isin(top_5['TERRITORIO_CODE'])].copy()
    df_viz['MUNICIPIO_NOMBRE'] = df_viz['NOMBRE'] + ' (' + df_viz['ISLA'] + ')'

    return df_viz

@asset(description="Gráfico evolución top 5 municipios")
def grafico_top5_evolucion_medidas(context: AssetExecutionContext, top_5_municipios: pd.DataFrame) -> MaterializeResult:
    output_path = OUTPUT_DIR / "grafico_top5_evolucion_medidas.png"
    grafico = (
        ggplot(top_5_municipios, aes('TIME_PERIOD#es', 'OBS_VALUE', color='MUNICIPIO_NOMBRE')) +
        geom_line(size=1.2) + geom_point(size=2) +
        facet_wrap('~MEDIDA_CORTA', scales='free_y') +
        labs(title='Top 5 Municipios por Ingresos (2015-2023)', x='Año', y='€', color='Municipio') +
        scale_x_continuous(breaks=[2015, 2017, 2019, 2021, 2023]) +
        scale_color_brewer(type='qual', palette='Set1') +
        theme_minimal() +
        theme(figure_size=(16, 10), axis_text_x=element_text(angle=45, hjust=1), legend_position='bottom')
    )
    grafico.save(str(output_path), dpi=300)
    return MaterializeResult(metadata={"path": MetadataValue.path(str(output_path))})

# ─────────────────────────────────────────
# CHECKS
# ─────────────────────────────────────────

@asset_check(asset=raw_codislas)
def check_carga_codislas(raw_codislas: pd.DataFrame) -> AssetCheckResult:
    cols_esperadas = {'CPRO', 'CMUN', 'ISLA', 'NOMBRE'}
    faltan = cols_esperadas - set(raw_codislas.columns)

    # ❌ CASO SUCIO: simulamos columna faltante
    # faltan = faltan | {'ISLA'}

    passed = bool(len(faltan) == 0)                            # ← bool()

    return AssetCheckResult(
        passed=passed,
        metadata={
            "columnas_faltantes": MetadataValue.text(str(list(faltan)) if faltan else "Ninguna"),
            "principio": MetadataValue.text("Integridad del catálogo de islas")
        }
    )

@asset_check(asset=municipios_con_islas)
def check_merge_islas(municipios_con_islas: pd.DataFrame) -> AssetCheckResult:
    total = len(municipios_con_islas)
    con_isla = int(municipios_con_islas['ISLA'].notna().sum())
    pct = float(con_isla / total * 100) if total > 0 else 0.0

    # CASO SUCIO CHECK 2 ✅: forzamos cobertura a 0 dentro del check
    # pct = 0.0

    passed = bool(pct >= 90)                                   # ← bool()

    return AssetCheckResult(
        passed=passed,
        metadata={
            "total_registros": MetadataValue.int(total),
            "con_isla": MetadataValue.int(con_isla),
            "porcentaje_cobertura": MetadataValue.float(round(pct, 2)),
            "principio": MetadataValue.text("Cobertura del merge codislas")
        }
    )

@asset_check(asset=grafico_top5_evolucion_medidas)
def check_viz_top5() -> AssetCheckResult:                      # ← sin context
    df_renta = pd.read_csv(DATA_DIR / "distribucion-renta-canarias.csv")
    df_cod   = pd.read_csv(DATA_DIR / "codislas.csv", encoding='latin-1', sep=';')
    df_cod['TERRITORIO_CODE'] = df_cod['CPRO'].astype(str) + df_cod['CMUN'].astype(str).str.zfill(3)
    df_muni  = df_renta[df_renta['TERRITORIO_CODE'].str.match(r'^\d{5}$', na=False)]
    df_merge = df_muni.merge(df_cod[['TERRITORIO_CODE', 'ISLA']], on='TERRITORIO_CODE', how='left')
    df_top5  = df_merge[
        (df_merge['TIME_PERIOD#es'] == 2023) &
        (df_merge['MEDIDAS#es'] == 'Sueldos y salarios') &
        (df_merge['ISLA'].notna())
    ].nlargest(5, 'OBS_VALUE')

    # ❌ CASO SUCIO: añadimos 5 municipios ficticios al conteo local
    # extras = pd.DataFrame({'TERRITORIO_CODE': ['99001', '99002', '99003', '99004', '99005']})
    # df_top5  = pd.concat([df_top5, extras], ignore_index=True)

    municipios = int(df_top5['TERRITORIO_CODE'].nunique())
    passed = bool(municipios <= 8)                             # ← bool()

    return AssetCheckResult(
        passed=passed,
        metadata={
            "municipios_en_leyenda": MetadataValue.int(municipios),
            "max_colores_paleta": MetadataValue.int(8),
            "principio": MetadataValue.text("Paleta Set1 legible con máx. 8 series")
        }
    )
