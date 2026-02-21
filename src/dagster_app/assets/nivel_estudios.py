"""
nivel_estudios.py — Pipeline con checks de carga, transformación y visualización
"""

import pandas as pd
from plotnine import *
from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue, asset_check, AssetCheckResult
from pathlib import Path
from src.dagster_app.assets.renta_islas import municipios_con_islas

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "images" / "dagster"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

NIVELES_VALIDOS = ['Primaria', 'ESO', 'Bachillerato', 'FP', 'Universidad']

NIVELES_DICT = {
    'Educación primaria e inferior': 'Primaria',
    'Primera etapa de Educación Secundaria y similar': 'ESO',
    'Segunda etapa de educación secundaria, con orientación general': 'Bachillerato',
    'Segunda etapa de Educación Secundaria, con orientación profesional (con y sin continuidad en la educación superior); Educación postsecundaria no superior': 'FP',
    'Educación superior': 'Universidad'
}

# ─────────────────────────────────────────
# ASSETS
# ─────────────────────────────────────────

@asset(description="Carga datos de nivel de estudios")
def raw_nivel_estudios(context: AssetExecutionContext) -> pd.DataFrame:
    file_path = DATA_DIR / "nivelestudios.xlsx"
    df = pd.read_excel(file_path, engine='openpyxl')

    context.log.info(f"✓ {len(df)} registros cargados")
    return df

@asset(description="Limpia y normaliza datos de estudios")
def cleaned_nivel_estudios(context: AssetExecutionContext, raw_nivel_estudios: pd.DataFrame) -> pd.DataFrame:
    df = raw_nivel_estudios.copy()
    df['CODIGO_MUNICIPIO'] = df['Municipios de 500 habitantes o más'].str.extract(r'(\d{5})')
    df['AÑO'] = df['Periodo'].dt.year
    df = df.rename(columns={'Total': 'POBLACION', 'Sexo': 'SEXO'})
    df['NIVEL_CORTO'] = df['Nivel de estudios en curso'].replace(NIVELES_DICT)
    df = df[df['NIVEL_CORTO'].isin(NIVELES_VALIDOS) & (df['POBLACION'] > 0)].copy()

    context.log.info(f"✓ {len(df)} registros limpios")
    return df

@asset(description="Perfil educativo por isla 2021-2023")
def educacion_temporal_por_isla(
    context: AssetExecutionContext,
    cleaned_nivel_estudios: pd.DataFrame,
    municipios_con_islas: pd.DataFrame
) -> pd.DataFrame:
    df = cleaned_nivel_estudios[cleaned_nivel_estudios['AÑO'].between(2021, 2023)].copy()
    df_cod = municipios_con_islas[['TERRITORIO_CODE', 'ISLA']].drop_duplicates()
    df_cod['TERRITORIO_CODE'] = df_cod['TERRITORIO_CODE'].astype(str).str.zfill(5)
    df['CODIGO_MUNICIPIO']    = df['CODIGO_MUNICIPIO'].astype(str).str.zfill(5)
    df = df.merge(df_cod, left_on='CODIGO_MUNICIPIO', right_on='TERRITORIO_CODE', how='left')
    df = df[df['ISLA'].notna()]
    df_agg = df.groupby(['ISLA', 'AÑO', 'NIVEL_CORTO'], as_index=False)['POBLACION'].sum()
    df_tot = df_agg.groupby(['ISLA', 'AÑO'])['POBLACION'].sum().reset_index(name='TOTAL')
    df_agg = df_agg.merge(df_tot, on=['ISLA', 'AÑO'])
    df_agg['PORCENTAJE'] = (df_agg['POBLACION'] / df_agg['TOTAL']) * 100
    return df_agg

@asset(description="Gráfico barras agrupadas educación por isla 2023")
def viz_educacion_isla_2023(context: AssetExecutionContext, educacion_temporal_por_isla: pd.DataFrame) -> MaterializeResult:
    output_path = OUTPUT_DIR / "perfil_educativo_isla_2023.png"
    df_2023 = educacion_temporal_por_isla[educacion_temporal_por_isla['AÑO'] == 2023].copy()
    grafico = (
        ggplot(df_2023, aes('ISLA', 'PORCENTAJE', fill='NIVEL_CORTO')) +
        geom_bar(stat='identity', position='dodge') +
        labs(title='Perfil Educativo por Isla (2023)', x='Isla', y='% Población', fill='Nivel') +
        scale_fill_brewer(type='qual', palette='Set2') +
        theme_minimal() +
        theme(figure_size=(16, 8), axis_text_x=element_text(angle=45, hjust=1), legend_position='bottom')
    )
    grafico.save(str(output_path), dpi=300)
    return MaterializeResult(metadata={"path": MetadataValue.path(str(output_path))})

# ─────────────────────────────────────────
# CHECKS
# ─────────────────────────────────────────

@asset_check(asset=raw_nivel_estudios)
def check_carga_estudios(raw_nivel_estudios: pd.DataFrame) -> AssetCheckResult:
    cols_esperadas = {'Municipios de 500 habitantes o más', 'Nivel de estudios en curso', 'Total', 'Periodo'}
    faltan = cols_esperadas - set(raw_nivel_estudios.columns)

    # CASO SUCIO CHECK 1 ✅: simulamos columna faltante solo dentro del check
    # faltan = faltan | {'Nivel de estudios en curso'}

    passed = bool(len(faltan) == 0)                            # ← bool()

    return AssetCheckResult(
        passed=passed,
        metadata={
            "columnas_faltantes": MetadataValue.text(str(list(faltan)) if faltan else "Ninguna"),
            "filas": MetadataValue.int(len(raw_nivel_estudios)),
            "principio": MetadataValue.text("Integridad estructural del Excel")
        }
    )

@asset_check(asset=cleaned_nivel_estudios)
def check_transform_estudios(cleaned_nivel_estudios: pd.DataFrame) -> AssetCheckResult:
    negativos = int((cleaned_nivel_estudios['POBLACION'] < 0).sum())
    niveles   = int(cleaned_nivel_estudios['NIVEL_CORTO'].nunique())

    # CASO SUCIO CHECK 2 ✅: simulamos negativos solo dentro del check
    # negativos = negativos + 5

    passed    = bool((negativos == 0) and (niveles == len(NIVELES_VALIDOS)))  # ← bool()

    return AssetCheckResult(
        passed=passed,
        metadata={
            "poblacion_negativa": MetadataValue.int(negativos),
            "niveles_detectados": MetadataValue.int(niveles),
            "niveles_esperados":  MetadataValue.int(len(NIVELES_VALIDOS)),
            "principio": MetadataValue.text("Similitud Gestalt: categorías consistentes")
        }
    )

@asset_check(asset=viz_educacion_isla_2023)
def check_viz_barras_isla() -> AssetCheckResult:               # ← sin context
    df = pd.read_excel(DATA_DIR / "nivelestudios.xlsx", engine='openpyxl')
    df['NIVEL_CORTO'] = df['Nivel de estudios en curso'].replace(NIVELES_DICT)

    # ❌ CASO SUCIO: añadimos 3 niveles ficticios al df local
    # extras = pd.DataFrame({'NIVEL_CORTO': ['Doctorado', 'Formacion_Extra', 'Sin_Estudios', 'Formacion_post_doc', 'certificacion_profesional']})
    # df = pd.concat([df, extras], ignore_index=True)
    # niveles = int(df[df['NIVEL_CORTO'].isin(NIVELES_VALIDOS + ['Doctorado', 'Formacion_Extra', 
    #                                                            'Sin_Estudios', 'Formacion_post_doc', 
    #                                                            'certificacion_profesional'])]['NIVEL_CORTO'].nunique())

    niveles = int(df[df['NIVEL_CORTO'].isin(NIVELES_VALIDOS)]['NIVEL_CORTO'].nunique())
    

    passed  = bool(niveles <= 6)                               # ← bool()

    return AssetCheckResult(
        passed=passed,
        metadata={
            "niveles_en_grafico": MetadataValue.int(niveles),
            "max_recomendado": MetadataValue.int(6),
            "principio": MetadataValue.text("Legibilidad barras agrupadas: máx. 6 grupos por isla")
        }
    )
