import pandas as pd
from plotnine import *
from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue, asset_check, AssetCheckResult
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "images" / "dagster"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

MEDIDAS_DICT = {'Sueldos y salarios': 'Sueldos', 'Otros ingresos': 'Otros', 
                'Otras prestaciones': 'Prestaciones', 'Pensiones': 'Pensiones', 
                'Prestaciones por desempleo': 'Desempleo'}

@asset(description="Carga dataset renta Canarias")
def raw_renta_canarias(context: AssetExecutionContext) -> pd.DataFrame:
    file_path = DATA_DIR / "distribucion-renta-canarias.csv"
    
    
    df = pd.read_csv(file_path)

    context.log.info(f"✓ {len(df)} registros cargados")
    return df

@asset_check(asset=raw_renta_canarias)
def check_carga_renta(raw_renta_canarias: pd.DataFrame) -> AssetCheckResult:
    filas = len(raw_renta_canarias)
    nulos_criticos = raw_renta_canarias[['OBS_VALUE', 'TERRITORIO_CODE']].isnull().sum().sum()
    
    # CASO SUCIO CHECK 1 ✅: forzamos fallo dentro del propio check, sin tocar el asset
    # nulos_criticos = nulos_criticos + 999

    passed = bool((filas > 1000) and (nulos_criticos == 0))   # ← bool()

    return AssetCheckResult(
        passed=passed,
        metadata={
            "filas": MetadataValue.int(filas),
            "nulos_criticos": MetadataValue.int(int(nulos_criticos)),
            "principio": MetadataValue.text("Integridad datos fuente")
        }
    )

@asset(description="Filtra Canarias ES70")
def cleaned_canarias(context: AssetExecutionContext, raw_renta_canarias: pd.DataFrame) -> pd.DataFrame:
    return raw_renta_canarias[raw_renta_canarias['TERRITORIO_CODE'] == 'ES70'].copy()

@asset(description="Transforma con medidas cortas")
def transformed_canarias(context: AssetExecutionContext, cleaned_canarias: pd.DataFrame) -> pd.DataFrame:
    df = cleaned_canarias.copy()
    df['MEDIDA_CORTA'] = df['MEDIDAS#es'].replace(MEDIDAS_DICT)
    
    context.log.info(f"✓ Medidas: {df['MEDIDA_CORTA'].nunique()}")
    return df

@asset_check(asset=transformed_canarias)
def check_transform_renta(transformed_canarias: pd.DataFrame) -> AssetCheckResult:
    negativos = int((transformed_canarias["OBS_VALUE"] < 0).sum())
    medidas = int(transformed_canarias['MEDIDA_CORTA'].nunique())

    # CASO SUCIO CHECK 2 ✅: forzamos fallo dentro del check
    # negativos = negativos + 10

    passed = bool((negativos == 0) and (medidas == 5))         # ← bool()

    return AssetCheckResult(
        passed=passed,
        metadata={
            "negativos": MetadataValue.int(negativos),
            "medidas_unicas": MetadataValue.int(medidas),
            "principio": MetadataValue.text("Datos preparados para viz")
        }
    )

@asset(description="Datos para gráfico")
def viz_data_canarias(context: AssetExecutionContext, transformed_canarias: pd.DataFrame) -> pd.DataFrame:
    return transformed_canarias[['TIME_PERIOD#es', 'MEDIDA_CORTA', 'OBS_VALUE']].copy()

@asset(description="Gráfico evolución Canarias")
def grafico_evolucion_canarias(context: AssetExecutionContext, viz_data_canarias: pd.DataFrame) -> MaterializeResult:
    output_path = OUTPUT_DIR / "evolucion_canarias.png"
    grafico = (
        ggplot(viz_data_canarias, aes('TIME_PERIOD#es', 'OBS_VALUE', color='MEDIDA_CORTA')) +
        geom_line(size=1.2) + geom_point(size=2) +
        labs(title='Evolución Rentas Canarias', x='Año', y='€') +
        scale_color_brewer(type='qual', palette='Set1') + theme_minimal()
    )
    grafico.save(str(output_path), dpi=300)
    return MaterializeResult(metadata={"path": MetadataValue.path(str(output_path))})

@asset_check(asset=grafico_evolucion_canarias)
def check_viz_paleta_renta() -> AssetCheckResult:             # ← sin context
    df = pd.read_csv(DATA_DIR / "distribucion-renta-canarias.csv")
    df = df[df['TERRITORIO_CODE'] == 'ES70'].copy()
    df['MEDIDA_CORTA'] = df['MEDIDAS#es'].replace(MEDIDAS_DICT)


    # CASO SUCIO CHECK 3 ✅: añadimos categorías extra solo en el df local del check
    # extras = pd.DataFrame({'MEDIDA_CORTA': ['Extra1', 'Extra2', 'Extra3', 'Extra4']})
    # df = pd.concat([df, extras], ignore_index=True)

    categorias = int(df['MEDIDA_CORTA'].nunique())
    passed = bool(categorias <= 8)                             # ← bool()

    return AssetCheckResult(
        passed=passed,
        metadata={
            "categorias": MetadataValue.int(categorias),
            "limite": MetadataValue.int(8),
            "principio": MetadataValue.text("Legibilidad paleta cualitativa")
        }
    )
