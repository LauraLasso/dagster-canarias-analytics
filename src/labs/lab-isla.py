"""
Gráfico único: Top 5 municipios con evolución temporal por tipo de medida
"""

import pandas as pd
from plotnine import *
from pathlib import Path

# Configuración
BASE_DIR = Path(__file__).resolve().parent.parent.parent
print(BASE_DIR)
# DATA_DIR = Path("./data")
# OUTPUT_DIR = Path("./images/test")

DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "images" / "test"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

MEDIDAS_DICT = {
    'Sueldos y salarios': 'Sueldos',
    'Otros ingresos': 'Otros',
    'Otras prestaciones': 'Prestaciones',
    'Pensiones': 'Pensiones',
    'Prestaciones por desempleo': 'Desempleo'
}

print("=" * 60)
print("GRÁFICO: TOP 5 MUNICIPIOS - EVOLUCIÓN TEMPORAL POR MEDIDA")
print("=" * 60)

# ============================================
# CARGAR Y PREPARAR DATOS
# ============================================

print("\n[1/3] Cargando datos...")

# Cargar renta
df_renta = pd.read_csv(DATA_DIR / "distribucion-renta-canarias.csv")
df_codislas = pd.read_csv(DATA_DIR / "codislas.csv", encoding='latin-1', sep=';')

# Filtrar municipios
df_municipios = df_renta[
    df_renta['TERRITORIO_CODE'].str.match(r'^\d{5}$', na=False)
].copy()

# Integrar nombres
df_codislas['TERRITORIO_CODE'] = (
    df_codislas['CPRO'].astype(str) + 
    df_codislas['CMUN'].astype(str).str.zfill(3)
)
df_codislas['ISLA'] = df_codislas['ISLA'].str.strip()
df_codislas['NOMBRE'] = df_codislas['NOMBRE'].str.strip()

df_merged = pd.merge(
    df_municipios,
    df_codislas[['TERRITORIO_CODE', 'ISLA', 'NOMBRE']],
    on='TERRITORIO_CODE',
    how='left'
)

df_merged['MEDIDA_CORTA'] = df_merged['MEDIDAS#es'].replace(MEDIDAS_DICT)

print(f"✓ Total municipios: {df_merged['TERRITORIO_CODE'].nunique()}")

# ============================================
# SELECCIONAR TOP 5 MUNICIPIOS
# ============================================

print("\n[2/3] Seleccionando top 5 municipios...")

df_valid = df_merged[df_merged['ISLA'].notna()].copy()

# Calcular renta promedio por municipio en 2023 (Sueldos)
df_ranking = df_valid[
    (df_valid['TIME_PERIOD#es'] == 2023) & 
    (df_valid['MEDIDA_CORTA'] == 'Sueldos')
].copy()

# Top 5 municipios con mayores sueldos
top_5 = df_ranking.nlargest(5, 'OBS_VALUE')[['TERRITORIO_CODE', 'NOMBRE', 'ISLA', 'OBS_VALUE']]

print("\nTop 5 municipios (por sueldos 2023):")
for idx, row in top_5.iterrows():
    print(f"  {row['NOMBRE']:30} ({row['ISLA']:15}) - {row['OBS_VALUE']:,.0f}€")

# Filtrar dataset completo con esos municipios
df_viz = df_valid[df_valid['TERRITORIO_CODE'].isin(top_5['TERRITORIO_CODE'])].copy()

# Crear nombre completo para leyenda (ahora hay menos municipios, caben nombres completos)
df_viz['MUNICIPIO_NOMBRE'] = df_viz['NOMBRE'] + ' (' + df_viz['ISLA'] + ')'

print(f"\n✓ Datos filtrados: {len(df_viz)} registros")
print(f"✓ Años: {sorted(df_viz['TIME_PERIOD#es'].unique())}")
print(f"✓ Medidas: {sorted(df_viz['MEDIDA_CORTA'].unique())}")

# ============================================
# GENERAR GRÁFICO
# ============================================

print("\n[3/3] Generando gráfico de líneas temporales...")

grafico = (
    ggplot(df_viz, aes(x='TIME_PERIOD#es', y='OBS_VALUE', 
                       color='MUNICIPIO_NOMBRE', group='MUNICIPIO_NOMBRE')) +
    geom_line(size=1.2) +
    geom_point(size=3) +
    facet_wrap('~MEDIDA_CORTA', scales='free_y', ncol=2) +
    labs(
        title='Evolución Temporal de Ingresos en Top 5 Municipios de Canarias (2015-2023)',
        subtitle='Por tipo de medida - Municipios con mayores sueldos en 2023',
        x='Año',
        y='Valor (€)',
        color='Municipio'
    ) +
    scale_x_continuous(breaks=[2015, 2017, 2019, 2021, 2023]) +
    scale_y_continuous(labels=lambda l: [f'{int(v):,}' for v in l]) +
    scale_color_brewer(type='qual', palette='Set1') +
    theme_minimal() +
    theme(
        figure_size=(16, 10),
        plot_title=element_text(size=16, weight='bold'),
        plot_subtitle=element_text(size=12, style='italic'),
        legend_position='bottom',
        legend_text=element_text(size=10),
        legend_key_size=12,
        axis_text_x=element_text(angle=45, hjust=1, size=10),
        axis_text_y=element_text(size=10),
        strip_text=element_text(size=12, weight='bold'),
        strip_background=element_rect(fill='#e8e8e8')
    )
)

output_path = OUTPUT_DIR / "grafico_top5_evolucion_medidas.png"
grafico.save(str(output_path), dpi=300)

print(f"\n✓ Gráfico guardado: {output_path}")

# ============================================
# ESTADÍSTICAS ADICIONALES
# ============================================

print("\n" + "=" * 60)
print(" ESTADÍSTICAS")
print("=" * 60)

# Calcular crecimiento 2015-2023 para cada municipio
for muni_code in top_5['TERRITORIO_CODE']:
    muni_data = df_viz[
        (df_viz['TERRITORIO_CODE'] == muni_code) & 
        (df_viz['MEDIDA_CORTA'] == 'Sueldos')
    ]
    
    valor_2015 = muni_data[muni_data['TIME_PERIOD#es'] == 2015]['OBS_VALUE'].values
    valor_2023 = muni_data[muni_data['TIME_PERIOD#es'] == 2023]['OBS_VALUE'].values
    
    if len(valor_2015) > 0 and len(valor_2023) > 0:
        crecimiento = ((valor_2023[0] - valor_2015[0]) / valor_2015[0]) * 100
        nombre = muni_data['NOMBRE'].iloc[0]
        print(f"{nombre:30} | Crecimiento 2015-2023: {crecimiento:+.1f}%")

# ============================================
# RESUMEN
# ============================================

print("\n" + "=" * 60)
print(" GRÁFICO COMPLETADO")
print("=" * 60)
print(f"Archivo: {output_path.name}")
print(f"Dimensiones: 16×10 pulgadas @ 300 DPI")
print(f"Municipios mostrados: 5")
print(f"Medidas: 5 (cada una en su panel)")
print(f"Periodo: 2015-2023")
print("\nCaracterísticas:")
print("  • 5 líneas de colores (una por municipio)")
print("  • Facetas por tipo de medida")
print("  • Escalas independientes (free_y)")
print("  • Nombres completos en leyenda")
print("  • Puntos más grandes para mejor visibilidad")