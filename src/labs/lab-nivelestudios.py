"""
Análisis: Evolución temporal de Renta y Educación por ISLA
Promedio de todos los municipios de cada isla (2015-2023)
"""

import pandas as pd
from plotnine import *
from pathlib import Path

# Configuración
BASE_DIR = Path(__file__).resolve().parent.parent.parent
# DATA_DIR = Path("./data")
# OUTPUT_DIR = Path("./images/test")

DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "images" / "test"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

print("=" * 80)
print("ANÁLISIS: RENTA TEMPORAL × PERFIL EDUCATIVO POR ISLA")
print("=" * 80)

# ============================================
# 1. CARGAR RENTA TEMPORAL POR ISLA
# ============================================

print("\n[1/3] Cargando evolución de renta por isla (2015-2023)...")

df_renta = pd.read_csv(DATA_DIR / "distribucion-renta-canarias.csv")
df_codislas = pd.read_csv(DATA_DIR / "codislas.csv", encoding='latin-1', sep=';')

# Filtrar municipios
df_municipios = df_renta[
    df_renta['TERRITORIO_CODE'].str.match(r'^\d{5}$', na=False)
].copy()

# Integrar islas
df_codislas['TERRITORIO_CODE'] = (
    df_codislas['CPRO'].astype(str) + 
    df_codislas['CMUN'].astype(str).str.zfill(3)
)
df_codislas['ISLA'] = df_codislas['ISLA'].str.strip()
df_codislas['NOMBRE'] = df_codislas['NOMBRE'].str.strip()

df_renta_isla = pd.merge(
    df_municipios,
    df_codislas[['TERRITORIO_CODE', 'ISLA', 'NOMBRE']],
    on='TERRITORIO_CODE',
    how='left'
)

# Filtrar solo válidos
df_renta_isla = df_renta_isla[df_renta_isla['ISLA'].notna()].copy()

# Calcular renta PROMEDIO por ISLA y año (promediando todos los municipios)
df_renta_isla_promedio = df_renta_isla.groupby(
    ['ISLA', 'TIME_PERIOD#es'],
    as_index=False
)['OBS_VALUE'].mean()

df_renta_isla_promedio = df_renta_isla_promedio.rename(columns={
    'TIME_PERIOD#es': 'AÑO',
    'OBS_VALUE': 'RENTA_PROMEDIO'
})

print(f"✓ Islas procesadas: {sorted(df_renta_isla_promedio['ISLA'].unique())}")
print(f"✓ Años: {sorted(df_renta_isla_promedio['AÑO'].unique())}")

# Estadísticas por isla en 2023
df_2023 = df_renta_isla_promedio[df_renta_isla_promedio['AÑO'] == 2023]
print("\nRenta promedio por isla (2023):")
for _, row in df_2023.sort_values('RENTA_PROMEDIO', ascending=False).iterrows():
    print(f"  {row['ISLA']:20} - {row['RENTA_PROMEDIO']:,.0f}€")

# ============================================
# 2. CARGAR PERFIL EDUCATIVO TEMPORAL POR ISLA
# ============================================

print("\n[2/3] Cargando evolución educativa por isla (2021-2023)...")

file_estudios = DATA_DIR / "nivelestudios.csv"
file_estudios_xlsx = DATA_DIR / "nivelestudios.xlsx"

if file_estudios.exists():
    df_estudios_raw = pd.read_csv(file_estudios)
elif file_estudios_xlsx.exists():
    df_estudios_raw = pd.read_excel(file_estudios_xlsx, engine='openpyxl')
else:
    print("❌ No se encuentra nivelestudios")
    exit(1)

df_estudios = df_estudios_raw.copy()

# Procesar educación
df_estudios['CODIGO_MUNICIPIO'] = df_estudios['Municipios de 500 habitantes o más'].str.extract(r'(\d{5})')
df_estudios['AÑO'] = pd.to_datetime(df_estudios['Periodo']).dt.year

df_estudios = df_estudios.rename(columns={
    'Sexo': 'SEXO',
    'Nivel de estudios en curso': 'NIVEL_ESTUDIOS',
    'Total': 'POBLACION'
})

# Categorizar niveles
df_estudios['NIVEL_CORTO'] = df_estudios['NIVEL_ESTUDIOS'].replace({
    'Educación primaria e inferior': 'Primaria',
    'Primera etapa de Educación Secundaria y similar': 'ESO',
    'Segunda etapa de educación secundaria, con orientación general': 'Bachillerato',
    'Segunda etapa de Educación Secundaria, con orientación profesional (con y sin continuidad en la educación superior); Educación postsecundaria no superior': 'FP',
    'Educación superior': 'Universidad'
})

# Filtrar 2021-2023, Total, solo niveles válidos
df_estudios_temporal = df_estudios[
    (df_estudios['AÑO'].between(2021, 2023)) &
    (df_estudios['SEXO'] == 'Total') &
    (df_estudios['NIVEL_CORTO'].isin(['Primaria', 'ESO', 'Bachillerato', 'FP', 'Universidad'])) &
    (df_estudios['POBLACION'] > 0)
].copy()

# Integrar isla a los datos de educación
df_estudios_temporal['CODIGO_MUNICIPIO'] = df_estudios_temporal['CODIGO_MUNICIPIO'].astype(str).str.zfill(5)
df_codislas['TERRITORIO_CODE'] = df_codislas['TERRITORIO_CODE'].astype(str).str.zfill(5)

df_estudios_temporal = df_estudios_temporal.merge(
    df_codislas[['TERRITORIO_CODE', 'ISLA']],
    left_on='CODIGO_MUNICIPIO',
    right_on='TERRITORIO_CODE',
    how='left'
)

# Filtrar solo con isla válida
df_estudios_temporal = df_estudios_temporal[df_estudios_temporal['ISLA'].notna()].copy()

# Agregar por ISLA, año y nivel educativo
df_edu_isla_agg = df_estudios_temporal.groupby(
    ['ISLA', 'AÑO', 'NIVEL_CORTO'],
    as_index=False
)['POBLACION'].sum()

# Calcular % por nivel y año para cada isla
df_total_edu_isla = df_edu_isla_agg.groupby(['ISLA', 'AÑO'])['POBLACION'].sum().reset_index()
df_total_edu_isla = df_total_edu_isla.rename(columns={'POBLACION': 'POBLACION_TOTAL'})

df_edu_isla_agg = df_edu_isla_agg.merge(df_total_edu_isla, on=['ISLA', 'AÑO'])
df_edu_isla_agg['PORCENTAJE'] = (df_edu_isla_agg['POBLACION'] / df_edu_isla_agg['POBLACION_TOTAL']) * 100

print(f"\n✓ Islas con datos educativos: {sorted(df_edu_isla_agg['ISLA'].unique())}")
print(f"✓ Años educativos: {sorted(df_edu_isla_agg['AÑO'].unique())}")
print(f"✓ Registros: {len(df_edu_isla_agg)}")

# ============================================
# 3. GENERAR GRÁFICOS
# ============================================

print("\n[3/3] Generando gráficos por isla...")

# GRÁFICO 1: Evolución temporal de renta por isla
# grafico1 = (
#     ggplot(df_renta_isla_promedio, aes(x='AÑO', y='RENTA_PROMEDIO', 
#                                         color='ISLA', group='ISLA')) +
#     geom_line(size=1.5) +
#     geom_point(size=3) +
#     labs(
#         title='Evolución de Renta Promedio por Isla (2015-2023)',
#         subtitle='Promedio de todos los municipios de cada isla',
#         x='Año',
#         y='Renta Promedio (€)',
#         color='Isla'
#     ) +
#     scale_x_continuous(breaks=range(2015, 2024, 2)) +
#     scale_y_continuous(labels=lambda l: [f'{int(v):,}' for v in l]) +
#     scale_color_brewer(type='qual', palette='Set1') +
#     theme_minimal() +
#     theme(
#         figure_size=(16, 8),
#         plot_title=element_text(size=16, weight='bold'),
#         plot_subtitle=element_text(size=12, style='italic'),
#         legend_position='bottom',
#         legend_text=element_text(size=10)
#     )
# )

# output_path1 = OUTPUT_DIR / "evolucion_renta_por_isla.png"
# grafico1.save(str(output_path1), dpi=300)
# print(f"✓ Gráfico 1 guardado: {output_path1}")

# GRÁFICO 2: Evolución del perfil educativo por isla (facetas)
# grafico2 = (
#     ggplot(df_edu_isla_agg, aes(x='AÑO', y='PORCENTAJE', 
#                                  color='NIVEL_CORTO', group='NIVEL_CORTO')) +
#     geom_line(size=1.2) +
#     geom_point(size=3) +
#     facet_wrap('~ISLA', ncol=2) +
#     labs(
#         title='Evolución del Perfil Educativo por Isla (2021-2023)',
#         subtitle='Porcentaje de población por nivel educativo | Promedio de municipios por isla',
#         x='Año',
#         y='Porcentaje de Población (%)',
#         color='Nivel Educativo'
#     ) +
#     scale_x_continuous(breaks=[2021, 2022, 2023]) +
#     scale_color_brewer(type='qual', palette='Set2') +
#     theme_minimal() +
#     theme(
#         figure_size=(16, 10),
#         plot_title=element_text(size=16, weight='bold'),
#         plot_subtitle=element_text(size=12, style='italic'),
#         legend_position='bottom',
#         legend_text=element_text(size=10),
#         strip_text=element_text(size=12, weight='bold'),
#         strip_background=element_rect(fill='#e8e8e8'),
#         axis_text_x=element_text(angle=0, hjust=0.5, size=9)
#     )
# )

# output_path2 = OUTPUT_DIR / "evolucion_perfil_educativo_por_isla.png"
# grafico2.save(str(output_path2), dpi=300)
# print(f"✓ Gráfico 2 guardado: {output_path2}")

# GRÁFICO 3: Comparativa de perfil educativo 2023 por isla (barras)
df_edu_2023 = df_edu_isla_agg[df_edu_isla_agg['AÑO'] == 2023].copy()

grafico3 = (
    ggplot(df_edu_2023, aes(x='ISLA', y='PORCENTAJE', fill='NIVEL_CORTO')) +
    geom_bar(stat='identity', position='dodge') +
    labs(
        title='Perfil Educativo por Isla en 2023',
        subtitle='Distribución de población por nivel educativo',
        x='Isla',
        y='Porcentaje de Población (%)',
        fill='Nivel Educativo'
    ) +
    scale_fill_brewer(type='qual', palette='Set2') +
    theme_minimal() +
    theme(
        figure_size=(16, 8),
        plot_title=element_text(size=16, weight='bold'),
        plot_subtitle=element_text(size=12, style='italic'),
        legend_position='bottom',
        axis_text_x=element_text(angle=45, hjust=1, size=10)
    )
)

output_path3 = OUTPUT_DIR / "perfil_educativo_isla_2023.png"
grafico3.save(str(output_path3), dpi=300)
print(f"✓ Gráfico 3 guardado: {output_path3}")

# ============================================
# 4. TABLA RESUMEN POR ISLA
# ============================================

print("\n" + "=" * 80)
print("📊 RESUMEN POR ISLA")
print("=" * 80)

for isla in sorted(df_renta_isla_promedio['ISLA'].unique()):
    print(f"\n🏝️  {isla.upper()}")
    
    # Renta
    df_isla_renta = df_renta_isla_promedio[df_renta_isla_promedio['ISLA'] == isla].sort_values('AÑO')
    renta_2015 = df_isla_renta[df_isla_renta['AÑO'] == 2015]['RENTA_PROMEDIO'].values
    renta_2023 = df_isla_renta[df_isla_renta['AÑO'] == 2023]['RENTA_PROMEDIO'].values
    
    if len(renta_2015) > 0 and len(renta_2023) > 0:
        crecimiento_renta = ((renta_2023[0] - renta_2015[0]) / renta_2015[0]) * 100
        print(f"  Renta 2015: {renta_2015[0]:,.0f}€")
        print(f"  Renta 2023: {renta_2023[0]:,.0f}€")
        print(f"  Crecimiento renta: {crecimiento_renta:+.1f}%")
    
    # Educación
    df_isla_edu = df_edu_isla_agg[
        (df_edu_isla_agg['ISLA'] == isla) & 
        (df_edu_isla_agg['AÑO'] == 2023)
    ].sort_values('PORCENTAJE', ascending=False)
    
    print(f"  Perfil educativo 2023:")
    for _, row in df_isla_edu.iterrows():
        print(f"    • {row['NIVEL_CORTO']:15}: {row['PORCENTAJE']:5.1f}%")
    
    # Calcular municipios por isla
    num_municipios = df_codislas[df_codislas['ISLA'] == isla]['TERRITORIO_CODE'].nunique()
    print(f"  Municipios incluidos: {num_municipios}")

print("\n" + "=" * 80)
print("✅ ANÁLISIS COMPLETADO - 3 GRÁFICOS POR ISLA GENERADOS")
print("=" * 80)
print("\nArchivos generados:")
print("  1. evolucion_renta_por_isla.png")
print("  2. evolucion_perfil_educativo_por_isla.png")
print("  3. perfil_educativo_isla_2023.png")