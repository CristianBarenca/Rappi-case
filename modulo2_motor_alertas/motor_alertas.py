###############################################
# Author: Cristian U. Barenca
#    
# Funcionalidades:
#   1. Carga y limpia datos históricos de pedidos y zonas.
#   2. Calcula métricas de oferta/demanda para identificar zonas estresadas.
#   3. Consulta pronóstico de lluvia mediante API.
#   4. Evalúa riesgos y genera alertas con recomendaciones de ajuste de earnings.
#   5. Evita alertas duplicadas usando historial temporal.
#   6. Funciona asíncronamente para monitoreo continuo cada 30 minutos.
#   7. Permite pruebas con lluvia simulada (FORCE_RAIN) sin depender de la API.
#
# Version:
#    v2.0 - Creación del ejercicio completo con comentarios detallados.
#    Fecha: 26 de marzo de 2025
###############################################
import asyncio                              # Para programación asíncrona y colas de alertas
import logging                              # Para registrar información y errores
import pandas as pd                         # Para registrar información y errores
import requests                             # Para manejo de datos tipo DataFrame
from shapely.wkt import loads               # Para hacer llamadas HTTP a la API del clima
from datetime import datetime, timedelta    # Para manejar fechas y diferencias de tiempo
import numpy as np                          # Para cálculos numéricos, promedios, etc.

# -------------------------
# CONFIGURACIÓN
# -------------------------
# Intervalo de chequeo del clima en minutos
CHECK_INTERVAL_MINUTES = 30

# -------------------------
# VARIABLES DE PRUEBA
# -------------------------
# Para pruebas locales, simula lluvia
# Descomenta para probar el flujo sin depender del clima real
FORCE_RAIN = True
FORCED_PRECIP_MM = 6

# Configuración del logging
logging.basicConfig(level=logging.INFO)

# -------------------------
# CARGA DE DATOS
# -------------------------
# DataFrame principal de datos históricos de pedidos y cobertura
df = pd.read_excel("rappi_delivery_case_data.xlsx", sheet_name="RAW_DATA")
# DataFrame principal de datos históricos de pedidos y cobertura
zones = pd.read_excel("rappi_delivery_case_data.xlsx", sheet_name="ZONE_INFO")
# Polígonos geográficos de las zonas
polygons_df = pd.read_excel("rappi_delivery_case_data.xlsx", sheet_name="ZONE_POLYGONS")

# Función segura para cargar geometría WKT
def safe_load_wkt(wkt):
    try:
        if pd.isna(wkt): return None
        wkt = str(wkt).strip()
        if not wkt.startswith("POLYGON"): return None
        return loads(wkt)
    except:
        return None

# Aplica la función y elimina filas sin geometría válida
polygons_df["geometry"] = polygons_df["GEOMETRY_WKT"].apply(safe_load_wkt)
polygons_df = polygons_df.dropna(subset=["geometry"])

# -------------------------
# FEATURE ENGINEERING
# -------------------------
# Filtra solo zonas con rutas conectadas > 0
df = df[df["CONNECTED_RT"] > 0].copy()
# Calcula la relación oferta/demanda
df["SUPPLY_DEMAND_RATIO"] = df["ORDERS"] / df["CONNECTED_RT"]

# Calcula el ratio de referencia para determinar alertas
baseline_ratio = df["SUPPLY_DEMAND_RATIO"].quantile(0.75)

# Agrupa la relación oferta/demanda por rangos de precipitación
precip_bins = [0,1,2,3,4,5,6,7,10,20]
grouped = df.groupby(pd.cut(df["PRECIPITATION_MM"], bins=precip_bins))["SUPPLY_DEMAND_RATIO"].mean()

# Determina el umbral de precipitación para disparar alerta
PRECIP_THRESHOLD = 2
for bin_range, ratio in grouped.items():
    if ratio > baseline_ratio:
        PRECIP_THRESHOLD = bin_range.left
        break

# Identifica zonas con alta presión de oferta/demanda para ajustar multiplicador
stress = df[df["SUPPLY_DEMAND_RATIO"] > 1.5]
MULTIPLIER = (stress["EARNINGS"].mean() / df["EARNINGS"].mean()) if len(stress) > 0 else 1.4

# Diccionario para guardar historial de alertas y evitar duplicadas
alert_history = {}

# -------------------------
# FUNCIONES
# -------------------------
def get_forecast(lat, lon):
    """
        Consulta la API de Open-Meteo para obtener precipitación horaria
        para latitud y longitud especificadas.
    """
    try:
        url = "https://api.open-meteo.com/v1/forecast"
        params = {"latitude": lat, "longitude": lon, "hourly": "precipitation", "forecast_days": 1}
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logging.error(f"Weather API error: {e}")
        return None

def evaluate_zone(zone_name, forecast_mm):
    """
        Evalúa si una zona requiere alerta según la precipitación y ratios históricos.
        Calcula riesgo, ratio proyectado y earnings recomendados.
        Implementa deduplicación temporal para no repetir alertas recientes.
    """
    zone_data = df[df["ZONE"]==zone_name]
    if zone_data.empty: return None

    base_earnings = zone_data["EARNINGS"].mean()
    corr = zone_data["PRECIPITATION_MM"].corr(zone_data["SUPPLY_DEMAND_RATIO"])

    # Ajusta umbral local según correlación
    local_threshold = PRECIP_THRESHOLD
    if corr is not None:
        if corr>0.4: local_threshold = max(1, PRECIP_THRESHOLD-1)
        elif corr<0.2: local_threshold = PRECIP_THRESHOLD+1

    # Si precipitación pronosticada es menor al umbral, no se alerta
    if forecast_mm < local_threshold: return None

    # Calcula ratio proyectado y riesgo
    projected_ratio = 1.0 + (forecast_mm / local_threshold) * 0.8
    risk = "ALTO" if projected_ratio > 1.7 else "MEDIO"
    new_earnings = int(base_earnings * MULTIPLIER)

    # Evita alertas repetidas en ventana de 2 horas y similar precipitación
    now = datetime.now()
    if zone_name in alert_history:
        last_time, last_precip = alert_history[zone_name]
        if (now - last_time < timedelta(hours=2)) and (forecast_mm <= last_precip * 1.2):
            return None
    alert_history[zone_name] = (now, forecast_mm)

    # Imprime resumen de alerta
    print('Zona:' + zone_name + ',' +
      'Precipitación esperada ' + str(round(forecast_mm,1)) + ' en las proximas 2 horas,' +
      "Riesgo: " + risk + ',' +
      "(ratio proyectado ~" + str(round(projected_ratio,2)) + ' basado en histórico),' +
      "Acción recomendada: subir earnings de " + str(int(base_earnings)) + ' a ' + str(new_earnings) +
      ' Zonas secundarias a monitorear: Carretera Nacional y Santiago.')

    return {"zone": zone_name, "precip": round(forecast_mm,1), "risk": risk,
            "ratio": round(projected_ratio,2), "earnings_from": int(base_earnings),
            "earnings_to": new_earnings}

# -------------------------
# PRODUCER
# -------------------------
async def alerts_producer(queue: asyncio.Queue):
    """
        Loop principal que consulta el pronóstico para cada zona y genera alertas.
        Las alertas se ponen en la cola asíncrona para que otro proceso las consuma.
    """
    logging.info("Alert producer started...")
    while True:
        try:
            for _, row in zones.iterrows():
                forecast = get_forecast(row["LATITUDE_CENTER"], row["LONGITUDE_CENTER"])
                if not forecast or "hourly" not in forecast: continue

                # Promedia las primeras 2 horas de precipitación
                precipitation = forecast["hourly"]["precipitation"][:2]
                if not precipitation: continue

                # Fuerza lluvia para pruebas locales si está activado
                avg_precip = FORCED_PRECIP_MM if FORCE_RAIN else np.mean(precipitation)

                # Evalúa si la zona requiere alerta
                result = evaluate_zone(row["ZONE"], avg_precip)
                if result:
                    await queue.put(result)
        except Exception as e:
            logging.error(f"Producer loop error: {e}")
        # Espera el intervalo configurado antes de la siguiente consulta
        await asyncio.sleep(CHECK_INTERVAL_MINUTES*60)

# -------------------------
# EJECUCIÓN
# -------------------------
if __name__=="__main__":
    # Cola asíncrona de alertas
    queue = asyncio.Queue()
    # Ejecuta el producer en loop asíncrono
    asyncio.run(alerts_producer(queue))