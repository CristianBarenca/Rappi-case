###############################################
# Author: Cristian U. Barenca
#    
# Funcionalidades:
#   1. Integración con Telegram y Google Generative AI
#   2. Memoria de contexto LLM
#   3. Deduplicación de alertas
#   4. Throttling (control de frecuencia)
#   5. Generación de decisiones mediante LLM
#   6. Formateo de mensajes para Telegram
#   7. Envío asíncrono a Telegram
#   8. Consumer asíncrono de alertas
#
# Version:
#    v2.0 - Creación del ejercicio completo con comentarios detallados.
#    Fecha: 26 de marzo de 2025
###############################################
import asyncio                                                  # Para el manejo de concurrencia asíncrona
import logging                                                  # Registro de información y errores
import hashlib                                                  # Generación de hashes
from datetime import datetime, timedelta                        # Manejo de fechas y tiempos
from telegram import Bot                                        #Integración con Telegram
from langchain_google_genai import ChatGoogleGenerativeAI       #Uso de un LLM de Google Generative AI
from langchain_core.messages import HumanMessage, SystemMessage # Crear, formatear mensajes y generar decisiones LLM
from modulo3_agente_telegram.credentials import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, GOOGLE_API_KEY # Carga de credenciales

# -------------------------
# CONFIGURACIÓN
# -------------------------
logging.basicConfig(level=logging.INFO)

# Inicializamos el bot de Telegram con el token
bot = Bot(token=TELEGRAM_TOKEN)

# Inicializamos el LLM de Google Generative AI
# Este LLM se usará como "agente" para generar decisiones sobre alertas
llm = ChatGoogleGenerativeAI(
    model="gemini-2.5-flash",
    temperature=0.3,
    google_api_key=GOOGLE_API_KEY
)

# -------------------------
# MEMORIA DE CONTEXTO
# -------------------------
# Guarda las alertas recientes por zona, para dar contexto al LLM
memory = {}  # {zone: [{"timestamp": datetime, "alert": {...}}, ...]}
MEMORY_TTL_MINUTES = 10  # Tiempo que se guarda cada alerta en memoria.

def add_to_memory(alert):
    """Agrega una alerta a la memoria con timestamp actual."""
    now = datetime.utcnow()
    zone = alert["zone"]
    memory.setdefault(zone, []).append({"timestamp": now, "alert": alert})

def get_recent_alerts(zone):
    """Devuelve las alertas recientes de una zona y limpia las viejas según TTL."""
    now = datetime.utcnow()
    alerts = memory.get(zone, [])
    recent = [a for a in alerts if now - a["timestamp"] < timedelta(minutes=MEMORY_TTL_MINUTES)]
    memory[zone] = recent
    return recent

def get_context_summary(zone):
    """Genera un resumen de contexto para la zona, que se usará en prompts al LLM."""
    recent = get_recent_alerts(zone)
    if not recent:
        return "Sin alertas recientes"

    avg_precip = sum(a["alert"]["precip"] for a in recent) / len(recent)
    max_risk = max(a["alert"]["risk"] for a in recent)
    return f"{len(recent)} alertas recientes | lluvia prom: {avg_precip:.1f} | riesgo máx: {max_risk}"

# -------------------------
# DEDUPLICACIÓN
# -------------------------
# Evita enviar la misma alerta varias veces
dedup_cache = {}  # {hash: timestamp}
# Ventana de tiempo para considerar duplicada una alerta
DEDUP_WINDOW_MINUTES = 10

def alert_hash(alert):
    """Crea un hash único de la alerta basado en zona, riesgo y precipitación."""
    key = f"{alert['zone']}-{alert['risk']}-{round(alert['precip'])}"
    return hashlib.md5(key.encode()).hexdigest()

def is_duplicate(alert):
    """Devuelve True si la alerta ya se envió recientemente, False si es nueva."""
    now = datetime.utcnow()
    h = alert_hash(alert)
    if h in dedup_cache and now - dedup_cache[h] < timedelta(minutes=DEDUP_WINDOW_MINUTES):
        return True
    dedup_cache[h] = now
    return False

# -------------------------
# THROTTLING
# -------------------------
# Evita spam de mensajes por zona
throttle_last_sent = {}  # {zone: timestamp}
# Tiempo mínimo entre envíos para la misma zona
THROTTLE_COOLDOWN_MINUTES = 5

def can_send_alert(zone):
    # Evita spam de mensajes por zona
    now = datetime.utcnow()
    last = throttle_last_sent.get(zone)
    if last is None or now - last > timedelta(minutes=THROTTLE_COOLDOWN_MINUTES):
        throttle_last_sent[zone] = now
        return True
    return False

# -------------------------
# AGENTE (LLM) DECISION
# -------------------------
def generate_decision(alert):
    """
    Usa el LLM para generar decisiones sobre la alerta:
    - impact: qué va a pasar
    - action: qué hacer
    Incluye contexto reciente de la zona.
    """
    context_summary = get_context_summary(alert["zone"])

    messages = [
        SystemMessage(content=(
            "Eres un Operations Manager experto. Devuelve SOLO JSON válido:\n"
            "{'impact': string, 'action': string}"
        )),
        HumanMessage(content=f"""
Zona: {alert['zone']}
Precipitación: {alert['precip']} mm/hr
Riesgo: {alert['risk']}
Ratio: {alert['ratio']}
Earnings: {alert['earnings_from']} -> {alert['earnings_to']}

Contexto reciente:
{context_summary}
""")
    ]

    try:
        response = llm.invoke(messages)
        # Convierte el JSON devuelto a dict
        return eval(response.content)
    except:
        # Fallback si el LLM falla
        return {"impact": "Alta demanda acumulada", "action": "Incrementar flota +25%"}

# -------------------------
# FORMATEO TELEGRAM
# -------------------------
def format_telegram_msg(alert, decision, secondary_zones):
    """
    Formatea la alerta y la decisión para enviarla a Telegram en Markdown.
    Incluye prioridad, zona, riesgo, impacto, acción, ventana y zonas secundarias.
    """
    priority = "P1" if alert["risk"] == "Alto" else "P2" if alert["risk"] == "Medio" else "P3"
    emoji = {"P1":"🚨","P2":"⚠️","P3":"ℹ️"}[priority]

    return f"""{emoji} *ALERTA {priority}*

*Zona:* {alert['zone']}
*Riesgo:* {alert['risk']}
*Lluvia:* {alert['precip']} mm/hr

*Impacto esperado:*
- {decision['impact']}

*Acción recomendada:*
- {decision['action']}

*Ventana:* 30 min
*Zonas secundarias:* {', '.join(secondary_zones)}
"""

# -------------------------
# ENVÍO ASYNC TELEGRAM
# -------------------------
async def send_telegram_async(msg):
    """Envía un mensaje a Telegram de manera asíncrona y loggea el resultado."""
    try:
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=msg,
            parse_mode="Markdown"
        )
        logging.info(f"Mensaje enviado ✅: {msg}")
    except Exception as e:
        logging.error(f"Telegram error: {e}")

# -------------------------
# CONSUMER
# -------------------------
async def alerts_consumer(queue: asyncio.Queue):
    """
    Consume alertas desde una cola asíncrona:
        1. Deduplicación
        2. Throttling
        3. Generación de decisión con LLM
        4. Formateo para Telegram
        5. Envío
        6. Guardar en memoria
    """
    logging.info("Alert consumer started...")

    while True:
        alert = await queue.get()
        try:
            zone = alert["zone"]

            # Evita alertas duplicadas
            if is_duplicate(alert):
                logging.info(f"Duplicada ignorada: {zone}")
                queue.task_done()
                continue

            # Throttling para no spamear la misma zona
            if not can_send_alert(zone):
                logging.info(f"Throttle activo en: {zone}")
                queue.task_done()
                continue

            # Genera impacto y acción usando LLM
            decision = generate_decision(alert)

            # Formatea el mensaje final para Telegram
            msg = format_telegram_msg(
                alert,
                decision,
                secondary_zones=["Carretera Nacional","Santa Catarina"]
            )

            # Envía el mensaje
            await send_telegram_async(msg)

            # Guarda alerta en memoria para contexto futuro
            add_to_memory(alert)

        except Exception as e:
            logging.error(f"Consumer error: {e}")

        finally:
            queue.task_done()