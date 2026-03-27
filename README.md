# Repositorio con la solución a un caso técnico para el puesto de AI Engineer en Rappi 💼

**Contexto del Problema**

Rappi opera flotas de repartidores en tiempo real en cientos de zonas a través de 9 países. El equipo de Operations depende de la disponibilidad de repartidores conectados para atender la demanda de órdenes de forma sostenible. Cuando la oferta de repartidores y la demanda de órdenes no están balanceadas, se generan dos tipos de problemas:

* Saturación: demasiadas órdenes relativas a repartidores disponibles — clientes esperan más tiempo, órdenes se pierden, NPS cae.

* Sobre-oferta: demasiados repartidores activos con pocas órdenes — ineficiencia operacional, costo sin retorno.

La métrica central de balance operacional es:

```
Ratio = Órdenes / Repartidores Conectados   
< 0.5: Sobre-oferta (ineficiencia de costo)   
 0.9 – 1.2: Rango saludable    
> 1.8: SATURACIÓN (pérdida de órdenes)
```

El caso está dividido en tres módulos progresivos. El Módulo 1 es el fundamento analítico: sin él, los Módulos 2 y 3 no tienen base. Prioriza en ese orden.

---

### Recrea el ambiente para la reproducibilidad del proyecto

1. Ve a anaconda navegator.
2. Selecciona la opción de ```Environments```
3. Haz clic en el botón Import.
4. Selecciona la opción Local drive.
5. Busca el archivo [environment.yaml](environment.yaml)
6. Finalmente, haz clic en el botón Import y listo.  

---
### Estrcutura del repositorio

```
El equipo de Operations actualmente gestiona el balance de forma reactiva.

Nuestra misión es construir un sistema que lo haga de forma proactiva: detectar condiciones que degradarán la operación antes de que ocurran y enviar alertas accionables con recomendaciones concretas al equipo a través de Telegram.

La estructura de este repositorio se justifica con base en el documento rappi_ai_engineer_case.docx, sección Repositorio:

rappi-case
│
├─ environment.yaml                                                                 <- Configuración del entorno y dependencias del proyecto.
├─ LICENSE                                                                          <- Términos legales de uso del proyecto.
├─ main_pipeline.py                                                                 <- Script principal que ejecuta todo el flujo del proyecto.
│
├─ modulo1_diagnostico                                                                  <- Análisis exploratorio y generación de insights.
│  ├─ diagnostico.ipynb                                                                 <- Notebook con el EDA y análisis de datos.
│  ├─ README.md                                                                         <- Explicación del análisis realizado.
│  ├─ Resumen de hallazgos                                                              <- Resumen ejecutivo del análisis.
│  │  └─ Resumen de hallazgos.pdf                                                       <- Principales hallazgos y conclusiones.
│  └─ Visualizaciones para cada hallazgo                                                <- Gráficas que respaldan los insights.
│     ├─ EDA                                                                            <- Visualizaciones generales del análisis.
│     │  ├─ 1.Variables_categóricas_conteo_DATE.png                                     <- Distribución por fecha.
│     │  ├─ 1.Variables_categóricas_conteo_RATIO_CATEGORY.png                           <- Conteo por categoría.
│     │  ├─ 1.Variables_categóricas_conteo_ZONE.png                                     <- Distribución por zona.
│     │  ├─ 2.Variables_numéricas_conteo_RATIO.png                                      <- Distribución del ratio.
│     │  ├─ 3.Correlacion_Pearson.png                                                   <- Correlación entre variables.
│     │  └─ 4.Correlacion_en_visual.png                                                 <- Visualización de correlaciones.
│     ├─ P1                                                                             <- Hallazgo 1.
│     │  └─ P1_heatmap_saturacion_critica_mayor_1_8.png                                 <- Zonas con saturación crítica.
│     ├─ P2                                                                             <- Hallazgo 2.
│     │  └─ Heatmap_de_saturación_por_hora_y_zona.png                                   <- Saturación por hora y zona.
│     ├─ P3                                                                             <- Hallazgo 3.
│     │  └─ P3.Promedio_de_saturacion_por_zona_(horizontal).png                         <- Promedio por zona.
│     ├─ P4                                                                             <- Hallazgo 4.
│     │  └─ P4.Deteccion_de_Gasto_Ineficiente_Dias_con_Sobrecostos_y_Baja_Demanda.png   <- Días ineficientes.
│     └─ P5                                                                             <- Hallazgo 5.
│        └─ P5.Earnings promedio por nivel de saturacion.png                            <- Earnings vs saturación.
│
├─ modulo2_motor_alertas                                                                <- Lógica para generación de alertas.
│  ├─ Justificación_de_los_umbrales_y_reglas_del_motor.pdf                              <- Explicación de reglas y umbrales.
│  ├─ motor_alertas.py                                                                  <- Implementación del motor de alertas.
│  ├─ Original-Monolito-Motor-Alertas.py                                                <- Versión inicial del motor.
│  └─ README.md                                                                         <- Documentación del módulo.
│
├─ modulo3_agente_telegram                                                              <- Envío de alertas por Telegram.
│  ├─ alerts_notify.py                                                                  <- Script de notificaciones.
│  └─ README.md                                                                         <- Configuración y uso.
│
├─ README.md                                                                            <- Descripción general del proyecto.
└─ requirements.txt                                                                     <- Dependencias necesarias.

```


# Anexos 

### ¿Cómo crear el ambiente de conda?
El entorno de desarrollo fue configurado utilizando Conda (2026). Siguiendo buenas prácticas de nomenclatura, el ambiente se denominó ```rappi_ai_project_py311```, integrando el nombre de la empresa, el tipo de proyecto y la versión de Python utilizada, lo que facilita su identificación y mantenimiento.

Se seleccionó Python 3.11 como versión base debido a su compatibilidad con el stack tecnológico del proyecto (LangChain, requests, Telegram API, pandas, seaborn y matplotlib), así como por sus mejoras de rendimiento frente a versiones anteriores como 3.10. Además, cuenta con soporte oficial hasta octubre de 2027, lo que asegura estabilidad, escalabilidad y mantenimiento a largo plazo.


**Bibliografía**

1. ¿Cómo crear un ambiente en Anaconda navegator?
https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html

2. How To install LangChain
https://docs.langchain.com/oss/python/langchain/install