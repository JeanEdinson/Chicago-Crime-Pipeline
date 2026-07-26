# 🚔 Chicago Crime Data Pipeline

<div align="center">

![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python)
![PySpark](https://img.shields.io/badge/PySpark-3.5-orange?logo=apachespark)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-E25A1C?logo=apachespark)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.10-017CEE?logo=apacheairflow)
![Docker](https://img.shields.io/badge/Docker-Latest-2496ED?logo=docker)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql)
![Google Cloud Storage](https://img.shields.io/badge/Google%20Cloud%20Storage-4285F4?logo=googlecloud)

</div>

---

## 📖 Descripción

Este proyecto implementa un **pipeline de Ingeniería de Datos** completamente automatizado para el procesamiento del dataset **Crimes - 2001 to Present** de la ciudad de Chicago, compuesto por más de **8 millones de registros históricos**.

La solución utiliza una arquitectura **Medallion (Bronze → Silver → Gold)** para transformar datos crudos en conjuntos de datos listos para el análisis, integrando herramientas ampliamente utilizadas en la industria como **Apache Airflow**, **Apache Spark (PySpark)**, **Docker**, **PostgreSQL** y **Google Cloud Storage**.

El pipeline fue diseñado siguiendo un enfoque incremental, procesando únicamente los archivos nuevos mediante una **tabla de control**, evitando reprocesamientos innecesarios y reduciendo el tiempo de ejecución.

---

## 🎯 Objetivos del proyecto

Los principales objetivos de este proyecto son:

- Implementar un pipeline de datos completamente automatizado.
- Aplicar una arquitectura Medallion utilizando Apache Spark.
- Orquestar todas las etapas del proceso mediante Apache Airflow.
- Procesar únicamente nuevos archivos de manera incremental.
- Generar tablas analíticas optimizadas para consumo en herramientas de Business Intelligence.
- Publicar automáticamente la capa Gold en Google Cloud Storage.

---

## ✨ Características principales

✔ Procesamiento distribuido mediante Apache Spark.

✔ Orquestación completa utilizando Apache Airflow.

✔ Arquitectura Medallion.

✔ Procesamiento incremental.

✔ Dynamic Task Mapping.

✔ API REST personalizada para ejecutar Spark Jobs.

✔ Tabla de control para seguimiento del procesamiento.

✔ Publicación automática de resultados en Google Cloud Storage.

✔ Totalmente dockerizado mediante Docker Compose.

---

## 📊 Dataset

El proyecto utiliza el dataset público:

**Crimes - 2001 to Present**

Este conjunto de datos contiene información histórica de incidentes criminales reportados por el Departamento de Policía de Chicago desde el año 2001.

Características generales del dataset:

- Más de **8 millones de registros**.
- Más de **20 columnas**.
- Actualización periódica por parte de la ciudad de Chicago.
- Información geográfica.
- Información temporal.
- Tipo de delito.
- Arrestos.
- Delitos domésticos.
- Distrito policial.
- Comunidad.
- Coordenadas geográficas.

> ⚠️ Debido al tamaño del dataset, el archivo original no se encuentra incluido dentro del repositorio.

### 🔗 Fuente del dataset

Por restricciones de seguridad, no es posible compartir el enlace. Sin embargo, basta con buscar “chicago crime data 2001 to present” en Google; el primer resultado contiene el dataset.

---

## 📸 Vista general del proyecto

### Arquitectura

> 📷 **Insertar aquí una imagen de la arquitectura del proyecto.**

---

### Contenedores Docker

> 📷 **Insertar aquí una captura mostrando todos los contenedores ejecutándose.**

---

### DAG ejecutándose en Apache Airflow

> 📷 **Insertar aquí una captura del DAG en Graph View.**

---

### Google Cloud Storage

> 📷 **Insertar aquí una captura del bucket con la carpeta Gold.**

---
