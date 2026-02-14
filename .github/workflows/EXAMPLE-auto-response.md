# Example: Auto-Response for Installation Issue

## Sample Issue
**Title:** "How to install ReplicaDB?"
**Body:** "I want to install ReplicaDB to replicate data from PostgreSQL to MySQL"

---

## Auto-Generated Response

👋 ¡Hola @user! Gracias por abrir este issue.

🤖 **Respuesta automática inicial del agente**

### 📦 Instalación

ReplicaDB requiere Java 11 o superior. Puedes instalarlo de las siguientes formas:

**Descarga directa:**
```bash
curl -o ReplicaDB-0.18.0.tar.gz -L "https://github.com/osalvador/ReplicaDB/releases/download/v0.18.0/ReplicaDB-0.18.0.tar.gz"
tar -xvzf ReplicaDB-0.18.0.tar.gz
./bin/replicadb --help
```

**Docker:**
```bash
docker pull osalvador/replicadb:latest
docker run -it osalvador/replicadb:latest --help
```

### 🗄️ Bases de Datos Soportadas

ReplicaDB soporta una amplia gama de bases de datos:
- **Relacionales:** Oracle, PostgreSQL, MySQL/MariaDB, SQL Server, SQLite, IBM DB2 (LUW y AS/400)
- **NoSQL:** MongoDB
- **Otros:** Denodo (virtualización), CSV, Amazon S3, Kafka
- **Cualquier base JDBC-compliant** (con algunas limitaciones)

Para bases de datos específicas, consulta la [documentación completa](https://osalvador.github.io/ReplicaDB/docs/docs.html).

### 📚 Recursos Adicionales

- 📖 [Documentación completa](https://osalvador.github.io/ReplicaDB/docs/docs.html)
- 🚀 [Guía de inicio rápido](https://github.com/osalvador/ReplicaDB#installation)
- 🏗️ [Decisiones de arquitectura](https://github.com/osalvador/ReplicaDB/blob/master/ARCHITECTURE_DECISIONS.md)
- 🤝 [Guía de contribución](https://github.com/osalvador/ReplicaDB/blob/master/CONTRIBUTING.md)
- 💬 [Discusiones en GitHub](https://github.com/osalvador/ReplicaDB/discussions)

---

*Esta respuesta fue generada automáticamente basándose en los temas detectados: installation, database.*

Si necesitas ayuda más específica, por favor proporciona:
- Versión de ReplicaDB que estás usando
- Bases de datos origen y destino (tipo y versión)
- Comando o archivo de configuración que estás ejecutando
- Mensajes de error completos si los hay

¡Un mantenedor humano revisará tu issue pronto! 🙂
