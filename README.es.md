# 🧠 Data ORCID-Chile 2.1

Aplicación **Flask** para consultar, **cachear** y exportar información pública de **ORCID** por institución (**ROR**).  
Incluye interfaz **AdminLTE 3**, autenticación básica y utilidades de exportación a **CSV/Excel**.

Los identificadores Ringgold incluidos fueron validados contra afiliaciones públicas de ORCID cuya fuente de desambiguación es `RINGGOLD`. ROR se mantiene como identificador institucional canónico porque ORCID ya no actualiza los datos del registro Ringgold.

> 📄 Proyecto desarrollado por **Gastón Olivares**.  
> Versión documentada: **2.1 (2026-08-15)**.
> Este README describe el comportamiento real del sistema según su código fuente.

## ✨ Novedades de la versión 2.1

- Trabajos durables de sincronización y exportación con progreso, recuperación,
  retención, limpieza por usuario y reutilización de exportaciones equivalentes.
- Generación CSV/XLSX en segundo plano, aviso flotante persistente, escritura
  incremental de XLSX y eliminación del antiguo límite de 100.000 filas.
- Publicación OAI-PMH por institución con selección de artículos, mapeo de
  metadatos, cargas DOI auditables y permisos para el rol `oai-user`.
- Cachés analíticas OpenAlex, revisión de calidad y duplicados, control global
  de módulos y seguimiento saneado de errores del sistema.
- Menú lateral organizado por tareas y Centro de ayuda buscable, visible para
  todos los perfiles autenticados, pero dedicado exclusivamente a Usuario y
  Usuario OAI y filtrado por los módulos activos.
- Landing pública autoexplicativa con llamadas a la acción y formulario de
  contacto con bandeja administrativa privada, avisos SMTP opcionales y
  activación global desde Administración.

---

## ⚙️ Stack principal

- **Backend:** Flask 3, Blueprints, Jinja2, CLI (`flask`).
- **Base de datos:** SQLAlchemy 2 + Flask-SQLAlchemy, Flask-Migrate, PostgreSQL en producción; SQLite para pruebas y MySQL como compatibilidad heredada.
- **Integración ORCID:** API pública v3 (`pub.orcid.org`) con OAuth Client Credentials (`/read-public`).
- **Exportación:** `pandas` + `openpyxl` (Excel) y CSV UTF-8 con BOM.
- **Interfaz:** AdminLTE 3, Font Awesome, DataTables.
- **Email:** SMTP configurable (TLS/SSL).
- **Hashing:** bcrypt.
- **Dependencias clave:** alembic · bcrypt · Flask · Flask-Migrate · Flask-SQLAlchemy · requests · pandas · openpyxl · PyMySQL · toml.

---

## 🗂️ Estructura del proyecto

```bash
app/
├── blueprints/        # Rutas enfocadas; works_* separa vistas, sync, datos y exportación
├── services/          # ORCID/OpenAlex, cachés, calidad, trabajos, exportación y acceso
├── utils/             # Email, flashes, contraseñas, sesión
├── templates/         # Plantillas Jinja2 y componentes compartidos
├── static/            # CSS, JavaScript, esquemas y presentación OAI
├── translations/      # Catálogos fuente de Babel
├── models.py
├── database.py
├── decorators.py
└── __init__.py
config/
└── config.toml.example
migrations/
tests/
run.py
requirements.txt
README.md
```

---

## ✨ Principales funcionalidades

- 🔐 **Autenticación** (login, logout, recuperación y cambio de contraseña).  
- 👥 **Roles:**
  - **Usuario:** consulta y exporta los módulos activos de su institución.
  - **Usuario OAI:** hereda el acceso estándar y gestiona selección, mapeo y cargas DOI OAI-PMH.
  - **Gestor:** opera datos y cuentas dentro de la institución asignada.
  - **Administrador:** controla usuarios, módulos, monitoreo y alcance institucional.
- 🏛️ **Contexto institucional (ROR):**
  - Consultas ORCID *expanded-search* por ROR, GRID y Ringgold verificados.
  - ROR se mantiene como identificador institucional canónico.
  - Selección dinámica de ROR en sesión.
- 🧩 **Caché ORCID:**
  - Asociaciones completas entre instituciones e investigadores, aunque no tengan obras o financiamientos públicos.
  - **Works** (`WorkCache`) y **Fundings** (`FundingCache`) por ROR.
  - Seguimiento con `WorkCacheRun` / `FundingCacheRun`.
- 📈 **Analítica ORCID/OpenAlex**, calidad de datos y revisión de posibles duplicados.
- ⬇️ **Exportaciones:**
  - Excel por ORCID (`/download/excel/<orcid_id>`).
  - CSV/Excel masivo desde caché y tablas analíticas.
  - Procesamiento durable en segundo plano, aviso flotante, deduplicación y vencimiento automático.
- 📡 **OAI-PMH institucional** con formatos `oai_dc`, `oai_openaire` y `dataorcid`.
- ❓ **Centro de ayuda dinámico** para Usuario y Usuario OAI, filtrado por módulos activos.
- 🧰 **CLI:** migraciones, reconstrucción de cachés, worker durable y limpieza por retención.

---

## 🔧 Configuración

El sistema lee `config/config.toml` y lo carga en `current_app.config`.

### 🔹 Base de datos
```toml
[database]
uri = "postgresql+psycopg://USER:PASS@host:5432/dbname"
```

### 🔹 Flask y seguridad
```toml
[flask]
secret_key     = "CHANGEME_IN_RUNTIME"
password_salt  = "CHANGE_ME_SALT"
session_cookie_secure   = true
session_cookie_httponly = true
session_cookie_samesite = "Lax"
allow_insecure_dev_config = false
permanent_session_days = 31
```
> En producción, prefiere `SECRET_KEY`/`ORCID_SECRET_KEY` y
> `SECURITY_PASSWORD_SALT`/`ORCID_PASSWORD_SALT` como variables de entorno.
> Contraseñas → **bcrypt**  
> Tokens → **itsdangerous** (firmados y con expiry)

### 🔹 Idiomas
```toml
[languages]
supported = ["en", "es", "fr", "pt", "de"]
default = "en"
```
> El código y los `msgid` de Babel usan inglés como idioma base. La interfaz
> incluye catálogos completos para español, francés, portugués y alemán.

### 🔹 ORCID
```toml
[orcid]
base_url_public = "https://pub.orcid.org/v3.0/"
base_url_member = "https://api.orcid.org/v3.0/"
token_url      = "https://orcid.org/oauth/token"
client_id      = "APP-XXXX"
client_secret  = "REPLACE_OR_USE_ENV"
```

### 🔹 Email (SMTP)
```toml
[mail]
enabled   = true
smtp_host = "smtp.mi-proveedor.com"
smtp_port = 587
use_tls   = true
use_ssl   = false
smtp_user = "usuario"
smtp_pass = "password"
from_name = "Data ORCID-Chile"
from_email= "no-reply@midominio.cl"
reply_to = "contacto@midominio.cl" # destinatario del formulario público
```

---

## 🚀 Puesta en marcha (dev)

```bash
git clone [<repo>](https://github.com/ConsorcioCINCEL/DataOrcid.git)
cd DataOrcid

# 1️⃣ Entorno virtual
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2️⃣ Configuración
cp config/config.toml.example config/config.toml
# → Completa credenciales de DB, ORCID y SMTP

# 3️⃣ Base de datos
export FLASK_APP=run.py
flask db upgrade
flask seed-db

# 4️⃣ Ejecución
python run.py
# o
export FLASK_APP=run.py && flask run
```

> El esquema se gestiona con migraciones. `flask seed-db` crea el usuario
> `admin` inicial si no existe.

---

## 🧭 CLI — reconstrucción de cachés

```bash
# Ambos tipos (works + fundings)
flask rebuild-caches --target both

# Solo works
flask rebuild-caches --target works

# Solo fundings
flask rebuild-caches --target fundings

# Dry-run (listar ROR sin ejecutar)
flask rebuild-caches --dry-run

# Reconstruir la capa intermedia de analítica OpenAlex
flask rebuild-openalex-analytics

# Reconstruirla para una sola institución
flask rebuild-openalex-analytics --ror 02ap3w078

# Poblar metadatos extendidos desde la caché OpenAlex existente
flask rebuild-openalex-metadata --batch-size 2000
# Reanudar después del último ID de caché bruto informado en el log
flask rebuild-openalex-metadata --batch-size 2000 --start-after-id 150000
```

📊 Muestra resumen por ROR (OK/Errores y conteos de filas).

La capa analítica se actualiza automáticamente después de sincronizar Works u
OpenAlex. Tras instalar esta versión sobre una base existente, ejecuta
`flask db upgrade` y luego `flask rebuild-openalex-analytics` para disponer de
los filtros optimizados inmediatamente.

Las exportaciones grandes CSV/XLSX de cachés y OpenAlex se envían al mismo
sistema durable de trabajos. Una notificación flotante conserva el estado en
cola o en ejecución al navegar y ofrece la descarga privada cuando el archivo
está listo. Los archivos quedan fuera del árbol estático, sólo son accesibles
por quien los solicitó (con supervisión administrativa) y expiran según la
retención configurada. Los archivos vencidos y los registros de sus trabajos
terminados se eliminan automáticamente; cada usuario también puede borrar todas
sus exportaciones finalizadas desde el centro flotante sin interrumpir trabajos
en cola o en ejecución. Los operadores pueden forzar la misma limpieza con
`flask cleanup-exports`. Las solicitudes idénticas de una misma cuenta reutilizan
un archivo terminado todavía vigente; una revisión de los datos fuente invalida
automáticamente esa reutilización y crea una exportación nueva. Los XLSX siguen
usando escritura incremental, sin el
límite anterior de 100.000 filas de la aplicación. Si la web y los workers se
ejecutan en hosts distintos, `exports.directory` debe apuntar a almacenamiento
compartido entre ambos.

Los DOI originales se conservan completos en una columna `TEXT`. Las búsquedas
y uniones utilizan una clave DOI validada y normalizada de hasta 255 caracteres;
los valores inválidos o excesivamente largos no se truncan ni se indexan como
DOI, evitando errores y posibles colisiones.

Las sincronizaciones largas iniciadas desde la web conservan el mismo
comportamiento de respuestas y resultados con `jobs.execution_mode = "thread"`.
En producción con múltiples procesos usa la cola persistente:

```toml
[app]
environment = "production"

[jobs]
execution_mode = "queue"
stale_minutes = 30
max_attempts = 3
heartbeat_seconds = 30

[exports]
directory = "instance/exports"
retention_hours = 24
max_active_per_user = 3
max_active_global = 20

[tracking]
retention_days = 90
error_monitoring_enabled = true
error_retention_days = 90

[proxy]
trusted_hops = 1 # sólo si existe exactamente un proxy de confianza
```

Ejecuta `flask run-job-worker` como un proceso separado y supervisado. El worker
reclama trabajos de forma atómica, conserva handler y argumentos en la base,
mantiene un heartbeat independiente y recupera ejecuciones interrumpidas.
También existe
`flask recover-interrupted-jobs` para recuperación manual. Antes de iniciar la
aplicación ejecuta siempre `flask db upgrade` y luego `flask seed-db` cuando
corresponda: el arranque web ya no crea tablas ni carga datos automáticamente.

Los administradores pueden revisar los fallos de ejecución saneados en
`/admin/errors`. Cada evento incluye la cuenta y la institución afectadas, la
vista, la ruta, el ID de correlación de la solicitud o tarea, el tipo de
excepción, una firma de recurrencia y la traza técnica. No se almacenan cadenas
de consulta, cuerpos de solicitudes, cookies, cabeceras de autorización ni
credenciales. La retención se aplica con `flask cleanup-system-errors`; la
actividad normal de solicitudes continúa usando `flask cleanup-tracking-logs`.

La disponibilidad global de las áreas opcionales se administra en
`/admin/modules`. Al desactivar un módulo, desaparece del menú y el servidor
responde con acceso denegado ante sus páginas, formularios, API, descargas y
endpoints públicos. El resumen, la autenticación, los ajustes personales y el
propio panel de módulos permanecen siempre activos para evitar un bloqueo
administrativo. La configuración se conserva en la tabla `system_module`.
El interruptor **Página pública de presentación** controla la portada para
visitantes y su formulario de contacto; al desactivarlo, `/` vuelve a enviar a
los visitantes directamente al inicio de sesión. Las cuentas autenticadas
siempre conservan el panel institucional en `/`. Las consultas válidas se
guardan de forma durable en la bandeja privada `/admin/contact-inquiries`
incluso si SMTP no está disponible; el correo es sólo un canal de aviso
opcional y no el registro principal.

El Centro de ayuda autenticado está disponible en `/help/` para todos los
roles. Su contenido describe intencionalmente sólo los flujos de Usuario y
Usuario OAI; la operación técnica y de personal permanece documentada en este
README. Los temas, bloques, enlaces, resultados de búsqueda y URLs directas de
la ayuda usan los mismos interruptores globales que la aplicación, por lo que
un módulo desactivado tampoco aparece documentado dentro del sistema.

Durante el despliegue compila los catálogos de idioma con
`pybabel compile -d app/translations`; los archivos `.mo` generados son
artefactos de ejecución y se excluyen intencionalmente de Git.

La URI de base puede entregarse con `ORCID_DATABASE_URI` o `DATABASE_URL`; las
credenciales admiten `SECRET_KEY`, `SECURITY_PASSWORD_SALT`, `ORCID_CLIENT_ID`,
`ORCID_CLIENT_SECRET`, `OPENALEX_API_KEY` y `MAIL_PASSWORD`, evitando guardar
secretos en TOML.

---

## 📡 OAI-PMH institucional

El módulo **OAI-PMH** convierte a DataORCID-Chile en un proveedor institucional.
DSpace, DSpace-CRIS u otro cosechador externo consulta el endpoint generado y
recibe únicamente los artículos autorizados para el ROR solicitado.

- Configuración inicial: `/oai-pmh/`
- Selección y auditoría de artículos: `/oai-pmh/articles/`
- Formatos y mapeo: `/oai-pmh/metadata/`
- Activación masiva e historial de cargas DOI: `/oai-pmh/doi-import/`
- Inventario para administradores y gestores: `/admin/oai-pmh`
- Proveedor público por institución: `/oai/<clave_publica>`
- Formatos publicados: Dublin Core no cualificado (`oai_dc`), OpenAIRE 4
  (`oai_openaire`) y perfil institucional mapeable (`dataorcid`)
- Verbos: `Identify`, `ListMetadataFormats`, `ListSets`, `GetRecord`,
  `ListIdentifiers` y `ListRecords`
- Catálogo fuente: trabajos canónicos y deduplicados de DataORCID-Chile
- Política predeterminada: solo artículos cuya afiliación al ROR está validada por OpenAlex
- Políticas opcionales: todos los artículos públicos o solo los seleccionados
- Inclusión y exclusión manual por artículo, individual o masiva
- Ordenamiento por título, autoría, año, tipo, afiliaciones, validación y exposición
- Afiliaciones OpenAlex por artículo, destacando la que coincide con el ROR activo
- Activación masiva por DOI mediante una plantilla XLSX, limitada a artículos del perfil institucional
- Historial auditable de cada archivo Excel, con totales y detalle por artículo; la última carga activa puede revertirse sin sobrescribir cambios manuales posteriores
- Exportación de auditoría CSV/XLSX con todos los resultados filtrados, todas sus afiliaciones y el origen de cada decisión; el XLSX usa escritura incremental y evita generar metadatos XML que no aparecen en la planilla
- Mapeo de nombres de destino por universidad para el perfil `dataorcid`
- Catálogo configurable de metadatos DataORCID/OpenAlex: los campos opcionales
  pueden agregarse u ocultarse sin alterar `oai_dc` ni `oai_openaire`
- Cosecha incremental de DSpace mediante `resumptionToken`
- Los listados públicos contienen únicamente artículos efectivamente expuestos
- Presentación web XSL personalizada; el XML de protocolo permanece intacto

La configuración y la selección respetan el ROR de la cuenta. La validación
automática requiere que OpenAlex vincule una autoría del artículo con el ROR
activo; las decisiones manuales de inclusión o exclusión prevalecen. Los
administradores pueden usar el selector institucional; los gestores solo
modifican la universidad asignada a su usuario. La URL pública se genera
automáticamente con una clave aleatoria de 192 bits que puede rotarse. No se
configura un origen OAI externo. El inventario administrativo resume por
universidad el estado del proveedor, los artículos asociados y expuestos, la
validación OpenAlex y las decisiones manuales.

El rol `oai-user` hereda el acceso de un usuario estándar y puede gestionar el
mapeo de metadatos, la selección de artículos y las cargas DOI dentro de su institución. No
puede habilitar el proveedor, cambiar su política general ni rotar la URL;
estas operaciones permanecen reservadas a gestores y administradores, quienes
también conservan todos los permisos de gestión OAI.

El formato `oai_openaire` está disponible por defecto como punto de partida
para la interoperabilidad con ANID, Espacio Ciencia y LA Referencia. Usa el
perfil OpenAIRE 4, vocabularios COAR y el set OAI `openaire`. Los nombres de `oai_dc` y
`oai_openaire` no son editables porque pertenecen a esquemas estándar; las
adaptaciones como `dc.titulo` se configuran de forma segura en `dataorcid`.

Ejemplos de proveedor:

```text
/oai/<clave_publica>?verb=Identify
/oai/<clave_publica>?verb=ListRecords&metadataPrefix=oai_dc
/oai/<clave_publica>?verb=ListRecords&metadataPrefix=oai_openaire
/oai/<clave_publica>?verb=ListRecords&metadataPrefix=dataorcid
```

---

## 📤 Exportaciones

- **Excel individual:** `/download/excel/<orcid_id>`  
  (Hoja *Personal* + subsecciones disponibles)
- **Masivos desde caché:**
  - Works → `/download/all-works/cache`
  - Fundings → `/download/all-fundings/cache`
- **CSV UTF-8 con BOM** para compatibilidad con Excel.

---

## 🖥️ Interfaz

- **Base:** AdminLTE 3 (`templates/base.html`)  
- **Incluye:** login, forgot/reset, dashboard, panel de cachés, listado de investigadores, guía de integración.  
- **Frontend:** DataTables, Font Awesome, breadcrumbs, botones de descarga.

---

## 🔐 Seguridad

- Hash de contraseñas: **bcrypt**
- Tokens: **itsdangerous** (con expiración)
- Cookies seguras (`Secure`, `HttpOnly`, `SameSite`)
- SMTP opcional para reset de contraseña
- CSRF habilitado en formularios; logout usa `POST`

---

## 🧱 Modelos (resumen)

| Modelo | Descripción |
|---------|--------------|
| `User` | Cuentas y roles (admin/manager), ROR, institución |
| `WorkCache` | Obras (works) cacheadas por ROR |
| `WorkCacheRun` | Seguimiento de reconstrucción de works |
| `FundingCache` | Financiamiento cacheado por ROR |
| `FundingCacheRun` | Seguimiento de reconstrucción de fundings |
| `InstitutionRegistry` | Registro canónico de instituciones por ROR |
| `InstitutionIdentifier` | Identificadores ROR, GRID y Ringgold verificados |
| `InstitutionResearcher` | Asociaciones encontradas entre instituciones y ORCID |
| `OrcidCache` | Almacenamiento JSON por año |
| `OpenAlexInstitutionWorkFact` | Capa intermedia indexada para filtros y métricas OpenAlex |
| `AnalyticsDataVersion` | Versión de datos usada para invalidar cachés analíticas |
| `ContactInquiry` | Consultas privadas recibidas desde la portada y estado de su gestión |

---

## 🧩 Buenas prácticas

- Mantén las claves secretas fuera del repo (`.env` + dotenv recomendado).
- Producción: `gunicorn -w 4 -b 0.0.0.0:5000 "run:app"` detrás de Nginx.  
- Ajusta el paralelismo con cuidado según límites de ORCID.
- Usa el registro institucional (`InstitutionRegistry`) para universidades; `populate_users()` solo crea el admin inicial.
- Revisa logs (`gunicorn --access-logfile - --error-logfile -`).

---

## ❓ Problemas comunes

| Error | Causa probable |
|-------|----------------|
| `SECRET_KEY` vacío | Tokens / sesiones no válidas |
| DB URI inválida | Base no creada o credenciales erróneas |
| SMTP desactivado | No se envían correos de recuperación |
| 429 de ORCID | Exceso de solicitudes; baja `workers` |

---

## 🪶 Licencia

Proyecto distribuido bajo licencia **MIT**.  
© 2025 Gastón Olivares.  
Desarrollado para fomentar la interoperabilidad y acceso abierto a datos de investigación ORCID en Chile.
