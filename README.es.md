# 🧠 Data ORCID-Chile

Aplicación **Flask** para consultar, **cachear** y exportar información pública de **ORCID** por institución (**ROR**).  
Incluye interfaz **AdminLTE 3**, autenticación básica y utilidades de exportación a **CSV/Excel**.

Los identificadores Ringgold incluidos fueron validados contra afiliaciones públicas de ORCID cuya fuente de desambiguación es `RINGGOLD`. ROR se mantiene como identificador institucional canónico porque ORCID ya no actualiza los datos del registro Ringgold.

> 📄 Proyecto desarrollado por **Gastón Olivares**.  
> Versión documentada: *2025-08-29*.  
> Este README describe el comportamiento real del sistema según su código fuente.

---

## ⚙️ Stack principal

- **Backend:** Flask 3, Blueprints, Jinja2, CLI (`flask`).
- **Base de datos:** SQLAlchemy 2 + Flask-SQLAlchemy, Flask-Migrate, MySQL (`PyMySQL`) o SQLite para desarrollo.
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
├── blueprints/        # Vistas y endpoints (auth, admin, works, fundings, etc.)
├── services/          # Integración ORCID y generación de cachés
├── utils/             # Email, flashes, contraseñas, sesión
├── templates/         # Plantillas Jinja2 (AdminLTE)
├── static/            # CSS, íconos, favicon
├── models.py
├── database.py
├── decorators.py
├── cli.py
├── routes.py
└── __init__.py
config/
└── config.toml.example
run.py
requirements.txt
README.md
```

---

## ✨ Principales funcionalidades

- 🔐 **Autenticación** (login, logout, recuperación y cambio de contraseña).  
- 👥 **Roles:**
  - **Administrador:** gestiona usuarios, enlaces de contraseña y ROR.
  - **Gestor:** acceso avanzado sin modificar usuarios.
- 🏛️ **Contexto institucional (ROR):**
  - Consultas ORCID *expanded-search* por ROR, GRID y Ringgold verificados.
  - ROR se mantiene como identificador institucional canónico.
  - Selección dinámica de ROR en sesión.
- 🧩 **Caché ORCID:**
  - Asociaciones completas entre instituciones e investigadores, aunque no tengan obras o financiamientos públicos.
  - **Works** (`WorkCache`) y **Fundings** (`FundingCache`) por ROR.
  - Seguimiento con `WorkCacheRun` / `FundingCacheRun`.
- 📈 **Dashboard** con métricas y conteos.
- ⬇️ **Exportaciones:**
  - Excel por ORCID (`/download/excel/<orcid_id>`).
  - CSV/Excel masivo desde caché.
- 🧰 **CLI:** reconstrucción de cachés concurrente (`flask rebuild-caches`).

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
```
> En producción, prefiere `SECRET_KEY`/`ORCID_SECRET_KEY` y
> `SECURITY_PASSWORD_SALT`/`ORCID_PASSWORD_SALT` como variables de entorno.
> Contraseñas → **bcrypt**  
> Tokens → **itsdangerous** (firmados y con expiry)

### 🔹 Idiomas
```toml
[languages]
supported = ["en", "es"]
default = "en"
```
> El código y los `msgid` de Babel usan inglés como idioma base.

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
```

📊 Muestra resumen por ROR (OK/Errores y conteos de filas).

Las sincronizaciones largas iniciadas desde la web se ejecutan en segundo plano
dentro del proceso Flask para evitar timeouts. En producción con múltiples
workers, prefiere CLI/cron o una cola persistente.

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
