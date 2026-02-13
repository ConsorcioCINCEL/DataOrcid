# 🧠 Data ORCID-Chile

Aplicación **Flask** para consultar, **cachear** y exportar información pública de **ORCID** por institución (**ROR**).  
Incluye interfaz **AdminLTE 3**, autenticación básica y utilidades de exportación a **CSV/Excel**.

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
  - **Administrador:** gestiona usuarios, contraseñas y ROR.
  - **Gestor:** acceso avanzado sin modificar usuarios.
- 🏛️ **Contexto institucional (ROR):**
  - Consultas ORCID *expanded-search* por ROR.
  - Selección dinámica de ROR en sesión.
- 🧩 **Caché ORCID:**
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
uri = "mysql+pymysql://USER:PASS@host/dbname"
```

### 🔹 Flask y seguridad
```toml
[flask]
secret_key     = "CAMBIA_ESTA_CLAVE"
password_salt  = "CAMBIA_EL_SALT"
session_cookie_secure   = true
session_cookie_httponly = true
session_cookie_samesite = "Lax"
```
> Contraseñas → **bcrypt**  
> Tokens → **itsdangerous** (firmados y con expiry)

### 🔹 ORCID
```toml
[orcid]
base_url_pub   = "https://pub.orcid.org/v3.0/"
token_url      = "https://orcid.org/oauth/token"
client_id      = "APP-XXXX"
client_secret  = "SECRET"
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
git clone <repo>
cd <repo>

# 1️⃣ Entorno virtual
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2️⃣ Configuración
cp config/config.toml.example config/config.toml
# → Completa credenciales de DB, ORCID y SMTP

# 3️⃣ Ejecución
python run.py
# o
export FLASK_APP=run.py && flask run
```

> En el primer arranque se crean las tablas y el usuario `admin` por defecto.  
> Modifica sus credenciales o elimínalo tras configurar la app.

---

## 🧭 CLI — reconstrucción de cachés

```bash
# Ambos tipos (works + fundings)
flask rebuild-caches --target both --workers 4

# Solo works
flask rebuild-caches --target works

# Solo fundings
flask rebuild-caches --target fundings

# Dry-run (listar ROR sin ejecutar)
flask rebuild-caches --dry-run
```

📊 Muestra resumen por ROR (OK/Errores y conteos de filas).

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
- *(Pendiente)* CSRF → si se exponen formularios públicos

---

## 🧱 Modelos (resumen)

| Modelo | Descripción |
|---------|--------------|
| `User` | Cuentas y roles (admin/manager), ROR, institución |
| `WorkCache` | Obras (works) cacheadas por ROR |
| `WorkCacheRun` | Seguimiento de reconstrucción de works |
| `FundingCache` | Financiamiento cacheado por ROR |
| `FundingCacheRun` | Seguimiento de reconstrucción de fundings |
| `OrcidCache` | Almacenamiento JSON por año |

---

## 🧩 Buenas prácticas

- Mantén las claves secretas fuera del repo (`.env` + dotenv recomendado).  
- Producción: `gunicorn -w 4 -b 0.0.0.0:5000 "run:app"` detrás de Nginx.  
- Ajusta `workers` según límites de ORCID.  
- Personaliza `populate_users()` antes de publicar.  
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