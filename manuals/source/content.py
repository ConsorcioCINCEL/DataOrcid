"""Parallel multilingual content for the Data ORCID-Chile 2.1 manuals.

The audience is limited to User and OAI User. Product behavior is checked
against the application based on commit d216e4d and its harvesting-access update; the supplied v1 PDF defines style.
"""

import json
from pathlib import Path

SOFTWARE_VERSION = "2.1"
LANGUAGES = ("en", "es", "fr", "pt", "de")
UPDATED = {"en": "September 2026", "es": "Septiembre de 2026", "fr": "Septembre 2026",
           "pt": "Setembro de 2026", "de": "September 2026"}
PAGES = []


def page(key, titles, paragraphs, image=None, captions=None, steps=None,
         note=None, table=None, chapter=None):
    """Keep the base editions on the same explicit page structure."""
    PAGES.append(dict(key=key, titles=dict(zip(("es", "en"), titles)),
        paragraphs=dict(zip(("es", "en"), paragraphs)), image=image,
        captions=dict(zip(("es", "en"), captions or ("", ""))),
        steps=dict(zip(("es", "en"), steps or ([], []))),
        note=dict(zip(("es", "en"), note or ("", ""))),
        table=dict(zip(("es", "en"), table or ([], []))), chapter=chapter))


page("purpose", ("1. Propósito y alcance", "1. Purpose and scope"), (
    ["DATA ORCID CHILE permite consultar y analizar información pública de ORCID enriquecida con metadatos de OpenAlex. Este manual acompaña el recorrido de Usuario y Usuario OAI: acceso, consulta, analíticas, descargas, integración y ajustes de cuenta.",
     "Ambos perfiles trabajan dentro de la institución asignada a su cuenta y de los módulos habilitados. Usuario OAI añade acciones sobre la selección de artículos, las cargas DOI, el mapeo de metadatos y el acceso de recolección OAI-PMH.",
     "Esta edición incorpora las exportaciones en segundo plano, la publicación OAI-PMH y la ayuda adaptada a los módulos disponibles."],
    ["DATA ORCID CHILE lets you explore and analyze public ORCID information enriched with OpenAlex metadata. This manual follows the User and OAI User journey: access, discovery, analytics, downloads, integration, and account settings.",
     "Both roles work within the institution assigned to their account and the modules that are enabled. OAI User adds actions for article selection, DOI imports, OAI-PMH metadata mapping, and harvesting access.",
     "This edition covers background exports, OAI-PMH publishing, and help adapted to the available modules."]),
    table=([
        ["ACCIÓN", "USUARIO", "USUARIO OAI"],
        ["Consultar módulos institucionales", "Disponible", "Disponible"],
        ["Descargar datos e informes visibles", "Disponible", "Disponible"],
        ["Revisar candidatos duplicados", "Consulta", "Consulta"],
        ["Consultar contenido OAI-PMH", "Consulta", "Disponible"],
        ["Seleccionar artículos y cargar DOI", "Consulta", "Disponible"],
        ["Editar mapeo dataorcid", "Consulta", "Disponible"],
        ["Editar cuenta y contraseña propias", "Disponible", "Disponible"]
    ], [
        ["ACTION", "USER", "OAI USER"],
        ["Explore institutional modules", "Available", "Available"],
        ["Download visible data and reports", "Available", "Available"],
        ["Inspect duplicate candidates", "Read only", "Read only"],
        ["Review OAI-PMH content", "Read only", "Available"],
        ["Select articles and import DOI", "Read only", "Available"],
        ["Edit dataorcid mapping", "Read only", "Available"],
        ["Edit own account and password", "Available", "Available"]
    ]), note=(
        "Las capturas muestran la interfaz de la versión 2.1 con datos ficticios de demostración. Los nombres, ORCID iD, DOI, ROR y direcciones de ejemplo no deben utilizarse como registros reales.",
        "Screenshots show the version 2.1 interface with fictional demonstration data. Example names, ORCID iDs, DOIs, RORs, and addresses must not be used as real records."), chapter=1)

page("login", ("2. Acceso y navegación", "2. Access and navigation"), (
    ["2.1 Inicio de sesión", "Para ingresar en www.orcid.cl, utiliza el nombre de usuario o correo institucional y la contraseña de tu cuenta. Mantener sesión iniciada es adecuado solamente en un equipo personal o administrado por tu institución.",
     "El correo de bienvenida incluye tus credenciales y un enlace a este manual PDF, que puedes descargar sin iniciar sesión. El enlace abre la edición correspondiente al idioma de tu cuenta: inglés, español, francés, portugués o alemán. Cambia la contraseña temporal al ingresar.",
     "Si no recuerdas la contraseña, abre el enlace de recuperación, indica el correo de tu cuenta y sigue el enlace recibido. La respuesta de la pantalla no confirma si una dirección está registrada. Si no llega el mensaje, revisa el correo no deseado y consulta al equipo responsable.",
     "El selector permite utilizar los idiomas habilitados: inglés, español, francés, portugués y alemán. La elección realizada durante una sesión queda asociada a tu preferencia de idioma."],
    ["2.1 Signing in", "Visit www.orcid.cl and sign in with your account username or institutional email and password. Remember me is appropriate only on a personal device or one managed by your institution.",
     "The welcome email includes your credentials and a link to this PDF manual, which you can download without signing in. The link opens the edition matching your account language: English, Spanish, French, Portuguese, or German. Change the temporary password when you sign in.",
     "If you have forgotten your password, open the recovery link, enter your account email, and follow the link you receive. The on-screen response does not disclose whether an address is registered. If no message arrives, check spam and contact the responsible support team.",
     "The selector offers the enabled languages: English, Spanish, French, Portuguese, and German. A language selected while signed in is saved as your account preference."]),
    image="login", captions=("Inicio de sesión y selección de idioma.", "Sign-in and language selection."), chapter=2)

page("navigation", ("2.2 Estructura de la interfaz", "2.2 Interface structure"), (
    ["El menú lateral organiza el trabajo en Explorar, Gestionar datos, Integrar y Soporte, además del acceso al Resumen. La barra superior muestra la institución activa, el idioma y las opciones personales. Los módulos disponibles dependen de la configuración del servicio y del rol de la cuenta. El enlace «Manual de usuario», con un icono PDF junto a las opciones de tu cuenta, descarga este manual en el idioma activo.",
     "Los indicadores de actualización ayudan a reconocer si la información está vigente o requiere atención. Consulta ese estado antes de interpretar una cifra o descargar un conjunto."],
    ["The sidebar organizes work into Explore, Manage data, Integrate, and Support, alongside the Overview shortcut. The top bar shows the active institution, language, and personal options. Available modules depend on service configuration and your account role. The “User manual” link, with a PDF icon beside your account options, downloads this manual in the active language.",
     "Update indicators help you recognize whether information is current or needs attention. Check this status before interpreting a figure or downloading a dataset."]),
    image="overview", captions=("Resumen institucional y navegación con el perfil Usuario.", "Institutional overview and navigation for the User role."),
    steps=([
        "Comprueba que la institución mostrada corresponda a tu cuenta.",
        "Abre un grupo del menú y selecciona la página que necesitas.",
        "Revisa filtros y fecha de actualización en cada vista.",
        "Al terminar, cierra la sesión desde la parte inferior del menú."
    ], [
        "Confirm that the displayed institution matches your account.",
        "Expand a menu group and choose the page you need.",
        "Review filters and update dates in each view.",
        "When finished, sign out at the bottom of the sidebar."
    ]))

page("overview", ("3. Explorar", "3. Explore"), (
    ["3.1 Resumen institucional",
     "El Resumen reúne investigadores institucionales, publicaciones académicas únicas, financiamientos y publicaciones enriquecidas con OpenAlex. Incluye tendencias, cobertura, calidad de datos y accesos a consultas y descargas.",
     "Es el punto de partida para revisar el estado general. Abre la analítica correspondiente para profundizar en una cifra y utiliza los accesos de calidad para interpretar brechas de información.",
     "Las cifras reflejan la información disponible en la plataforma. Un investigador puede pertenecer al directorio institucional sin contar con obras o financiamientos públicos en la caché."],
    ["3.1 Institutional overview",
     "The Overview brings together institutional researchers, unique scholarly outputs, funding, and OpenAlex-enriched publications. It includes trends, coverage, data quality, and shortcuts to discovery and downloads.",
     "Start here to check the overall state. Open the relevant analytics page to investigate a figure and use the quality shortcuts to understand information gaps.",
     "Figures reflect information available in the platform. A researcher may appear in the institutional directory without any public works or funding records in the cache."]),
    image="overview", captions=("Indicadores generales de la institución de demostración.", "General indicators for the demonstration institution."),
    note=("Los registros ORCID cuentan apariciones de origen. Las salidas canónicas consolidan posibles repeticiones. Estas cifras pueden diferir sin que exista un error.",
          "ORCID records count source occurrences. Canonical outputs consolidate possible repetitions. These figures can differ without indicating an error."), chapter=3)

page("directory", ("3.2 Directorio de investigadores", "3.2 Researcher directory"), (
    ["El Directorio permite buscar por nombre, ORCID iD o correo, filtrar por Affiliation Manager y por evidencia de relación institucional, y ordenar los resultados. Puedes elegir 10, 25 o 50 filas por página.",
     "La búsqueda y el orden se aplican al conjunto de resultados. CSV y Excel permiten exportar los resultados filtrados, incluidos los que no están en la página visible.",
     "La evidencia verificada puede provenir de identificadores ROR, GRID o Ringgold. Una relación inferida desde caché sirve como señal operativa y debe distinguirse de una asociación verificada."],
    ["The Directory lets you search by name, ORCID iD, or email, filter by Affiliation Manager and institutional relationship evidence, and sort the results. You can display 10, 25, or 50 rows per page.",
     "Search and sorting apply to the result set. CSV and Excel export the filtered results, including records beyond the visible page.",
     "Verified evidence may come from ROR, GRID, or Ringgold identifiers. A cache-inferred relationship is an operational signal and should be distinguished from a verified association."]),
    image="directory", captions=("Directorio con filtros, orden, paginación y exportaciones.", "Directory with filters, sorting, pagination, and exports."),
    steps=(["Escribe un criterio y aplica los filtros pertinentes.", "Selecciona un encabezado para cambiar el orden.", "Abre un ORCID iD para consultar el portafolio o descarga el conjunto filtrado."],
           ["Enter a search term and apply the relevant filters.", "Select a column heading to change the order.", "Open an ORCID iD to view its portfolio or export the filtered set."]))

page("portfolio", ("3.3 Portafolio de investigador", "3.3 Researcher portfolio"), (
    ["Al abrir un ORCID iD desde el Directorio, puedes consultar datos del registro público, biografía, identificadores, resumen visual, contexto institucional y actividades disponibles.",
     "Actualizar desde ORCID solicita nuevamente ese perfil público; no actualiza toda la institución. Descargar informe completo genera un Excel. También puedes exportar secciones disponibles, como educación, empleos, obras y financiamientos.",
     "El enlace ORCID.org abre el registro público original. La ausencia de una sección puede deberse a que no tiene datos públicos. El contexto institucional permite distinguir los registros de obras de las salidas únicas consolidadas."],
    ["Opening an ORCID iD from the Directory lets you inspect public record details, biography, identifiers, visual summaries, institutional context, and available activities.",
     "Refresh from ORCID requests that public profile again; it does not update the entire institution. Download full report produces an Excel file. You can also export available sections such as education, employment, works, and funding.",
     "The ORCID.org link opens the original public record. A section may be absent because it contains no public data. Institutional context helps distinguish source work records from consolidated unique outputs."]),
    image="portfolio", captions=("Portafolio público de un investigador ficticio.", "Public portfolio of a fictional researcher."))

page("orcid-overview", ("3.4 Analítica ORCID", "3.4 ORCID analytics"), (
    ["Analítica ORCID trabaja sobre los registros institucionales almacenados desde ORCID. Los filtros permiten combinar período, tipo de obra, tipo de financiamiento e investigador. Un filtro vacío incluye todos los valores disponibles.",
     "Las secciones Resumen, Publicaciones, Financiamiento e Investigadores ofrecen distintas lecturas del conjunto filtrado. Comprueba los filtros al cambiar de sección.",
     "Las exportaciones de cada gráfico recuperan sus datos en CSV o Excel. Cuando aparece el control de imagen, también puedes guardar la visualización."],
    ["ORCID analytics uses institutional records cached from ORCID. Filters combine period, work type, funding type, and researcher. An empty filter includes all available values.",
     "Overview, Publications, Funding, and Researchers offer different views of the filtered set. Check the filters when switching sections.",
     "Each chart's data exports provide CSV or Excel. Where an image control is present, you can also save the visualization."]),
    image="orcid-overview", captions=("Resumen de Analítica ORCID y filtros comunes.", "ORCID analytics overview and shared filters."))

page("orcid-publications", ("3.4.1 Publicaciones", "3.4.1 Publications"), (
    ["Publicaciones presenta la evolución anual, los tipos de obra y las principales revistas o fuentes. Los indicadores se construyen con los registros ORCID incluidos en el ámbito y los filtros activos.",
     "Utiliza el período para acotar la consulta y el tipo de obra para comparar conjuntos equivalentes. Antes de incorporar un gráfico a un informe, conserva su fecha de consulta y los criterios utilizados.",
     "Una obra puede aparecer en más de un perfil ORCID. Por eso una suma de registros no equivale necesariamente a publicaciones únicas de la institución."],
    ["Publications shows annual trends, work types, and leading journals or sources. Indicators use ORCID records within the institutional scope and active filters.",
     "Use the period filter to narrow the query and the work type filter to compare equivalent sets. Before adding a chart to a report, retain its retrieval date and the criteria used.",
     "A work may appear in more than one ORCID profile. A sum of records therefore does not necessarily represent unique institutional publications."]),
    image="orcid-publications", captions=("Indicadores de publicaciones ORCID.", "ORCID publication indicators."))

page("orcid-funding", ("3.4.2 Financiamiento", "3.4.2 Funding"), (
    ["Financiamiento agrupa registros por año de inicio, tipo y organización financiadora. Puedes combinar los filtros de período, tipo de financiamiento e investigador para revisar la actividad declarada.",
     "Los datos proceden de registros públicos ORCID. La falta de monto, moneda o número de proyecto describe una brecha de información disponible.",
     "La presencia simultánea de una obra y un financiamiento en el perfil de una persona no prueba que esa publicación haya sido financiada por ese proyecto. Las tablas y gráficos deben interpretarse como contexto de actividad registrada."],
    ["Funding groups records by start year, type, and funding organization. Combine period, funding type, and researcher filters to review declared activity.",
     "The data comes from public ORCID records. Missing amounts, currencies, or project numbers describe gaps in the available information.",
     "A work and funding record appearing in the same person's profile do not establish that the project funded that publication. Interpret these tables and charts as context for recorded activity."]),
    image="orcid-funding", captions=("Indicadores de financiamiento visible públicamente en ORCID.", "Indicators for public funding records in ORCID."))

page("orcid-researchers", ("3.4.3 Investigadores", "3.4.3 Researchers"), (
    ["Investigadores permite reconocer quiénes concentran registros de obras o financiamientos dentro del conjunto filtrado. Los nombres y ORCID iD permiten volver al portafolio individual y las listas pueden exportarse.",
     "Una posición en estas listas refleja la actividad registrada y su cobertura. No constituye una evaluación integral de desempeño ni incluye necesariamente toda la producción de una persona.",
     "Para verificar un resultado, conserva el período y los filtros, abre el portafolio y contrasta la información con el registro público de origen."],
    ["Researchers identifies people with the most work or funding records in the filtered set. Names and ORCID iDs provide access to individual portfolios, and the lists can be exported.",
     "A position in these lists reflects recorded activity and its coverage. It is not a comprehensive performance assessment and may not include a person's entire output.",
     "To verify a result, retain the period and filters, open the portfolio, and compare the information with the public source record."]),
    image="orcid-researchers", captions=("Listas de investigadores por actividad registrada.", "Researcher lists by recorded activity."))

page("downloads", ("4. Gestionar datos", "4. Manage data"), (
    ["4.1 Sincronización y descargas",
     "Para Usuario y Usuario OAI, esta página es un monitor y centro de descargas. Informa la vigencia de obras, financiamientos, perfiles y metadatos OpenAlex, junto con la última ejecución disponible.",
     "Las descargas institucionales incluyen obras ORCID, financiamientos, investigadores y enriquecimiento OpenAlex, según la disponibilidad de cada conjunto. Los archivos reflejan el estado de la caché indicado en la página.",
     "Estos perfiles consultan el estado y descargan los datos disponibles. Si necesitas una sincronización institucional o aparece una ejecución fallida, solicita su revisión al equipo responsable."],
    ["4.1 Synchronization and downloads",
     "For User and OAI User, this page is a status monitor and download center. It shows the freshness of works, funding, profiles, and OpenAlex metadata, together with the latest available run.",
     "Institutional downloads include ORCID works, funding, researchers, and OpenAlex enrichment, according to dataset availability. Files reflect the cache state indicated on the page.",
     "These roles inspect status and download available data. If an institutional synchronization is needed or a failed run appears, ask the responsible team to review it."]),
    image="downloads", captions=("Estado institucional y conjuntos descargables.", "Institutional status and downloadable datasets."), chapter=4)

page("exports", ("4.2 Exportaciones en segundo plano", "4.2 Background exports"), (
    ["Las descargas grandes en CSV o Excel se preparan en segundo plano. El centro flotante Exportaciones muestra la espera, el progreso y el enlace privado cuando el archivo está listo. Puedes seguir navegando dentro de la plataforma.",
     "Una solicitud equivalente puede reutilizar un archivo vigente. Si cambian los datos fuente, se prepara una nueva exportación. El archivo tiene una vigencia limitada; si ya venció, vuelve a solicitarlo desde la vista de origen.",
     "Borrar todo elimina tus exportaciones finalizadas y sus archivos. Conserva los trabajos en cola o en ejecución. Cerrar o minimizar un aviso sólo cambia su visualización."],
    ["Large CSV and Excel downloads are prepared in the background. The floating Exports center shows waiting status, progress, and a private link when the file is ready. You can continue navigating within the platform.",
     "An equivalent request may reuse a file that is still valid. If source data changes, a new export is prepared. Files have a limited lifetime; request an expired file again from its source view.",
     "Delete all removes your completed exports and their files. Queued or running jobs are preserved. Closing or minimizing a notification only changes its display."]),
    image="exports", captions=("Centro flotante con una exportación de demostración lista para descargar.", "Floating center with a demonstration export ready to download."),
    steps=(["Aplica los filtros y el orden que necesitas.", "Selecciona CSV o Excel y revisa el aviso flotante.", "Cuando indique que está listo, pulsa Descargar archivo.", "Conserva el archivo junto con la fecha, el ámbito y los filtros de la consulta."],
           ["Apply the filters and sorting you need.", "Select CSV or Excel and check the floating notification.", "When it is ready, select Download file.", "Keep the file with the query date, institutional scope, and filters."]))

page("quality", ("4.3 Calidad de datos", "4.3 Data quality"), (
    ["Calidad de datos separa cuatro perspectivas: Resumen, Evidencia de investigadores, Contexto de financiamiento e Integridad técnica. Permite revisar cobertura de DOI y años, completitud de financiamientos, relaciones verificadas o inferidas y consistencia de OpenAlex.",
     "Los porcentajes describen campos disponibles. Una ausencia se informa como brecha de cobertura y no debe interpretarse automáticamente como un error del sistema.",
     "Utiliza estas vistas para documentar límites de un informe y localizar registros que requieren revisión. Si detectas una inconsistencia, registra el módulo y el identificador pertinente y comunícala al equipo responsable."],
    ["Data quality separates four perspectives: Overview, Researcher evidence, Funding context, and Technical integrity. It reports DOI and year coverage, funding completeness, verified or inferred relationships, and OpenAlex consistency.",
     "Percentages describe available fields. An absence is a coverage gap and should not automatically be interpreted as a system error.",
     "Use these views to document a report's limitations and locate records that need review. If you identify an inconsistency, record the relevant module and identifier and report it to the responsible team."]),
    image="quality", captions=("Resumen de calidad de los datos institucionales.", "Institutional data quality overview."))

page("duplicates", ("4.4 Perfiles duplicados", "4.4 Duplicate profiles"), (
    ["Este módulo identifica candidatos algorítmicos que podrían representar más de un ORCID iD para una misma persona. Puedes buscar, filtrar por confianza o estado, cambiar entre las vistas disponibles, consultar la metodología, refrescar el análisis y exportar resultados.",
     "Usuario y Usuario OAI consultan los candidatos y su evidencia. Un candidato no confirma un duplicado y refrescar el análisis no fusiona ni modifica los registros ORCID.",
     "Compara nombres, identificadores y evidencia disponible antes de informar un caso. La coincidencia de nombres, por sí sola, no basta para concluir que se trata de la misma persona."],
    ["This module identifies algorithmic candidates that may represent multiple ORCID iDs for one person. You can search, filter by confidence or status, switch between available views, read the methodology, refresh the analysis, and export results.",
     "User and OAI User inspect candidates and their evidence. A candidate does not confirm a duplicate, and refreshing the analysis does not merge or modify ORCID records.",
     "Compare names, identifiers, and available evidence before reporting a case. A name match alone does not establish that two records belong to the same person."]),
    image="duplicates", captions=("Consulta de posibles perfiles duplicados.", "Review of possible duplicate profiles."))

page("enrichment", ("4.5 Enriquecimiento OpenAlex", "4.5 OpenAlex enrichment"), (
    ["La revisión de enriquecimiento clasifica artículos como coincidentes, pendientes, no encontrados, con error o sin DOI. Puedes buscar por título, DOI, ORCID iD, fuente o tema, ordenar, cambiar el tamaño de página y desplegar el detalle de una fila.",
     "La exportación conserva los filtros del conjunto. El enlace Analítica conduce al análisis agregado. La cobertura se calcula sobre artículos ORCID elegibles y no sobre toda la producción posible de la institución.",
     "La coincidencia prioriza DOI. Algunos registros pueden vincularse mediante una comparación conservadora de título, año y tipo. Un registro sin coincidencia sigue existiendo en ORCID aunque no aporte metadatos enriquecidos."],
    ["The enrichment review classifies articles as matched, pending, not found, error, or without DOI. You can search by title, DOI, ORCID iD, source, or topic, sort results, change page size, and expand row details.",
     "Exports preserve the dataset filters. The Analytics link opens aggregate analysis. Coverage uses eligible ORCID articles, rather than all possible institutional output, as its basis.",
     "Matching prioritizes DOI. Some records may be linked through a conservative comparison of title, year, and type. An unmatched record still exists in ORCID even if it contributes no enriched metadata."]),
    image="enrichment", captions=("Artículos y estados de enriquecimiento OpenAlex.", "Articles and OpenAlex enrichment states."))

page("openalex-overview", ("5. Analítica OpenAlex", "5. OpenAlex analytics"), (
    ["5.1 Filtros, métricas y exportaciones",
     "Analítica OpenAlex complementa los artículos ORCID de la institución asignada con citas, acceso abierto, autorías, afiliaciones, temas, idiomas, fuentes y FWCI. Revisa la cobertura del conjunto antes de interpretar los resultados.",
     "Los filtros generales incluyen período, tipo documental, acceso abierto, idioma y afiliación. Métricas visibles personaliza las tarjetas y los botones de información explican definiciones y métodos.",
     "Los gráficos ofrecen PNG o SVG donde aparece el control. Las tablas ofrecen CSV o Excel. En las tablas con búsqueda, orden y paginación, estos criterios se aplican al conjunto completo, no solamente a las filas visibles."],
    ["5.1 Filters, metrics, and exports",
     "OpenAlex analytics supplements ORCID articles from your assigned institution with citations, open access, authorships, affiliations, topics, languages, sources, and FWCI. Check dataset coverage before interpreting results.",
     "General filters include period, document type, open access, language, and affiliation. Visible metrics customizes the cards, and information buttons explain definitions and methods.",
     "Charts offer PNG or SVG where the control is available. Tables offer CSV or Excel. In tables with search, sorting, and pagination, these criteria apply to the whole set, not only visible rows."]),
    image="openalex-overview", captions=("Resumen OpenAlex con filtros, métricas y tendencia anual.", "OpenAlex overview with filters, metrics, and annual trend."), chapter=5)

page("openalex-oa", ("5.2 Acceso abierto · ANID", "5.2 Open Access · ANID"), (
    ["Esta sección presenta producción, citas, fuentes y tendencias de artículos OA Diamante y OA Verde que cumplen los filtros activos. Las categorías corresponden al estado de acceso abierto registrado por OpenAlex.",
     "Las categorías son mutuamente excluyentes según ese estado. El porcentaje de cada grupo utiliza como denominador todos los artículos enriquecidos que coinciden con los filtros; no sólo los artículos de acceso abierto.",
     "Las visualizaciones permiten revisar evolución, composición y revistas con mayor producción o citación. En gráficos extensos, usa el desplazamiento vertical interno para recorrer las categorías."],
    ["This section presents output, citations, sources, and trends for Diamond OA and Green OA articles matching the active filters. Categories correspond to the open-access status recorded by OpenAlex.",
     "The categories are mutually exclusive according to that status. Each group's percentage uses all enriched articles matching the filters as its denominator, not only open-access articles.",
     "Visualizations show trends, composition, and journals with the most output or citations. Use internal vertical scrolling to explore categories in long charts."]),
    image="openalex-oa", captions=("Indicadores de OA Diamante y OA Verde.", "Diamond OA and Green OA indicators."),
    note=("OA Verde es una condición del artículo por disponibilidad en repositorio. No implica que toda la revista sea verde ni certifica por sí sola el cumplimiento de una política.",
          "Green OA describes an article's repository availability. It does not mean the entire journal is green or independently certify policy compliance."))

page("openalex-oa-tables", ("5.2.1 Tablas de acceso abierto", "5.2.1 Open-access tables"), (
    ["Las tablas de revistas y artículos más citados ofrecen búsqueda, orden por columna, tamaño de página, paginación y exportación. Los resultados respetan los filtros generales de la página.",
     "Para revisar una fuente, busca su nombre y ordena por producción o citas. Para identificar artículos, usa la tabla correspondiente y revisa el DOI, el año y el estado de acceso abierto.",
     "Conserva el denominador y la fecha de consulta al comunicar un porcentaje. Un cambio puede deberse al período, a los filtros, a una actualización de OpenAlex o a una mayor cobertura de coincidencias."],
    ["Journal and most-cited article tables provide search, column sorting, page size, pagination, and export. Results respect the page's general filters.",
     "To review a source, search its name and sort by output or citations. To identify articles, use the relevant table and check DOI, year, and open-access status.",
     "Retain the denominator and retrieval date when reporting a percentage. A change may reflect the period, filters, an OpenAlex update, or increased matching coverage."]),
    image="openalex-oa-tables", captions=("Tablas de fuentes y artículos de acceso abierto.", "Open-access source and article tables."))

page("openalex-collaboration", ("5.3 Colaboración", "5.3 Collaboration"), (
    ["Colaboración muestra autores con afiliación chilena y países e instituciones presentes en las autorías OpenAlex. Estas visualizaciones describen las afiliaciones de los artículos enriquecidos incluidos en los filtros.",
     "Un artículo puede contar en más de un país o institución. Por eso la suma de categorías puede superar el total de artículos. La afiliación registrada en una obra tampoco garantiza una vinculación laboral actual.",
     "Utiliza la información de cada gráfico para revisar el método y exporta los datos si necesitas documentar una colaboración. Estas vistas no constituyen un censo de todos los perfiles ORCID de la institución."],
    ["Collaboration shows authors with Chilean affiliations and countries and institutions present in OpenAlex authorships. These views describe affiliations on enriched articles included by the filters.",
     "One article may count in several countries or institutions. Category totals can therefore exceed the article count. An affiliation recorded on a work also does not establish a current employment relationship.",
     "Use each chart's information control to check the method and export its data to document collaboration. These views are not a census of every ORCID profile at the institution."]),
    image="openalex-collaboration", captions=("Colaboración derivada de autorías y afiliaciones OpenAlex.", "Collaboration derived from OpenAlex authorships and affiliations."))

page("openalex-topics", ("5.4 Temas y fuentes", "5.4 Topics and sources"), (
    ["Esta sección distribuye artículos por dominios y campos temáticos, tipo documental, acceso abierto, idioma y fuente. Los temas proceden de la clasificación de OpenAlex.",
     "Los idiomas se presentan con nombre localizado a partir del código disponible. Los valores ausentes se agrupan como desconocidos; esa categoría no debe interpretarse como un idioma o una disciplina adicional.",
     "Puedes usar los filtros para revisar un período o conjunto específico y exportar tablas o gráficos. La distribución describe los artículos enriquecidos presentes, no toda la actividad disciplinaria de la institución."],
    ["This section distributes articles by thematic domains and fields, document type, open access, language, and source. Topics come from OpenAlex classification.",
     "Languages are displayed with localized names based on the available codes. Missing values are grouped as unknown; that category should not be interpreted as an additional language or discipline.",
     "Use filters to review a specific period or set and export tables or charts. The distribution describes the enriched articles available, not all disciplinary activity at the institution."]),
    image="openalex-topics", captions=("Campos temáticos, idiomas, tipos documentales y fuentes.", "Thematic fields, languages, document types, and sources."))

page("openalex-impact", ("5.5 Impacto de citas", "5.5 Citation impact"), (
    ["Impacto de citas ordena artículos filtrados por su contador actual de citas en OpenAlex. El total puede cambiar en actualizaciones posteriores. Revisa DOI, año y fuente al comparar publicaciones.",
     "La tendencia de citas agrupa ese contador por año de publicación. No representa las citas recibidas durante cada año calendario.",
     "FWCI aporta una lectura de impacto normalizada por campo, año y tipo documental cuando OpenAlex dispone del valor. Un dato ausente no equivale a cero. Consulta la definición de la métrica antes de utilizarla en un informe."],
    ["Citation impact ranks filtered articles by their current OpenAlex citation count. Totals may change after later updates. Check DOI, year, and source when comparing publications.",
     "The citation trend groups that current count by publication year. It does not represent citations received during each calendar year.",
     "FWCI provides an impact measure normalized by field, year, and document type where OpenAlex supplies a value. A missing value is not zero. Read the metric definition before using it in a report."]),
    image="openalex-impact", captions=("Artículos con mayor contador actual de citas.", "Articles with the highest current citation counts."))

page("read-api", ("6. Integrar", "6. Integrate"), (
    ["6.1 Leer desde la API ORCID",
     "Leer desde la API reúne ejemplos cURL de búsquedas por institución, ROR, nombre y país, además de una referencia de endpoints públicos y enlaces de prueba. Está dirigida a personas que necesitan consultar la fuente desde una herramienta técnica.",
     "Selecciona el ejemplo que corresponda, revisa los parámetros y reemplaza los valores de muestra antes de ejecutarlo en tu entorno. Las consultas recuperan datos con visibilidad pública.",
     "La guía explica cómo leer ORCID. No constituye una operación de sincronización de toda tu institución ni concede permisos sobre registros privados."],
    ["6.1 Read from the ORCID API",
     "Read from the API contains cURL search examples by institution, ROR, name, and country, plus a reference to public endpoints and trial links. It is intended for people who need to query the source with a technical tool.",
     "Choose the relevant example, review its parameters, and replace sample values before running it in your environment. Queries retrieve publicly visible data.",
     "The guide explains how to read ORCID. It does not run a synchronization of your entire institution or grant access to private records."]),
    image="read-api", captions=("Guía de consulta de la API pública de ORCID.", "Guide to querying the public ORCID API."), chapter=6)

page("write-orcid", ("6.2 Escribir en ORCID", "6.2 Write to ORCID"), (
    ["Escribir en ORCID documenta un proyecto descargable, sus requisitos, configuración, estructura CSV y flujo de autorización. Permite comprender cómo una integración autorizada incorpora información a un registro.",
     "Revisa las instrucciones del proyecto y coordina su uso con el equipo institucional responsable de ORCID. La cuenta de DATA ORCID CHILE no sustituye las credenciales y autorizaciones exigidas por ORCID.",
     "Escribir en un registro requiere credenciales apropiadas y autorización explícita de la persona titular del ORCID iD. No incluyas secretos, contraseñas ni tokens en planillas o solicitudes de soporte."],
    ["Write to ORCID documents a downloadable project, its requirements, configuration, CSV structure, and authorization flow. It explains how an authorized integration adds information to a record.",
     "Read the project instructions and coordinate its use with the institutional team responsible for ORCID. A DATA ORCID CHILE account does not replace the credentials and authorizations required by ORCID.",
     "Writing to a record requires appropriate credentials and explicit authorization from the ORCID iD holder. Do not include secrets, passwords, or tokens in spreadsheets or support requests."]),
    image="write-orcid", captions=("Guía y proyecto de referencia para escritura en ORCID.", "Guide and reference project for writing to ORCID."))

page("affiliation-manager", ("6.3 Affiliation Manager", "6.3 Affiliation Manager"), (
    ["Esta opción guarda en tu cuenta el Client ID de la aplicación utilizada por el Affiliation Manager institucional. El valor suele comenzar con APP- y sirve para reconocer registros gestionados mediante esa aplicación.",
     "Consulta el valor institucional correcto antes de editarlo, introdúcelo en el campo correspondiente y guarda. No es una contraseña, un secreto ni una clave de API.",
     "El identificador ayuda a interpretar el estado de registros gestionados. Guardarlo no otorga por sí solo autorización de escritura sobre el ORCID iD de una persona."],
    ["This option stores the Client ID of the institutional Affiliation Manager application in your account. The value usually begins with APP- and helps identify records managed through that application.",
     "Confirm the correct institutional value before editing, enter it in the field, and save. It is not a password, secret, or API key.",
     "The identifier helps interpret managed-record status. Saving it does not itself grant permission to write to someone's ORCID iD."]),
    image="affiliation-manager", captions=("Identificador del Affiliation Manager en la cuenta.", "Affiliation Manager identifier in the account."),
    note=("Modifica este dato sólo si conoces el Client ID correcto o recibiste instrucciones del equipo responsable de ORCID.",
          "Change this value only if you know the correct Client ID or have instructions from the team responsible for ORCID."))

page("resources", ("6.4 Recursos ORCID", "6.4 ORCID resources"), (
    ["Recursos ORCID se encuentra en Soporte y reúne enlaces sobre membresía, credenciales API, Affiliation Manager, plantillas CSV e integraciones con plataformas como OJS, DSpace-CRIS, VIVO y Dataverse.",
     "Elige el recurso según tu tarea: comprender una integración, preparar una plantilla o consultar documentación. Los enlaces externos se abren fuera de DATA ORCID CHILE y pueden tener sus propios requisitos de acceso.",
     "Para dudas sobre las funciones de esta plataforma, utiliza primero el Centro de ayuda, que adapta su contenido a los módulos habilitados."],
    ["ORCID resources appears under Support and collects links about membership, API credentials, Affiliation Manager, CSV templates, and integrations with platforms such as OJS, DSpace-CRIS, VIVO, and Dataverse.",
     "Choose the resource for your task: understanding an integration, preparing a template, or reading documentation. External links open outside DATA ORCID CHILE and may have their own access requirements.",
     "For questions about this platform's functions, start with the Help center, which adapts its content to the enabled modules."]),
    image="resources", captions=("Recursos y documentación ORCID disponibles en Soporte.", "ORCID resources and documentation under Support."))

page("oai-overview", ("6.5 Publicación OAI-PMH", "6.5 OAI-PMH publishing"), (
    ["Publicación OAI-PMH permite consultar el repositorio institucional desde el cual otros sistemas pueden recolectar metadatos de artículos seleccionados. Las pestañas organizan la vista general, los artículos, el mapeo de metadatos, las cargas DOI y el acceso de recolección.",
     "Usuario puede consultar el contenido disponible. Usuario OAI también puede exponer o excluir artículos, cargar decisiones por DOI, deshacer la última carga activa, personalizar el formato dataorcid y administrar URL privadas de recolección.",
     "La vista general muestra artículos disponibles, validados por OpenAlex, expuestos y no expuestos. La validación exige una afiliación OpenAlex que coincida con el ROR activo. La política predeterminada y las decisiones manuales determinan la selección efectiva.",
     "Si el proveedor está deshabilitado o falta su configuración, solicita la habilitación al equipo responsable. Un artículo seleccionado se ofrece cuando el proveedor está habilitado y la URL utilizada tiene acceso."],
    ["OAI-PMH publishing lets you review the institutional repository from which other systems can harvest metadata for selected articles. Tabs organize the overview, articles, metadata mapping, DOI imports, and harvesting access.",
     "User can inspect available content. OAI User can also expose or exclude articles, import DOI decisions, undo the latest active import, customize the dataorcid format, and manage private harvesting URLs.",
     "The overview shows available articles, OpenAlex-validated articles, exposed articles, and articles not exposed. Validation requires an OpenAlex affiliation matching the active ROR. The default policy and manual decisions determine the effective selection.",
     "If the provider is disabled or unconfigured, ask the responsible team to enable it. A selected article is available while the provider is enabled and the requested URL has access."]),
    image="oai-overview", captions=("Vista general OAI-PMH disponible para Usuario OAI.", "OAI-PMH overview available to OAI User."))

page("oai-articles", ("6.6 Selección de artículos OAI-PMH", "6.6 OAI-PMH article selection"), (
    ["La tabla permite buscar y filtrar por estado OAI, validación de afiliación, tipo documental y origen de activación. Puedes ordenar por las columnas disponibles y exportar la selección filtrada para su revisión.",
     "Con Usuario OAI, Exponer o Excluir cambia un artículo. Para actuar sobre varios, marca las filas y utiliza Exponer seleccionados o Excluir seleccionados. Seleccionar esta página sólo marca las filas de la página actual.",
     "Las decisiones manuales prevalecen sobre la política automática. Excluir de OAI-PMH no elimina el artículo de ORCID, OpenAlex o la caché de consulta."],
    ["The table supports search and filters for OAI status, affiliation validation, document type, and activation origin. You can sort by the available columns and export the filtered selection for review.",
     "With OAI User, Expose or Exclude changes an individual article. For multiple articles, mark their rows and use Expose selected or Exclude selected. Select this page marks only rows on the current page.",
     "Manual decisions override the automatic policy. Excluding an article from OAI-PMH does not delete it from ORCID, OpenAlex, or the browsing cache."]),
    image="oai-articles", captions=("Selección de artículos con las acciones del perfil Usuario OAI.", "Article selection with OAI User actions."),
    steps=(["Filtra y revisa DOI, título y afiliaciones antes de seleccionar.", "Aplica la acción a las filas correctas y comprueba el nuevo estado.", "Consulta el proveedor habilitado para revisar el resultado publicado."],
           ["Filter and review DOI, title, and affiliations before selecting.", "Apply the action to the intended rows and check their new status.", "Inspect the enabled provider to review the published result."]))

page("oai-metadata", ("6.7 Formatos y mapeo de metadatos", "6.7 Metadata formats and mapping"), (
    ["El proveedor ofrece tres formatos: oai_dc, oai_openaire y dataorcid. Los dos primeros conservan su estructura estándar. El editor personaliza únicamente el formato dataorcid.",
     "Con Usuario OAI, selecciona un campo del catálogo y pulsa Agregar campo. Puedes cambiar su nombre de destino u ocultarlo. Título e identificador son obligatorios; los campos ocultos no se emiten.",
     "Guarda el mapeo para aplicar los cambios. Restaurar valores predeterminados vuelve al perfil inicial cuando el control está disponible. Usuario puede consultar el mapeo, pero no editarlo.",
     "La URL base generada y los enlaces de prueba permiten revisar la respuesta del proveedor habilitado. Coordina el formato requerido con quien recibe los metadatos. El mapeo cambia la salida publicada, no los datos originales de ORCID."],
    ["The provider offers three formats: oai_dc, oai_openaire, and dataorcid. The first two retain their standard structure. The editor customizes only the dataorcid format.",
     "With OAI User, select a catalog field and choose Add field. You can change its destination name or hide it. Title and identifier are required; hidden fields are not emitted.",
     "Save mapping applies your changes. Restore defaults returns to the initial profile when the control is available. User can inspect the mapping but cannot edit it.",
     "The generated base URL and trial links let you inspect the enabled provider's response. Coordinate the required format with the metadata recipient. Mapping changes the published output, not the original ORCID data."]),
    image="oai-metadata", captions=("Editor del formato dataorcid para Usuario OAI.", "dataorcid format editor for OAI User."))

page("oai-import", ("6.8 Activación masiva por DOI", "6.8 Bulk DOI activation"), (
    ["Usuario OAI puede activar artículos institucionales mediante una planilla XLSX. El sistema reconoce DOI que ya pertenecen a artículos públicos del ámbito institucional. La carga no incorpora publicaciones externas ni sincroniza ORCID.",
     "Utiliza Descargar plantilla y completa un DOI por fila en la columna indicada. Selecciona el archivo XLSX y pulsa Validar y activar. Revisa el resultado antes de considerar completa la tarea.",
     "Los DOI inválidos, repetidos o no encontrados se informan sin modificar artículos ajenos al ámbito. La carga conserva un historial con el archivo, fecha, resultados de validación y artículos activados."],
    ["OAI User can activate institutional articles using an XLSX spreadsheet. The system recognizes DOIs already belonging to public articles within the institutional scope. Importing does not add outside publications or synchronize ORCID.",
     "Use Download template and enter one DOI per row in the indicated column. Select the XLSX file and choose Validate and activate. Review the result before considering the task complete.",
     "Invalid, duplicate, or unmatched DOIs are reported without changing articles outside the scope. Each import retains a history with its file, date, validation results, and activated articles."]),
    image="oai-import", captions=("Carga de una planilla DOI e historial de importaciones.", "DOI spreadsheet upload and import history."),
    note=("Una carga DOI registra decisiones de publicación OAI-PMH. No certifica una afiliación faltante en OpenAlex ni modifica el registro público de origen.",
          "A DOI import records OAI-PMH publication decisions. It does not certify a missing OpenAlex affiliation or modify the public source record."))

page("oai-audit", ("6.9 Auditoría y reversión de cargas", "6.9 Import audit and undo"), (
    ["En el historial de cargas, Auditar abre el detalle del archivo y permite revisar los artículos activados y el estado previo. Puedes distinguir cargas aplicadas, deshechas y la última carga activa reversible.",
     "Usuario OAI puede deshacer la última carga activa. Revisa el detalle, selecciona Deshacer y confirma la acción en la plataforma. Para revertir una carga anterior debes deshacer primero las posteriores.",
     "La reversión restaura los cambios atribuibles a esa carga y conserva cambios manuales posteriores. Comprueba el resultado y vuelve a la tabla de artículos para revisar el estado efectivo.",
     "Usuario puede consultar el historial y la auditoría disponibles. Si necesitas una corrección y no dispones del permiso de edición OAI, solicita la revisión al equipo responsable."],
    ["In the upload history, Audit opens the file detail so you can review activated articles and their previous state. You can distinguish applied imports, undone imports, and the latest active reversible import.",
     "OAI User can undo the latest active import. Review the detail, select Undo, and confirm in the platform. To reverse an older import, undo the newer ones first.",
     "Undo restores changes attributable to that import while preserving later manual changes. Check the result and return to the articles table to inspect effective status.",
     "User can review the available history and audit. If a correction is needed and you lack OAI editing permission, ask the responsible team to review it."]),
    image="oai-audit", captions=("Auditoría de una carga DOI de demostración.", "Audit of a demonstration DOI import."))

page("oai-access", ("6.10 Acceso de recolección", "6.10 Harvesting access"), (
    ["En Acceso de recolección, Usuario OAI puede registrar la URI de cada repositorio de su institución y generar una URL privada para el recolector. Usuario consulta los repositorios y su estado; las claves y los controles quedan reservados a quienes tienen permiso de edición OAI.",
     "Introduce la dirección HTTP o HTTPS del repositorio, sin credenciales, parámetros ni fragmentos, y pulsa Generar URL privada. Copia la dirección completa. La URI identifica al destinatario; la clave aleatoria de la URL concede el acceso sin depender de la IP o de Cloudflare.",
     "En DSpace-CRIS, configura la URL privada como OAI Provider, el formato Simple Dublin Core (oai_dc) y la recolección de solo metadatos. Inicia o programa la cosecha en DSpace. No requiere iniciar sesión en DataORCID.",
     "Después de configurar el recolector, activa Permitir recolección solo mediante URL privadas registradas y guarda el modo de acceso. La URL general dejará de funcionar. Revocar acceso bloquea una URL; Generar nueva URL invalida la anterior y exige actualizar el recolector."],
    ["In Harvesting access, OAI User can register each institutional repository URI and generate a private URL for its harvester. User can review repositories and their status; credentials and controls are reserved for accounts with OAI editing permission.",
     "Enter the repository HTTP or HTTPS address without credentials, query parameters, or a fragment, then choose Generate private URL. Copy the complete address. The URI identifies the recipient; the random URL key grants access independently of the IP address or Cloudflare.",
     "In DSpace-CRIS, configure the private URL as OAI Provider, choose Simple Dublin Core (oai_dc), and harvest metadata only. Start or schedule harvesting in DSpace. No DataORCID sign-in is required.",
     "After configuring the harvester, enable Allow harvesting only through registered private URLs and save the access mode. The general URL will stop working. Revoke access blocks one URL; Generate new URL invalidates the old one and requires updating the harvester."]),
    image="oai-access", captions=("Repositorio de demostración con su URL privada de recolección.", "Demonstration repository and its private harvesting URL."),
    note=("Quien conozca una URL privada puede consultar el XML, incluso desde un navegador. Mantenla confidencial. Revocar todas las URL conserva el bloqueo de la URL general mientras el modo restringido esté activo.",
          "Anyone who knows a private URL can read the XML, including in a browser. Keep it confidential. Revoking every URL keeps the general URL blocked while restricted mode remains active."))

page("help", ("7. Centro de ayuda", "7. Help center"), (
    ["El Centro de ayuda se encuentra en Soporte. Incluye búsqueda instantánea y temas sobre primeros pasos, fuentes, flujo de datos, métricas, descargas, diccionario, permisos, integraciones, solución de problemas y notas de versión.",
     "El contenido acompaña los flujos de Usuario y Usuario OAI. Los temas y enlaces se ajustan a los módulos habilitados; un módulo deshabilitado tampoco aparece documentado dentro de la ayuda activa.",
     "Escribe una palabra como DOI, exportación o acceso abierto para localizar explicaciones. Abre el tema y utiliza sus enlaces para volver a la función correspondiente. Consulta esta ayuda antes de escalar una duda operativa."],
    ["The Help center is under Support. It includes instant search and topics covering first steps, sources, data flow, metrics, downloads, the data dictionary, permissions, integrations, troubleshooting, and release notes.",
     "Content follows the User and OAI User workflows. Topics and links adapt to enabled modules; a disabled module is also absent from the active help content.",
     "Enter a term such as DOI, export, or open access to locate explanations. Open the topic and use its links to return to the relevant function. Consult this help before escalating an operational question."]),
    image="help", captions=("Buscador y temas del Centro de ayuda.", "Help center search and topics."), chapter=7)

page("profile", ("8. Cuenta y seguridad", "8. Account and security"), (
    ["8.1 Mi perfil",
     "Desde las opciones personales puedes actualizar nombre, apellido, correo y cargo. Revisa los valores, realiza los cambios y guarda. El correo se utiliza para notificaciones y recuperación de contraseña.",
     "La institución, el ROR y el rol no se modifican en esta pantalla. Si no corresponden a tu situación, solicita su revisión al equipo responsable.",
     "El selector de idioma permite mantener la interfaz en el idioma que prefieras entre los habilitados. Tus cambios de cuenta no alteran el registro público ORCID de una persona investigadora."],
    ["8.1 My profile",
     "Personal options let you update your first name, last name, email, and position. Review the values, make your changes, and save. Email is used for notifications and password recovery.",
     "Institution, ROR, and role cannot be changed on this screen. If they do not match your circumstances, ask the responsible team to review them.",
     "The language selector lets you keep the interface in your preferred enabled language. Account changes do not alter a researcher's public ORCID record."]),
    image="profile", captions=("Edición de los datos personales de la cuenta ficticia.", "Editing personal details of the fictional account."), chapter=8)

page("password", ("8.2 Contraseña y cierre de sesión", "8.2 Password and sign-out"), (
    ["Para cambiar la contraseña, ingresa la actual, una nueva de al menos ocho caracteres y su confirmación. El sistema también limita su longitud a 72 bytes UTF-8; algunos caracteres ocupan más de un byte.",
     "Guarda el cambio y revisa la confirmación. Utiliza una contraseña exclusiva para esta cuenta y no la compartas. Si desconoces la actual, utiliza la recuperación disponible en el inicio de sesión.",
     "Cerrar sesión se encuentra al pie del menú lateral. Utilízalo al terminar, especialmente en equipos compartidos. Evita incluir credenciales o información personal privada en búsquedas, archivos o solicitudes de soporte."],
    ["To change your password, enter the current password, a new password with at least eight characters, and its confirmation. The system also limits it to 72 UTF-8 bytes; some characters use more than one byte.",
     "Save the change and check the confirmation. Use a password exclusive to this account and do not share it. If you do not know your current password, use the recovery option on the sign-in page.",
     "Sign out is at the bottom of the sidebar. Use it when finished, especially on shared devices. Avoid including credentials or private personal information in searches, files, or support requests."]),
    image="password", captions=("Cambio de contraseña de la cuenta.", "Account password change."))

page("methodology", ("9. Fuentes, metodología y buenas prácticas", "9. Sources, methods, and good practice"), (
    ["9.1 Cómo se construye el ámbito institucional",
     "ROR es la clave institucional principal. Los identificadores GRID o Ringgold verificados complementan la búsqueda en ORCID cuando están disponibles. Los resultados se combinan y deduplican por ORCID iD, conservando evidencia de su procedencia.",
     "9.2 Relación entre ORCID, OpenAlex y OAI-PMH",
     "ORCID aporta perfiles y registros públicos. OpenAlex añade metadatos analíticos para artículos elegibles con una coincidencia aceptada. OAI-PMH publica metadatos de la selección institucional efectiva; no descarga el texto completo ni modifica ORCID.",
     "9.3 Recomendaciones de uso"],
    ["9.1 How institutional scope is built",
     "ROR is the main institutional key. Verified GRID or Ringgold identifiers complement ORCID discovery when available. Results are combined and deduplicated by ORCID iD while retaining provenance evidence.",
     "9.2 How ORCID, OpenAlex, and OAI-PMH relate",
     "ORCID supplies public profiles and records. OpenAlex adds analytical metadata for eligible articles with an accepted match. OAI-PMH publishes metadata for the effective institutional selection; it does not download full text or modify ORCID.",
     "9.3 Good practice"]),
    steps=([
        "Revisa fecha de actualización, institución y filtros antes de citar una cifra.",
        "Distingue registros ORCID, salidas canónicas y artículos enriquecidos.",
        "Consulta definición y método en los botones de información.",
        "Conserva fecha, filtros y ámbito junto con cada exportación.",
        "Trata duplicados y brechas de cobertura como señales sujetas a revisión.",
        "Compara conjuntos con períodos, denominadores y coberturas equivalentes.",
        "Revisa la selección OAI-PMH y el formato antes de compartir su URL con el sistema receptor."
    ], [
        "Check the update date, institution, and filters before citing a figure.",
        "Distinguish ORCID records, canonical outputs, and enriched articles.",
        "Read definitions and methods through information buttons.",
        "Keep date, filters, and scope with each export.",
        "Treat duplicate candidates and coverage gaps as signals requiring review.",
        "Compare sets with equivalent periods, denominators, and coverage.",
        "Review OAI-PMH selection and format before sharing its URL with the receiving system."
    ]), chapter=9)

page("glossary", ("10. Glosario", "10. Glossary"), ([], []), table=([
    ["CONCEPTO", "DESCRIPCIÓN"],
    ["ORCID iD", "Identificador persistente de una persona investigadora."],
    ["ROR", "Identificador institucional principal utilizado por la plataforma."],
    ["GRID y Ringgold", "Identificadores institucionales históricos y verificados que complementan el descubrimiento; no reemplazan al ROR."],
    ["Registro ORCID", "Dato público incorporado a un perfil, como una obra o un financiamiento."],
    ["Salida canónica", "Publicación consolidada por DOI normalizado o, de manera conservadora, por título y año."],
    ["Caché", "Copia local de información recuperada. Su fecha permite interpretar la vigencia del conjunto."],
    ["Cobertura OpenAlex", "Proporción de artículos ORCID elegibles que cuentan con coincidencia y metadatos OpenAlex."],
    ["Citas", "Contador actual de citas de una publicación según la información OpenAlex disponible."],
    ["FWCI", "Impacto de citas ponderado por campo, año y tipo documental."],
    ["OA Diamante", "Categoría OpenAlex para artículos en revistas completamente abiertas sin cargos de publicación para autores."],
    ["OA Verde", "Categoría OpenAlex para acceso mediante una copia en repositorio."],
    ["AM", "Affiliation Manager de ORCID; su Client ID ayuda a reconocer registros gestionados."],
], [
    ["TERM", "DESCRIPTION"],
    ["ORCID iD", "Persistent identifier for a researcher."],
    ["ROR", "Main institutional identifier used by the platform."],
    ["GRID and Ringgold", "Verified legacy institutional identifiers that complement discovery; they do not replace ROR."],
    ["ORCID record", "Public data added to a profile, such as a work or funding record."],
    ["Canonical output", "Publication consolidated by normalized DOI or, conservatively, by title and year."],
    ["Cache", "Local copy of retrieved information. Its date helps establish dataset freshness."],
    ["OpenAlex coverage", "Proportion of eligible ORCID articles with a match and OpenAlex metadata."],
    ["Citations", "Current publication citation count according to available OpenAlex information."],
    ["FWCI", "Field-weighted citation impact, normalized by field, year, and document type."],
    ["Diamond OA", "OpenAlex category for articles in fully open journals without author publication charges."],
    ["Green OA", "OpenAlex category for access through a repository copy."],
    ["AM", "ORCID Affiliation Manager; its Client ID helps identify managed records."],
]), chapter=10)

page("glossary-oai", ("10.1 Términos de exportación e integración", "10.1 Export and integration terms"), ([], []), table=([
    ["CONCEPTO", "DESCRIPCIÓN"],
    ["Exportación en segundo plano", "Preparación de un archivo mientras continúas usando la plataforma."],
    ["Archivo vigente", "Exportación finalizada que todavía puede descargarse y, si corresponde, reutilizarse."],
    ["OAI-PMH", "Protocolo utilizado para que un sistema recolecte metadatos de otro."],
    ["Proveedor", "Servicio que expone la selección institucional a través de una URL base."],
    ["Recolector", "Sistema receptor que consulta el proveedor e incorpora los metadatos."],
    ["Artículo expuesto", "Artículo incluido en la selección efectiva; su disponibilidad pública también requiere un proveedor habilitado."],
    ["Validación OpenAlex", "Coincidencia de una afiliación del artículo con el ROR de la institución activa."],
    ["Decisión manual", "Inclusión o exclusión que prevalece sobre la política automática."],
    ["oai_dc", "Formato de metadatos Dublin Core del proveedor."],
    ["oai_openaire", "Formato OpenAIRE del proveedor."],
    ["dataorcid", "Formato cuyo conjunto de campos y nombres de destino puede personalizar Usuario OAI."],
    ["Carga DOI", "Planilla XLSX que activa artículos ya presentes en el ámbito institucional y conserva su auditoría."],
    ["Reversión de carga", "Acción que deshace la última carga activa preservando cambios manuales posteriores."],
], [
    ["TERM", "DESCRIPTION"],
    ["Background export", "File preparation while you continue using the platform."],
    ["Valid file", "Completed export that is still downloadable and, where applicable, reusable."],
    ["OAI-PMH", "Protocol used by one system to harvest metadata from another."],
    ["Provider", "Service exposing the institutional selection through a base URL."],
    ["Harvester", "Receiving system that queries the provider and ingests metadata."],
    ["Exposed article", "Article included in the effective selection; public availability also requires an enabled provider."],
    ["OpenAlex validation", "An article affiliation matching the active institution's ROR."],
    ["Manual decision", "Inclusion or exclusion that overrides the automatic policy."],
    ["oai_dc", "Provider's Dublin Core metadata format."],
    ["oai_openaire", "Provider's OpenAIRE metadata format."],
    ["dataorcid", "Format whose fields and destination names can be customized by OAI User."],
    ["DOI import", "XLSX spreadsheet that activates articles already in the institutional scope and retains an audit."],
    ["Import undo", "Action reversing the latest active import while preserving later manual changes."],
]))

page("troubleshooting", ("11. Solución de problemas", "11. Troubleshooting"), (
    ["No aparecen datos",
     "Restablece los filtros y revisa el indicador de actualización. Si el conjunto institucional no está disponible, solicita al equipo responsable que revise el estado de los datos.",
     "Una cifra difiere entre módulos",
     "Comprueba si cuenta registros ORCID, salidas canónicas o artículos OpenAlex. Revisa período, tipo, acceso abierto, afiliación y fecha de actualización.",
     "Una exportación está vacía, pendiente o vencida",
     "Confirma que la vista tenga resultados y elimina filtros demasiado restrictivos. Revisa el centro flotante para conocer el estado. Un archivo vencido debe solicitarse nuevamente; si una tarea no avanza, comunica la vista y el momento de la solicitud.",
     "No puedo editar artículos o mapeos OAI-PMH",
     "Usuario tiene acceso de consulta. La selección, las cargas DOI y el mapeo requieren Usuario OAI asignado a la institución. Si ese permiso corresponde a tu tarea, solicita su revisión.",
     "Un DOI no se activa o no puedo deshacer una carga",
     "Revisa la plantilla y el informe de DOI inválidos, repetidos o no encontrados. La carga sólo activa artículos del ámbito institucional. Sólo se puede deshacer la última carga activa; revisa el historial y los cambios posteriores.",
     "No aparece un módulo o falla el proveedor público",
     "La disponibilidad depende de los módulos habilitados y del estado del proveedor. Consulta el Centro de ayuda y solicita revisión al equipo responsable. Incluye página, fecha, filtros y una captura sin datos sensibles."],
    ["No data appears",
     "Reset filters and check the update indicator. If the institutional dataset is unavailable, ask the responsible team to review its status.",
     "A figure differs between modules",
     "Check whether it counts ORCID records, canonical outputs, or OpenAlex articles. Review period, type, open access, affiliation, and update date.",
     "An export is empty, pending, or expired",
     "Confirm that the view has results and remove overly restrictive filters. Check the floating center for status. Request an expired file again; if a task does not progress, report the view and request time.",
     "I cannot edit OAI-PMH articles or mappings",
     "User has read-only access. Selection, DOI imports, and mapping require OAI User assigned to the institution. If this permission is needed for your work, request a review.",
     "A DOI is not activated or I cannot undo an import",
     "Check the template and the report of invalid, duplicate, or unmatched DOIs. Imports only activate articles in institutional scope. Only the latest active import can be undone; review the history and later changes.",
     "A module is missing or the public provider fails",
     "Availability depends on enabled modules and provider status. Consult the Help center and ask the responsible team to review. Include page, date, filters, and a screenshot without sensitive information."]), chapter=11)


def load_translations():
    """Require complete editions with matching paragraphs, steps, and table shapes."""
    fields = ("titles", "paragraphs", "captions", "steps", "note", "table")

    def validate_shape(source, translated, location):
        if type(source) is not type(translated) or bool(source) != bool(translated):
            raise ValueError(f"Missing or invalid manual translation: {location}")
        if isinstance(source, list):
            if len(source) != len(translated):
                raise ValueError(f"Manual translation structure differs: {location}")
            for index, (original, value) in enumerate(zip(source, translated)):
                validate_shape(original, value, f"{location}[{index}]")

    for language in LANGUAGES:
        if language in {"en", "es"}:
            continue
        path = Path(__file__).with_name("locales") / f"{language}.json"
        translations = json.loads(path.read_text(encoding="utf-8"))
        if set(translations) != {entry["key"] for entry in PAGES}:
            raise ValueError(f"Manual translation page keys differ: {language}")
        for entry in PAGES:
            translated = translations[entry["key"]]
            if set(translated) - set(fields):
                raise ValueError(f"Unknown manual translation fields: {language}/{entry['key']}")
            for field in fields:
                source = entry[field]["en"]
                value = translated.get(field, type(source)())
                validate_shape(source, value, f"{language}/{entry['key']}/{field}")
                entry[field][language] = value


load_translations()
