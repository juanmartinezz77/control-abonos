import streamlit as st
import sqlite3
import pandas as pd
import logging
from io import BytesIO
from datetime import date
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font, PatternFill, Border, Side, Alignment

DB_PATH = "control_abonos.db"

# ------------------ Logging ------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler()],
)

# ------------------ DATABASE HELPERS ------------------


def get_connection():
    """
    Devuelve una conexión SQLite con PRAGMA foreign_keys=ON.
    """
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn
    except sqlite3.Error:
        logging.exception("Error conectando a la DB")
        st.error("Error al conectar con la base de datos. Revisa los logs.")
        st.stop()


def init_db(conn):
    """Crea las tablas si no existen (idempotente)."""
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS casos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cliente TEXT NOT NULL,
            descripcion TEXT,
            valor_acordado REAL NOT NULL DEFAULT 0,
            etapa TEXT,
            observaciones TEXT,
            creado_en TEXT DEFAULT (DATE('now'))
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS abonos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha TEXT NOT NULL,
            monto REAL NOT NULL,
            caso_id INTEGER NOT NULL,
            observaciones TEXT,
            creado_en TEXT DEFAULT (DATE('now')),
            FOREIGN KEY(caso_id) REFERENCES casos(id) ON DELETE CASCADE
        )
        """
    )
    conn.commit()


# ------------------ CRUD HELPERS ------------------


def fetch_casos(conn, cliente_filter=None, etapa_filter=None):
    q = "SELECT * FROM casos"
    params, conditions = [], []
    if cliente_filter:
        conditions.append("cliente = ?")
        params.append(cliente_filter)
    if etapa_filter:
        conditions.append("etapa = ?")
        params.append(etapa_filter)
    if conditions:
        q += " WHERE " + " AND ".join(conditions)
    q += " ORDER BY id"
    return pd.read_sql_query(q, conn, params=params)


def fetch_abonos(conn, caso_id=None):
    q = """SELECT abonos.*, casos.cliente, casos.descripcion
           FROM abonos JOIN casos ON abonos.caso_id = casos.id"""
    params = []
    if caso_id:
        q += " WHERE caso_id = ?"
        params.append(caso_id)
    q += " ORDER BY fecha DESC, id DESC"
    return pd.read_sql_query(q, conn, params=params)


def add_caso(conn, cliente, descripcion, valor_acordado, etapa, observaciones):
    if not cliente or str(cliente).strip() == "":
        raise ValueError("El nombre del cliente es obligatorio.")
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM casos WHERE cliente = ? AND descripcion = ?", (cliente, descripcion))
    if c.fetchone()[0] > 0:
        raise ValueError("Ya existe un caso con ese cliente y descripción.")
    c.execute(
        "INSERT INTO casos (cliente, descripcion, valor_acordado, etapa, observaciones) VALUES (?,?,?,?,?)",
        (cliente.strip(), descripcion, float(valor_acordado or 0), etapa, observaciones),
    )
    conn.commit()
    logging.info("Caso agregado: %s - %s", cliente, descripcion)
    return c.lastrowid


def edit_caso(conn, caso_id, cliente, descripcion, valor_acordado, etapa, observaciones):
    c = conn.cursor()
    c.execute(
        "UPDATE casos SET cliente=?, descripcion=?, valor_acordado=?, etapa=?, observaciones=? WHERE id=?",
        (cliente, descripcion, float(valor_acordado or 0), etapa, observaciones, caso_id),
    )
    conn.commit()
    logging.info("Caso editado id=%s", caso_id)
    return c.rowcount


def delete_caso(conn, caso_id):
    c = conn.cursor()
    c.execute("DELETE FROM abonos WHERE caso_id = ?", (caso_id,))
    c.execute("DELETE FROM casos WHERE id = ?", (caso_id,))
    conn.commit()
    logging.info("Caso eliminado id=%s", caso_id)


def add_abono(conn, fecha, monto, caso_id, observaciones):
    """
    Valida existencia de caso e inserta el abono.
    Lanza ValueError para validaciones de usuario.
    """
    c = conn.cursor()
    try:
        caso_id_int = int(caso_id)
    except Exception:
        raise ValueError("ID de caso inválido.")

    c.execute("SELECT 1 FROM casos WHERE id = ?", (caso_id_int,))
    if c.fetchone() is None:
        raise ValueError(f"No existe el caso con id {caso_id_int}.")

    try:
        monto_val = float(monto)
    except Exception:
        raise ValueError("Monto inválido.")
    if monto_val <= 0:
        raise ValueError("El monto debe ser mayor que cero.")

    c.execute(
        "INSERT INTO abonos (fecha, monto, caso_id, observaciones) VALUES (?,?,?,?)",
        (fecha, monto_val, caso_id_int, observaciones),
    )
    conn.commit()
    logging.info("Abono agregado: caso_id=%s monto=%s fecha=%s", caso_id_int, monto_val, fecha)
    return c.lastrowid


def edit_abono(conn, abono_id, fecha, monto, caso_id, observaciones):
    c = conn.cursor()
    c.execute(
        "UPDATE abonos SET fecha=?, monto=?, caso_id=?, observaciones=? WHERE id=?",
        (fecha, float(monto), int(caso_id), observaciones, int(abono_id)),
    )
    conn.commit()
    logging.info("Abono editado id=%s", abono_id)
    return c.rowcount


def delete_abono(conn, abono_id):
    c = conn.cursor()
    c.execute("DELETE FROM abonos WHERE id = ?", (abono_id,))
    conn.commit()
    logging.info("Abono eliminado id=%s", abono_id)


# ------------------ REPORTS / EXPORTS ------------------


def to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def to_excel_bytes(df: pd.DataFrame) -> bytes:
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Resumen")
        ws = writer.sheets["Resumen"]

        header_fill = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
        header_font = Font(color="FFFFFF", bold=True)
        header_alignment = Alignment(horizontal="center", vertical="center")
        thin = Side(border_style="thin", color="AAAAAA")
        border = Border(left=thin, right=thin, top=thin, bottom=thin)

        for cell in ws[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = header_alignment

        for row in ws.iter_rows(min_row=1, max_row=ws.max_row):
            for cell in row:
                cell.border = border
                cell.alignment = Alignment(vertical="center")

        for column_cells in ws.columns:
            length = max(len(str(cell.value)) if cell.value is not None else 0 for cell in column_cells)
            ws.column_dimensions[get_column_letter(column_cells[0].column)].width = min(length + 4, 60)

        for col in ws.iter_cols(min_row=2, max_row=ws.max_row):
            if all((isinstance(c.value, (int, float)) or c.value is None) for c in col):
                for cell in col:
                    cell.number_format = "#,##0.00"

    buffer.seek(0)
    return buffer.read()


# ------------------ UI HELPERS ------------------


def money(v):
    try:
        return f"${float(v):,.2f}"
    except Exception:
        return v


# ------------------ AUTH ------------------


def check_password(user: str, password: str) -> bool:
    """
    Comprueba credenciales en st.secrets de forma robusta (soporta AttrDict/dict/flat).
    Devuelve True si coincide.
    """
    creds = st.secrets.get("credentials", None)
    if creds is None:
        # Fallback local para desarrollo (evitar en producción)
        return (user == "admin" and password == "1234")

    stored = None
    try:
        if hasattr(creds, "__contains__") and user in creds:
            stored = creds[user]
    except Exception:
        stored = None

    if stored is None:
        try:
            stored = getattr(creds, user)
        except Exception:
            stored = None

    if stored is None and hasattr(creds, "get"):
        try:
            stored = creds.get(user)
        except Exception:
            stored = None

    if isinstance(stored, str):
        return password == stored

    if stored is not None:
        try:
            if hasattr(stored, "get"):
                pw = stored.get("password", None)
                if pw is not None:
                    return pw == password
        except Exception:
            pass
        try:
            pw = getattr(stored, "password", None)
            if pw is not None:
                return pw == password
        except Exception:
            pass

    return False


# ------------------ MAIN APP ------------------


def main():
    st.set_page_config(page_title="Control de Abonos - Dashboard", layout="wide")
    conn = get_connection()
    init_db(conn)

    if "logged_in" not in st.session_state:
        st.session_state["logged_in"] = False
        st.session_state["usuario"] = None

    # --------- LOGIN ----------
    if not st.session_state["logged_in"]:
        st.title("🔐 Acceso")
        user = st.text_input("Usuario")
        password = st.text_input("Contraseña", type="password")
        login_clicked = st.button("Iniciar sesión")
        if login_clicked:
            if check_password(user, password):
                st.session_state["logged_in"] = True
                st.session_state["usuario"] = user
                st.success(f"Bienvenido, {user} ✅")
                # No usar st.experimental_rerun (compatibilidad con distintas versiones).
                # Al no llamar a st.stop() aquí, la ejecución continua y la app mostrará la UI principal.
            else:
                st.error("Usuario o contraseña incorrectos.")
        # Si aún no se ha autenticado, detener la ejecución para que solo se muestre la pantalla de login.
        if not st.session_state["logged_in"]:
            st.stop()

    # UI principal
    st.markdown(
        """
    <style>
        .big-title { font-size:28px; font-weight:700; color:#0b3d91; }
        .subtle { color: #4b5563; }
        .card { background: linear-gradient(180deg, #ffffff, #fbfbfd);
                padding:12px; border-radius:8px;
                box-shadow: 0 2px 8px rgba(15,23,42,0.06); }
    </style>
    """,
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns([1, 4])
    with col1:
        # usar on_click para cambiar el estado y permitir que Streamlit vuelva a ejecutar la app automáticamente
        st.button("Cerrar sesión", on_click=lambda: logout())
    with col2:
        st.markdown('<div class="big-title">⚖️ Control de Abonos — Dashboard</div>', unsafe_allow_html=True)
        st.markdown('<div class="subtle">Gestión de casos, registro de abonos y reportes.</div>', unsafe_allow_html=True)

    st.write("---")

    # Cargar datos
    casos_df = fetch_casos(conn)
    abonos_df = fetch_abonos(conn)

    tab_casos, tab_abonos, tab_resumen, tab_reportes = st.tabs(["Casos", "Abonos", "Resumen", "Reportes"])

    # ------------------ TAB CASOS ------------------
    with tab_casos:
        st.subheader("📁 Casos")
        st.markdown("Agregar o editar casos. Si editas, selecciona el caso y modifica los campos.")
        with st.form("form_caso_nuevo"):
            col_a, col_b = st.columns(2)
            with col_a:
                cliente = st.text_input("Cliente", key="cliente_new")
                valor_acordado = st.number_input("Valor acordado", min_value=0.0, key="valor_new")
            with col_b:
                descripcion = st.text_input("Descripción", key="desc_new")
                etapa = st.text_input("Etapa", key="etapa_new")
            observaciones = st.text_area("Observaciones", key="obs_new")
            if st.form_submit_button("Agregar Caso"):
                try:
                    add_caso(conn, cliente, descripcion, valor_acordado, etapa, observaciones)
                    st.success("Caso agregado correctamente.")
                except ValueError as e:
                    st.error(str(e))
                except Exception:
                    logging.exception("Error agregando caso")
                    st.error("Error al agregar caso. Revisa los logs.")

        if not casos_df.empty:
            st.markdown("### Lista de casos")
            st.dataframe(casos_df, use_container_width=True)

            # Editar caso
            st.markdown("#### Editar / Eliminar caso")
            opciones_casos = [(int(r["id"]), f"{r['id']} — {r['cliente']} — {r['descripcion'] or ''}") for _, r in casos_df.iterrows()]
            seleccionado = st.selectbox("Selecciona caso", options=opciones_casos, format_func=lambda x: x[1])
            caso_id_sel = seleccionado[0] if isinstance(seleccionado, tuple) else seleccionado

            with st.form("form_caso_edit"):
                c_row = casos_df.loc[casos_df["id"] == caso_id_sel].iloc[0]
                cliente_e = st.text_input("Cliente", value=c_row["cliente"], key="cliente_edit")
                descripcion_e = st.text_input("Descripción", value=c_row["descripcion"], key="desc_edit")
                valor_e = st.number_input("Valor acordado", value=float(c_row["valor_acordado"]), min_value=0.0, key="valor_edit")
                etapa_e = st.text_input("Etapa", value=c_row["etapa"], key="etapa_edit")
                obs_e = st.text_area("Observaciones", value=c_row["observaciones"], key="obs_edit")
                btns = st.columns([1, 1])
                if btns[0].form_submit_button("Guardar cambios"):
                    try:
                        edit_caso(conn, caso_id_sel, cliente_e, descripcion_e, valor_e, etapa_e, obs_e)
                        st.success("Caso actualizado.")
                    except Exception:
                        logging.exception("Error editando caso")
                        st.error("Error al actualizar el caso. Revisa los logs.")
                if btns[1].form_submit_button("Eliminar caso"):
                    if st.confirm(f"¿Deseas eliminar el caso id={caso_id_sel}? Esto eliminará también sus abonos."):
                        try:
                            delete_caso(conn, caso_id_sel)
                            st.success("Caso eliminado.")
                        except Exception:
                            logging.exception("Error eliminando caso")
                            st.error("Error al eliminar el caso. Revisa los logs.")

    # ------------------ TAB ABONOS ------------------
    with tab_abonos:
        st.subheader("💰 Abonos")
        if casos_df.empty:
            st.info("Registra primero al menos un caso para agregar abonos.")
        else:
            st.markdown("Agregar un abono al caso seleccionado.")
            opciones = [(int(r["id"]), f"{r['id']} — {r['cliente']} — {r['descripcion'] or ''}") for _, r in casos_df.iterrows()]

            with st.form("nuevo_abono_form"):
                caso_sel = st.selectbox("Selecciona Caso", options=opciones, format_func=lambda x: x[1])
                caso_id_seleccionado = caso_sel[0] if isinstance(caso_sel, tuple) else caso_sel
                fecha = st.date_input("Fecha", value=date.today())
                monto = st.number_input("Monto", min_value=0.0, format="%.2f")
                observaciones = st.text_area("Observaciones")
                if st.form_submit_button("Agregar Abono"):
                    try:
                        add_abono(conn, fecha.isoformat(), monto, caso_id_seleccionado, observaciones)
                        st.success("Abono agregado correctamente.")
                    except ValueError as e:
                        st.error(str(e))
                    except sqlite3.IntegrityError:
                        logging.exception("IntegrityError al insertar abono")
                        st.error("Error de integridad en la base de datos al insertar el abono.")
                    except Exception:
                        logging.exception("Error inesperado al insertar abono")
                        st.error("Ocurrió un error inesperado. Revisa los logs.")

        # Mostrar abonos
        abonos = fetch_abonos(conn)
        if not abonos.empty:
            st.markdown("### Últimos abonos")
            st.dataframe(abonos, use_container_width=True)

            # Editar / Eliminar abono
            st.markdown("#### Editar / Eliminar abono")
            opciones_abonos = [(int(r["id"]), f"{r['id']} — {r['cliente']} — {r['fecha']} — ${float(r['monto']):,.2f}") for _, r in abonos.iterrows()]
            elegido = st.selectbox("Selecciona abono", options=opciones_abonos, format_func=lambda x: x[1])
            abono_id_sel = elegido[0] if isinstance(elegido, tuple) else elegido

            with st.form("form_abono_edit"):
                a_row = abonos.loc[abonos["id"] == abono_id_sel].iloc[0]
                caso_for_edit = st.selectbox("Caso (editar)", options=opciones, format_func=lambda x: x[1], index=[o[0] for o in opciones].index(int(a_row["caso_id"])))
                fecha_e = st.date_input("Fecha", value=pd.to_datetime(a_row["fecha"]).date(), key="fecha_edit")
                monto_e = st.number_input("Monto", value=float(a_row["monto"]), min_value=0.0, format="%.2f", key="monto_edit")
                obs_e = st.text_area("Observaciones", value=a_row["observaciones"], key="obs_abono_edit")
                btns_ab = st.columns([1, 1])
                if btns_ab[0].form_submit_button("Guardar cambios"):
                    try:
                        edit_abono(conn, abono_id_sel, fecha_e.isoformat(), monto_e, caso_for_edit[0], obs_e)
                        st.success("Abono actualizado.")
                    except Exception:
                        logging.exception("Error editando abono")
                        st.error("Error al actualizar el abono. Revisa los logs.")
                if btns_ab[1].form_submit_button("Eliminar abono"):
                    if st.confirm(f"¿Deseas eliminar el abono id={abono_id_sel}?"):
                        try:
                            delete_abono(conn, abono_id_sel)
                            st.success("Abono eliminado.")
                        except Exception:
                            logging.exception("Error eliminando abono")
                            st.error("Error al eliminar el abono. Revisa los logs.")

    # ------------------ TAB RESUMEN ------------------
    with tab_resumen:
        st.subheader("📊 Resumen por Caso")
        resumen_df = resumen_por_caso(conn) if "resumen_por_caso" in globals() else pd.DataFrame()
        if resumen_df.empty:
            st.info("No hay datos disponibles.")
        else:
            display = resumen_df.copy()
            display["valor_acordado"] = display["valor_acordado"].apply(money)
            display["total_abonado"] = display["total_abonado"].apply(money)
            display["saldo_pendiente"] = display["saldo_pendiente"].apply(money)
            st.dataframe(display, use_container_width=True)

    # ------------------ TAB REPORTES ------------------
    with tab_reportes:
        st.subheader("📑 Exportes")
        df_export = resumen_por_caso(conn) if "resumen_por_caso" in globals() else pd.DataFrame()
        if df_export.empty:
            st.info("No hay datos para exportar.")
        else:
            st.download_button("⬇️ Exportar a CSV", data=to_csv_bytes(df_export), file_name="resumen_abonos.csv", mime="text/csv")
            st.download_button("⬇️ Exportar a Excel", data=to_excel_bytes(df_export), file_name="resumen_abonos.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


def logout():
    st.session_state["logged_in"] = False
    st.session_state["usuario"] = None
    # No usar experimental_rerun: el botón on_click provoca un rerun automático.


if __name__ == "__main__":
    main()
