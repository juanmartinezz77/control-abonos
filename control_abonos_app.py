import streamlit as st
import sqlite3
import pandas as pd
from io import BytesIO
from datetime import datetime, date
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font, PatternFill, Border, Side, Alignment

DB_PATH = "control_abonos.db"

# ------------------ DATABASE HELPERS ------------------

def get_connection():
    """Establece conexión SQLite con manejo de errores y claves foráneas activadas."""
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn
    except sqlite3.Error as e:
        st.error(f"Error al conectar con la base de datos: {e}")
        st.stop()

def init_db(conn):
    """Crea las tablas si no existen."""
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS casos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cliente TEXT NOT NULL,
            descripcion TEXT,
            valor_acordado REAL NOT NULL DEFAULT 0,
            etapa TEXT,
            observaciones TEXT,
            creado_en TEXT DEFAULT (DATE('now'))
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS abonos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha TEXT NOT NULL,
            monto REAL NOT NULL,
            caso_id INTEGER NOT NULL,
            observaciones TEXT,
            creado_en TEXT DEFAULT (DATE('now')),
            FOREIGN KEY(caso_id) REFERENCES casos(id) ON DELETE CASCADE
        )
    ''')
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
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM casos WHERE cliente = ? AND descripcion = ?", (cliente, descripcion))
    if c.fetchone()[0] > 0:
        raise ValueError("Ya existe un caso con ese cliente y descripción.")
    c.execute(
        "INSERT INTO casos (cliente, descripcion, valor_acordado, etapa, observaciones) VALUES (?,?,?,?,?)",
        (cliente, descripcion, valor_acordado, etapa, observaciones),
    )
    conn.commit()
    return c.lastrowid

def edit_caso(conn, caso_id, cliente, descripcion, valor_acordado, etapa, observaciones):
    c = conn.cursor()
    c.execute(
        "UPDATE casos SET cliente=?, descripcion=?, valor_acordado=?, etapa=?, observaciones=? WHERE id=?",
        (cliente, descripcion, valor_acordado, etapa, observaciones, caso_id),
    )
    conn.commit()
    return c.rowcount

def delete_caso(conn, caso_id):
    c = conn.cursor()
    c.execute("DELETE FROM abonos WHERE caso_id = ?", (caso_id,))
    c.execute("DELETE FROM casos WHERE id = ?", (caso_id,))
    conn.commit()

def add_abono(conn, fecha, monto, caso_id, observaciones):
    c = conn.cursor()
    c.execute(
        "INSERT INTO abonos (fecha, monto, caso_id, observaciones) VALUES (?,?,?,?)",
        (fecha, monto, caso_id, observaciones),
    )
    conn.commit()
    return c.lastrowid

def edit_abono(conn, abono_id, fecha, monto, caso_id, observaciones):
    c = conn.cursor()
    c.execute(
        "UPDATE abonos SET fecha=?, monto=?, caso_id=?, observaciones=? WHERE id=?",
        (fecha, monto, caso_id, observaciones, abono_id),
    )
    conn.commit()

def delete_abono(conn, abono_id):
    c = conn.cursor()
    c.execute("DELETE FROM abonos WHERE id = ?", (abono_id,))
    conn.commit()

# ------------------ REPORTS / CALCULATIONS ------------------

def resumen_por_caso(conn, cliente_filter=None, etapa_filter=None):
    casos = fetch_casos(conn, cliente_filter, etapa_filter)
    if casos.empty:
        return pd.DataFrame(columns=["id","cliente","descripcion","valor_acordado","total_abonado","saldo_pendiente","etapa","observaciones"])
    abonos = pd.read_sql_query("SELECT caso_id, SUM(monto) as total_abonado FROM abonos GROUP BY caso_id", conn)
    merged = casos.merge(abonos, left_on="id", right_on="caso_id", how="left")
    merged["total_abonado"] = merged["total_abonado"].fillna(0)
    merged["saldo_pendiente"] = merged["valor_acordado"] - merged["total_abonado"]
    return merged[["id","cliente","descripcion","valor_acordado","total_abonado","saldo_pendiente","etapa","observaciones"]]

# ------------------ EXPORTS ------------------

def to_csv_bytes(df):
    return df.to_csv(index=False).encode("utf-8")

def to_excel_bytes(df):
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
            length = max(len(str(cell.value)) if cell.value else 0 for cell in column_cells)
            ws.column_dimensions[get_column_letter(column_cells[0].column)].width = min(length + 2, 40)

        for col in ws.iter_cols(min_row=2, max_row=ws.max_row):
            if all(isinstance(c.value, (int, float)) or c.value is None for c in col):
                for cell in col:
                    cell.number_format = '#,##0.00'
    buffer.seek(0)
    return buffer.read()

# ------------------ UI HELPERS ------------------

def money(v):
    try:
        return f"${float(v):,.2f}"
    except:
        return v

def show_success(msg):
    st.success(msg)
    st.rerun()

def show_error(msg):
    st.error(msg)

# ------------------ MAIN APP ------------------

def main():
    st.set_page_config(page_title="Control de Abonos - Dashboard", layout="wide")

    # Initialize session state
    if "logged_in" not in st.session_state:
        st.session_state["logged_in"] = False

    # LOGIN: robust handling for secrets formats (nested or flat), fallback to hardcoded credentials
    if not st.session_state["logged_in"]:
        st.title("🔐 Acceso restringido")
        user = st.text_input("👤 Usuario")
        password = st.text_input("🔒 Contraseña", type="password")
        if st.button("Iniciar sesión"):
            logged = False
            # If credentials exist in secrets, try to validate
            if "credentials" in st.secrets:
                creds = st.secrets["credentials"]
                stored = creds.get(user) if isinstance(creds, dict) else None
                if stored is None:
                    # user not found in secrets
                    logged = False
                else:
                    # support nested dict style: credentials.user.password
                    if isinstance(stored, dict) and "password" in stored:
                        if password == stored["password"]:
                            logged = True
                    # support flat string style: credentials.user = "password"
                    elif isinstance(stored, str):
                        if password == stored:
                            logged = True
            else:
                # fallback to the original hardcoded check (legacy)
                if user == "admin" and password == "1234":
                    logged = True

            if logged:
                st.session_state["logged_in"] = True
                st.success("Acceso concedido ✅")
                st.rerun()
            else:
                st.error("Usuario o contraseña incorrectos 🚫")
        st.stop()

    st.markdown("""
        <style>
            .big-title { font-size:32px; font-weight:700; color:#0b3d91; }
            .subtle { color: #4b5563; }
            .card { background: linear-gradient(180deg, #ffffff, #fbfbfd);
                    padding:12px; border-radius:12px;
                    box-shadow: 0 2px 8px rgba(15,23,42,0.06); }
        </style>
    """, unsafe_allow_html=True)

    st.markdown('<div class="big-title">⚖️ Control de Abonos — Dashboard</div>', unsafe_allow_html=True)
    st.markdown('<div class="subtle">Gestión de casos, registro de abonos y reportes actualizados.</div>', unsafe_allow_html=True)
    st.write("---")

    conn = get_connection()
    init_db(conn)

    tab_casos, tab_abonos, tab_resumen, tab_reportes = st.tabs(["Casos", "Abonos", "Resumen", "Reportes"])

    # ------------------ TAB CASOS ------------------
    with tab_casos:
        st.subheader("📁 Gestión de Casos")
        with st.form("nuevo_caso_form"):
            cliente = st.text_input("Cliente")
            descripcion = st.text_input("Descripción del Caso")
            valor_acordado = st.number_input("Valor acordado", min_value=0.0)
            etapa = st.text_input("Etapa")
            observaciones = st.text_area("Observaciones")
            if st.form_submit_button("Agregar Caso"):
                if cliente:
                    try:
                        add_caso(conn, cliente, descripcion, valor_acordado, etapa, observaciones)
                        show_success("Caso agregado correctamente.")
                    except ValueError as e:
                        show_error(str(e))
                else:
                    show_error("El nombre del cliente es obligatorio.")

        casos_df = fetch_casos(conn)
        if not casos_df.empty:
            st.dataframe(casos_df, use_container_width=True)
            eliminar = st.number_input("ID de caso a eliminar", min_value=0, step=1)
            if st.button("Eliminar caso"):
                delete_caso(conn, eliminar)
                show_success("Caso eliminado.")

    # ------------------ TAB ABONOS ------------------
    with tab_abonos:
        st.subheader("💰 Registro de Abonos")
        casos = fetch_casos(conn)
        if casos.empty:
            st.info("Primero debes registrar al menos un caso.")
        else:
            with st.form("nuevo_abono_form"):
                caso = st.selectbox("Selecciona Caso", casos["descripcion"])
                fecha = st.date_input("Fecha", value=date.today())
                monto = st.number_input("Monto", min_value=0.0)
                observaciones = st.text_area("Observaciones")
                if st.form_submit_button("Agregar Abono"):
                    caso_id = casos.loc[casos["descripcion"] == caso, "id"].iloc[0]
                    add_abono(conn, fecha.isoformat(), monto, caso_id, observaciones)
                    show_success("Abono agregado correctamente.")

            abonos_df = fetch_abonos(conn)
            if not abonos_df.empty:
                st.dataframe(abonos_df, use_container_width=True)

    # ------------------ TAB RESUMEN ------------------
    with tab_resumen:
        st.subheader("📊 Resumen por Caso")
        resumen_df = resumen_por_caso(conn)
        if not resumen_df.empty:
            resumen_df["valor_acordado"] = resumen_df["valor_acordado"].apply(money)
            resumen_df["total_abonado"] = resumen_df["total_abonado"].apply(money)
            resumen_df["saldo_pendiente"] = resumen_df["saldo_pendiente"].apply(money)
            st.dataframe(resumen_df, use_container_width=True)
        else:
            st.info("No hay datos disponibles.")

    # ------------------ TAB REPORTES ------------------
    with tab_reportes:
        st.subheader("📑 Exportar Reportes")
        df = resumen_por_caso(conn)
        if not df.empty:
            st.download_button("⬇️ Exportar a CSV", data=to_csv_bytes(df), file_name="resumen_abonos.csv", mime="text/csv")
            st.download_button("⬇️ Exportar a Excel", data=to_excel_bytes(df), file_name="resumen_abonos.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        else:
            st.info("No hay datos para exportar.")

if __name__ == "__main__":
    main()
