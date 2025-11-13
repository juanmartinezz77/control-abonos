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

def seed_example_data(conn):
    """Inserta datos de ejemplo si la base está vacía."""
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM casos")
    if c.fetchone()[0] == 0:
        ejemplos = [
            ("María López", "Divorcio y liquidación de bienes", 1500.00, "Inicio", "Caso prioritario"),
            ("Juan Pérez", "Defensa penal - audiencia preparatoria", 2500.00, "En curso", "Pago en cuotas"),
        ]
        c.executemany(
            "INSERT INTO casos (cliente, descripcion, valor_acordado, etapa, observaciones) VALUES (?,?,?,?,?)",
            ejemplos,
        )
        conn.commit()

    c.execute("SELECT COUNT(*) FROM abonos")
    if c.fetchone()[0] == 0:
        hoy = datetime.now().strftime("%Y-%m-%d")
        abonos = [
            (hoy, 500.00, 1, "Primer abono - María"),
            (hoy, 300.00, 1, "Segundo abono - María"),
            (hoy, 1000.00, 2, "Primer abono - Juan"),
        ]
        c.executemany(
            "INSERT INTO abonos (fecha, monto, caso_id, observaciones) VALUES (?,?,?,?)",
            abonos,
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
    seed_example_data(conn)

    tab_casos, tab_abonos, tab_resumen, tab_reportes = st.tabs(["Casos", "Abonos", "Resumen", "Reportes"])

    # ------------------ CASOS TAB ------------------
    with tab_casos:
        st.header("Gestión de Casos")
        col_left, col_right = st.columns([1, 1.2])

        with col_left:
            st.subheader("Agregar nuevo caso")
            with st.form("form_nuevo_caso", clear_on_submit=True):
                cliente = st.text_input("Cliente")
                descripcion = st.text_area("Descripción", height=120)
                valor_acordado = st.number_input("Valor acordado (USD)", min_value=0.0, format="%.2f")
                etapa = st.selectbox("Etapa", ["Inicio", "En curso", "Finalizado", "Archivo"])
                observaciones = st.text_area("Observaciones", height=80)
                if st.form_submit_button("Guardar caso"):
                    if not cliente.strip():
                        st.warning("El campo 'Cliente' es obligatorio.")
                    else:
                        try:
                            cid = add_caso(conn, cliente.strip(), descripcion.strip(), float(valor_acordado), etapa, observaciones.strip())
                            show_success(f"Caso creado con ID {cid}.")
                        except Exception as e:
                            show_error(f"Error al crear el caso: {e}")

        with col_right:
            st.subheader("Editar o eliminar casos")
            df_casos = fetch_casos(conn)
            if df_casos.empty:
                st.info("No hay casos registrados.")
            else:
                casos_map = {f"{r['id']} — {r['cliente']} ({r['etapa']})": r['id'] for _, r in df_casos.iterrows()}
                seleccionado = st.selectbox("Seleccionar caso", list(casos_map.keys()))
                caso_id = casos_map[seleccionado]
                casor = conn.execute("SELECT * FROM casos WHERE id = ?", (caso_id,)).fetchone()
                if casor:
                    with st.form("form_editar_caso"):
                        e_cliente = st.text_input("Cliente", casor["cliente"])
                        e_descripcion = st.text_area("Descripción", casor["descripcion"] or "", height=120)
                        e_valor = st.number_input("Valor acordado (USD)", min_value=0.0, format="%.2f", value=float(casor["valor_acordado"]))
                        e_etapa = st.selectbox("Etapa", ["Inicio", "En curso", "Finalizado", "Archivo"], index=["Inicio","En curso","Finalizado","Archivo"].index(casor["etapa"]))
                        e_observ = st.text_area("Observaciones", casor["observaciones"] or "", height=80)
                        col_a, col_b = st.columns(2)
                        with col_a:
                            if st.form_submit_button("Guardar cambios"):
                                try:
                                    edit_caso(conn, caso_id, e_cliente.strip(), e_descripcion.strip(), float(e_valor), e_etapa, e_observ.strip())
                                    show_success("Caso actualizado correctamente.")
                                except Exception as e:
                                    show_error(f"Error al actualizar: {e}")
                        with col_b:
                            if st.form_submit_button("Eliminar caso"):
                                delete_caso(conn, caso_id)
                                show_success(f"Caso {caso_id} y sus abonos eliminados.")

    # ------------------ ABONOS TAB ------------------
    with tab_abonos:
        st.header("Registrar y administrar abonos")
        col_a, col_b = st.columns([1, 1.4])
        with col_a:
            st.subheader("Registrar nuevo abono")
            casos_df = fetch_casos(conn)
            if casos_df.empty:
                st.info("No hay casos para asociar. Crea un caso primero.")
            else:
                with st.form("form_nuevo_abono", clear_on_submit=True):
                    f_fecha = st.date_input("Fecha", value=date.today())
                    f_monto = st.number_input("Monto (USD)", min_value=0.01, format="%.2f")
                    opciones = {f"{r['id']} — {r['cliente']}": r['id'] for _, r in casos_df.iterrows()}
                    f_caso_sel = st.selectbox("Caso asociado", list(opciones.keys()))
                    f_observ = st.text_area("Observaciones", height=80)
                    if st.form_submit_button("Registrar abono"):
                        try:
                            add_abono(conn, f_fecha.strftime("%Y-%m-%d"), float(f_monto), opciones[f_caso_sel], f_observ.strip())
                            show_success("Abono registrado correctamente.")
                        except Exception as e:
                            show_error(f"Error al registrar abono: {e}")

        with col_b:
            st.subheader("Últimos abonos (editar / eliminar)")
            df_abonos = fetch_abonos(conn)
            if df_abonos.empty:
                st.info("Aún no hay abonos registrados.")
            else:
                df_abonos["fecha"] = pd.to_datetime(df_abonos["fecha"]).dt.date
                df_abonos["label"] = df_abonos.apply(lambda r: f'{r["id"]} — {r["cliente"]} — {r["fecha"]} — {money(r["monto"])}', axis=1)
                ab_map = {r["label"]: int(r["id"]) for _, r in df_abonos.iterrows()}
                sel_ab_label = st.selectbox("Seleccionar abono", list(ab_map.keys()))
                ab_id = ab_map[sel_ab_label]
                row = conn.execute("SELECT * FROM abonos WHERE id = ?", (ab_id,)).fetchone()
                if row:
                    with st.form("form_editar_abono"):
                        a_fecha = st.date_input("Fecha", value=pd.to_datetime(row["fecha"]).date())
                        a_monto = st.number_input("Monto (USD)", min_value=0.01, format="%.2f", value=float(row["monto"]))
                        casos_for_ab = fetch_casos(conn)
                        options_ab = {f"{r['id']} — {r['cliente']}": r['id'] for _, r in casos_for_ab.iterrows()}
                        current_label = [k for k,v in options_ab.items() if v == row["caso_id"]]
                        a_caso_sel = st.selectbox("Caso asociado", list(options_ab.keys()), index=list(options_ab.keys()).index(current_label[0]) if current_label else 0)
                        a_observ = st.text_area("Observaciones", row["observaciones"] or "", height=80)
                        c1, c2 = st.columns(2)
                        with c1:
                            if st.form_submit_button("Guardar cambios"):
                                try:
                                    edit_abono(conn, ab_id, a_fecha.strftime("%Y-%m-%d"), float(a_monto), options_ab[a_caso_sel], a_observ.strip())
                                    show_success("Abono actualizado.")
                                except Exception as e:
                                    show_error(f"Error actualizando abono: {e}")
                        with c2:
                            if st.form_submit_button("Eliminar abono"):
                                delete_abono(conn, ab_id)
                                show_success(f"Abono {ab_id} eliminado.")

    # ------------------ RESUMEN TAB ------------------
    with tab_resumen:
        st.header("Resumen financiero por caso")
        colf1, colf2, colf3 = st.columns([2,2,1])
        cliente_filter = colf1.selectbox("Filtrar por cliente", [""] + [r[0] for r in conn.execute("SELECT DISTINCT cliente FROM casos").fetchall()])
        etapa_filter = colf2.selectbox("Filtrar por etapa", [""] + [r[0] for r in conn.execute("SELECT DISTINCT etapa FROM casos").fetchall()])
        incluir_finalizados = colf3.checkbox("Incluir finalizados/archivo", value=True)

        df_resumen = resumen_por_caso(conn, cliente_filter or None, etapa_filter or None)
        if not incluir_finalizados:
            df_resumen = df_resumen[~df_resumen["etapa"].isin(["Finalizado","Archivo"])]

        total_valor = df_resumen["valor_acordado"].sum()
        total_abonado = df_resumen["total_abonado"].sum()
        total_saldo = df_resumen["saldo_pendiente"].sum()

        m1, m2, m3 = st.columns(3)
        m1.metric("Valor total acordado", money(total_valor))
        m2.metric("Total abonado", money(total_abonado))
        m3.metric("Saldo pendiente", money(total_saldo))

        st.dataframe(df_resumen, use_container_width=True)
        csv_bytes = to_csv_bytes(df_resumen)
        excel_bytes = to_excel_bytes(df_resumen)

        st.download_button("📄 Descargar CSV", csv_bytes, "resumen_casos.csv", "text/csv")
        st.download_button("📘 Descargar Excel", excel_bytes, "resumen_casos.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # ------------------ REPORTES ------------------
    with tab_reportes:
        st.header("Reportes generales")
        total_casos = conn.execute("SELECT COUNT(*) FROM casos").fetchone()[0]
        total_abonos = conn.execute("SELECT COUNT(*) FROM abonos").fetchone()[0]
        total_monto_abonos = conn.execute("SELECT SUM(monto) FROM abonos").fetchone()[0] or 0
        st.markdown(f"""
        **Casos registrados:** {total_casos}  
        **Abonos registrados:** {total_abonos}  
        **Monto total abonado:** {money(total_monto_abonos)}
        """)
        st.info("Este módulo mostrará en el futuro gráficos e informes comparativos.")

if __name__ == "__main__":
    main()
