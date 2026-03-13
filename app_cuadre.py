# app_cuadre.py
import streamlit as st
from datetime import date, datetime
import os
import json
import bcrypt
import pandas as pd
from io import BytesIO
from dotenv import load_dotenv
from supabase import create_client, Client
from cuadre_core import (
    obtener_todas_facturas,
    facturas_a_dataframe,
    calcular_cuadre,
    validar_relacion_tarjetas_b02,
    validar_secuencia_b01
)

# -------------------- CONFIGURACIÓN INICIAL --------------------
st.set_page_config(page_title="Cuadre Revi", page_icon="🧾", layout="wide")

load_dotenv()

# Credenciales de Alegra
try:
    email = st.secrets["ALEGRA_EMAIL"]
    token = st.secrets["ALEGRA_TOKEN"]
except (AttributeError, KeyError, Exception):
    email = os.getenv('ALEGRA_EMAIL')
    token = os.getenv('ALEGRA_TOKEN')

if not email or not token:
    st.error("❌ Credenciales de Alegra no encontradas.")
    st.stop()

# Credenciales de Supabase
try:
    supabase_url = st.secrets["SUPABASE_URL"]
    supabase_key = st.secrets["SUPABASE_KEY"]
except (AttributeError, KeyError, Exception):
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_KEY')

if not supabase_url or not supabase_key:
    st.error("❌ Credenciales de Supabase no encontradas.")
    st.stop()

# Crear cliente de Supabase
try:
    supabase: Client = create_client(supabase_url, supabase_key)
except Exception as e:
    st.error(f"❌ Error al conectar con Supabase: {e}")
    st.stop()

# -------------------- FUNCIONES DE BASE DE DATOS --------------------
def init_db():
    """Verifica que las tablas existan y crea usuario admin si no existe."""
    try:
        response = supabase.table('usuarios').select('*').eq('username', 'gruporevi').execute()
        if not response.data:
            password_hash = bcrypt.hashpw('revigrupo1'.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
            supabase.table('usuarios').insert({
                'username': 'gruporevi',
                'password_hash': password_hash,
                'nombre_completo': 'Administrador',
                'rol': 'admin'
            }).execute()
    except Exception as e:
        st.error(f"❌ Error en init_db: {e}")
        st.stop()

def verificar_login(username, password):
    username_lower = username.lower()
    response = supabase.table('usuarios').select('*').eq('username', username_lower).eq('activo', True).execute()
    if not response.data:
        return None
    user = response.data[0]
    if user.get('password_hash'):
        if bcrypt.checkpw(password.encode('utf-8'), user['password_hash'].encode('utf-8')):
            return {'id': user['id'], 'username': user['username'], 'nombre': user['nombre_completo'], 'rol': user['rol']}
    elif user.get('pin_hash'):
        if bcrypt.checkpw(password.encode('utf-8'), user['pin_hash'].encode('utf-8')):
            return {'id': user['id'], 'username': user['username'], 'nombre': user['nombre_completo'], 'rol': user['rol']}
    return None

def guardar_cuadre(resultados, sucursal, turno, usuario_id, fondo_inicial, gastos, pagos_atrasados, conteo_efectivo,
                   validacion_b02_ok=None, validacion_b01_ok=None, validacion_b01_inconsistencias=None,
                   retiro_manual=None):
    billetes_json = json.dumps(resultados['billetes_a_retirar'])
    data = {
        'fecha': resultados['fecha'],
        'sucursal': sucursal,
        'turno': turno,
        'usuario_id': usuario_id,
        'fondo_inicial': fondo_inicial,
        'total_gastos': resultados['total_gastos'],
        'total_pagos_atrasados': resultados['total_pagos_atrasados'],
        'ventas_efectivo': resultados['efectivo'],
        'ventas_tarjeta': resultados['tarjeta'],
        'ventas_transferencia': resultados['transferencia'],
        'ventas_credito': resultados['credito'],
        'total_facturas': resultados['total_facturas'],
        'efectivo_esperado': resultados['efectivo_esperado'],
        'efectivo_real': resultados['efectivo_real'],
        'diferencia': resultados['diferencia'],
        'cuadre_aceptable': int(resultados['cuadre_aceptable']),
        'billetes_retirados': billetes_json,
        'validacion_b02_ok': int(validacion_b02_ok) if validacion_b02_ok is not None else None,
        'validacion_b01_ok': int(validacion_b01_ok) if validacion_b01_ok is not None else None,
        'validacion_b01_inconsistencias': validacion_b01_inconsistencias,
        'retiro_manual': retiro_manual
    }
    supabase.table('cuadres').insert(data).execute()

def calcular_total_retirado(fecha_inicio, fecha_fin, sucursal=None):
    query = supabase.table('cuadres').select('billetes_retirados', 'retiro_manual').gte('fecha', fecha_inicio).lte('fecha', fecha_fin)
    if sucursal and sucursal != "Todas":
        query = query.eq('sucursal', sucursal)
    response = query.execute()
    total = 0
    for row in response.data:
        if row['retiro_manual'] is not None:
            total += row['retiro_manual']
        elif row['billetes_retirados']:
            billetes = json.loads(row['billetes_retirados'])
            total += sum(int(denom) * cant for denom, cant in billetes.items())
    return total

def calcular_total_gastos(fecha_inicio, fecha_fin, sucursal=None):
    query = supabase.table('cuadres').select('total_gastos').gte('fecha', fecha_inicio).lte('fecha', fecha_fin)
    if sucursal and sucursal != "Todas":
        query = query.eq('sucursal', sucursal)
    response = query.execute()
    total = sum(row['total_gastos'] for row in response.data)
    return total

def obtener_ultimo_cuadre(sucursal, fecha=None):
    query = supabase.table('cuadres').select('*').eq('sucursal', sucursal)
    if fecha:
        query = query.lte('fecha', fecha)
    response = query.order('fecha', desc=True).order('timestamp', desc=True).limit(1).execute()
    if response.data:
        ultimo = response.data[0]
        # Determinar el total retirado
        if ultimo.get('retiro_manual') is not None:
            total_retirado = float(ultimo['retiro_manual'])
        else:
            billetes_json = ultimo.get('billetes_retirados')
            total_retirado = 0
            if billetes_json:
                billetes = json.loads(billetes_json)
                total_retirado = sum(int(denom) * cant for denom, cant in billetes.items())
        fondo_siguiente = ultimo['efectivo_real'] - total_retirado
        return {
            'existe': True,
            'fondo_siguiente': fondo_siguiente,
            'efectivo_real': ultimo['efectivo_real'],
            'diferencia': ultimo['diferencia'],
            'fecha': ultimo['fecha'],
            'turno': ultimo['turno']
        }
    else:
        return {'existe': False}

def obtener_totales_previos(sucursal, fecha, turno_actual):
    if turno_actual == "Tarde":
        response = supabase.table('cuadres') \
            .select('total_facturas, ventas_efectivo, ventas_tarjeta, ventas_transferencia, ventas_credito') \
            .eq('sucursal', sucursal) \
            .eq('fecha', fecha) \
            .eq('turno', 'Mañana') \
            .execute()
        if response.data:
            totales = {
                'total_facturas': sum(r['total_facturas'] for r in response.data),
                'efectivo': sum(r['ventas_efectivo'] for r in response.data),
                'tarjeta': sum(r['ventas_tarjeta'] for r in response.data),
                'transferencia': sum(r['ventas_transferencia'] for r in response.data),
                'credito': sum(r['ventas_credito'] for r in response.data)
            }
            return totales
    return None

def obtener_historial(fecha_inicio=None, fecha_fin=None, sucursal=None):
    query = supabase.table('cuadres').select('*, usuarios(nombre_completo)').order('timestamp', desc=True)
    if fecha_inicio:
        query = query.gte('fecha', fecha_inicio)
    if fecha_fin:
        query = query.lte('fecha', fecha_fin)
    if sucursal and sucursal != "Todas":
        query = query.eq('sucursal', sucursal)
    response = query.execute()
    return response.data

# -------------------- PÁGINA DE LOGIN --------------------
def mostrar_login():
    # Logo más grande a la izquierda del título
    col_logo, col_titulo = st.columns([1, 4])
    with col_logo:
        st.image("assets/logo_revi.png", width=150)  # Aumentado de 80 a 150
    with col_titulo:
        st.title("🔐 Iniciar Sesión - Cuadre Revi")
    
    with st.form("login_form"):
        username = st.text_input("Usuario")
        password = st.text_input("Contraseña o PIN", type="password")
        submitted = st.form_submit_button("Ingresar")
        if submitted:
            user = verificar_login(username, password)
            if user:
                st.session_state['autenticado'] = True
                st.session_state['usuario_id'] = user['id']
                st.session_state['usuario_nombre'] = user['nombre']
                st.session_state['usuario_rol'] = user['rol']
                st.rerun()
            else:
                st.error("Usuario o contraseña incorrectos")
    return False

# -------------------- PANEL DE ADMINISTRACIÓN --------------------
def admin_panel_usuarios():
    st.subheader("Crear nueva cajera")
    with st.form("new_user"):
        username = st.text_input("Nombre de usuario (único)")
        nombre_completo = st.text_input("Nombre completo")
        pin = st.text_input("PIN de 4 dígitos", type="password", max_chars=4)
        confirm_pin = st.text_input("Confirmar PIN", type="password", max_chars=4)
        submitted = st.form_submit_button("Crear usuario")
        if submitted:
            if len(pin) != 4 or not pin.isdigit():
                st.error("El PIN debe tener exactamente 4 dígitos numéricos")
            elif pin != confirm_pin:
                st.error("Los PIN no coinciden")
            else:
                pin_hash = bcrypt.hashpw(pin.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                try:
                    supabase.table('usuarios').insert({
                        'username': username.lower(),
                        'pin_hash': pin_hash,
                        'nombre_completo': nombre_completo,
                        'rol': 'cajera',
                        'activo': True
                    }).execute()
                    st.success(f"Usuario {username} creado correctamente")
                except Exception as e:
                    st.error(f"Error al crear usuario: {e}")

    st.markdown("---")
    st.subheader("Usuarios existentes")
    response = supabase.table('usuarios').select('id', 'username', 'nombre_completo', 'rol', 'activo').execute()
    if response.data:
        for user in response.data:
            col1, col2, col3, col4, col5 = st.columns([3, 3, 2, 1, 2])
            with col1:
                st.write(user['username'])
            with col2:
                st.write(user['nombre_completo'])
            with col3:
                st.write(user['rol'])
            with col4:
                estado = "✅" if user['activo'] else "❌"
                st.write(estado)
            with col5:
                if user['username'] != 'gruporevi' and user['username'] != st.session_state.get('username', ''):
                    if st.button("Desactivar" if user['activo'] else "Activar", key=f"toggle_{user['id']}"):
                        supabase.table('usuarios').update({'activo': not user['activo']}).eq('id', user['id']).execute()
                        st.rerun()
                else:
                    st.write("(admin)")

# -------------------- PÁGINA DE HISTORIAL --------------------
def mostrar_historial():
    st.subheader("📜 Historial")
    es_admin = st.session_state['usuario_rol'] == 'admin'

    # Filtros en un contenedor con borde
    with st.container(border=True):
        st.subheader("🔍 Filtros")
        col_f1, col_f2, col_f3 = st.columns(3)
        with col_f1:
            fecha_inicio = st.date_input("Fecha inicio", value=None, key="hist_fecha_ini")
        with col_f2:
            fecha_fin = st.date_input("Fecha fin", value=None, key="hist_fecha_fin")
        with col_f3:
            sucursales = ["Todas"] + ["LA ROMANA", "SD", "BAVARO"]
            sucursal_filtro = st.selectbox("Sucursal", sucursales, key="hist_sucursal")

    # Obtener datos
    historial = obtener_historial(
        fecha_inicio.strftime("%Y-%m-%d") if fecha_inicio else None,
        fecha_fin.strftime("%Y-%m-%d") if fecha_fin else None,
        sucursal_filtro
    )

    if not historial:
        st.info("No hay cuadres registrados con esos filtros.")
        return

    df_hist = pd.DataFrame(historial)
    df_hist['usuario'] = df_hist['usuarios'].apply(lambda x: x['nombre_completo'] if x else 'Desconocido')
    df_hist['aceptable'] = df_hist['cuadre_aceptable'].apply(lambda x: "✅" if x else "❌")
    df_hist['fecha'] = pd.to_datetime(df_hist['fecha']).dt.strftime('%Y-%m-%d')

    # Añadir columnas de validaciones fiscales
    df_hist['B02'] = df_hist['validacion_b02_ok'].apply(lambda x: "✅" if x else "❌" if x is not None else "")
    df_hist['B01'] = df_hist['validacion_b01_ok'].apply(lambda x: "✅" if x else "❌" if x is not None else "")

    # Preparar columnas para mostrar
    columnas_disponibles = ['id', 'fecha', 'sucursal', 'turno', 'usuario', 'total_facturas', 'diferencia']
    columnas_extra = ['B02', 'B01', 'aceptable']
    columnas_a_usar = [col for col in columnas_disponibles if col in df_hist.columns] + columnas_extra
    df_mostrar = df_hist[columnas_a_usar].copy()
    df_mostrar.columns = ['ID', 'Fecha', 'Sucursal', 'Turno', 'Usuario', 'Ventas', 'Diferencia', 'B02', 'B01', 'Estado']

    st.caption(f"Mostrando {len(df_hist)} cuadres")
    evento = st.dataframe(
        df_mostrar,
        use_container_width=True,
        hide_index=True,
        column_config={
            "ID": st.column_config.NumberColumn(format="%d"),
            "Ventas": st.column_config.NumberColumn(format="RD$ %.2f"),
            "Diferencia": st.column_config.NumberColumn(format="RD$ %.2f")
        },
        on_select="rerun",
        selection_mode="single-row"
    )

    # Si se selecciona una fila, mostrar detalles
    if evento.selection.rows:
        idx = evento.selection.rows[0]
        fila = df_hist.iloc[idx]
        with st.expander(f"📄 Detalles del cuadre #{fila['id']}", expanded=True):
            col1, col2 = st.columns(2)
            with col1:
                st.write(f"**Fecha:** {fila['fecha']}")
                st.write(f"**Sucursal:** {fila['sucursal']}")
                st.write(f"**Turno:** {fila['turno']}")
                st.write(f"**Usuario:** {fila['usuario']}")
                st.write(f"**Fondo inicial:** RD$ {fila['fondo_inicial']:,.2f}")
                st.write(f"**Gastos:** RD$ {fila['total_gastos']:,.2f}")
                st.write(f"**Pagos atrasados:** RD$ {fila['total_pagos_atrasados']:,.2f}")
            with col2:
                st.write(f"**Ventas efectivo:** RD$ {fila['ventas_efectivo']:,.2f}")
                st.write(f"**Ventas tarjeta:** RD$ {fila['ventas_tarjeta']:,.2f}")
                st.write(f"**Ventas transferencia:** RD$ {fila['ventas_transferencia']:,.2f}")
                st.write(f"**Ventas crédito:** RD$ {fila['ventas_credito']:,.2f}")
                st.write(f"**Total ventas:** RD$ {fila['total_facturas']:,.2f}")
                st.write(f"**Efectivo esperado:** RD$ {fila['efectivo_esperado']:,.2f}")
                st.write(f"**Efectivo contado:** RD$ {fila['efectivo_real']:,.2f}")
                st.write(f"**Diferencia:** RD$ {fila['diferencia']:,.2f}")
                st.write(f"**Aceptable:** {'✅' if fila['cuadre_aceptable'] else '❌'}")
                # Mostrar retiro (manual o por billetes)
                if fila.get('retiro_manual') is not None:
                    st.write(f"**Total retirado:** RD$ {fila['retiro_manual']:,.2f} (retiro manual)")
                elif fila['billetes_retirados']:
                    billetes = json.loads(fila['billetes_retirados'])
                    total_retirado = sum(int(denom) * cant for denom, cant in billetes.items())
                    st.write(f"**Billetes retirados:** {', '.join(f'{v} de {k}' for k, v in billetes.items())}")
                    st.write(f"**Total retirado:** RD$ {total_retirado:,.2f}")
                else:
                    st.write("**Total retirado:** RD$ 0")

            # Validaciones fiscales como texto plano
            if fila.get('validacion_b02_ok') is not None:
                st.markdown("---")
                st.write("**Validaciones fiscales:**")
                st.write(f"- **B02:** {'✅ Correcta' if fila['validacion_b02_ok'] else '❌ Incorrecta'}")
                st.write(f"- **B01:** {'✅ Correcta' if fila['validacion_b01_ok'] else '❌ Con inconsistencias'}")
                if not fila['validacion_b01_ok'] and fila.get('validacion_b01_inconsistencias'):
                    st.write("  **Inconsistencias:**")
                    incons = json.loads(fila['validacion_b01_inconsistencias'])
                    for inc in incons[:5]:
                        st.write(f"  - {inc}")

            if es_admin:
                st.markdown("---")
                st.warning("⚠️ Zona de administración: eliminar este cuadre")
                confirmar = st.checkbox(f"Confirmar eliminación del cuadre #{fila['id']}", key=f"confirm_{fila['id']}")
                if st.button(f"🗑️ Eliminar cuadre #{fila['id']}", key=f"del_{fila['id']}"):
                    if confirmar:
                        supabase.table('cuadres').delete().eq('id', fila['id']).execute()
                        st.success("Cuadre eliminado correctamente.")
                        st.rerun()
                    else:
                        st.error("Debes marcar la casilla de confirmación para eliminar.")

    # Sección de administración (solo para admin)
    if es_admin:
        st.markdown("---")
        with st.expander("💰 Retiros y Gastos en cuadres", expanded=False):
            col_filtros = st.columns(3)
            with col_filtros[0]:
                fecha_ini = st.date_input("Fecha inicio", value=date.today().replace(day=1), key="admin_fecha_ini")
            with col_filtros[1]:
                fecha_fin = st.date_input("Fecha fin", value=date.today(), key="admin_fecha_fin")
            with col_filtros[2]:
                suc_sel = st.selectbox("Sucursal", ["Todas"] + ["LA ROMANA", "SD", "BAVARO"], key="admin_suc")

            opcion = st.radio("Selecciona qué calcular", ["Total retirado", "Total gastos"], horizontal=True, key="admin_opcion")
            if st.button("Calcular", key="admin_calcular"):
                if opcion == "Total retirado":
                    total = calcular_total_retirado(
                        fecha_ini.strftime("%Y-%m-%d"),
                        fecha_fin.strftime("%Y-%m-%d"),
                        suc_sel if suc_sel != "Todas" else None
                    )
                    st.success(f"💰 Total retirado: RD$ {total:,.2f}")
                else:
                    total = calcular_total_gastos(
                        fecha_ini.strftime("%Y-%m-%d"),
                        fecha_fin.strftime("%Y-%m-%d"),
                        suc_sel if suc_sel != "Todas" else None
                    )
                    st.success(f"💸 Total gastos: RD$ {total:,.2f}")

        # Exportación con un solo botón
        st.markdown("---")
        if st.button("📥 Exportar historial completo a Excel", type="secondary", key="export_excel"):
            cols_export = ['id', 'fecha', 'sucursal', 'turno', 'usuario', 'fondo_inicial',
                           'total_gastos', 'total_pagos_atrasados', 'ventas_efectivo',
                           'ventas_tarjeta', 'ventas_transferencia', 'ventas_credito',
                           'total_facturas', 'efectivo_esperado', 'efectivo_real',
                           'diferencia', 'cuadre_aceptable', 'billetes_retirados', 'retiro_manual',
                           'validacion_b02_ok', 'validacion_b01_ok']
            # Filtrar solo las columnas que existen
            cols_existentes = [col for col in cols_export if col in df_hist.columns]
            df_export = df_hist[cols_existentes].copy()
            if 'cuadre_aceptable' in df_export.columns:
                df_export['cuadre_aceptable'] = df_export['cuadre_aceptable'].apply(lambda x: 'Sí' if x else 'No')
            if 'validacion_b02_ok' in df_export.columns:
                df_export['validacion_b02_ok'] = df_export['validacion_b02_ok'].apply(lambda x: 'Sí' if x else 'No' if x is not None else '')
            if 'validacion_b01_ok' in df_export.columns:
                df_export['validacion_b01_ok'] = df_export['validacion_b01_ok'].apply(lambda x: 'Sí' if x else 'No' if x is not None else '')
            if 'billetes_retirados' in df_export.columns:
                df_export['billetes_retirados'] = df_export['billetes_retirados'].apply(
                    lambda x: ', '.join(f"{v} de {k}" for k, v in json.loads(x).items()) if x else ''
                )
            if 'retiro_manual' in df_export.columns:
                df_export['retiro_manual'] = df_export['retiro_manual'].apply(lambda x: f"RD$ {x:,.2f}" if x else "")
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df_export.to_excel(writer, index=False, sheet_name='Detalle de cuadres')
            st.download_button(
                label="Descargar Excel",
                data=buffer.getvalue(),
                file_name=f"cuadres_detalle_{date.today()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

# -------------------- INTERFAZ PRINCIPAL --------------------
def main_app():
    # CSS personalizado
    st.markdown("""
    <style>
        .stNumberInput input {
            width: 80px;
            padding: 0.25rem;
        }
        .stNumberInput label {
            font-size: 0.8rem;
        }
        div.row-widget.stRadio > div {
            flex-direction: row;
            align-items: center;
        }
        div.row-widget.stRadio > div > label {
            margin-right: 10px;
            background-color: #f0f2f6;
            padding: 5px 15px;
            border-radius: 20px;
            font-size: 0.9rem;
        }
        div.row-widget.stRadio > div > label:hover {
            background-color: #e0e2e6;
        }
    </style>
    """, unsafe_allow_html=True)

    with st.sidebar:
        st.header(f"👤 Usuario: {st.session_state['usuario_nombre']}")
        st.caption(f"Rol: {st.session_state['usuario_rol']}")
        if st.button("Cerrar sesión"):
            st.session_state.clear()
            st.rerun()
        st.markdown("---")
        modo = st.radio("", ["📋 Nuevo cuadre", "📜 Historial"], label_visibility="collapsed")
        st.markdown("---")
        st.header("🔐 Estado de conexión")
        st.success("✅ Conectado a Alegra y Supabase")
        st.caption(f"Email: {email[:3]}...{email[-5:] if len(email) > 8 else ''}")
        if st.session_state['usuario_rol'] == 'admin':
            with st.expander("⚙️ Administrar usuarios", expanded=False):
                admin_panel_usuarios()

    # --- Contenido principal según el modo ---
    if modo == "📋 Nuevo cuadre":
        # Logo y título (fuera de la barra lateral)
        col_logo, col_titulo = st.columns([1, 3])
        with col_logo:
            st.image("assets/logo_revi.png", width=150)
        with col_titulo:
            st.markdown("<h1 style='text-align: left;'>Cuadre de caja<br><span style='font-size: 0.6em;'>GRUPO REVI</span></h1>", unsafe_allow_html=True)
        st.markdown("---")

        col1, col2 = st.columns(2)
        with col1:
            with st.container(border=True):
                st.subheader("📅 Datos del cuadre")
                fecha = st.date_input("Fecha", value=date.today())
                sucursal = st.selectbox("Sucursal", ["LA ROMANA", "SD", "BAVARO"])
                turno = st.selectbox("Turno", ["Mañana", "Tarde", "Completo (único)"])

            st.divider()

            # Fondo inicial
            ultimo = obtener_ultimo_cuadre(sucursal, fecha.strftime("%Y-%m-%d"))
            if 'fondo_input_val' not in st.session_state:
                st.session_state.fondo_input_val = 0.0

            fondo_sugerido = 0.0
            fecha_origen = ""
            turno_origen = ""
            hay_cuadre_anterior = ultimo['existe']
            if hay_cuadre_anterior:
                fondo_sugerido = float(ultimo['fondo_siguiente'])
                fecha_origen = ultimo['fecha']
                turno_origen = ultimo['turno']

            with st.container(border=True):
                st.subheader("💰 Fondo inicial")
                if turno == "Tarde" and hay_cuadre_anterior:
                    st.info(f"💰 Fondo inicial tomado del turno anterior ({fecha_origen} - {turno_origen}): RD$ {fondo_sugerido:,.2f}")
                    campo_deshabilitado = True
                    st.session_state.fondo_input_val = fondo_sugerido
                else:
                    if hay_cuadre_anterior:
                        usar_mismo = st.checkbox("Usar fondo del cuadre anterior", value=True)
                        if usar_mismo:
                            st.info(f"💰 Fondo inicial tomado del cuadre anterior ({fecha_origen} - {turno_origen}): RD$ {fondo_sugerido:,.2f}")
                            st.session_state.fondo_input_val = fondo_sugerido
                            campo_deshabilitado = True
                        else:
                            campo_deshabilitado = False
                    else:
                        campo_deshabilitado = False

                fondo_inicial = st.number_input(
                    "Fondo inicial (RD$)",
                    min_value=0.0,
                    step=100.0,
                    format="%.2f",
                    value=st.session_state.fondo_input_val,
                    disabled=campo_deshabilitado,
                    key="fondo_input"
                )
                if not campo_deshabilitado:
                    st.session_state.fondo_input_val = fondo_inicial

        with col2:
            with st.container(border=True):
                st.subheader("💰 Efectivo en caja")
                st.caption("Ingresa la cantidad de billetes y monedas")
                denominaciones = [2000, 1000, 500, 200, 100, 50, 25, 10, 5, 1]
                conteo = {}
                cols_internas = st.columns(2)
                for i, denom in enumerate(denominaciones):
                    if i < 5:
                        with cols_internas[0]:
                            conteo[denom] = st.number_input(
                                f"{denom}",
                                min_value=0,
                                step=1,
                                value=0,
                                key=f"denom_{denom}",
                                label_visibility="visible"
                            )
                    else:
                        with cols_internas[1]:
                            conteo[denom] = st.number_input(
                                f"{denom}",
                                min_value=0,
                                step=1,
                                value=0,
                                key=f"denom_{denom}",
                                label_visibility="visible"
                            )
                usar_manual = st.checkbox("Colocar retiro manualmente", value=False, key="usar_retiro_manual")
                if usar_manual:
                    retiro_manual = st.number_input("Monto total a retirar (RD$)", min_value=0.0, step=100.0, format="%.2f", key="retiro_manual_monto")
                else:
                    retiro_manual = None

        # Gastos y pagos atrasados dinámicos
        col_exp1, col_exp2 = st.columns(2)
        with col_exp1:
            with st.expander("➕ Gastos del turno", expanded=False):
                gastos = []
                if 'gastos_count' not in st.session_state:
                    st.session_state.gastos_count = 1
                # Mostrar filas de gastos
                for i in range(st.session_state.gastos_count):
                    cols = st.columns(2)
                    with cols[0]:
                        concepto = st.text_input(f"Concepto {i+1}", key=f"gasto_concepto_{i}")
                    with cols[1]:
                        monto = st.number_input(f"Monto {i+1}", min_value=0.0, format="%.2f", key=f"gasto_monto_{i}")
                    if concepto and monto > 0:
                        gastos.append({'concepto': concepto, 'monto': monto})
                # Si la última fila tiene datos, agregar una nueva fila vacía
                if i + 1 == st.session_state.gastos_count and concepto and monto > 0:
                    st.session_state.gastos_count += 1
                    st.rerun()

        with col_exp2:
            with st.expander("🔄 Pagos atrasados (facturas o con fechas anteriores)", expanded=False):
                pagos_atrasados = []
                if 'pagos_count' not in st.session_state:
                    st.session_state.pagos_count = 1
                for i in range(st.session_state.pagos_count):
                    cols = st.columns(2)
                    with cols[0]:
                        ref = st.text_input(f"Referencia {i+1}", key=f"pago_ref_{i}")
                    with cols[1]:
                        monto_p = st.number_input(f"Monto {i+1}", min_value=0.0, format="%.2f", key=f"pago_monto_{i}")
                    if ref and monto_p > 0:
                        pagos_atrasados.append({'referencia': ref, 'monto': monto_p})
                if i + 1 == st.session_state.pagos_count and ref and monto_p > 0:
                    st.session_state.pagos_count += 1
                    st.rerun()

        st.markdown("---")

        if st.button("🚀 Calcular cuadre", type="primary"):
            with st.spinner("Obteniendo facturas de Alegra y calculando..."):
                facturas = obtener_todas_facturas(email, token, fecha.strftime("%Y-%m-%d"))
                if not facturas:
                    st.warning("No se encontraron facturas para esta fecha.")
                else:
                    st.info(f"📄 Facturas obtenidas: {len(facturas)}")
                    df = facturas_a_dataframe(facturas)

                    totales_previos = None
                    if turno == "Tarde":
                        totales_previos = obtener_totales_previos(sucursal, fecha.strftime("%Y-%m-%d"), turno)
                        if totales_previos:
                            st.info(f"📊 Ventas de turno anterior: RD$ {totales_previos['total_facturas']:,.2f}")

                    resultados = calcular_cuadre(
                        df, sucursal, fondo_inicial, gastos, pagos_atrasados, conteo,
                        totales_previos=totales_previos,
                        retiro_manual=retiro_manual
                    )
                    
                    resultados['fecha'] = fecha.strftime("%Y-%m-%d")

                    es_valido1, msg1, ventas_tarjeta, total_b02 = validar_relacion_tarjetas_b02(df, sucursal, resultados['tarjeta'])
                    es_valido2, msg2, inconsistencias = validar_secuencia_b01(df, fecha.strftime("%Y-%m-%d"))

                    guardar_cuadre(resultados, sucursal, turno, st.session_state['usuario_id'],
                                   fondo_inicial, gastos, pagos_atrasados, conteo,
                                   validacion_b02_ok=es_valido1,
                                   validacion_b01_ok=es_valido2,
                                   validacion_b01_inconsistencias=json.dumps(inconsistencias) if inconsistencias else None,
                                   retiro_manual=retiro_manual)
                                   
                    st.balloons()
                    st.success("✅ Cuadre calculado y guardado en historial")

                    with st.container(border=True):
                        st.subheader("📊 Resultados del cuadre")
                        col_res1, col_res2, col_res3 = st.columns(3)
                        col_res1.metric("Ventas totales del turno", f"RD$ {resultados['total_facturas']:,.2f}")
                        col_res2.metric("Total pagado", f"RD$ {resultados['total_pagado']:,.2f}")
                        col_res3.metric("Efectivo esperado", f"RD$ {resultados['efectivo_esperado']:,.2f}")

                        col_res4, col_res5 = st.columns(2)
                        col_res4.metric("Efectivo contado", f"RD$ {resultados['efectivo_real']:,.2f}")
                        delta_color = "off" if resultados['cuadre_aceptable'] else "inverse"
                        col_res5.metric("Diferencia", f"RD$ {resultados['diferencia']:,.2f}", delta_color=delta_color)

                        if resultados['cuadre_aceptable']:
                            st.success("✅ CUADRE ACEPTABLE (dentro del rango ±50)")
                        else:
                            st.error("❌ CUADRE FUERA DE RANGO - Revisar")

                    with st.expander("📊 Ver detalle de ventas por método de pago", expanded=False):
                        detalle = {
                            "Efectivo": f"RD$ {resultados['efectivo']:,.2f}",
                            "Tarjeta": f"RD$ {resultados['tarjeta']:,.2f}",
                            "Transferencia": f"RD$ {resultados['transferencia']:,.2f}",
                            "Crédito": f"RD$ {resultados['credito']:,.2f}"
                        }
                        st.json(detalle)

                    with st.expander("🧾 Validaciones fiscales", expanded=False):
                        col_v1, col_v2 = st.columns(2)
                        with col_v1:
                            if es_valido1:
                                st.success(msg1)
                            else:
                                st.error(msg1)
                            st.caption(f"Ventas tarjeta: RD$ {ventas_tarjeta:,.2f} | Total B02: RD$ {total_b02:,.2f}")
                        with col_v2:
                            if es_valido2:
                                st.success(msg2)
                            else:
                                st.error(msg2)
                                if inconsistencias:
                                    for inc in inconsistencias[:5]:
                                        st.warning(inc)

                    if resultados['total_a_retirar'] > 0:
                        st.info(f"💰 Sugerencia de retiro para este turno: RD$ {resultados['total_a_retirar']:,.2f}")
                        if retiro_manual is not None:
                            st.caption("(Retiro manual aplicado)")
                        else:
                            desglose = ", ".join(f"{cant} de {denom}" for denom, cant in resultados['billetes_a_retirar'].items())
                            st.write(f"**Desglose:** {desglose}")

    else:  # Modo historial
        mostrar_historial()

# -------------------- CONTROL DE SESIÓN --------------------
if 'autenticado' not in st.session_state:
    st.session_state['autenticado'] = False

init_db()

if not st.session_state['autenticado']:
    mostrar_login()
else:
    main_app()