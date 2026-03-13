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
load_dotenv()

# Credenciales de Alegra
try:
    email = st.secrets["ALEGRA_EMAIL"]
    token = st.secrets["ALEGRA_TOKEN"]
except (AttributeError, KeyError, Exception):
    email = os.getenv('ALEGRA_EMAIL')
    token = os.getenv('ALEGRA_TOKEN')

if not email or not token:
    st.error("Error de configuración: credenciales de Alegra no encontradas.")
    st.stop()

# Credenciales de Supabase
try:
    supabase_url = st.secrets["SUPABASE_URL"]
    supabase_key = st.secrets["SUPABASE_KEY"]
except (AttributeError, KeyError, Exception):
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_KEY')

if not supabase_url or not supabase_key:
    st.error("Error de configuración: credenciales de Supabase no encontradas.")
    st.stop()

supabase: Client = create_client(supabase_url, supabase_key)

# -------------------- FUNCIONES DE BASE DE DATOS --------------------
def init_db():
    """Verifica que las tablas existan y crea usuario admin si no existe."""
    response = supabase.table('usuarios').select('*').eq('username', 'gruporevi').execute()
    if not response.data:
        password_hash = bcrypt.hashpw('revigrupo1'.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        supabase.table('usuarios').insert({
            'username': 'gruporevi',
            'password_hash': password_hash,
            'nombre_completo': 'Administrador',
            'rol': 'admin'
        }).execute()

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
                   validacion_b02_ok=None, validacion_b01_ok=None, validacion_b01_inconsistencias=None):
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
        'validacion_b01_inconsistencias': validacion_b01_inconsistencias
    }
    supabase.table('cuadres').insert(data).execute()

def calcular_total_retirado(fecha_inicio, fecha_fin, sucursal=None):
    query = supabase.table('cuadres').select('billetes_retirados', 'fecha', 'sucursal').gte('fecha', fecha_inicio).lte('fecha', fecha_fin)
    if sucursal and sucursal != "Todas":
        query = query.eq('sucursal', sucursal)
    response = query.execute()
    total = 0
    for row in response.data:
        if row['billetes_retirados']:
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
    st.subheader("📜 Historial de Cuadres")
    es_admin = st.session_state['usuario_rol'] == 'admin'

    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        fecha_inicio = st.date_input("Fecha inicio", value=None, key="hist_fecha_ini")
    with col_f2:
        fecha_fin = st.date_input("Fecha fin", value=None, key="hist_fecha_fin")
    with col_f3:
        sucursales = ["Todas"] + ["LA ROMANA", "SD", "BAVARO"]
        sucursal_filtro = st.selectbox("Sucursal", sucursales, key="hist_sucursal")

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

    df_mostrar = df_hist[['id', 'fecha', 'sucursal', 'turno', 'usuario', 'total_facturas', 'diferencia', 'aceptable']].copy()
    df_mostrar.columns = ['ID', 'Fecha', 'Sucursal', 'Turno', 'Usuario', 'Ventas', 'Diferencia', 'Estado']

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
                if fila['billetes_retirados']:
                    billetes = json.loads(fila['billetes_retirados'])
                    st.write(f"**Billetes retirados:** {', '.join(f'{v} de {k}' for k, v in billetes.items())}")
                                # Mostrar validaciones fiscales si existen
            if fila.get('validacion_b02_ok') is not None:
                st.markdown("---")
                st.subheader("🧾 Validaciones fiscales del cuadre")
                col_v1, col_v2 = st.columns(2)
                with col_v1:
                    if fila['validacion_b02_ok']:
                        st.success("✅ Relación tarjetas vs B02 correcta")
                    else:
                        st.error("❌ Relación tarjetas vs B02 incorrecta")
                with col_v2:
                    if fila['validacion_b01_ok']:
                        st.success("✅ Secuencia B01 correcta")
                    else:
                        st.error("❌ Secuencia B01 con inconsistencias")
                        if fila.get('validacion_b01_inconsistencias'):
                            incons = json.loads(fila['validacion_b01_inconsistencias'])
                            for inc in incons[:5]:
                                st.warning(inc)

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

    if es_admin:
        st.markdown("---")
        with st.expander("💰 Administración - Totales y exportación", expanded=False):
            col_filtros = st.columns(3)
            with col_filtros[0]:
                fecha_ini = st.date_input("Fecha inicio", value=date.today().replace(day=1), key="admin_fecha_ini")
            with col_filtros[1]:
                fecha_fin = st.date_input("Fecha fin", value=date.today(), key="admin_fecha_fin")
            with col_filtros[2]:
                suc_sel = st.selectbox("Sucursal", ["Todas"] + ["LA ROMANA", "SD", "BAVARO"], key="admin_suc")

            col_tot1, col_tot2 = st.columns(2)
            with col_tot1:
                st.subheader("💰 Total retirado")
                if st.button("Calcular retirado", key="calc_retirado"):
                    total_ret = calcular_total_retirado(
                        fecha_ini.strftime("%Y-%m-%d"),
                        fecha_fin.strftime("%Y-%m-%d"),
                        suc_sel if suc_sel != "Todas" else None
                    )
                    st.success(f"RD$ {total_ret:,.2f}")
            with col_tot2:
                st.subheader("💸 Total gastos")
                if st.button("Calcular gastos", key="calc_gastos"):
                    total_gas = calcular_total_gastos(
                        fecha_ini.strftime("%Y-%m-%d"),
                        fecha_fin.strftime("%Y-%m-%d"),
                        suc_sel if suc_sel != "Todas" else None
                    )
                    st.success(f"RD$ {total_gas:,.2f}")

            st.markdown("---")
            st.subheader("📥 Exportar historial completo")
            cols_export = ['id', 'fecha', 'sucursal', 'turno', 'usuario', 'fondo_inicial',
                           'total_gastos', 'total_pagos_atrasados', 'ventas_efectivo',
                           'ventas_tarjeta', 'ventas_transferencia', 'ventas_credito',
                           'total_facturas', 'efectivo_esperado', 'efectivo_real',
                           'diferencia', 'cuadre_aceptable', 'billetes_retirados']
            df_export = df_hist[cols_export].copy()
            df_export['cuadre_aceptable'] = df_export['cuadre_aceptable'].apply(lambda x: 'Sí' if x else 'No')
            df_export['billetes_retirados'] = df_export['billetes_retirados'].apply(
                lambda x: ', '.join(f"{v} de {k}" for k, v in json.loads(x).items()) if x else ''
            )
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df_export.to_excel(writer, index=False, sheet_name='Detalle de cuadres')
            st.download_button(
                label="📥 Descargar Excel (detalle completo)",
                data=buffer.getvalue(),
                file_name=f"cuadres_detalle_{date.today()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

# -------------------- INTERFAZ PRINCIPAL --------------------
def main_app():
    st.set_page_config(page_title="Cuadre Revi", page_icon="🧾", layout="wide")

    with st.sidebar:
        st.header(f"👤 Usuario: {st.session_state['usuario_nombre']}")
        st.caption(f"Rol: {st.session_state['usuario_rol']}")
        if st.button("Cerrar sesión"):
            st.session_state.clear()
            st.rerun()
        st.markdown("---")
        modo = st.radio("Seleccionar modo", ["📋 Nuevo cuadre", "📜 Historial"])
        st.markdown("---")
        st.header("🔐 Estado de conexión")
        st.success("✅ Conectado a Alegra y Supabase")
        st.caption(f"Email: {email[:3]}...{email[-5:] if len(email) > 8 else ''}")
        if st.session_state['usuario_rol'] == 'admin':
            with st.expander("⚙️ Administrar usuarios", expanded=False):
                admin_panel_usuarios()

    if modo == "📋 Nuevo cuadre":
        st.title("🧾 Nuevo Cuadre de Caja - REVI")
        st.markdown("---")

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("📅 Datos del cuadre")
            fecha = st.date_input("Fecha", value=date.today())
            sucursal = st.selectbox("Sucursal", ["LA ROMANA", "SD", "BAVARO"])
            turno = st.selectbox("Turno", ["Mañana", "Tarde", "Completo (único)"])

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
            st.subheader("💰 Conteo de efectivo")
            st.caption("Ingresa la cantidad de billetes y monedas")
            denominaciones = [2000, 1000, 500, 200, 100, 50, 25, 10, 5, 1]
            conteo = {}
            cols_internas = st.columns(2)
            for i, denom in enumerate(denominaciones):
                with cols_internas[i % 2]:
                    conteo[denom] = st.number_input(
                        f"{denom}",
                        min_value=0,
                        step=1,
                        value=0,
                        key=f"denom_{denom}"
                    )

        with st.expander("➕ Gastos del turno"):
            gastos = []
            num_gastos = st.number_input("Número de gastos", min_value=0, step=1, value=0, key="num_gastos")
            for i in range(int(num_gastos)):
                col_g1, col_g2 = st.columns(2)
                with col_g1:
                    concepto = st.text_input(f"Concepto {i+1}", key=f"concepto_{i}")
                with col_g2:
                    monto = st.number_input(f"Monto {i+1}", min_value=0.0, format="%.2f", key=f"monto_gasto_{i}")
                if concepto and monto:
                    gastos.append({'concepto': concepto, 'monto': monto})

        with st.expander("🔄 Pagos atrasados (de otros días)"):
            pagos_atrasados = []
            num_pagos = st.number_input("Número de pagos atrasados", min_value=0, step=1, value=0, key="num_pagos")
            for i in range(int(num_pagos)):
                col_p1, col_p2 = st.columns(2)
                with col_p1:
                    ref = st.text_input(f"Referencia {i+1}", key=f"ref_{i}")
                with col_p2:
                    monto_p = st.number_input(f"Monto {i+1}", min_value=0.0, format="%.2f", key=f"monto_pago_{i}")
                if ref and monto_p:
                    pagos_atrasados.append({'referencia': ref, 'monto': monto_p})

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
                        totales_previos=totales_previos
                    )
                    resultados['fecha'] = fecha.strftime("%Y-%m-%d")

                    # Calcular validaciones fiscales
                    es_valido1, msg1, ventas_tarjeta, total_b02 = validar_relacion_tarjetas_b02(df, sucursal, resultados['tarjeta'])
                    es_valido2, msg2, inconsistencias = validar_secuencia_b01(df, fecha.strftime("%Y-%m-%d"))

                    # Guardar incluyendo validaciones
                    guardar_cuadre(resultados, sucursal, turno, st.session_state['usuario_id'],
                                   fondo_inicial, gastos, pagos_atrasados, conteo,
                                   validacion_b02_ok=es_valido1,
                                   validacion_b01_ok=es_valido2,
                                   validacion_b01_inconsistencias=json.dumps(inconsistencias) if inconsistencias else None)

                    st.balloons()
                    st.success("✅ Cuadre calculado y guardado en historial")

                    # Mostrar métricas principales
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

                    # Detalle de ventas
                    with st.expander("📊 Ver detalle de ventas por método de pago", expanded=False):
                        detalle = {
                            "Efectivo": f"RD$ {resultados['efectivo']:,.2f}",
                            "Tarjeta": f"RD$ {resultados['tarjeta']:,.2f}",
                            "Transferencia": f"RD$ {resultados['transferencia']:,.2f}",
                            "Crédito": f"RD$ {resultados['credito']:,.2f}"
                        }
                        st.json(detalle)

                    # Validaciones fiscales (usando variables ya calculadas)
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

                    # Sugerencia de retiro
                    if resultados['total_a_retirar'] > 0:
                        st.info(f"💰 Sugerencia de retiro para este turno: RD$ {resultados['total_a_retirar']:,.2f}")
                        desglose = ", ".join(f"{cant} de {denom}" for denom, cant in resultados['billetes_a_retirar'].items())
                        st.write(f"**Desglose:** {desglose}")

    else:
        mostrar_historial()

# -------------------- CONTROL DE SESIÓN --------------------
if 'autenticado' not in st.session_state:
    st.session_state['autenticado'] = False

init_db()

if not st.session_state['autenticado']:
    mostrar_login()
else:
    main_app()