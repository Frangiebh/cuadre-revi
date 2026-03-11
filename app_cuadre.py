# app_cuadre.py
import streamlit as st
from datetime import date
import os
from dotenv import load_dotenv
from cuadre_core import obtener_todas_facturas, facturas_a_dataframe, calcular_cuadre

# --- CARGA DE CREDENCIALES (Prioridad: st.secrets en la nube, .env en local) ---
load_dotenv()

try:
    email = st.secrets["ALEGRA_EMAIL"]
    token = st.secrets["ALEGRA_TOKEN"]
except (AttributeError, KeyError, Exception):
    email = os.getenv('ALEGRA_EMAIL')
    token = os.getenv('ALEGRA_TOKEN')

if not email or not token:
    st.error("""
    ⚠️ **Error de configuración**: No se encontraron las credenciales de Alegra.

    Si estás en **desarrollo local**, crea un archivo `.env` en la misma carpeta con:
    ALEGRA_EMAIL=tu_email
    ALEGRA_TOKEN=tu_token

    Si estás en **Streamlit Cloud**, configura los secrets en la sección "Settings" de tu app.
    """)
    st.stop()

# --- CONFIGURACIÓN DE LA PÁGINA ---
st.set_page_config(
    page_title="Cuadre de caja GRUPO REVI",
    page_icon="🧾",
    layout="wide"
)

st.title("🧾 Cuadre de caja GRUPO REVI")
st.markdown("---")

# --- BARRA LATERAL (solo info, sin credenciales) ---
with st.sidebar:
    st.header("🔐 Estado de conexión")
    st.success("✅ Conectado a Alegra")
    st.caption(f"Email: {email[:3]}...{email[-5:] if len(email) > 8 else ''}")

# --- ÁREA PRINCIPAL ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("📅 Datos del cuadre")
    fecha = st.date_input("Fecha", value=date.today())
    sucursal = st.selectbox("Sucursal", ["LA ROMANA", "SD", "BAVARO"])
    fondo_inicial = st.number_input("Fondo inicial (RD$)", min_value=0.0, step=100.0, format="%.2f")

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

# --- GASTOS Y PAGOS ATRASADOS ---
with st.expander("➕ Gastos del turno"):
    gastos = []
    num_gastos = st.number_input("Número de gastos", min_value=0, step=1, value=0)
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

# --- BOTÓN PARA CALCULAR ---
if st.button("🚀 Calcular cuadre", type="primary"):
    with st.spinner("Obteniendo facturas de Alegra y calculando..."):
        facturas = obtener_todas_facturas(email, token, fecha.strftime("%Y-%m-%d"))
        if not facturas:
            st.warning("No se encontraron facturas para esta fecha.")
        else:
            st.info(f"📄 Facturas obtenidas: {len(facturas)}")
            df = facturas_a_dataframe(facturas)
            resultados = calcular_cuadre(df, sucursal, fondo_inicial, gastos, pagos_atrasados, conteo)

            st.balloons()
            st.success("✅ Cuadre calculado con éxito")

            col_res1, col_res2, col_res3 = st.columns(3)
            col_res1.metric("Ventas totales (facturas únicas)", f"RD$ {resultados['total_facturas']:,.2f}")
            col_res2.metric("Total pagado (efectivo+tarjeta+transf)", f"RD$ {resultados['total_pagado']:,.2f}")
            col_res3.metric("Efectivo esperado", f"RD$ {resultados['efectivo_esperado']:,.2f}")

            col_res4, col_res5 = st.columns(2)
            col_res4.metric("Efectivo contado", f"RD$ {resultados['efectivo_real']:,.2f}")
            delta_color = "off" if resultados['cuadre_aceptable'] else "inverse"
            col_res5.metric("Diferencia", f"RD$ {resultados['diferencia']:,.2f}", delta_color=delta_color)

            if resultados['cuadre_aceptable']:
                st.success("✅ CUADRE ACEPTABLE (dentro del rango ±50)")
            else:
                st.error("❌ CUADRE FUERA DE RANGO - Revisar")

            with st.expander("📊 Ver detalle de ventas por método de pago"):
                detalle = {
                    "Efectivo": f"RD$ {resultados['efectivo']:,.2f}",
                    "Tarjeta": f"RD$ {resultados['tarjeta']:,.2f}",
                    "Transferencia": f"RD$ {resultados['transferencia']:,.2f}",
                    "Crédito": f"RD$ {resultados['credito']:,.2f}"
                }
                st.json(detalle)

            if resultados['total_a_retirar'] > 0:
                st.info(f"💰 Sugerencia de retiro: RD$ {resultados['total_a_retirar']:,.2f}")
                desglose = ", ".join(f"{cant} de {denom}" for denom, cant in resultados['billetes_a_retirar'].items())
                st.write(f"**Desglose:** {desglose}")