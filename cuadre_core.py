import requests
import pandas as pd
import time
import concurrent.futures

# ------------------------------------------------------------
# 1. OBTENER TODAS LAS FACTURAS DE UNA FECHA (EN PARALELO CON REINTENTOS)
# ------------------------------------------------------------
def obtener_todas_facturas(email, token, fecha, max_paginas=100, timeout=15, max_reintentos=10):
    url = "https://api.alegra.com/api/v1/invoices"
    primera_pagina = None
    for intento in range(max_reintentos):
        try:
            params = {
                "date": fecha,
                "start": 0,
                "limit": 30,
                "order_field": "id",
                "order_direction": "ASC"
            }
            response = requests.get(url, auth=(email, token), params=params, timeout=timeout)
            if response.status_code == 200:
                primera_pagina = response.json()
                break
            elif response.status_code == 503:
                print(f"⚠️ Servidor no disponible (página 1). Reintentando en {2**intento} seg...")
                time.sleep(2 ** intento)
            else:
                print(f"Error en primera página: {response.status_code}")
                return []
        except Exception as e:
            print(f"Excepción: {e}. Reintentando...")
            time.sleep(2 ** intento)

    if not primera_pagina:
        return []

    facturas_totales = list(primera_pagina)
    if len(primera_pagina) < 30:
        return facturas_totales

    paginas_a_solicitar = list(range(1, max_paginas))

    def obtener_pagina(pagina):
        start = pagina * 30
        for intento in range(max_reintentos):
            try:
                params = {
                    "date": fecha,
                    "start": start,
                    "limit": 30,
                    "order_field": "id",
                    "order_direction": "ASC"
                }
                response = requests.get(url, auth=(email, token), params=params, timeout=timeout)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 503:
                    print(f"⚠️ Página {pagina+1} no disponible, reintento {intento+1}")
                    time.sleep(2 ** intento)
                else:
                    return []
            except Exception:
                time.sleep(2 ** intento)
        return []

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {}
        for p in paginas_a_solicitar:
            time.sleep(0.2)
            futures[executor.submit(obtener_pagina, p)] = p
        for future in concurrent.futures.as_completed(futures):
            resultado = future.result()
            if resultado:
                facturas_totales.extend(resultado)

    print(f"📅 Total facturas obtenidas: {len(facturas_totales)}")
    return facturas_totales

# ------------------------------------------------------------
# 2. CONVERTIR FACTURAS A DATAFRAME (CON MÉTODOS DE PAGO Y NCF)
# ------------------------------------------------------------
def facturas_a_dataframe(facturas):
    filas = []
    metodo_map = {
        'cash': 'Efectivo',
        'credit-card': 'Tarjeta',
        'debit-card': 'Tarjeta',
        'transfer': 'Transferencia',
        'check': 'Cheque',
        'credit': 'Crédito'
    }

    for f in facturas:
        factura_id = f['id']
        fecha = f.get('date')
        fecha_hora = f.get('datetime', fecha)
        total = f.get('total', 0)
        estado = f.get('status')
        sucursal = f.get('warehouse', {}).get('name', 'Sin sucursal') if f.get('warehouse') else 'Sin sucursal'

        numero_comprobante = ''
        if f.get('numberTemplate'):
            numero_comprobante = f['numberTemplate'].get('fullNumber', '')

        if 'payments' in f and f['payments']:
            for pago in f['payments']:
                metodo_codigo = pago.get('paymentMethod', 'Desconocido')
                metodo = metodo_map.get(metodo_codigo, metodo_codigo)
                monto_pago = pago.get('amount', 0)
                filas.append({
                    'id_factura': factura_id,
                    'fecha': fecha,
                    'fecha_hora': fecha_hora,
                    'total_factura': total,
                    'metodo_pago': metodo,
                    'monto_pago': monto_pago,
                    'estado': estado,
                    'sucursal': sucursal,
                    'numero_comprobante': numero_comprobante
                })
        else:
            filas.append({
                'id_factura': factura_id,
                'fecha': fecha,
                'fecha_hora': fecha_hora,
                'total_factura': total,
                'metodo_pago': 'Crédito',
                'monto_pago': 0,
                'estado': estado,
                'sucursal': sucursal,
                'numero_comprobante': numero_comprobante
            })
    return pd.DataFrame(filas)

# ------------------------------------------------------------
# 3. CÁLCULO DE TOTALES DEL DÍA (INTERNO)
# ------------------------------------------------------------
def _calcular_totales_dia(df, sucursal):
    df_suc = df[df['sucursal'] == sucursal].copy()
    total_facturas = df_suc.groupby('id_factura')['total_factura'].first().sum()
    ventas_efectivo = df_suc[df_suc['metodo_pago'] == 'Efectivo']['monto_pago'].sum()
    df_suc['metodo_pago_simple'] = df_suc['metodo_pago'].apply(
        lambda x: 'Tarjeta' if 'Tarjeta' in str(x) else x
    )
    ventas_tarjeta = df_suc[df_suc['metodo_pago_simple'] == 'Tarjeta']['monto_pago'].sum()
    ventas_transferencia = df_suc[df_suc['metodo_pago'] == 'Transferencia']['monto_pago'].sum()
    ventas_credito = df_suc[df_suc['metodo_pago'] == 'Crédito']['total_factura'].sum()
    return {
        'total_facturas': total_facturas,
        'efectivo': ventas_efectivo,
        'tarjeta': ventas_tarjeta,
        'transferencia': ventas_transferencia,
        'credito': ventas_credito
    }

# ------------------------------------------------------------
# 4. CÁLCULO DEL CUADRE (VERSIÓN CON TURNOS)
# ------------------------------------------------------------
def calcular_cuadre(df, sucursal, fondo_inicial, gastos, pagos_atrasados, conteo_efectivo, totales_previos=None):
    totales_dia = _calcular_totales_dia(df, sucursal)
    if totales_previos:
        totales_turno = {
            'total_facturas': totales_dia['total_facturas'] - totales_previos.get('total_facturas', 0),
            'efectivo': totales_dia['efectivo'] - totales_previos.get('efectivo', 0),
            'tarjeta': totales_dia['tarjeta'] - totales_previos.get('tarjeta', 0),
            'transferencia': totales_dia['transferencia'] - totales_previos.get('transferencia', 0),
            'credito': totales_dia['credito'] - totales_previos.get('credito', 0)
        }
    else:
        totales_turno = totales_dia
    for k in totales_turno:
        totales_turno[k] = max(0, totales_turno[k])

    total_gastos = sum(g['monto'] for g in gastos)
    total_pagos_atrasados = sum(p['monto'] for p in pagos_atrasados)
    efectivo_esperado = totales_turno['efectivo'] + fondo_inicial - total_gastos + total_pagos_atrasados
    efectivo_real = sum(denom * cant for denom, cant in conteo_efectivo.items())
    diferencia = efectivo_real - efectivo_esperado
    cuadre_aceptable = -50 <= diferencia <= 50
    total_pagado = totales_turno['efectivo'] + totales_turno['tarjeta'] + totales_turno['transferencia']
    denominaciones_grandes = [2000, 1000, 500, 200]
    billetes_a_retirar = {d: conteo_efectivo.get(d, 0) for d in denominaciones_grandes if conteo_efectivo.get(d, 0) > 0}
    total_a_retirar = sum(d * cant for d, cant in billetes_a_retirar.items())
    return {
        'total_facturas': totales_turno['total_facturas'],
        'total_pagado': total_pagado,
        'efectivo': totales_turno['efectivo'],
        'tarjeta': totales_turno['tarjeta'],
        'transferencia': totales_turno['transferencia'],
        'credito': totales_turno['credito'],
        'fondo_inicial': fondo_inicial,
        'total_gastos': total_gastos,
        'total_pagos_atrasados': total_pagos_atrasados,
        'efectivo_esperado': efectivo_esperado,
        'efectivo_real': efectivo_real,
        'diferencia': diferencia,
        'cuadre_aceptable': cuadre_aceptable,
        'billetes_a_retirar': billetes_a_retirar,
        'total_a_retirar': total_a_retirar
    }

# ------------------------------------------------------------
# 5. VALIDACIONES FISCALES (NCF)
# ------------------------------------------------------------
def validar_relacion_tarjetas_b02(df, sucursal, ventas_tarjeta):
    """
    Valida que el total de ventas con tarjeta de una sucursal sea <= total de facturas B02 de esa misma sucursal.
    """
    if 'numero_comprobante' not in df.columns:
        return False, "❌ No hay datos de comprobante", 0, 0
    # Filtrar por la sucursal actual
    df_suc = df[df['sucursal'] == sucursal]
    df_b02 = df_suc[df_suc['numero_comprobante'].str.startswith('B02', na=False)]
    total_b02 = df_b02.groupby('id_factura')['total_factura'].first().sum()
    es_valido = ventas_tarjeta <= total_b02 + 1  # tolerancia de 1 peso
    mensaje = f"✅ Ventas con tarjeta (RD$ {ventas_tarjeta:,.2f}) ≤ Facturas B02 (RD$ {total_b02:,.2f})" if es_valido else f"❌ ALERTA: Ventas con tarjeta (RD$ {ventas_tarjeta:,.2f}) > Facturas B02 (RD$ {total_b02:,.2f})"
    return es_valido, mensaje, ventas_tarjeta, total_b02

def validar_secuencia_b01(df, fecha_actual):
    if 'numero_comprobante' not in df.columns or 'fecha_hora' not in df.columns:
        return False, "❌ No hay datos de comprobante o fecha_hora", []
    df_b01 = df[df['numero_comprobante'].str.startswith('B01', na=False)].copy()
    if df_b01.empty:
        return True, "✅ No hay facturas B01 en este período", []
    df_b01['fecha_real'] = pd.to_datetime(df_b01['fecha_hora']).dt.date
    inconsistencias = []
    for fecha_real, grupo in df_b01.groupby('fecha_real'):
        grupo['numero'] = grupo['numero_comprobante'].str.extract(r'B01(\d{8})').astype(int)
        grupo = grupo.sort_values('fecha_hora')
        numeros = grupo['numero'].tolist()
        for i in range(1, len(numeros)):
            if numeros[i] <= numeros[i-1]:
                inconsistencias.append(f"Secuencia rota el día {fecha_real}: {grupo.iloc[i-1]['numero_comprobante']} ({grupo.iloc[i-1]['fecha_hora']}) → {grupo.iloc[i]['numero_comprobante']} ({grupo.iloc[i]['fecha_hora']})")
        grupo['fecha_comp'] = pd.to_datetime(grupo['fecha']).dt.date
        desfases = grupo[grupo['fecha_comp'] != fecha_real]
        for _, row in desfases.iterrows():
            inconsistencias.append(f"Factura {row['numero_comprobante']} tiene fecha de comprobante {row['fecha']} pero fue creada el {fecha_real} (en Alegra)")
    es_valido = len(inconsistencias) == 0
    mensaje = "✅ Todas las facturas B01 tienen secuencia válida y fechas coherentes" if es_valido else "❌ Se encontraron inconsistencias en facturas B01"
    return es_valido, mensaje, inconsistencias