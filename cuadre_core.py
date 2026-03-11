import requests
import pandas as pd
import time
import concurrent.futures
import math

# ------------------------------------------------------------
# 1. OBTENER TODAS LAS FACTURAS DE UNA FECHA (EN PARALELO CON REINTENTOS)
# ------------------------------------------------------------
import concurrent.futures
import time

def obtener_todas_facturas(email, token, fecha, max_paginas=100, timeout=90, max_reintentos=2):
    """
    Versión optimizada: menos workers, reintentos limitados y retraso controlado.
    """
    url = "https://api.alegra.com/api/v1/invoices"
    
    # --- Obtener primera página (con reintentos) ---
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
    
    # --- Preparar páginas siguientes (limitamos a un máximo razonable) ---
    # Si la primera página tiene menos de 30 facturas, no hay más
    if len(primera_pagina) < 30:
        return facturas_totales
    
    paginas_a_solicitar = list(range(1, max_paginas))
    
    # --- Función para obtener página con timeout y reintentos simples ---
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
                    return []  # error no recuperable
            except Exception:
                time.sleep(2 ** intento)
        return []  # falló después de reintentos
    
    # --- Ejecutar con menos workers y espaciado ---
    facturas_totales = list(primera_pagina)
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {}
        for p in paginas_a_solicitar:
            time.sleep(0.2)  # pequeño retraso para no saturar
            futures[executor.submit(obtener_pagina, p)] = p
        
        for future in concurrent.futures.as_completed(futures):
            resultado = future.result()
            if resultado:
                facturas_totales.extend(resultado)
    
    print(f"📅 Total facturas obtenidas: {len(facturas_totales)}")
    return facturas_totales

# ------------------------------------------------------------
# 2. CONVERTIR FACTURAS A DATAFRAME (CON MÉTODOS DE PAGO)
# ------------------------------------------------------------
def facturas_a_dataframe(facturas):
    """
    Convierte lista de facturas en DataFrame.
    Extrae método de pago de 'paymentMethod' en cada pago.
    """
    filas = []
    # Mapeo de códigos a nombres legibles
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
                    'sucursal': sucursal
                })
        else:
            # Factura sin pagos (crédito o pendiente)
            filas.append({
                'id_factura': factura_id,
                'fecha': fecha,
                'fecha_hora': fecha_hora,
                'total_factura': total,
                'metodo_pago': 'Crédito',
                'monto_pago': 0,
                'estado': estado,
                'sucursal': sucursal
            })
    return pd.DataFrame(filas)

# ------------------------------------------------------------
# 3. CÁLCULO DEL CUADRE (VERSIÓN FINAL)
# ------------------------------------------------------------
def calcular_cuadre(df, sucursal, fondo_inicial, gastos, pagos_atrasados, conteo_efectivo):
    """
    Calcula el cuadre de caja y devuelve un diccionario con todos los resultados.
    """
    # Filtrar por sucursal y crear copia
    df_suc = df[df['sucursal'] == sucursal].copy()

    # Ventas totales del día (sin duplicar facturas)
    ventas_totales_unicas = df_suc.groupby('id_factura')['total_factura'].first().sum()

    # Ventas por método
    ventas_efectivo = df_suc[df_suc['metodo_pago'] == 'Efectivo']['monto_pago'].sum()
    # Agrupar tarjetas (puede ser 'Tarjeta Crédito' o 'Tarjeta Débito')
    df_suc['metodo_pago_simple'] = df_suc['metodo_pago'].apply(
        lambda x: 'Tarjeta' if 'Tarjeta' in str(x) else x
    )
    ventas_tarjeta = df_suc[df_suc['metodo_pago_simple'] == 'Tarjeta']['monto_pago'].sum()
    ventas_transferencia = df_suc[df_suc['metodo_pago'] == 'Transferencia']['monto_pago'].sum()
    ventas_credito = df_suc[df_suc['metodo_pago'] == 'Crédito']['total_factura'].sum()

    # Total pagado (efectivo + tarjeta + transferencia)
    total_pagado = ventas_efectivo + ventas_tarjeta + ventas_transferencia

    # Gastos y pagos atrasados
    total_gastos = sum(g['monto'] for g in gastos)
    total_pagos_atrasados = sum(p['monto'] for p in pagos_atrasados)

    # Efectivo esperado
    efectivo_esperado = ventas_efectivo + fondo_inicial - total_gastos + total_pagos_atrasados

    # Efectivo real
    efectivo_real = sum(denom * cant for denom, cant in conteo_efectivo.items())
    diferencia = efectivo_real - efectivo_esperado

    # ¿Cuadre aceptable?
    cuadre_aceptable = -50 <= diferencia <= 50

    # Sugerencia de retiro (billetes grandes)
    denominaciones_grandes = [2000, 1000, 500, 200]
    billetes_a_retirar = {d: conteo_efectivo.get(d, 0) for d in denominaciones_grandes if conteo_efectivo.get(d, 0) > 0}
    total_a_retirar = sum(d * cant for d, cant in billetes_a_retirar.items())

    # Diccionario de resultados
    resultados = {
        'total_facturas': ventas_totales_unicas,
        'total_pagado': total_pagado,
        'efectivo': ventas_efectivo,
        'tarjeta': ventas_tarjeta,
        'transferencia': ventas_transferencia,
        'credito': ventas_credito,
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
    return resultados