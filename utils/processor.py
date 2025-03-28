import csv
import os
import re
from io import StringIO

import joblib
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import LabelEncoder


#Evalúa los valores de cada log (una fila del dataset) y devuelve uno de estos niveles de criticidad: maxima, alta, media y baja
def assign_criticidad(row):
    action = str(row.get('action', '')).lower()
    service = str(row.get('service', '')).lower()
    level = str(row.get('level', '')).lower()
    type_ = str(row.get('type', '')).lower()
    dstport = str(row.get('dstport', ''))
    proto = str(row.get('proto', ''))
    dstowner = str(row.get('dstowner', '')).lower()
    dstcountry = str(row.get('dstcountry', '')).lower()
    rcvdbyte = row.get('rcvdbyte', '0')
    sentbyte = row.get('sentbyte', '0')

    try: rcvdbyte = int(rcvdbyte)
    except: rcvdbyte = 0
    try: sentbyte = int(sentbyte)
    except: sentbyte = 0

    volumen = rcvdbyte + sentbyte
    dstport_num = int(dstport) if dstport.isdigit() else 0
    proto_num = int(proto) if proto.isdigit() else 0

    # Relación de fuga
    fuga = sentbyte > rcvdbyte * 5 and volumen > 100000

    # Listas de confianza
    destinos_confiables = ['fortinet', 'microsoft', 'google']
    paises_confiables = ['argentina', 'united states', 'spain']

    # Nivel crítico
    if 'alert' in level or 'critical' in level:
        return 'maxima'

    # Tipo sospechoso
    if type_ in ['attack', 'virus', 'utm', 'event']:
        if 'deny' in action or volumen > 100000:
            return 'maxima'

    # Destino malicioso
    if not any(kw in dstowner for kw in destinos_confiables) and dstcountry not in paises_confiables:
        if 'deny' in action or fuga:
            return 'alta'

    # Servicio crítico + tráfico alto
    if action == 'deny' and dstport_num in [3389, 22] and volumen > 500000:
        return 'maxima'
    if any(s in service for s in ['rdp', 'ftp', 'smb']):
        if action == 'deny' and volumen > 100000:
            return 'maxima'

    # Fuga de datos
    if fuga:
        return 'alta'

    # Puerto sospechoso
    if dstport_num in [21, 23, 25, 135, 139, 445]:
        return 'alta'

    # Protocolo común pero tráfico elevado
    if proto_num in [6, 17] and volumen > 100000:
        return 'alta'

    # Volumen moderado
    if volumen > 5000:
        return 'media'

    # Evento rutinario
    if 'accept' in action and volumen <= 5000 and level in ['notice', 'information']:
        return 'baja'

    return 'media'

def process_file(file_path, output_folder):
    parsed_logs = []

    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            line_clean = re.sub(r'"+', '"', line).strip('"')
            try:
                tokens = list(csv.reader(StringIO(line_clean), delimiter=',', quotechar='"'))[0]
            except:
                continue

            log_dict = {}
            for token in tokens:
                token = token.strip()
                if '=' in token:
                    key, value = token.split('=', 1)
                elif ':' in token:
                    key, value = token.split(':', 1)
                else:
                    continue
                log_dict[key.lower().strip()] = value.lower().strip()

            if log_dict:
                parsed_logs.append(log_dict)

    if not parsed_logs:
        return None

    df = pd.DataFrame(parsed_logs)

    # Crear columna de ID con date + time
    df['log_id'] = df['date'].str.strip('"') + ' ' + df['time'].str.strip('"')

    # Asignar criticidad en texto
    df['criticidad'] = df.apply(assign_criticidad, axis=1)

    # Mapeo explícito de criticidad
    mapeo_criticidad = {
        'baja': 0,
        'media': 1,
        'alta': 2,
        'maxima': 3
    }

    # Asignar codificación fija
    df['criticidad'] = df['criticidad'].map(mapeo_criticidad)

    # Selección de columnas
    columnas_clave = ['log_id', 'type', 'subtype', 'action', 'service', 'proto', 'srcip', 'dstip',
                      'dstport', 'duration', 'rcvdbyte', 'sentbyte', 'criticidad']
    df = df[[col for col in columnas_clave if col in df.columns]]

    # Conversión de tipos numéricos
    for col in ['dstport', 'duration', 'rcvdbyte', 'sentbyte']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Codificación de variables categóricas
    df_encoded = df.copy()
    cat_cols = ['type', 'subtype', 'action', 'service', 'proto', 'srcip', 'dstip']
    for col in cat_cols:
        if col in df_encoded.columns:
            df_encoded[col] = LabelEncoder().fit_transform(df_encoded[col].astype(str))

    # Imputación de valores faltantes
    imputer = SimpleImputer(strategy='most_frequent')
    df_encoded = pd.DataFrame(imputer.fit_transform(df_encoded), columns=df_encoded.columns)

    # Guardar CSV final
    output_file = os.path.join(output_folder, 'dataset_procesado_mejorado.csv')
    df_encoded.to_csv(output_file, index=False, encoding='utf-8')

    return output_file



def process_file_sin_criticidad(file_path, output_folder):
    parsed_logs = []

    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            line_clean = re.sub(r'"+', '"', line).strip('"')
            try:
                tokens = list(csv.reader(StringIO(line_clean), delimiter=',', quotechar='"'))[0]
            except:
                continue

            log_dict = {}
            for token in tokens:
                token = token.strip()
                if '=' in token:
                    key, value = token.split('=', 1)
                elif ':' in token:
                    key, value = token.split(':', 1)
                else:
                    continue
                log_dict[key.lower().strip()] = value.lower().strip()

            if log_dict:
                parsed_logs.append(log_dict)

    if not parsed_logs:
        return None

    df = pd.DataFrame(parsed_logs)

    # Crear columna de ID con date + time
    df['log_id'] = df['date'].str.strip('"') + ' ' + df['time'].str.strip('"')

    # Columnas esperadas (sin criticidad)
    columnas_clave = ['log_id', 'type', 'subtype', 'action', 'service', 'proto', 'srcip', 'dstip',
                      'dstport', 'duration', 'rcvdbyte', 'sentbyte']
    df = df[[col for col in columnas_clave if col in df.columns]]

    # Conversión de tipos numéricos
    for col in ['dstport', 'duration', 'rcvdbyte', 'sentbyte']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Cargar transformadores entrenados
    encoders = joblib.load(os.path.join(output_folder, 'label_encoders.pkl'))
    imputer = joblib.load(os.path.join(output_folder, 'imputer.pkl'))

    # Codificar categóricas con los encoders entrenados
    cat_cols = ['type', 'subtype', 'action', 'service', 'proto', 'srcip', 'dstip']
    for col in cat_cols:
        if col in df.columns and col in encoders:
            le = encoders[col]
            df[col] = df[col].astype(str).apply(lambda x: x if x in le.classes_ else le.classes_[0])
            df[col] = le.transform(df[col])

    # Imputación
    df_encoded = pd.DataFrame(imputer.transform(df.drop(columns=['log_id'])), columns=df.drop(columns=['log_id']).columns)

    # Reagregar log_id para trazabilidad
    df_encoded.insert(0, 'log_id', df['log_id'].values)

    # Guardar CSV listo para predicción
    output_file = os.path.join(output_folder, 'dataset_limpio_para_prediccion.csv')
    df_encoded.to_csv(output_file, index=False, encoding='utf-8')

    return output_file