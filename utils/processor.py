import csv
import os
import re
from io import StringIO

import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import LabelEncoder


def assign_criticidad(row):
    action = str(row.get('action', '')).lower()
    service = str(row.get('service', '')).lower()
    level = str(row.get('level', '')).lower()
    dstport = str(row.get('dstport', ''))
    proto = str(row.get('proto', ''))
    rcvdbyte = row.get('rcvdbyte', '0')
    sentbyte = row.get('sentbyte', '0')

    try: rcvdbyte = int(rcvdbyte)
    except: rcvdbyte = 0
    try: sentbyte = int(sentbyte)
    except: sentbyte = 0

    volumen = rcvdbyte + sentbyte
    dstport_num = int(dstport) if dstport.isdigit() else 0
    proto_num = int(proto) if proto.isdigit() else 0

    # Criticidad máxima
    if 'alert' in level or 'critical' in level:
        return 'maxima'
    if action == 'deny' and dstport_num in [3389, 22] and volumen > 500000:
        return 'maxima'
    if 'rdp' in service or 'ftp' in service or 'smb' in service:
        if action == 'deny' and volumen > 100000:
            return 'maxima'

    # Criticidad alta
    if 'deny' in action:
        return 'alta'
    if dstport_num in [21, 23, 25, 135, 139, 445]:
        return 'alta'
    if proto_num in [17, 6] and volumen > 100000:
        return 'alta'

    # Criticidad media
    if volumen > 5000:
        return 'media'

    # Criticidad baja
    if 'accept' in action and volumen <= 5000:
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

    # Asignar criticidad
    df['criticidad'] = df.apply(assign_criticidad, axis=1)

    # Selección de columnas relevantes
    columnas_clave = ['type', 'subtype', 'action', 'service', 'proto', 'srcip', 'dstip',
                      'dstport', 'duration', 'rcvdbyte', 'sentbyte', 'criticidad']
    df = df[[col for col in columnas_clave if col in df.columns]]

    # Convertir columnas numéricas
    for col in ['dstport', 'duration', 'rcvdbyte', 'sentbyte']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Codificar variables categóricas
    df_encoded = df.copy()
    le_target = LabelEncoder()
    df_encoded['criticidad'] = le_target.fit_transform(df_encoded['criticidad'].astype(str))

    cat_cols = ['type', 'subtype', 'action', 'service', 'proto', 'srcip', 'dstip']
    for col in cat_cols:
        if col in df_encoded.columns:
            df_encoded[col] = LabelEncoder().fit_transform(df_encoded[col].astype(str))

    # Imputar valores faltantes
    imputer = SimpleImputer(strategy='most_frequent')
    df_encoded = pd.DataFrame(imputer.fit_transform(df_encoded), columns=df_encoded.columns)

    # Guardar dataset procesado
    output_file = os.path.join(output_folder, 'dataset_procesado.csv')
    df_encoded.to_csv(output_file, index=False, encoding='utf-8')

    return output_file
