import csv
import os
import re
from collections import Counter
from io import StringIO

import pandas as pd
from imblearn.over_sampling import SMOTE
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import LabelEncoder


def process_file(file_path, output_folder):
    parsed_logs = []
    
    # 1. Parseo del archivo línea por línea
    with open(file_path, 'r', encoding='utf-8') as file:
        for idx, line in enumerate(file, start=1):
            line = line.strip()
            if not line:
                continue
            
            # Limpiar múltiples comillas y espacios
            line_clean = re.sub(r'"+', '"', line)
            line_clean = line_clean.strip('"')
            
            try:
                # Utilizar csv.reader para separar tokens
                tokens = list(csv.reader(StringIO(line_clean), delimiter=',', quotechar='"'))[0]
            except Exception as e:
                print(f"[Línea {idx}] Error en csv.reader: {e}")
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
            else:
                print(f"[Línea {idx}] No se extrajeron datos.")
    
    print(f"Total de registros parseados: {len(parsed_logs)}")
    if not parsed_logs:
        print("No se encontraron registros. Revisa el formato de entrada.")
        return None

    # 2. Convertir en DataFrame
    df = pd.DataFrame(parsed_logs)
    print("Columnas detectadas:")
    print(df.columns.tolist())
    print("Primeros registros:")
    print(df.head())

    # 3. Seleccionar las características relevantes
    features = ['type', 'subtype', 'action', 'service', 
                'proto', 'srcip', 'dstip', 'dstport', 
                'duration', 'rcvdbyte', 'sentbyte', 'criticidad']
    features = [col for col in features if col in df.columns]
    print("Características seleccionadas:")
    print(features)

    # 4. Si 'criticidad' no está presente, inicializarla con 'desconocido'
    if 'criticidad' not in df.columns:
        df['criticidad'] = 'desconocido'
    df = df[features]

    # 5. Convertir columnas numéricas
    numeric_columns = ['dstport', 'duration', 'rcvdbyte', 'sentbyte']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            nan_count = df[col].isna().sum()
            if nan_count > 0:
                print(f"La columna '{col}' tiene {nan_count} valores no numéricos.")

    # 6. Codificación de características categóricas
    df_encoded = df.copy()
    
    # Codificar la variable objetivo 'criticidad' con un encoder separado
    target_encoder = LabelEncoder()
    df_encoded['criticidad'] = target_encoder.fit_transform(df_encoded['criticidad'].astype(str))
    
    # Codificar las demás columnas categóricas
    categorical_columns = ['type', 'subtype', 'action', 'service', 'proto', 'srcip', 'dstip']
    for col in categorical_columns:
        if col in df_encoded.columns:
            df_encoded[col] = LabelEncoder().fit_transform(df_encoded[col].astype(str))
    
    # 7. Separar características (X) y etiqueta (y)
    X = df_encoded.drop(columns=['criticidad'])
    y = df_encoded['criticidad']

    # 8. Manejo de NaNs usando SimpleImputer
    print("Número de NaNs por columna antes de imputación:")
    print(X.isna().sum())
    
    # Usando SimpleImputer para completar valores faltantes con la moda
    imputer = SimpleImputer(strategy='most_frequent')
    X = pd.DataFrame(imputer.fit_transform(X), columns=X.columns)
    
    print("Número de NaNs por columna después de imputación:")
    print(X.isna().sum())

    # 9. Verificar el número de muestras por clase
    class_counts = Counter(y)
    print("Distribución de clases antes del balanceo:")
    print(class_counts)
    
    # 10. Ajuste de n_neighbors dinámico
    min_samples = min(class_counts.values())
    n_neighbors = min(5, min_samples - 1) if min_samples > 1 else 1
    print(f"Usando n_neighbors = {n_neighbors} para SMOTE")

    # 11. Balanceo de clases con SMOTE (solo si hay al menos dos clases)
    if len(y.unique()) > 1 and n_neighbors >= 1:
        smote = SMOTE(random_state=42, k_neighbors=n_neighbors)
        X_resampled, y_resampled = smote.fit_resample(X, y)
        # Volver a la forma original de la etiqueta
        y_resampled_labels = target_encoder.inverse_transform(y_resampled)
    else:
        X_resampled, y_resampled_labels = X, y

    # 12. Crear el DataFrame final
    df_resampled = pd.DataFrame(X_resampled, columns=X.columns)
    df_resampled['criticidad'] = y_resampled_labels
    print("Primeros registros del DataFrame final:")
    print(df_resampled.head())
    print(f"Total de registros en el DataFrame final: {len(df_resampled)}")

    # 13. Guardar el dataset final
    output_file = os.path.join(output_folder, 'logs_etiquetados_balanceado_final.csv')
    df_resampled.to_csv(output_file, index=False, encoding='utf-8')
    print(f"Archivo procesado guardado en: {output_file}")

    return output_file


