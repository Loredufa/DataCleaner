# app.py
import os

from flask import Flask, jsonify, request, send_file

from utils.processor import process_file, process_file_sin_criticidad

app = Flask(__name__)

UPLOAD_FOLDER = 'data/input'
OUTPUT_FOLDER = 'data/output'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

#Sube el archivo
@app.route('/prediction', methods=['POST'])
def prediction():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
    
    file_path = os.path.join(UPLOAD_FOLDER, file.filename)
    file.save(file_path)
    output_file = process_file_sin_criticidad(file_path, OUTPUT_FOLDER)
    if not output_file:
        return jsonify({'error': 'Processing failed'}), 500

    return jsonify({
        'message': 'File uploaded and processed successfully',
        'input_file': file_path,
        'output_file': output_file
    }), 200



#Sube el archivo y lo procesa
@app.route('/upload-and-process', methods=['POST'])
def upload_and_process():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    file_path = os.path.join(UPLOAD_FOLDER, file.filename)
    file.save(file_path)

    output_file = process_file(file_path, OUTPUT_FOLDER)
    if not output_file:
        return jsonify({'error': 'Processing failed'}), 500

    return jsonify({
        'message': 'File uploaded and processed successfully',
        'input_file': file_path,
        'output_file': output_file
    }), 200


#Server
if __name__ == '__main__':
    app.run(debug=True)
