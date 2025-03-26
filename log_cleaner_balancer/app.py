# app.py
import os

from flask import Flask, jsonify, request, send_file

from utils.processor import process_file

app = Flask(__name__)

UPLOAD_FOLDER = 'data/input'
OUTPUT_FOLDER = 'data/output'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

#Sube el archivo
@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
    
    file_path = os.path.join(UPLOAD_FOLDER, file.filename)
    file.save(file_path)
    return jsonify({'message': 'File uploaded successfully', 'file_path': file_path}), 200

#Procesa el archivo
@app.route('/process', methods=['POST'])
def process_dataset():
    file_path = request.json.get('file_path')
    if not file_path or not os.path.exists(file_path):
        return jsonify({'error': 'File not found'}), 400

    output_file = process_file(file_path, OUTPUT_FOLDER)
    return jsonify({'message': 'File processed successfully', 'output_file': output_file}), 200

#Descarga el archivo procesado
@app.route('/download', methods=['GET'])
def download_file():
    filename = request.args.get('filename')
    if not filename:
        return jsonify({'error': 'Filename is required'}), 400
    
    file_path = os.path.join(OUTPUT_FOLDER, filename)
    if not os.path.exists(file_path):
        return jsonify({'error': 'File not found'}), 404
    
    return send_file(file_path, as_attachment=True)

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
