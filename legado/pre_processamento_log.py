#!/usr/bin/env python3
import io
from flask import Flask, request, send_file

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def upload_log():
    if request.method == 'POST':
        # Verifica se o arquivo foi enviado na requisição
        if 'log_file' not in request.files:
            return "Nenhum arquivo enviado.", 400

        arquivo = request.files['log_file']

        if arquivo.filename == '':
            return "Nenhum arquivo selecionado.", 400

        # Processa o conteúdo do arquivo, ignorando as linhas que iniciam com '#'
        linhas_processadas = []
        for linha in arquivo.stream:
            # Se a linha for do tipo bytes, decodifica para string
            linha_str = linha.decode('utf-8') if isinstance(linha, bytes) else linha
            if linha_str.strip().startswith("#"):
                continue
            linhas_processadas.append(linha_str)

        conteudo_processado = "".join(linhas_processadas)

        # Cria um objeto BytesIO para retornar o arquivo processado
        return send_file(
            io.BytesIO(conteudo_processado.encode('utf-8')),
            as_attachment=True,
            download_name="saida_pre_processada.csv",
            mimetype="text/plain"
        )

    # Para método GET, exibe um formulário simples para upload do arquivo.
    return '''
    <!doctype html>
    <html>
    <head>
        <title>Upload de Log</title>
    </head>
    <body>
        <h1>Envie o arquivo de log para processamento</h1>
        <form method="post" enctype="multipart/form-data">
            <input type="file" name="log_file">
            <input type="submit" value="Enviar">
        </form>
    </body>
    </html>
    '''

if __name__ == '__main__':
    app.run(debug=True)
