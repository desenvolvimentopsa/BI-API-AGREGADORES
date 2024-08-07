#!/usr/bin/python3

import os
import redis

def main(diretorio):
    r = redis.Redis(host='127.0.0.1', port=6389, db=0)

    for nome_arquivo in os.listdir(diretorio):
        caminho_arquivo = os.path.join(diretorio, nome_arquivo)
        if os.path.isfile(caminho_arquivo) and nome_arquivo.split('.')[1] != 'py':
            with open(caminho_arquivo, 'r', encoding='utf-8') as arquivo:
                conteudo_arquivo = arquivo.read()

            nome_arquivo = '.'.join(nome_arquivo.split('.')[:-1])
            r.set(nome_arquivo, conteudo_arquivo)
            print(f'Arquivo "{nome_arquivo}" gravado no Redis.')

diretorio_alvo = "." 
main(diretorio_alvo)
