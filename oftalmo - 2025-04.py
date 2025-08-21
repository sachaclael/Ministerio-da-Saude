>>> from pysus.online_data.SIH import download
... import pandas as pd
... from datetime import datetime
... import os
... import re
... 
... # --------------- CONFIGURAÇÃO ---------------
... # UF a ser usada no download (ajuste se precisar)
... UFS = ['ac','al','ap','am','ba','ce','df','es','go','ma','mt','ms','mg','pa','pb','pr','pe','pi','rj','rn','rs','ro','rr','sc','sp','se','to']
... # Lista bruta de procedimentos (cole aqui os códigos entre colchetes se quiser editar)
... procedimentos_raw = '''0211060011|0211060020|0211060038|0211060054|0211060062
... 0211060070|0211060089|0211060097|0211060100|0211060119|0211060127|0211060135
... 0211060143|0211060151|0211060160|0211060178|0211060186|0211060208|0211060216
... 0211060224|0211060232|0211060240|0211060259|0211060267|0211060275|0211060283
... 0301010102|0303050012|0303050020|0303050039|0303050047|0303050055|0303050063
... 0303050071|0303050080|0303050098|0303050101|0303050110|0303050136|0303050144
... 0303050152|0303050160|0303050179|0303050187|0303050195|0303050209|0303050217
... 0303050225|0303050233|0405010010|0405010028|0405010036|0405010044|0405010052
... 0405010060|0405010079|0405010087|0405010109|0405010117|0405010125|0405010133
... 0405010141|0405010150|0405010168|0405010176|0405010184|0405010192|0405010206
... 0405020015|0405020023|0405030010|0405030029|0405030037|0405030045|0405030053
... 0405030070|0405030096|0405030100|0405030118|0405030126|0405030134|0405030142
... 0405030150|0405030169|0405030177|0405030185|0405030193|0405030207|0405030215
... 0405030223|0405030231|0405040016|0405040024|0405040040|0405040059|0405040067
... 0405040075|0405040083|0405040091|0405040105|0405040130|0405040148|0405040156
... 0405040164|0405040180|0405040199|0405040202|0405040210|0405050011|0405050020
... 0405050038|0405050046|0405050054|0405050062|0405050070|0405050089|0405050097
... 0405050100|0405050119|0405050127|0405050135|0405050143|0405050151|0405050160
... 0405050178|0405050186|0405050194|0405050208|0405050216|0405050224|0405050232
... 0405050240|0405050259|0405050267|0405050283|0405050291|0405050305|0405050313
... 0405050321|0405050356|0405050364|0405050372|0405050380|0405050399|0405050402'''
... 
... # --------------- FIM CONFIGURAÇÃO ---------------
... 
... # Extrai os códigos (apenas dígitos) e cria um set para pesquisa (mantendo zeros à esquerda)
procedimentos = set(re.findall(r"\d{10}", procedimentos_raw))

# 1. Definir a data de execução
hoje = datetime.now()

# Definir a pasta de saída
pasta_saida = f"SIH_{hoje.strftime('%Y%m%d_%H%M%S')}"
os.makedirs(pasta_saida, exist_ok=True)

# Criar o arquivo de log dentro da pasta de saída
log_filename = os.path.join(pasta_saida, f"log_download_{hoje.strftime('%Y%m%d_%H%M%S')}.txt")


def escreve_log(mensagem):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_filename, "a", encoding='utf-8') as log_file:
        log_file.write(f"[{timestamp}] {mensagem}\n")
    print(f"[{timestamp}] {mensagem}")

escreve_log("Início do processo de download com filtro de procedimentos.")

# 2. Rodar apenas para abril de 2025 (substitui o cálculo dos últimos 12 meses)
datas = [(2025, 4)]
escreve_log("Processando somente abril de 2025.")

# 3. Loop de download por mês e filtro
for ano, mes in reversed(datas):  # reversed para manter ordem cronológica
    for UF in UFS:
        escreve_log(f"Iniciando download de {mes:02d}/{ano} para UF={UF}...")
        try:
            sih_obj = download(UF, ano, mes, 'rd')
            dados = sih_obj.to_dataframe()

            # Normalizar nomes de coluna (mapa do uppercase -> original)
            cols_map = {c.upper(): c for c in dados.columns}

            # Preferência: PROC_REA (procedimento realizado). Se não existir, tentamos PROC_SOLIC.
            proc_col = None
            if 'PROC_REA' in cols_map:
                proc_col = cols_map['PROC_REA']
            elif 'PROC_SOLIC' in cols_map:
                proc_col = cols_map['PROC_SOLIC']
            else:
                # fallback: busca qualquer coluna que contenha 'PROC' no nome
                candidates = [orig for up, orig in cols_map.items() if 'PROC' in up]
                if candidates:
                    proc_col = candidates[0]

            if not proc_col:
                escreve_log(f"Nenhuma coluna de procedimento encontrada para {mes:02d}/{ano} UF={UF}. Nomes de colunas: {list(dados.columns)[:10]}...")
                continue

            escreve_log(f"Usando coluna de procedimento: {proc_col}")

            # Garantir que o valor seja string e sem espaços
            dados[proc_col] = dados[proc_col].astype(str).str.strip()

            # Filtrar somente os procedimentos desejados
            dados_filtrados = dados[dados[proc_col].isin(procedimentos)].copy()

            if dados_filtrados.empty:
                escreve_log(f"Nenhum registro contendo os procedimentos informados em {mes:02d}/{ano} UF={UF}.")
                # opcional: salvar um CSV vazio com cabeçalho para indicar que o mês foi processado
                nome_csv_vazio = f"SIH_{UF}_{ano}{mes:02d}_filtered_empty.csv"
                dados.head(0).to_csv(os.path.join(pasta_saida, nome_csv_vazio), index=False)
                escreve_log(f"Salvo arquivo vazio: {nome_csv_vazio}")
            else:
                # Salvar CSV contendo somente os procedimentos filtrados
                nome_csv = f"SIH_{UF}_{ano}{mes:02d}_procedimentos_filtrados.csv"
                caminho_csv = os.path.join(pasta_saida, nome_csv)
                dados_filtrados.to_csv(caminho_csv, index=False)
                escreve_log(f"Arquivo salvo: {nome_csv} (registros: {len(dados_filtrados)})")

        except Exception as e:
            escreve_log(f"Erro ao baixar/processar {mes:02d}/{ano} UF={UF}: {e}")

escreve_log("Script finalizado.")

# FIM
