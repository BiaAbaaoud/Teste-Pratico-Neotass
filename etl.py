import pandas as pd
import json
import os
import locale
import numpy as np

# Define o locale para português para o nome dos dias da semana
try:
    locale.setlocale(locale.LC_TIME, 'pt_BR.utf8')
except locale.Error:
    try:
        # Tenta a convenção Windows
        locale.setlocale(locale.LC_TIME, 'Portuguese_Brazil.1252')
    except locale.Error:
        pass


# --- Configuração de Caminhos ---
PATH_OPORTUNIDADES = os.path.join('data', 'registros_oportunidades.json')
PATH_SELLOUT = os.path.join('data', 'sellout.parquet')
OUTPUT_FILE = os.path.join('output', 'banco_dimensional_neotass.xlsx')

# --- Funções do ETL ---

def extract_data():
    """Extrai os dados brutos dos arquivos JSON e Parquet."""
    print("Iniciando Extração...")

    # 1. Extrair Oportunidades (JSON)
    df_oportunidades = None
    try:
        with open(PATH_OPORTUNIDADES, 'r', encoding='utf-8') as f:
            data_oportunidades = json.load(f)
        df_oportunidades = pd.json_normalize(data_oportunidades)
        # Cria ID PADRÃO para a Tabela Fato Oportunidade (Requisito de PK)
        df_oportunidades['id_oportunidade_criado'] = df_oportunidades.index + 1
        print("    - JSON de Oportunidades lido com sucesso.")
    except Exception as e:
        print(f"ERRO ao ler JSON de Oportunidades: {e}")
    
    # 2. Extrair Sellout (PARQUET)
    df_sellout = None
    try:
        df_sellout = pd.read_parquet(PATH_SELLOUT)
        # Cria ID PADRÃO para a Tabela Fato Sellout (Requisito de PK)
        df_sellout['id_sellout_criado'] = df_sellout.index + 1
        print("    - Parquet de Sellout lido com sucesso.")
    except Exception as e:
        print(f"ERRO ao ler Parquet de Sellout: {e}")

    return df_oportunidades, df_sellout

def transform_data(df_oportunidades, df_sellout):
    """Aplica as transformações para criar o Modelo Dimensional."""
    print("Iniciando Transformação...")
    
    # --- 1. PREPARAÇÃO E LIMPEZA DE COLUNAS DE CNPJ E DATA ---
    
    # Normaliza a coluna de CNPJ em ambos os DataFrames
    # Encontra a coluna de CNPJ em Oportunidades (tratamento para capitalização)
    cnpj_oportunidades_col = next((col for col in df_oportunidades.columns if 'CNPJ Parceiro' in col), None)
    if cnpj_oportunidades_col:
        df_oportunidades.rename(columns={cnpj_oportunidades_col: 'id_parceiro'}, inplace=True)
    
    # Encontra a coluna de CNPJ em Sellout (tratamento para capitalização)
    cnpj_sellout_col = next((col for col in df_sellout.columns if 'CNpj Parceiro' in col), None)
    if cnpj_sellout_col:
        df_sellout.rename(columns={cnpj_sellout_col: 'id_parceiro'}, inplace=True)

    # Converte colunas de data para o formato datetime
    df_oportunidades['Data de Registro'] = pd.to_datetime(df_oportunidades['Data de Registro'])
    df_sellout['Data_Fatura'] = pd.to_datetime(df_sellout['Data_Fatura'])

    # --- 2. DIMENSÕES (CRIAÇÃO DAS CHAVES MESTRAS) ---
    
    # DIMENSÃO PARCEIRO
    # Consolida parceiros de ambas as fontes
    df_parceiros_oport = df_oportunidades[['Nome Fantasia', 'id_parceiro']].drop_duplicates()
    df_parceiros_sellout = df_sellout[['Nome Fantasia', 'id_parceiro']].drop_duplicates()
    
    # Combina e mantém o CNPJ como chave (id_parceiro)
    df_parceiro = pd.concat([df_parceiros_oport, df_parceiros_sellout]).drop_duplicates(subset=['id_parceiro'])
    df_parceiro.rename(columns={'Nome Fantasia': 'nome_parceiro'}, inplace=True)
    df_parceiro = df_parceiro[['id_parceiro', 'nome_parceiro']]
    print("    - Dimensão Parceiro criada.")

    # DIMENSÃO PRODUTO
    # Consolida produtos de ambas as fontes (usa Nome Produto como chave única id_produto)
    df_produtos_oport = df_oportunidades['Nome Produto'].unique()
    df_produtos_sellout = df_sellout['Nome_Produto'].unique()
    
    produtos = np.unique(np.concatenate((df_produtos_oport, df_produtos_sellout)))
    df_produto = pd.DataFrame(produtos, columns=['descricao_produto'])
    df_produto['id_produto'] = df_produto['descricao_produto']
    
    df_produto = df_produto[['id_produto', 'descricao_produto']]
    print("    - Dimensão Produto criada.")

    # DIMENSÃO TEMPO
    # Cria uma lista única de todas as datas
    datas_oport = df_oportunidades['Data de Registro'].dt.date.unique()
    datas_sellout = df_sellout['Data_Fatura'].dt.date.unique()
    todas_datas = np.unique(np.concatenate((datas_oport, datas_sellout)))
    
    df_tempo = pd.DataFrame(todas_datas, columns=['data'])
    df_tempo['data'] = pd.to_datetime(df_tempo['data'])
    
    # Cria os atributos de tempo
    df_tempo['id_tempo'] = (df_tempo.index + 1) # Chave sequencial
    df_tempo['ano'] = df_tempo['data'].dt.year
    df_tempo['mes'] = df_tempo['data'].dt.month
    df_tempo['nome_mes'] = df_tempo['data'].dt.strftime('%B').str.capitalize()
    df_tempo['dia_semana'] = df_tempo['data'].dt.strftime('%A').str.capitalize()
    
    df_tempo = df_tempo[['id_tempo', 'data', 'ano', 'mes', 'nome_mes', 'dia_semana']]
    print("    - Dimensão Tempo criada.")


    # --- 3. TABELAS FATO ---

    # FATO REGISTRO OPORTUNIDADE (fato_registro_oportunidade)
    fato_oportunidade = df_oportunidades.rename(columns={
        'id_oportunidade_criado': 'id_oportunidade',           
        'Data de Registro': 'data_registro',
        'Nome Produto': 'id_produto'
    }).copy()
    
    # Garante a coluna valor_total e ajusta as colunas
    fato_oportunidade['valor_total'] = fato_oportunidade['quantidade'] * fato_oportunidade['Valor Unitário']
    
    # Adiciona chaves estrangeiras (id_tempo)
    fato_oportunidade = pd.merge(
        fato_oportunidade, 
        df_tempo[['data', 'id_tempo']], 
        left_on=fato_oportunidade['data_registro'].dt.date, 
        right_on=df_tempo['data'].dt.date, 
        how='left'
    ).drop(columns=['key_0'])
    
    # Seleciona as colunas finais
    fato_oportunidade = fato_oportunidade[[
        'id_oportunidade', 'id_parceiro', 'id_produto', 'id_tempo', 
        'data_registro', 'quantidade', 'valor_total', 'status'
    ]]
    print("    - Tabela Fato: Oportunidade criada.")


    # FATO SELLOUT (fato_sellout)
    fato_sellout = df_sellout.rename(columns={
        'id_sellout_criado': 'id_sellout',           
        'Data_Fatura': 'data_fatura',
        'Nome_Produto': 'id_produto',
        'NF': 'nf'
    }).copy()
    
    # Garante a coluna valor_total e ajusta as colunas
    fato_sellout['valor_total'] = fato_sellout['Quantidade'] * fato_sellout['Valor_Unitario']
    fato_sellout.rename(columns={'Quantidade': 'quantidade'}, inplace=True)
    
    # Adiciona chaves estrangeiras (id_tempo)
    fato_sellout = pd.merge(
        fato_sellout, 
        df_tempo[['data', 'id_tempo']], 
        left_on=fato_sellout['data_fatura'].dt.date, 
        right_on=df_tempo['data'].dt.date, 
        how='left'
    ).drop(columns=['key_0'])
    
    # Seleciona as colunas finais
    fato_sellout = fato_sellout[[
        'id_sellout', 'id_parceiro', 'id_produto', 'id_tempo', 
        'data_fatura', 'nf', 'quantidade', 'valor_total'
    ]]
    print("    - Tabela Fato: Sellout criada.")

    # Retorna o dicionário de tabelas para a etapa de Load
    return {
        'dim_parceiro': df_parceiro,
        'dim_produto': df_produto,
        'dim_tempo': df_tempo,
        'fato_registro_oportunidade': fato_oportunidade,
        'fato_sellout': fato_sellout
    }

def load_data(transformed_data):
    """Carrega os dados transformados em um único arquivo Excel (.xlsx)."""
    print("Iniciando Carregamento...")
    try:
        # Cria a pasta de output se não existir
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        
        # Usa ExcelWriter para criar um único arquivo Excel com várias abas
        with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
            for table_name, df in transformed_data.items():
                print(f"    - Salvando tabela: {table_name}")
                df.to_excel(writer, sheet_name=table_name, index=False)
        
        print(f"SUCESSO! Dados salvos em: {OUTPUT_FILE}")
    except Exception as e:
        print(f"ERRO ao salvar dados: {e}")


# --- Função Principal de Orquestração ---
def main_etl():
    """Orquestra o processo completo de ETL."""
    print("=" * 50)
    print("INICIANDO PROCESSO ETL PARA NEOTASS MARKETING")
    print("=" * 50)
    
    # 1. Extract
    df_oportunidades, df_sellout = extract_data()
    
    if df_oportunidades is not None and df_sellout is not None:
        # 2. Transform
        try:
            transformed_data = transform_data(df_oportunidades, df_sellout)
            
            # 3. Load
            load_data(transformed_data)
        except KeyError as e:
            print("-" * 50)
            print(f"ERRO CRÍTICO NA TRANSFORMAÇÃO: Coluna faltando. {e}")
            print("Verifique a capitalização das colunas CNPJ e Valor Unitário nas bases originais.")
            print("-" * 50)
        except Exception as e:
            print(f"ERRO INESPERADO no processamento: {e}")

# Executa o ETL ao rodar o script
if __name__ == "__main__":
    main_etl()
