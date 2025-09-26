-- ######################################################################
-- SCRIPT SQL PARA CRIAÇÃO DO BANCO DE DADOS DIMENSIONAL NEOTASS
-- OBJETIVO: Atender ao Requisito Opcional 2 do Teste Prático (MySQL)
-- ######################################################################

-- 1. Criação do Esquema (Database)
CREATE DATABASE IF NOT EXISTS neotass_dimensional;
USE neotass_dimensional;

-- ######################################################################
-- TABELAS DIMENSÃO (MASTER DATA)
-- ######################################################################

-- 2. Dimensão Tempo
CREATE TABLE dim_tempo (
    id_tempo INT PRIMARY KEY,
    data DATE NOT NULL,
    ano SMALLINT NOT NULL,
    mes TINYINT NOT NULL,
    nome_mes VARCHAR(20) NOT NULL,
    dia_semana VARCHAR(20) NOT NULL
);

-- 3. Dimensão Parceiro
CREATE TABLE dim_parceiro (
    id_parceiro VARCHAR(20) PRIMARY KEY, -- CNPJ como PK
    nome_parceiro VARCHAR(255)
);

-- 4. Dimensão Produto
CREATE TABLE dim_produto (
    id_produto VARCHAR(20) PRIMARY KEY, -- ID criado no ETL como PK
    descricao_produto VARCHAR(255) NOT NULL
);

-- ######################################################################
-- TABELAS FATO (TRANSAÇÕES)
-- ######################################################################

-- 5. Tabela Fato: Registro Oportunidade
CREATE TABLE fato_registro_oportunidade (
    -- Chaves Primárias
    id_oportunidade INT PRIMARY KEY, -- ID criado no ETL

    -- Chaves Estrangeiras (Foreign Keys)
    id_parceiro VARCHAR(20) NOT NULL,
    id_produto VARCHAR(20) NOT NULL,
    id_tempo INT NOT NULL,

    -- Atributos da Fato
    data_registro DATE NOT NULL,
    quantidade INT,
    valor_total DECIMAL(10, 2), -- Valor Total
    status VARCHAR(50),

    -- Definição das Chaves Estrangeiras
    FOREIGN KEY (id_parceiro) REFERENCES dim_parceiro(id_parceiro),
    FOREIGN KEY (id_produto) REFERENCES dim_produto(id_produto),
    FOREIGN KEY (id_tempo) REFERENCES dim_tempo(id_tempo)
);

-- 6. Tabela Fato: Sellout (Vendas)
CREATE TABLE fato_sellout (
    -- Chaves Primárias
    id_sellout INT PRIMARY KEY, -- ID criado no ETL

    -- Chaves Estrangeiras (Foreign Keys)
    id_parceiro VARCHAR(20) NOT NULL,
    id_produto VARCHAR(20) NOT NULL,
    id_tempo INT NOT NULL,

    -- Atributos da Fato
    data_fatura DATE NOT NULL,
    nf VARCHAR(50), -- Número da Nota Fiscal
    quantidade INT,
    valor_total DECIMAL(10, 2), -- Valor Total

    -- Definição das Chaves Estrangeiras
    FOREIGN KEY (id_parceiro) REFERENCES dim_parceiro(id_parceiro),
    FOREIGN KEY (id_produto) REFERENCES dim_produto(id_produto),
    FOREIGN KEY (id_tempo) REFERENCES dim_tempo(id_tempo)
);

-- ######################################################################
-- FIM DO SCRIPT
-- ######################################################################