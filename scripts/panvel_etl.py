from pyspark.sql import SparkSession
import os
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, count, floor, datediff, current_date
from py4j.protocol import Py4JJavaError

# Definindo os diretórios de dados como variáveis para facilitar a manutenção
DATA_DIR_RAW = "C:\\Users\\Theuzao\\Desktop\\panvel_datalake\\data\\raw\\"
DATA_DIR_TRANSFORMED = "C:\\Users\\Theuzao\\Desktop\\panvel_datalake\\data\\transformed\\"
DATA_DIR_PROCESSED = "C:\\Users\\Theuzao\\Desktop\\panvel_datalake\\data\\processed\\"

# Definindo a sessão spark e desabilitando a leitura vetorizada
spark = SparkSession.builder.appName("ScriptCompleto").getOrCreate()
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")

# Função para carregar todos os dados dos formatos especificados do diretório de dados brutos
def carregar_todos_os_dados(diretorio: str = DATA_DIR_RAW, formatos: list = None):
    dataframes = {}

    # Define formatos padrão caso não sejam especificados
    if formatos is None:
        formatos = ['json', 'parquet']

    # Itera por todos os arquivos no diretório e carrega aqueles que estão nos formatos definidos
    for arquivo in os.listdir(diretorio):
        extensao = arquivo.split('.')[-1]
        
        if extensao in formatos:
            caminho_completo = os.path.join(diretorio, arquivo)
            df = spark.read.format(extensao).load(caminho_completo)

            dataframes[arquivo] = df
            print(f"Arquivo carregado e processado: {caminho_completo}")

    print("Script rodando corretamente.")
    return dataframes

def converter_colunas_para_string(df):

    return df.select([F.col(col_name).cast("string").alias(col_name) for col_name in df.columns])


# Função para transformar os dados do DataFrame de clientes
def transform_clientes(dataframes):
    try:
        df_clientes = dataframes.get("clientes.parquet")
        df_clientes.select([count(when(col(c).isNull(), c)).alias(c) for c in df_clientes.columns]).show()
        df_clientes = df_clientes.fillna({"v_sx_cli": "N", "n_est_cvl": "0", "d_dt_nasc": "Desconhecido"})
        default_date = '2001-01-01'
        df_clientes = df_clientes.withColumn(
            "d_dt_nasc",
            F.when(F.col("d_dt_nasc") == "Desconhecido", default_date).otherwise(F.col("d_dt_nasc"))
        )
        df_clientes = df_clientes.withColumn("idade",
                                             floor(datediff(current_date(), col("d_dt_nasc")) / 365))
        df_clientes = df_clientes.fillna({"idade": "0"})
        df_clientes = converter_colunas_para_string(df_clientes)

         # Debug: Mostrar dados antes de salvar
        df_clientes.show(5)  
        print("Salvando DataFrame de clientes...")
        
        df_clientes.write.mode("overwrite").option("header", "true").csv(os.path.join(DATA_DIR_TRANSFORMED, "clientes"))
        return df_clientes
    
    except Py4JJavaError as e:
        print(f"Ocorreu um erro ao transformar os dados: {e}")
        print(e.java_exception)  # Exibe a exceção Java específica para debugging

# Função para transformar o DataFrame de vendas
def transform_vendas(dataframes):
    try:
        df_vendas = dataframes.get("vendas.parquet")
        df_vendas = converter_colunas_para_string(df_vendas)
        df_vendas = df_vendas.fillna("Desconhecido")
        df_vendas.write.mode("overwrite").option("header", "true").csv(os.path.join(DATA_DIR_TRANSFORMED, "vendas"))
        return df_vendas
    
    except Py4JJavaError as e:

         print(f"Ocorreu um erro ao transformar os dados: {e}")

# Função para transformar o DataFrame de pedidos
def transform_pedidos(dataframes):
    try:
        df_pedidos = dataframes.get("pedidos.parquet")
        df_pedidos = df_pedidos.withColumn("n_id_pdd",
            F.when(F.col("n_id_pdd").isNull(), "Desconhecido").otherwise(F.col("n_id_pdd"))
        )
        df_pedidos = df_pedidos.withColumn("d_dt_eft_pdd",
            F.when(F.col("d_dt_eft_pdd").isNull(), F.lit("2001-01-01")).otherwise(F.col("d_dt_eft_pdd"))
        ).withColumn("d_dt_entr_pdd",
            F.when(F.col("d_dt_entr_pdd").isNull(), F.lit("2001-01-01")).otherwise(F.col("d_dt_entr_pdd"))
        )
        df_pedidos = converter_colunas_para_string(df_pedidos)
        df_pedidos.write.mode("overwrite").option("header", "true").csv(os.path.join(DATA_DIR_TRANSFORMED, "pedidos"))
        return df_pedidos
    
    except Py4JJavaError as e:
         print(f"Ocorreu um erro ao transformar os dados: {e}")

# Função para transformar o DataFrame de pedido_venda
def transform_pedido_venda(dataframes):
    try:
        df_pedido_venda = dataframes.get("pedido_venda.parquet")
        df_pedido_venda = converter_colunas_para_string(df_pedido_venda)
        df_pedido_venda.write.mode("overwrite").option("header", "true").csv(os.path.join(DATA_DIR_TRANSFORMED, "pedido_venda"))
        return df_pedido_venda
    
    except Py4JJavaError as e:
         print(f"Ocorreu um erro ao transformar os dados: {e}")

# Função para transformar o DataFrame de itens_venda
def transform_itens_venda(dataframes):
    try:
        df_itens_vendas = dataframes.get("itens_vendas.parquet")
        df_itens_vendas = converter_colunas_para_string(df_itens_vendas)
        df_itens_vendas.write.mode("overwrite").option("header", "true").csv(os.path.join(DATA_DIR_TRANSFORMED, "itens_vendas"))
        return df_itens_vendas
    
    except Py4JJavaError as e:
         print(f"Ocorreu um erro ao transformar os dados: {e}")

# Função para transformar o DataFrame de clientes_opt
def transform_clientes_opt(dataframes):
    try:
        df_clientes_opt = dataframes.get("clientes_opt.json")
        df_clientes_opt = converter_colunas_para_string(df_clientes_opt)
        df_clientes_opt = df_clientes_opt.withColumn(
            "b_call",
            F.when(F.col("b_call").isNull(), "Desconhecido")
            .when(F.col("b_call") == True, "Sim")
            .otherwise("Não")
        ).withColumn(
            "b_email",
            F.when(F.col("b_email").isNull(), "Desconhecido")
            .when(F.col("b_email") == True, "Sim")
            .otherwise("Não")
        ).withColumn(
            "b_push",
            F.when(F.col("b_push").isNull(), "Desconhecido")
            .when(F.col("b_push") == True, "Sim")
            .otherwise("Não")
        ).withColumn(
            "b_sms",
            F.when(F.col("b_sms").isNull(), "Desconhecido")
            .when(F.col("b_sms") == True, "Sim")
            .otherwise("Não")
        )
        df_clientes_opt.write.mode("overwrite").option("header", "true").csv(os.path.join(DATA_DIR_TRANSFORMED, "clientes_opt"))
        return df_clientes_opt
    
    except Py4JJavaError as e:
         print(f"Ocorreu um erro ao transformar os dados: {e}")

# Função para transformar o DataFrame de endereco_clientes
def transform_endereco_clientes(dataframes):
    try:
        df_enderecos_clientes = dataframes.get("enderecos_clientes.parquet")
        # Verifica se o DataFrame foi carregado corretamente
        if df_enderecos_clientes is None:
            raise ValueError("DataFrame 'enderecos_clientes.parquet' não foi carregado corretamente.")
        # Aplica a transformação na coluna 'd_dt_exc'
        df_enderecos_clientes = df_enderecos_clientes.withColumn(
            "d_dt_exc",
            F.when(F.col("d_dt_exc").isNull(), "Desconhecido").otherwise(F.col("d_dt_exc"))
        )
        df_enderecos_clientes = converter_colunas_para_string(df_enderecos_clientes)
        df_enderecos_clientes.write.mode("overwrite").option("header", "true").csv(
            os.path.join(DATA_DIR_TRANSFORMED, "enderecos_clientes")
        )

        print("Transformação do DataFrame 'enderecos_clientes' concluída com sucesso.")
        return df_enderecos_clientes

    except Py4JJavaError as e:
        print(f"Ocorreu um erro ao transformar os dados: {e}")
    except ValueError as ve:
        print(f"Erro de valor: {ve}")




# Executa a função principal se o script for executado diretamente
if __name__ == "__main__":
    dataframes = carregar_todos_os_dados()
    print(f"Total de DataFrames carregados: {len(dataframes)}")

"""
    transform_clientes(dataframes)
    transform_vendas(dataframes)
    transform_pedidos(dataframes)
    transform_pedido_venda(dataframes)
    transform_itens_venda(dataframes)
    transform_clientes_opt(dataframes)
    transform_endereco_clientes(dataframes) 
"""