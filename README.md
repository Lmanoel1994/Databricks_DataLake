# DataBricks +  Azure Data lake <br />
**1- Criando um Cluster no Databricks <br />
2- Criando um Notebook <br />
3- Conectando o Databricks no Data Lake <br />
4- Criando um DataFrame <br />
5- Exportando o DataFrame dentro do Azure Data Lake <br />
6- Editando o Arquivo**<br />
<br />

# 1- Criando o Cluster <br />

1- Na pagina inicial do **Databricks** clique em **Cluster** <br />
2- Clique em **+Create Cluster** <br />
![alt text](https://github.com/Lmanoel1994/Databricks_DataLake/blob/master/Pictures/1.png) <br />

3- Edite todas as informações do Cluster e Marque a opção **Enable Credential Passthrough for user-level data access** <br />
4 - Clique em  **Create Cluster** <br />
![alt text](https://github.com/Lmanoel1994/Databricks_DataLake/blob/master/Pictures/2.png) <br />
<br />

**Veja se o Cluster está em execução**<br />
![alt text](https://github.com/Lmanoel1994/Databricks_DataLake/blob/master/Pictures/3.png) <br />
<br />

# 2 Criando o Notebook <br />
5- Clique em **Azure Databricks** para volta a tela de inicio <br />
6- Clique em **New Notebook**  <br />
7- Preencha os Campo de **Name**, em **Default Language** selecione **Python** e **Cluster**  selecione oque acabamos de criar <br />
![alt text](https://github.com/Lmanoel1994/Databricks_DataLake/blob/master/Pictures/4.png) <br />
<br />

# 3- Conectando o Databricks no Data Lake 
8 - Precisamos Obter as chaves das credenciais da Azure Data Lake. <br />
Para obter as chaves basta seguir a documentação  <br />
https://docs.databricks.com/data/data-sources/azure/azure-datalake-gen2.html 
````
#Agrupado chaves relevantes
ClientId = ''
ClientSecret = ''
TenantID = ''

#Combinando DirectoryID em uma string
Endpoint = 'https://login.microsoftonline.com/{}/oauth2/token'.format(TenantID)

#Criando configurações para conexão
spark.conf.set('fs.azure.account.auth.type','Oauth')
spark.conf.set('fs.azure.account.oauth.provider.type', 'org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider')
spark.conf.set('fs.azure.account.oauth.client.id', ClientId)
spark.conf.set('fs.azure.account.oauth.client.secret', ClientSecret)
spark.conf.set('fs.azure.account.oauth.client.endpoint', Endpoint)

# https://docs.databricks.com/data/data-sources/azure/azure-datalake-gen2.html 
````
![alt text](https://github.com/Lmanoel1994/Databricks_DataLake/blob/master/Pictures/5.png) <br />


# 4 Criando o DataFrame <br />
9- Vamos criar um **DataFrame** para usarmos como Exemplo :
````
Dados = [('Lucas',26,'SãoPaulo','1994-09-01','M',4000),
  ('Luana',20,'SãoPaulo','2000-09-05','F',1200),
  ('Katia',35,'SãoPaulo','1985-03-16','F',1600)
]
Colunas = ["Nome","Idade","Estado","DataNascimento","Sexo","Salario"]
df = spark.createDataFrame(data=Dados, schema = Colunas)
df.show()
````
![alt text](https://github.com/Lmanoel1994/Databricks_DataLake/blob/master/Pictures/6.png) <br />
<br />

# 5- Exportando o DataFrame dentro do Azure Data Lake 
10 - Vamos Exporta o nosso **DataFrame** para o **Data Lake** dentro da pasta **DF** com o nome Teste.csv <br />
````
df.repartition(1).write.format("csv").option("header", "true").save("adl://teste.azuredatalakestore.net/DF/Teste.csv")
````
**Veja que o Spark criou uma pasta chamada Teste.csv e dentro desta pasta ele criou varios arquivos, e o nosso Dataframe seria o "part-00000-tid....."** <br />
![alt text](https://github.com/Lmanoel1994/Databricks_DataLake/blob/master/Pictures/7.png) <br />

11- Agora vamos criar uma Variavel na qual irá copiar o caminho do ultimo arquivo que está dentro da pasta **Teste.csv** 

````
#Copiando o caminho do ultimo arquivo na pasta 'Teste'
file=dbutils.fs.ls('adl://teste.azuredatalakestore.net/DF/Teste.csv')[-1].path 
print(file)
````

![alt text](https://github.com/Lmanoel1994/Databricks_DataLake/blob/master/Pictures/8.png) <br />

12 - Agora vamos Mover o nosso **Dataframe**  a pasta que queriamos salvar, na qual seria a **DF** <br />

````
#Movendo o arquivo para pasta 'DF'
dbutils.fs.mv(file, 'adl://teste.azuredatalakestore.net/DF/')
````
![alt text](https://github.com/Lmanoel1994/Databricks_DataLake/blob/master/Pictures/9.png) <br />


13 - Novamente iremos salvar dentro de uma variavel o caminho do ultimo arquivo, mas agora sera dentro da pasta **DF**
````
#Copiando o caminho do ultimo arquivo na pasta 'DF'
file=dbutils.fs.ls('adl://teste.azuredatalakestore.net/DF')[-1].path 
print(file)
````
![alt text](https://github.com/Lmanoel1994/Databricks_DataLake/blob/master/Pictures/10.png) <br />

14-  Agora vamos renomear o nosso arquivo para **NovoNome.csv**
````
#Renomeando o Arquivo para 'NovoNome.csv'
dbutils.fs.mv(file, 'adl://teste.azuredatalakestore.net/DF/NovoNome.csv')
````
![alt text](https://github.com/Lmanoel1994/Databricks_DataLake/blob/master/Pictures/11.png) <br />

15 -  Agora vamos excluir a pasta **Teste.csv**

````
#Excluindo a pasta Teste
dbutils.fs.rm('adl://teste.azuredatalakestore.net/DF/Teste.csv/', recurse=True)
````
![alt text](https://github.com/Lmanoel1994/Databricks_DataLake/blob/master/Pictures/12.png) <br />

<br />
# Pronto <br />
![alt text](https://github.com/Lmanoel1994/Databricks_DataLake/blob/master/Pictures/13.png) <br />



