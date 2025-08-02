# 📺 TV Shows Data Lake Pipeline

Este projeto implementa um pipeline de ingestão e transformação de dados com múltiplas camadas (Bronze → Silver → Gold), utilizando:

- **MinIO** como data lake (compatível com S3)
- **MariaDB** como metastore para controle de carga
- **Trino** para consultas SQL em arquivos CSV e ORC
- **Python** para orquestração
- **Dados de entrada**: API pública do TVMaze: [`http://api.tvmaze.com/search/shows?q=postman`](http://api.tvmaze.com/search/shows?q=postman)

---

## 📌 Objetivo

Processar dados de shows de TV e estruturá-los em diferentes camadas de qualidade de dados:

- **Bronze**: Dados crus no formato CSV
- **Silver**: Dados limpos e tipados no formato ORC
- **Gold**: Dados agregados (ex: médias, contagens e datas)

---

## 📁 Estrutura de diretórios no MinIO

