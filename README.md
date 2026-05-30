# Pipeline de Engenharia de Dados: Ingestão Multi-Engine

Este repositório contém a implementação de um ecossistema completo de engenharia de dados focado em estratégias de ingestão de dados transacionais (OLTP) para um ambiente de armazenamento de objetos (Object Storage), simulando as camadas iniciais (*Landing* e *Raw*) de um Data Lake analítico. 

O projeto demonstra e compara de forma prática duas abordagens distintas de engenharia: processamento centralizado e atômico em **Python Nativo** e processamento distribuído escalável via **Apache Spark (PySpark)**.