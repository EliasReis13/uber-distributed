# Relatório do projeto — Sistema Distribuído de Consultas Uber NYC 2014

Trabalho prático UNEB · LPIII

## O que é este projeto

É um sistema que consulta dados de corridas da Uber em Nova York (abril, maio e junho de 2014).

Em vez de um único computador guardar tudo, o trabalho fica dividido em **três servidores**. Cada um cuida de **um mês**. Você pode perguntar só a um servidor (consulta local) ou a todos de uma vez (consulta distribuída).

---

## O que foi feito até agora

1. **Três servidores** rodando ao mesmo tempo:
  - Servidor 01 (porta 8001) → dados de abril/2014
  - Servidor 02 (porta 8002) → dados de maio/2014
  - Servidor 03 (porta 8003) → dados de junho/2014
2. **Dois tipos de consulta**
  - **Local:** conta só o que aquele servidor tem.
  - **Distribuída:** um servidor pergunta aos outros e junta os resultados.
3. **Interface no navegador** — tela para escolher datas, filtrar por “base” (opcional) e ver o resultado.
4. **Script de inicialização** (`start.py`) — sobe ou encerra os três servidores com um comando.
5. **Testes automatizados** — verificam se as rotas principais respondem certo.
6. **Documentação no código** — type hints e docstrings para deixar o código mais claro.

---



## Como funciona 

Imagine três pastas, cada uma com o CSV de um mês. Cada servidor abre a sua pasta e guarda os registros na memória.

- Se você pede **só local** no servidor de abril e escolhe datas de maio, o resultado é **zero** — aquele servidor não tem maio.
- Se você pede **distribuído** com datas de maio, o servidor de abril pergunta ao de maio, soma tudo e devolve o total.

Fluxo da consulta distribuída:

```
Você → servidor coordenador → pergunta aos vizinhos → junta os números → resposta
```

Se algum vizinho estiver fora do ar, o sistema avisa (lista de servidores que falharam) e marca que o resultado não está completo.

---



## Ferramentas usadas e para que servem


| Ferramenta                                  | Para que serve                                         |
| ------------------------------------------- | ------------------------------------------------------ |
| **Python**                                  | Linguagem em que o projeto foi escrito                 |
| **FastAPI**                                 | Cria a API HTTP (as rotas `/health`, `/summary`, etc.) |
| **Uvicorn**                                 | Coloca o FastAPI no ar, ouvindo nas portas 8001–8003   |
| **httpx**                                   | Permite que um servidor chame outro pela rede          |
| **HTML + JavaScript** (`static/index.html`) | Tela de consulta no navegador                          |
| **pytest**                                  | Roda os testes de integração                           |
| **CSV (FiveThirtyEight)**                   | Fonte pública dos dados Uber NYC 2014                  |
| **venv + pip**                              | Ambiente isolado e instalação das bibliotecas          |


Arquivo de dependências: `requirements.txt` (FastAPI, Uvicorn e httpx).

---



## Estrutura principal dos arquivos

```
uber-distributed/
├── start.py           → sobe e para o cluster
├── server/app.py      → lógica de cada servidor (carregar CSV, consultar, agregar)
├── static/index.html  → interface web
├── tests/             → testes (pytest)
├── requirements.txt   → bibliotecas Python
├── README.md          → guia de uso (como instalar e rodar)
└── RELATORIO.md       → este relatório
```

---





