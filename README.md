# Sistema Distribuído de Consultas — Uber NYC 2014

Trabalho prático UNEB · LPIII

**3 servidores FastAPI** (abril–junho/2014). Qualquer nó responde localmente e pode coordenar `/summary` agregando os demais.

## Como rodar

```powershell
cd uber-distributed
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt

python start.py
```

Aguarde os 3 servidores subirem (na primeira vez os CSVs são baixados da internet).

### Interface web

1. Com o cluster rodando, abra o navegador em: **http://localhost:8001/**
2. Veja o status dos 3 nós no painel **Cluster**.
3. Escolha as datas (e opcionalmente a base) e o escopo (distribuído ou local).
4. Clique em **Executar consulta**.

A mesma interface também está em http://localhost:8002/ e http://localhost:8003/.

```powershell
python start.py stop   # encerra tudo
python start.py 1      # sobe só o servidor 01
```

## Estrutura

```
uber-distributed/
├── start.py
├── server/app.py
├── static/index.html
├── tests/
└── requirements.txt
```

| Servidor    | Mês        | Porta | Interface              |
|-------------|------------|-------|------------------------|
| servidor_01 | Abril/2014 | 8001  | http://localhost:8001/ |
| servidor_02 | Maio/2014  | 8002  | http://localhost:8002/ |
| servidor_03 | Junho/2014 | 8003  | http://localhost:8003/ |

## Endpoints

| Rota | O que faz |
|------|-----------|
| `GET /health` | Status do nó |
| `GET /local/summary` | Só dados locais |
| `GET /summary` | Agrega os 3 nós |

Parâmetros: `start_date`, `end_date` (obrigatórios) e `base` (opcional).

```bash
curl http://localhost:8001/health
curl "http://localhost:8001/summary?start_date=2014-04-01&end_date=2014-06-30"
```

Docs: http://localhost:8001/docs

## Como funciona

1. Cada servidor guarda **só o seu mês**.
2. `/local/summary` conta só neste nó.
3. `/summary` agrega chamando `/local/summary` nos vizinhos (sem ciclo).
4. Vizinho fora do ar → `failed_servers` e `"complete": false`.

## Testes

Com o cluster no ar:

```powershell
pip install pytest
pytest tests/test_server.py -v
```

## Dados

Fonte: https://github.com/fivethirtyeight/uber-tlc-foil-response/tree/master/uber-trip-data

Para usar CSV local, altere `data_file` na tabela `NODES` em `server/app.py`.
