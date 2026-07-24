# Sistema Distribuído de Consultas — Dataset Uber NYC 2014

Trabalho prático UNEB · LPIII

**6 servidores FastAPI** (abril–setembro/2014). Qualquer nó responde localmente e pode coordenar `/summary` agregando os demais.

---

## Como rodar

```powershell
cd uber-distributed
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt

python start.py          # sobe os 6 servidores
```

Abra: **http://localhost:8001/**

```powershell
python start.py stop     # encerra tudo
python start.py 1        # sobe só o servidor 01
```

Na primeira subida os CSVs são baixados da internet (pode demorar ~30s por servidor).

---

## Estrutura

```
uber-distributed/
├── start.py              # sobe / para o cluster
├── server/app.py         # código do servidor
├── static/index.html     # interface
├── config/servidor_XX.env
├── tests/
└── requirements.txt
```

| Servidor    | Mês           | Porta | Interface                 |
|-------------|---------------|-------|---------------------------|
| servidor_01 | Abril/2014    | 8001  | http://localhost:8001/    |
| servidor_02 | Maio/2014     | 8002  | http://localhost:8002/    |
| servidor_03 | Junho/2014    | 8003  | http://localhost:8003/    |
| servidor_04 | Julho/2014    | 8004  | http://localhost:8004/    |
| servidor_05 | Agosto/2014   | 8005  | http://localhost:8005/    |
| servidor_06 | Setembro/2014 | 8006  | http://localhost:8006/    |

---

## Endpoints

| Rota | O que faz |
|------|-----------|
| `GET /health` | Status do nó |
| `GET /metadata` | Partição + servidores conhecidos |
| `GET /local/summary` | Consulta só nos dados locais |
| `GET /summary` | Consulta distribuída (coordenador) |
| `GET /top-bases` | Top N bases (local) |

Parâmetros comuns: `start_date`, `end_date`, opcionais `base`, `lat_min`, `lat_max`, `lon_min`, `lon_max`.

```bash
curl http://localhost:8001/health
curl "http://localhost:8001/summary?start_date=2014-04-01&end_date=2014-06-30"
```

Docs interativos: http://localhost:8001/docs

---

## Dados (opcional offline)

Por padrão cada `config/servidor_XX.env` usa URL do GitHub. Para offline, baixe os CSVs para `data/` e troque `DATA_FILES`:

```env
DATA_FILES=data/uber-raw-data-apr14.csv
```

Fonte: https://github.com/fivethirtyeight/uber-tlc-foil-response/tree/master/uber-trip-data

---

## Consulta distribuída

- `/summary` chama só `/local/summary` nos outros nós (sem ciclo).
- Remoto fora do ar → entra em `failed_servers` e `"complete": false`.
- Descoberta estática via `KNOWN_SERVERS` em cada `.env`.

---

## Testes

Com o cluster (ou pelo menos o 01) no ar:

```powershell
pip install pytest
pytest tests/test_server.py -v
```

---

## Variáveis de ambiente

| Variável       | Descrição                                      |
|----------------|------------------------------------------------|
| SERVER_ID      | Identificador do servidor                      |
| SERVER_PORT    | Porta                                          |
| DATA_FILES     | Caminho(s) ou URL(s) de CSV                    |
| KNOWN_SERVERS  | URLs dos outros servidores                     |
| DATE_START / DATE_END | Faixa da partição (informativo)         |
| PARTITION_DESC | Descrição da partição                          |
| HTTP_TIMEOUT   | Timeout (s) das chamadas remotas (padrão: 10)  |
