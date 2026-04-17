from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from datetime import datetime
import pandas as pd

# ──────────────────────────────────────────────
# Colunas reais do arquivo dados-stream.csv
# Separador: ponto e vírgula  ( ; )
# id_genero | data_execucao | nota | nome_musica | nome_artista
# ──────────────────────────────────────────────

SEP = ";"
CAMINHO_BASE = "/opt/airflow/data"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
}

# ──────────────────────────────────────────────
# Funções Python
# ──────────────────────────────────────────────

def task2_tratar_datas():
    """
    TASK-2: Converte datas no formato americano (aaaa-mm-dd)
    para o formato brasileiro (dd/mm/aaaa) e salva task2.csv.
    """
    df = pd.read_csv(f"{CAMINHO_BASE}/entrada.csv", sep=SEP)

    def parse_data(valor):
        """Tenta múltiplos formatos e sempre retorna dd/mm/aaaa."""
        formatos = ["%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"]
        for fmt in formatos:
            try:
                return datetime.strptime(str(valor).strip(), fmt).strftime("%d/%m/%Y")
            except (ValueError, TypeError):
                pass
        return valor  # devolve original se não reconhecer

    df["data_execucao"] = df["data_execucao"].apply(parse_data)
    df.to_csv(f"{CAMINHO_BASE}/task2.csv", sep=SEP, index=False)
    print(f"[TASK-2] Datas tratadas. Total de linhas: {len(df)}")


def task3_remover_sem_musica(**context):
    """
    TASK-3: Remove linhas onde nome_musica está vazio,
    salva task3.csv e empurra a quantidade de descartados via XCom.
    """
    df = pd.read_csv(f"{CAMINHO_BASE}/task2.csv", sep=SEP)
    total_antes = len(df)

    df_limpo = df[
        df["nome_musica"].notna() & (df["nome_musica"].str.strip() != "")
    ].copy()

    descartados = total_antes - len(df_limpo)
    df_limpo.to_csv(f"{CAMINHO_BASE}/task3.csv", sep=SEP, index=False)

    # Passa a quantidade de descartados para a TASK-4 via XCom
    context["ti"].xcom_push(key="qtd_descartados", value=descartados)
    print(f"[TASK-3] Descartados {descartados} registros sem nome_musica.")


def task6_enriquecer_genero(**context):
    """
    TASK-6: Cruza task3.csv com o resultado da TASK-5 (tabela genero_musical),
    adiciona a coluna nome_genero e salva task4.csv.
    """
    # Recupera lista de tuplas [(id_genero, nome_genero), ...] via XCom
    registros = context["ti"].xcom_pull(
        task_ids="task5_consultar_genero", key="return_value"
    )

    df_genero = pd.DataFrame(registros, columns=["id_genero", "nome_genero"])
    df = pd.read_csv(f"{CAMINHO_BASE}/task3.csv", sep=SEP)

    # Padroniza o campo de join para 3 dígitos com zero à esquerda
    df["id_genero"] = df["id_genero"].astype(str).str.zfill(3)
    df_genero["id_genero"] = df_genero["id_genero"].astype(str).str.zfill(3)

    df_enriquecido = df.merge(df_genero, on="id_genero", how="left")
    df_enriquecido.to_csv(f"{CAMINHO_BASE}/task4.csv", sep=SEP, index=False)
    print(f"[TASK-6] Enriquecimento concluído. Linhas: {len(df_enriquecido)}")


def task7_media_avaliacao():
    """
    TASK-7: Calcula a média da coluna 'nota' agrupada por nome_musica
    e salva media_avaliacao.csv.
    """
    df = pd.read_csv(f"{CAMINHO_BASE}/task4.csv", sep=SEP)

    media = (
        df.groupby("nome_musica")["nota"]
        .mean()
        .reset_index()
        .rename(columns={"nota": "media_avaliacao"})
        .sort_values("media_avaliacao", ascending=False)
    )
    media["media_avaliacao"] = media["media_avaliacao"].round(2)

    media.to_csv(f"{CAMINHO_BASE}/media_avaliacao.csv", sep=SEP, index=False)
    print(f"[TASK-7] Média de avaliação gerada para {len(media)} músicas.")


def task8_total_artista():
    """
    TASK-8: Calcula o total de execuções agrupado por nome_artista
    e salva total_artista.csv.
    """
    df = pd.read_csv(f"{CAMINHO_BASE}/task4.csv", sep=SEP)

    total = (
        df.groupby("nome_artista")
        .size()
        .reset_index(name="total_musicas")
        .sort_values("total_musicas", ascending=False)
    )

    total.to_csv(f"{CAMINHO_BASE}/total_artista.csv", sep=SEP, index=False)
    print(f"[TASK-8] Total por artista gerado para {len(total)} artistas.")


# ──────────────────────────────────────────────
# Definição da DAG
# ──────────────────────────────────────────────

with DAG(
    dag_id="pipeline_stream_musical",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Pipeline de dados de streaming musical - AV-02-SSA",
    tags=["streaming", "musica", "ssa"],
) as dag:

    # ── TASK-1 ──────────────────────────────────
    task1 = BashOperator(
        task_id="task1_copiar_arquivo",
        bash_command=(
            "cp /opt/airflow/data/dados-stream.csv "
            "/opt/airflow/data/entrada.csv"
        ),
    )

    # ── TASK-2 ──────────────────────────────────
    task2 = PythonOperator(
        task_id="task2_tratar_datas",
        python_callable=task2_tratar_datas,
    )

    # ── TASK-3 ──────────────────────────────────
    task3 = PythonOperator(
        task_id="task3_remover_sem_musica",
        python_callable=task3_remover_sem_musica,
    )

    # ── TASK-4 ──────────────────────────────────
    # Recebe via XCom a quantidade de descartados da TASK-3
    task4 = SQLExecuteQueryOperator(
        task_id="task4_inserir_descartados",
        conn_id="postgres_default",
        sql="""
            INSERT INTO descartados (total)
            VALUES ({{ ti.xcom_pull(task_ids='task3_remover_sem_musica',
                                   key='qtd_descartados') }});
        """,
    )

    # ── TASK-5 ──────────────────────────────────
    # O resultado fica disponível via XCom (return_value) para a TASK-6
    task5 = SQLExecuteQueryOperator(
        task_id="task5_consultar_genero",
        conn_id="postgres_default",
        sql="SELECT id_genero, nome_genero FROM genero_musical;",
    )

    # ── TASK-6 ──────────────────────────────────
    task6 = PythonOperator(
        task_id="task6_enriquecer_genero",
        python_callable=task6_enriquecer_genero,
    )

    # ── TASK-7 (paralela com TASK-8) ────────────
    task7 = PythonOperator(
        task_id="task7_media_avaliacao",
        python_callable=task7_media_avaliacao,
    )

    # ── TASK-8 (paralela com TASK-7) ────────────
    task8 = PythonOperator(
        task_id="task8_total_artista",
        python_callable=task8_total_artista,
    )

    # ── TASK-9 ──────────────────────────────────
    # trigger_rule=ALL_DONE: executa mesmo se TASK-7 ou TASK-8 falharem
    task9 = BashOperator(
        task_id="task9_remover_entrada",
        bash_command="rm -f /opt/airflow/data/entrada.csv",
        trigger_rule="all_done",
    )

    # ── TASK-10 ─────────────────────────────────
    task10 = EmptyOperator(
        task_id="task10_fim_processamento",
    )

    # ──────────────────────────────────────────────
    # Dependências
    #
    #  task1 → task2 → task3 → task4 ─────────────────────┐
    #                       ↘                              ↓
    #                        task5 → task6 → task7 ──→ task9 → task10
    #                                      ↘ task8 ──↗
    # ──────────────────────────────────────────────
    task1 >> task2 >> task3 >> [task4, task5]
    task5 >> task6 >> [task7, task8]
    [task7, task8] >> task9 >> task10