import streamlit as st
import pandas as pd
import psycopg2
import math
import re
from streamlit_tags import st_tags
from pymongo import MongoClient
from datetime import datetime, timezone
from unidecode import unidecode
from sqlalchemy import create_engine
from zoneinfo import ZoneInfo
from bson import ObjectId
import io
import pandas as pd
import numpy as np
import re
from unidecode import unidecode
import pandas as pd
import numpy as np  

# Page Setup
st.set_page_config(page_title="ETL FEFAL", layout="wide")

# Global Variables
default_year = datetime.now().year + 1
prefixes = [
    r"^\s*(municipio|município|camara municipal|cm|c m)(\s+(de|do|da|dos|das))?\s+",
    r"^\s*(freguesia|junta de freguesia|uniao de freguesias|uniao das freguesias)(\s+(de|do|da|dos|das))?\s+"
]

# Functions
def normalize_text(text):
    if not isinstance(text, str):
        return ""
    text = text.strip()
    text = unidecode(text)
    text = re.sub(r"\s+", " ", text)
    return text.lower()
def create_map(list, old_key, new_key):
    return {
        normalize_text(i[old_key]): i[new_key]
        for i in list
        if old_key in i and new_key in i
    }
def rename_cols(df, map, strict=False):
    renamed_cols = {}
    for col in df.columns:
        col_norm = normalize_text(col)
        if col_norm in map:
            renamed_cols[col] = map[col_norm]
    df_renomeado = df.rename(columns=renamed_cols)
    if strict:
        df_renomeado = df_renomeado[list(renamed_cols.values())]

    return df_renomeado
def remove_prefixes(text, prefixes):
    text = normalize_text(text)
    for prefix in prefixes:
        text = re.sub(prefix, "", text)
    return text.strip()
def query_to_df(cur, sql: str) -> pd.DataFrame:
    cur.execute(sql)
    data = cur.fetchall()
    column_names = [desc[0] for desc in cur.description]
    return pd.DataFrame(data, columns=column_names)
def run_etl(year: int, df: pd.DataFrame, mongo_db, cur_sii) -> tuple[dict[str, pd.DataFrame], pd.DataFrame, pd.DataFrame]:
    configs = load_mongo_configs(mongo_db, year)
    group_dfs = split_column_groups(df, configs["groups"])

    # Rname Cols
    for name, df in group_dfs.items():
        df.columns = [normalize_text(col).strip() for col in df.columns]
        group_dfs[name] = rename_cols(df, configs["map_ren_col"], strict=(name == "identificacao"))


    # Process identification
    df_id = group_dfs["identificacao"]  
    df_id = df_id[~df_id["nome_entidade"].apply(normalize_text).isin(["", "nd", "nan", "n/a", "na", "não definido", "sem dados", None])]

    if "tipo_entidade" in df_id:
        df_id["tipo_entidade"] = df_id["tipo_entidade"].apply(lambda x: configs["map_ent"].get(normalize_text(x), x))
    else:
        df_id["tipo_entidade"] = "Municípios"

    df_id["nome_entidade_norm"] = df_id["nome_entidade"].apply(lambda x: remove_prefixes(normalize_text(x), prefixes))

    df_sii = query_to_df(cur_sii, "SELECT id_entidades, ent_nome, ent_tipo FROM entidades")
    df_sii["ent_nome"] = df_sii["ent_nome"].apply(lambda x: remove_prefixes(x, prefixes))
    df_sii["ent_tipo"] = df_sii["ent_tipo"].apply(normalize_text)

    df_id["entity_key"] = df_id["nome_entidade_norm"] + "||" + df_id["tipo_entidade"].apply(normalize_text)
    df_sii["entity_key"] = df_sii["ent_nome"] + "||" + df_sii["ent_tipo"]
    map_entity = dict(zip(df_sii["entity_key"], df_sii["id_entidades"]))
    df_id["id_entidade"] = df_id["entity_key"].map(map_entity)
    group_dfs["identificacao"] = df_id

    group_dfs = process_completion_percentage(group_dfs)
    group_dfs = initialize_time_fields(group_dfs)
    group_dfs = process_additional_fields(group_dfs, year)
    group_dfs = process_formations(group_dfs)
    # group_dfs = process_interests(group_dfs, configs["interests_keys"])
    group_dfs = process_availability(group_dfs)
    group_dfs = validate_preferences(group_dfs)

    full_data = pd.concat(group_dfs.values(), axis=1).reset_index(drop=True)
    df_id_full = group_dfs["identificacao"].reset_index(drop=True)

    valid_id_mask = df_id_full["id_entidade"].notna()
    duplicate_mask_partial = df_id_full[valid_id_mask].duplicated(subset="id_entidade", keep="first")
    duplicate_mask = pd.Series(False, index=df_id_full.index)
    duplicate_mask[duplicate_mask_partial.index] = duplicate_mask_partial

    unmatched_mask = df_id_full["id_entidade"].isna()

    duplicate_df = full_data.iloc[duplicate_mask[duplicate_mask].index].reset_index(drop=True)
    unmatched_df = full_data.iloc[unmatched_mask[unmatched_mask].index].reset_index(drop=True)

    cols_to_remove = ["nome_entidade_norm", "entity_key", "data_inicio", "data_fim", "__pct", "__tempo"]
    duplicate_df.drop(columns=[col for col in cols_to_remove if col in duplicate_df.columns], inplace=True, errors="ignore")
    unmatched_df.drop(columns=[col for col in cols_to_remove if col in unmatched_df.columns], inplace=True, errors="ignore")

    valid_idxs = df_id_full[~(duplicate_mask | unmatched_mask)].index
    for group in group_dfs:
        group_dfs[group] = group_dfs[group].reset_index(drop=True).loc[valid_idxs].reset_index(drop=True)

    group_dfs["identificacao"].drop(columns=cols_to_remove, inplace=True, errors="ignore")

    return group_dfs, duplicate_df, unmatched_df
def load_mongo_configs(mongo_db, year: int) -> dict:
    interests_keys = mongo_db["ConfigAdvence"].find_one({"_id": ObjectId("682b5773188a7521e801a4e5")})
    ren_col = list(mongo_db["ConfigRenCol"].find({}, {"_id": 0}))
    col_map = mongo_db["ConfigColMap"].find_one({"year": year})
    ent_map = list(mongo_db["ConfigMapEnt"].find({}, {"_id": 0}))
    return {
        "map_ren_col": create_map(ren_col, "original_name", "new_name"),
        "map_ent": create_map(ent_map, "tipo_entidade_inq", "tipo_entidade_norm"),
        "groups": col_map["groups"],
        "interests_keys": {
            "comment_keys": interests_keys.get("keys", {}).get("comment_keys", []) if interests_keys else [],
            "formando_keys": interests_keys.get("keys", {}).get("formando_keys", []) if interests_keys else [],
            "default_type": interests_keys.get("default_type", "interesse") if interests_keys else "interesse"
        }
    }
def split_column_groups(df: pd.DataFrame, groups: dict) -> dict[str, pd.DataFrame]:
    return {
        name: df.iloc[:, lims["start"] - 1:lims["end"]]
        for name, lims in groups.items()
    }
def process_identification(group_dfs, configs, cur_sii):
    df_id = group_dfs["identificacao"]
    df_id.columns = [normalize_text(col) for col in df_id.columns]
    df_id = rename_cols(df_id, configs["map_ren_col"], True)
    if "nome_entidade" not in df_id:
        return {}, pd.DataFrame()
    df_id = df_id[~df_id["nome_entidade"].apply(normalize_text).isin(["", "nd", "nan", "n/a", "na", "não definido", "sem dados", None])]
    if "tipo_entidade" in df_id:
        df_id["tipo_entidade"] = df_id["tipo_entidade"].apply(lambda x: configs["map_ent"].get(normalize_text(x), x))
    else:
        df_id["tipo_entidade"] = "Municípios"
    df_id["nome_entidade_norm"] = df_id["nome_entidade"].apply(lambda x: remove_prefixes(normalize_text(x), prefixes))
    df_sii = query_to_df(cur_sii, "SELECT id_entidades, ent_nome, ent_tipo FROM entidades")
    df_sii["ent_nome"] = df_sii["ent_nome"].apply(lambda x: remove_prefixes(x, prefixes))
    df_sii["ent_tipo"] = df_sii["ent_tipo"].apply(normalize_text)
    df_id["entity_key"] = df_id["nome_entidade_norm"] + "||" + df_id["tipo_entidade"].apply(normalize_text)
    df_sii["entity_key"] = df_sii["ent_nome"] + "||" + df_sii["ent_tipo"]
    map_entity = dict(zip(df_sii["entity_key"], df_sii["id_entidades"]))
    df_id["id_entidade"] = df_id["entity_key"].map(map_entity)
    valid_idxs = df_id[df_id["id_entidade"].notna()].index
    invalid_idxs = df_id[df_id["id_entidade"].isna()].index
    unmatched_df = df_id.loc[invalid_idxs].copy()
    for group in group_dfs:
        group_dfs[group] = group_dfs[group].loc[valid_idxs].reset_index(drop=True)
    group_dfs["identificacao"] = df_id.loc[valid_idxs].reset_index(drop=True)
    return group_dfs, unmatched_df
def process_completion_percentage(group_dfs):
    df = group_dfs["identificacao"]
    if "percentagem_preenchido" in df.columns:
        df["percentagem_preenchido"] = pd.to_numeric(df["percentagem_preenchido"], errors="coerce")
        df.loc[df["percentagem_preenchido"] < 0, "percentagem_preenchido"] = pd.NA
        max_pct = df["percentagem_preenchido"].max(skipna=True)
        if pd.notna(max_pct) and max_pct > 0:
            df["percentagem_preenchido"] = ((df["percentagem_preenchido"] / max_pct) * 100).round().astype("Int64")
    else:
        df["percentagem_preenchido"] = pd.NA
    group_dfs["identificacao"] = df
    return group_dfs
def initialize_time_fields(group_dfs):
    df = group_dfs["identificacao"]
    if {"data_inicio", "data_fim"}.issubset(df.columns):
        df["data_inicio"] = pd.to_datetime(df["data_inicio"], errors="coerce")
        df["data_fim"] = pd.to_datetime(df["data_fim"], errors="coerce")
        df["tempo_realizacao"] = (df["data_fim"] - df["data_inicio"]).dt.total_seconds()
        df.loc[df["tempo_realizacao"] <= 0, "tempo_realizacao"] = pd.NA
        df["tempo_realizacao"] = df["tempo_realizacao"].astype("Int64")
    else:
        df["tempo_realizacao"] = pd.Series([pd.NA] * len(df), dtype="Int64")
    group_dfs["identificacao"] = df
    return group_dfs
def remove_entity_duplicates(group_dfs):
    df = group_dfs["identificacao"].copy()
    df["_pct"] = df["percentagem_preenchido"].fillna(-1)
    df["_tempo"] = df["tempo_realizacao"].fillna(-1)
    df.sort_values(["id_entidade", "_pct", "_tempo"], ascending=[True, False, False], inplace=True)
    keep_mask = ~df.duplicated("id_entidade", keep="first")
    valid_idxs = df[keep_mask].index

    for group in group_dfs:
        group_dfs[group] = group_dfs[group].loc[valid_idxs].reset_index(drop=True)

    duplicate_df = df[~keep_mask].drop(columns=["_pct", "_tempo"]).reset_index(drop=True)
    return group_dfs, duplicate_df
def process_additional_fields(group_dfs, year):
    df = group_dfs["identificacao"]
    df["ano"] = year
    if "nome_responsavel" not in df.columns:
        df["nome_responsavel"] = pd.NA
    df["data_submissao"] = pd.to_datetime(df.get("data_submissao", pd.NaT), errors="coerce")
    if "data_fim" in df.columns:
        df["data_fim"] = pd.to_datetime(df["data_fim"], errors="coerce")
        df["data_submissao"] = df["data_submissao"].fillna(df["data_fim"])
    group_dfs["identificacao"] = df
    return group_dfs
def process_formations(group_dfs):
    if "formacoes" not in group_dfs or group_dfs["formacoes"].empty:
        return group_dfs

    def clean_column_names(text):
        return str(text).strip() if pd.notna(text) else ""

    def validate_numeric(v):
        try:
            num = int(float(v))
            return max(num, 0)
        except:
            return 0

    df = group_dfs["formacoes"]
    df.columns = [clean_column_names(col) for col in df.columns]
    for col in df.columns:
        df[col] = df[col].apply(validate_numeric).astype("Int64")
    group_dfs["formacoes"] = df
    return group_dfs
def process_interests(group_dfs, interests_keys):
    if "interesses" not in group_dfs or group_dfs["interesses"].empty:
        return group_dfs

    comment_keys = [normalize_text(k) for k in interests_keys.get("comment_keys", [])]

    def map_interest(val):
        v = normalize_text(str(val))
        if v == "sim": return 1
        if v == "nao": return 0
        return pd.NA

    df = group_dfs["interesses"]
    for col in df.columns:
        if all(kw not in normalize_text(col) for kw in comment_keys):
            df[col] = df[col].apply(map_interest).astype("Int64")

    group_dfs["interesses"] = df
    return group_dfs
def process_availability(group_dfs):
    if "disponibilidade" not in group_dfs or group_dfs["disponibilidade"].empty:
        return group_dfs

    def map_disp(val):
        v = normalize_text(str(val))
        if v == "sim": return 1
        if v == "nao": return 0
        return pd.NA

    df = group_dfs["disponibilidade"]
    for col in df.columns:
        df[col] = df[col].apply(map_disp).astype("Int64")

    group_dfs["disponibilidade"] = df
    return group_dfs
def validate_preferences(group_dfs):
    df = group_dfs.get("tipo de ensino")
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return group_dfs

    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype("Int64")

    group_dfs["tipo de ensino"] = df
    return group_dfs
def numeric_input(label, value=0, min_value=0, key=None):

    val = st.text_input(label, value=str(value if value != 0 else ""), key=key)
    try:
        num = int(val)
        if num < min_value:
            st.warning(f"{label} deve ser ≥ {min_value}")
            return min_value
        return num
    except ValueError:
        return min_value
def normalize_text(texto):
    if not isinstance(texto, str) or not texto.strip():  
        return ""  
    
    texto = texto.strip()                # Remover espaços no início e no fim
    texto = unidecode(texto)             # Remover acentos
    texto = re.sub(r"\s+", " ", texto)   # Substituir múltiplos espaços por um único espaço
    texto = texto.lower()                # Converter para minusculas
    
    return texto
def connect_sii():
    try:
        conn = psycopg2.connect(
            host="10.90.0.50",
            database="siianon",
            user="isec",
            password="isec",
            options="-c client_encoding=utf8"
        )
        cur = conn.cursor()
        st.session_state["sii_conn"] = conn
        st.session_state["sii_cur"] = cur
        return True
    except Exception as e:
        st.session_state["sii_conn"] = None
        st.session_state["sii_cur"] = None
        print(f"\nError connecting SII: {e}\n")
    return False
def connect_mongo():
    try:
        client = MongoClient(
            "mongodb://isec:EH9abf9u@10.90.0.50:27017/?authSource=isec",
            serverSelectionTimeoutMS=3000
        )
        client.server_info()  # Testa a ligação
        st.session_state.mdb = client["isec"]
        st.session_state.mongo_connected = True
        return True
    except Exception as e:
        st.session_state.mdb = None
        st.session_state.mongo_connected = False
        print(f"\nError connecting MongoDB: {e}\n")
    return False
def move_group(indice, direcao):
    nova_ordem = st.session_state.ordem_grupos.copy()
    novo_indice = indice + direcao
    if 0 <= novo_indice < len(nova_ordem):
        nova_ordem[indice], nova_ordem[novo_indice] = nova_ordem[novo_indice], nova_ordem[indice]
        st.session_state.ordem_grupos = nova_ordem
        st.rerun()
def get_max_id(cur, tabela, campo_id):
    cur.execute(f"SELECT COALESCE(MAX({campo_id}), 0) FROM {tabela}")
    return cur.fetchone()[0]
def descarregar_dados_para_postgres(group_dfs: dict[str, pd.DataFrame]):
    import psycopg2
    import pandas as pd

    TRUNCAR_TABELAS = False

    conn_inq = psycopg2.connect(
        host="10.90.0.50",
        database="postgres",
        user="isec",
        password="isec",
        options="-c client_encoding=utf8"
    )
    cur_inq = conn_inq.cursor()

    def truncar_tabelas():
        tabelas = [
            "resposta_formacao_inquerito",
            "resposta_interesse_area_inquerito",
            "resposta_preferencia_ensino_inquerito",
            "resposta_disponibilidade_horaria_inquerito",
            "formacao",
            "area_tematica",
            "preferencia_ensino",
            "disponibilidade_horaria",
            "inquerito"
        ]
        for tabela in tabelas:
            cur_inq.execute(f"TRUNCATE {tabela} CASCADE;")

    if TRUNCAR_TABELAS:
        truncar_tabelas()

    def get_max_id(cursor, table, id_field):
        cursor.execute(f"SELECT COALESCE(MAX({id_field}), 0) FROM {table};")
        return cursor.fetchone()[0]

    base_ids = {}
    if not TRUNCAR_TABELAS:
        base_ids["formacao"] = get_max_id(cur_inq, "formacao", "id_formacao") + 1
        base_ids["inquerito"] = get_max_id(cur_inq, "inquerito", "id_inquerito") + 1
        base_ids["res_formacao"] = get_max_id(cur_inq, "resposta_formacao_inquerito", "id_resposta_formacao_inquerito") + 1
        base_ids["area"] = get_max_id(cur_inq, "area_tematica", "id_interesse") + 1
        base_ids["res_area"] = get_max_id(cur_inq, "resposta_interesse_area_inquerito", "id_resposta_interesse_area_inquerito") + 1
        base_ids["pref"] = get_max_id(cur_inq, "preferencia_ensino", "id_preferencia") + 1
        base_ids["res_pref"] = get_max_id(cur_inq, "resposta_preferencia_ensino_inquerito", "id_resposta_preferencia_ensino_inquerito") + 1
        base_ids["disp"] = get_max_id(cur_inq, "disponibilidade_horaria", "id_horario") + 1
        base_ids["res_disp"] = get_max_id(cur_inq, "resposta_disponibilidade_horaria_inquerito", "id_resposta_disponibilidade_horaria_inquerito") + 1
    else:
        base_ids = dict.fromkeys([
            "formacao", "inquerito", "res_formacao", "area",
            "res_area", "pref", "res_pref", "disp", "res_disp"
        ], 1)

    df_inq = group_dfs.get("identificacao")
    if df_inq is not None:
        if "existe_responsavel" in df_inq.columns:
            df_inq["existe_responsavel"] = df_inq["existe_responsavel"].map({"Sim": 1, "Não": 0, "sim": 1, "não": 0})
        else:
            df_inq["existe_responsavel"] = None

        for idx, row in df_inq.iterrows():
            cur_inq.execute("""
                INSERT INTO inquerito (id_inquerito, id_entidade, ano, data_submissao, existe_responsavel,
                                    nome_responsavel, percentagem_preenchido, tempo_realizacao)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                base_ids["inquerito"] + idx,
                int(row['id_entidade']) if pd.notna(row['id_entidade']) else None,
                int(row['ano']) if pd.notna(row['ano']) else None,
                row['data_submissao'].strftime("%Y-%m-%d") if pd.notna(row['data_submissao']) else None,
                row['existe_responsavel'] if pd.notna(row['existe_responsavel']) else None,
                row['nome_responsavel'] if pd.notna(row['nome_responsavel']) else None,
                int(row['percentagem_preenchido']) if pd.notna(row['percentagem_preenchido']) else None,
                int(row['tempo_realizacao']) if pd.notna(row['tempo_realizacao']) else None
            ))

    df_formacao = group_dfs.get("formacoes")
    if df_formacao is not None:
        nomes_formacoes = df_formacao.columns
        for idx, nome in enumerate(nomes_formacoes):
            cur_inq.execute("""
                INSERT INTO formacao (id_formacao, nome_formacao, id_formacao_base, id_grupo_formacao)
                VALUES (%s, %s, NULL, NULL)
            """, (base_ids["formacao"] + idx, nome))

        resposta_id = base_ids["res_formacao"]
        for i, row in df_formacao.iterrows():
            id_inquerito = base_ids["inquerito"] + i
            for j, n_formandos in enumerate(row):
                if pd.notna(n_formandos) and n_formandos > 0:
                    cur_inq.execute("""
                        INSERT INTO resposta_formacao_inquerito (id_resposta_formacao_inquerito, n_formandos, id_inquerito, id_formacao)
                        VALUES (%s, %s, %s, %s)
                    """, (resposta_id, int(n_formandos), id_inquerito, base_ids["formacao"] + j))
                    resposta_id += 1

    df_interesses = group_dfs.get("interesses")
    if df_interesses is not None:
        colunas_interesse = [c for c in df_interesses.columns if not c.endswith("[Comentário]")]
        for idx, nome in enumerate(colunas_interesse):
            cur_inq.execute("""
                INSERT INTO area_tematica (id_interesse, nome_area)
                VALUES (%s, %s)
            """, (base_ids["area"] + idx, nome))

        resposta_id = base_ids["res_area"]
        for i, row in df_interesses.iterrows():
            id_inquerito = base_ids["inquerito"] + i
            for j, col in enumerate(colunas_interesse):
                tem_interesse = row[col]
                comentario = row.get(col + "[Comentário]", None)
                comentario = comentario if pd.notna(comentario) else None
                if pd.notna(tem_interesse) and str(tem_interesse).strip().isdigit() and int(tem_interesse) > 0:
                    cur_inq.execute("""
                        INSERT INTO resposta_interesse_area_inquerito (id_resposta_interesse_area_inquerito, tem_interesse, n_formandos, comentario, id_inquerito, id_interesse)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (resposta_id, 1, int(tem_interesse), comentario, id_inquerito, base_ids["area"] + j))
                    resposta_id += 1

    df_pref = group_dfs.get("tipo de ensino")
    if df_pref is not None:
        colunas_pref = df_pref.columns
        for idx, nome in enumerate(colunas_pref):
            cur_inq.execute("""
                INSERT INTO preferencia_ensino (id_preferencia, descricao_preferencia)
                VALUES (%s, %s)
            """, (base_ids["pref"] + idx, nome))

        resposta_id = base_ids["res_pref"]
        for i, row in df_pref.iterrows():
            id_inquerito = base_ids["inquerito"] + i
            for j, col in enumerate(colunas_pref):
                valor = row[col]
                if pd.notna(valor):
                    cur_inq.execute("""
                        INSERT INTO resposta_preferencia_ensino_inquerito (id_resposta_preferencia_ensino_inquerito, valor_preferencia, id_inquerito, id_preferencia)
                        VALUES (%s, %s, %s, %s)
                    """, (resposta_id, int(valor), id_inquerito, base_ids["pref"] + j))
                    resposta_id += 1

    df_disp = group_dfs.get("disponibilidade")
    if df_disp is not None:
        colunas_disp = df_disp.columns
        for idx, nome in enumerate(colunas_disp):
            cur_inq.execute("""
                INSERT INTO disponibilidade_horaria (id_horario, descricao_horario)
                VALUES (%s, %s)
            """, (base_ids["disp"] + idx, nome))

        resposta_id = base_ids["res_disp"]
        for i, row in df_disp.iterrows():
            id_inquerito = base_ids["inquerito"] + i
            for j, col in enumerate(colunas_disp):
                tem_disp = row[col]
                if pd.notna(tem_disp):
                    cur_inq.execute("""
                        INSERT INTO resposta_disponibilidade_horaria_inquerito (id_resposta_disponibilidade_horaria_inquerito, tem_disponibilidade, id_inquerito, id_horario)
                        VALUES (%s, %s, %s, %s)
                    """, (resposta_id, int(tem_disp), id_inquerito, base_ids["disp"] + j))
                    resposta_id += 1

    conn_inq.commit()
    cur_inq.close()
    conn_inq.close()

    print("Inserção concluída com sucesso.")

# BD SII Conenction
if "sii_connected" not in st.session_state:
    st.session_state["sii_connected"] = connect_sii()

# MongoDB Conenction
if "mongo_connected" not in st.session_state:
    st.session_state["mongo_connected"] = connect_mongo()

# Página inicial
if "page" not in st.session_state:
    st.session_state.page = "home"

# Pages
def show_conection_sii_status():
    if st.session_state.get("sii_connected") is True:
        st.markdown("""
        <div style='position: fixed; top: 60px; right: 20px; 
                    background-color: #d4edda; color: #155724; 
                    padding: 10px 15px; border-left: 5px solid #28a745; 
                    border-radius: 4px; font-size: 13px; z-index: 1000; 
                    box-shadow: 0 0 5px rgba(0,0,0,0.1);
                    opacity: 0; animation: fadeIn 0.8s forwards;'>
            ✅ Ligação ao SII estabelecida
        </div>
        <style>
        @keyframes fadeIn {
            to { opacity: 1; }
        }
        </style>
        """, unsafe_allow_html=True)
    elif st.session_state.get("sii_connected") is False:
        st.markdown("""
        <div style='position: fixed; top: 60px; right: 20px; 
                    background-color: #fff3cd; color: #856404; 
                    padding: 10px 15px; border-left: 5px solid #ffeeba; 
                    border-radius: 4px; font-size: 13px; z-index: 1000; 
                    box-shadow: 0 0 5px rgba(0,0,0,0.1);
                    opacity: 0; animation: fadeIn 0.8s forwards;'>
            ⚠️ Sem ligação ao SII
        </div>
        <style>
        @keyframes fadeIn {
            to { opacity: 1; }
        }
        </style>
        """, unsafe_allow_html=True)
def show_conection_mongo_status():
    if  st.session_state.get("mongo_connected") is True:
        st.markdown("""
        <div style='position: fixed; top: 110px; right: 20px; 
                    background-color: #d4edda; color: #155724; 
                    padding: 10px 15px; border-left: 5px solid #28a745; 
                    border-radius: 4px; font-size: 13px; z-index: 1000; 
                    box-shadow: 0 0 5px rgba(0,0,0,0.1);
                    opacity: 0; animation: fadeIn 0.8s forwards;'>
            ✅ Ligação ao MongoDB estabelecida
        </div>
        <style>
        @keyframes fadeIn {
            to { opacity: 1; }
        }
        </style>
        """, unsafe_allow_html=True)
    elif st.session_state.get("mongo_connected") is False:
        st.markdown("""
        <div style='position: fixed; top: 110px; right: 20px; 
                    background-color: #fff3cd; color: #856404; 
                    padding: 10px 15px; border-left: 5px solid #ffeeba; 
                    border-radius: 4px; font-size: 13px; z-index: 1000; 
                    box-shadow: 0 0 5px rgba(0,0,0,0.1);
                    opacity: 0; animation: fadeIn 0.8s forwards;'>
            ⚠️ Sem ligação ao MongoDB
        </div>
        <style>
        @keyframes fadeIn {
            to { opacity: 1; }
        }
        </style>
        """, unsafe_allow_html=True)    
def show_home():
    st.markdown(
        "<div style='text-align: center; font-size: 40px; font-weight: bold;'>Bem-vindo ao sistema ETL</div>",
        unsafe_allow_html=True
    )

    st.markdown(
        "<div style='text-align: center; font-size: 18px;'>Configure e execute processos de ETL baseados em inquéritos da FEFAL</div>",
        unsafe_allow_html=True
    )

    st.markdown("<div style='margin-top: 50px;'></div>", unsafe_allow_html=True)

    col1, col2, col3, col4, col5 = st.columns([1,4,1,4,1])

    with col2:
        with st.container(height=400, border=True):
            st.markdown(
                """
                <div style='text-align: center; padding-top: 20px;'>
                    <h4>Criar nova configuração de ETL</h4>
                    <p>Defina os parâmetros e mapeamentos para um novo processo de extração, transformação e carregamento de dados.</p>
                    <p></p>
                    <p></p>
                </div>
                """,
                unsafe_allow_html=True
            )
            col_a1, col_a2, col_a3 = st.columns([1, 2, 1]) 
            with col_a2:
                if st.button("Criar nova configuração de ETL"):
                    st.session_state.page = "config"
                    st.rerun()

    with col4:
        with st.container(height=400, border=True):
            st.markdown(
                """
                <div style='text-align: center; padding-top: 20px;'>
                    <h4>Iniciar novo Processo de ETL</h4>
                    <p>Execute o processo de ETL com base numa configuração previamente definida.</p>
                </div>
                """,
                unsafe_allow_html=True
            )
            col_b1, col_b2, col_b3 = st.columns([1, 2, 1]) 
            with col_b2:
                if st.button("Iniciar novo Processo de ETL"):
                    st.session_state.page = "processo"
                    st.rerun()

    show_conection_mongo_status()
    show_conection_sii_status()
def show_config_page():
    st.title("Criar nova configuração de ETL para processo autumático")
    st.markdown("⚠️Esta funcionalidade ainda não está disponível⚠️")

    show_conection_mongo_status()
    show_conection_sii_status()

    st.markdown("<div style='margin-top: 20px;'></div>", unsafe_allow_html=True)
    if st.button("⬅️ Voltar"):
        st.session_state.page = "home"
        st.rerun()
def show_processo_page():
    st.title("Iniciar novo Processo de ETL")
    st.write("Execução do processo ETL com base numa configuração.")
    show_conection_mongo_status()
    show_conection_sii_status()

    # Selecionar ano
    valid_years = list(range(2020, datetime.now().year + 11))
    valid_years.reverse()
    default_year = datetime.now().year + 1
    if "selected_year" not in st.session_state:
        index_default = valid_years.index(default_year) if default_year in valid_years else 0
        selected_year = st.selectbox("Selecione o ano do inquérito", valid_years, index=index_default)
    else:
        selected_year = st.selectbox("Selecione o ano do inquérito", valid_years, index=valid_years.index(st.session_state.selected_year))
    st.session_state.selected_year = selected_year

    # Selecionar ficheiro
    uploaded_file = st.file_uploader(
        "Carregue o ficheiro Excel ou CSV do inquérito",
        type=["xlsx", "xls", "csv"]
    )
    if uploaded_file is not None:
        st.session_state.uploaded_file = uploaded_file
    uploaded_file = st.session_state.get("uploaded_file", None)
    if uploaded_file is not None:
        st.success(f"Ficheiro `{uploaded_file.name}` mantido.")
        try:
            if uploaded_file.name.endswith(".csv"):
                df = pd.read_csv(uploaded_file)
            else:
                df_original = pd.read_excel(uploaded_file)
            st.session_state.df_original = df_original
            df_original = df_original.astype(str)
            st.dataframe(df_original.head(10),hide_index=True)
        except Exception as e:
            st.error(f"Erro ao ler o ficheiro: {e}")

    # Botões de navegação
    if "confirm_back" not in st.session_state:
        st.session_state.confirm_back = False
    if "invalid_advance" not in st.session_state:
        st.session_state.invalid_advance = False
    if st.session_state.invalid_advance:
        if uploaded_file is None:
            st.error("Por favor, carregue um ficheiro antes de prosseguir.")
        elif selected_year not in valid_years:
            st.error("Ano selecionado inválido.")
    if st.session_state.confirm_back:
        st.warning("⚠️ Os dados preenchidos serão perdidos. Tem a certeza que quer voltar? ⚠️")
        col1, col2 = st.columns([10.5, 1])
        with col1:
            if st.button("✅ Confirmar"):
                st.session_state.confirm_back = False
                st.session_state.invalid_advance = False
                st.session_state.page = "home"
                st.rerun()
        with col2:
            if st.button("❌ Cancelar"):
                st.session_state.confirm_back = False
                st.rerun()
    else:
        col1, col2 = st.columns([11, 1])
        with col1:
            if st.button("⬅️ Voltar"):
                st.session_state.confirm_back = True
                st.rerun()
        with col2:
            if st.button("Avançar ➡️"):
                if uploaded_file is None or selected_year not in valid_years:
                    st.session_state.invalid_advance = True
                else:
                    st.session_state.invalid_advance = False
                    st.session_state.page = "process_col_remover"
                st.rerun()
def show_process_col_remover_page():
    st.title("Processo de ETL - Remoção de Colunas")
    st.write("Selecione as colunas que devem ser removidas durante a transformação dos dados.")

    df_original = st.session_state.get("df_original")
    colunas = list(df_original.columns)
    total_colunas = len(colunas)

    # Inicializar estados
    if "columns_to_remove" not in st.session_state:
        st.session_state.columns_to_remove = set()
    if "mostrar_confirmacao" not in st.session_state:
        st.session_state.mostrar_confirmacao = False
    if "confirmar_limpeza" not in st.session_state:
        st.session_state.confirmar_limpeza = False

    # Limpar seleção se confirmado
    if st.session_state.confirmar_limpeza:
        st.session_state.columns_to_remove.clear()
        st.session_state.mostrar_confirmacao = False
        st.session_state.confirmar_limpeza = False
        st.rerun()

    tab1, tab2 = st.tabs(["Por índice", "Por nome"])

    with tab1:
        col_esq, col_dir = st.columns([1, 2])

        with col_esq:
            with st.container(border=True):
                st.subheader("Remover colunas por índice")
                st.markdown("<div style='height: 18px;'></div>", unsafe_allow_html=True)

                # Inicializar mensagens
                if "msg_tipo" not in st.session_state:
                    st.session_state.msg_tipo = None
                if "msg_texto" not in st.session_state:
                    st.session_state.msg_texto = ""

                # Formulário de remoção individual
                with st.form("remover_individual"):
                    idx_unico = st.number_input(
                        "Índice da coluna a remover",
                        min_value=1, max_value=total_colunas, step=1,
                        key="idx_unico"
                    )
                    if st.form_submit_button("🗑️"):
                        if idx_unico in st.session_state.columns_to_remove:
                            st.session_state.msg_tipo = "warning"
                            st.session_state.msg_texto = f"Índice {idx_unico} já está marcado."
                        else:
                            st.session_state.columns_to_remove.add(idx_unico)
                            st.session_state.msg_tipo = "success"
                            st.session_state.msg_texto = f"Índice {idx_unico} adicionado."

                # Bloco de mensagem centralizado entre os forms
                st.markdown("<div style='height: 5px;'></div>", unsafe_allow_html=True)
                if st.session_state.msg_tipo == None:
                    st.info("Selecione as colunas que pretende remover")
                if st.session_state.msg_tipo == "success":
                    st.success(st.session_state.msg_texto)
                elif st.session_state.msg_tipo == "warning":
                    st.warning(st.session_state.msg_texto)
                elif st.session_state.msg_tipo == "error":
                    st.error(st.session_state.msg_texto)

                st.markdown("<div style='height: 5px;'></div>", unsafe_allow_html=True)

                # Formulário de remoção por intervalo
                with st.form("remover_intervalo"):
                    col1, col2 = st.columns(2)
                    with col1:
                        inicio = st.number_input("Início", min_value=1, max_value=total_colunas, step=1)
                    with col2:
                        fim = st.number_input("Fim", min_value=1, max_value=total_colunas, step=1)

                    if st.form_submit_button("🗑️"):
                        if inicio > fim:
                            st.session_state.msg_tipo = "error"
                            st.session_state.msg_texto = "Início não pode ser maior que o fim."
                        else:
                            novos = {i for i in range(inicio, fim + 1)}
                            novos -= st.session_state.columns_to_remove
                            if novos:
                                st.session_state.columns_to_remove.update(novos)
                                st.session_state.msg_tipo = "success"
                                st.session_state.msg_texto = f"Intervalo {inicio}-{fim} adicionado."
                            else:
                                st.session_state.msg_tipo = "warning"
                                st.session_state.msg_texto = "Todos os índices já estavam selecionados."

                st.markdown("<div style='height: 20px;'></div>", unsafe_allow_html=True)


        with col_dir:
            with st.container(border=True):
                st.subheader("Visualização das colunas")

                ctrl1, ctrl2, ctrl3 = st.columns([1, 1, 1])
                with ctrl1:
                    colunas_por_pagina = st.number_input(
                        "Nº de colunas por página", min_value=5, max_value=100, value=10, step=5
                    )
                num_paginas = math.ceil(total_colunas / colunas_por_pagina)
                with ctrl2:
                    pagina_atual = st.number_input(
                        "Página", min_value=1, max_value=num_paginas, value=1, step=1
                    )
                with ctrl3:
                    st.markdown(
                        f"""
                        <div style="margin-top:30px; font-weight:bold; font-size:20px">
                            Total de colunas: {total_colunas}
                        </div>
                        """, unsafe_allow_html=True
                    )

                inicio = (pagina_atual - 1) * colunas_por_pagina
                fim = min(inicio + colunas_por_pagina, total_colunas)
                st.markdown(f"**Colunas {inicio + 1} a {fim}**")

                for i in range(inicio, fim):
                    idx_visual = i + 1
                    nome = colunas[i]
                    if idx_visual in st.session_state.columns_to_remove:
                        cor = "#990000" if idx_visual % 2 == 0 else "#cc0000"
                    else:
                        cor = "#326d00" if i % 2 == 0 else "#121212"
                    st.markdown(
                        f"""
                        <div title="{nome}" style="
                            background-color:{cor};
                            padding:6px 10px;
                            border-radius:6px;
                            margin-bottom:4px;
                            font-size:13px;
                            color:white;
                            height:32px;
                            line-height:20px;
                            overflow:hidden;
                            white-space:nowrap;
                            text-overflow:ellipsis;
                        ">
                            <strong>{idx_visual}</strong>: {nome}
                        </div>
                        """, unsafe_allow_html=True
                    )

    with tab2:
        st.header("Remover colunas por nome")

        if "palavras_chave" not in st.session_state:
            st.session_state["palavras_chave"] = ""

        with st.container():
            st.markdown("#### Palavra(s)")
            palavras_chave = st.text_input(
                "Insira palavra(s) separadas por espaço:",
                key="palavras_chave"
            )

        with st.container():
            st.markdown("#### Colunas encontradas")

            # Normalizar colunas
            colunas_normalizadas = [normalize_text(c) for c in colunas]
            colunas_encontradas = []
            if palavras_chave:
                termos = palavras_chave.split()
                termos_normalizados = [normalize_text(t) for t in termos]
                for i, col_norm in enumerate(colunas_normalizadas):
                    if any(t in col_norm for t in termos_normalizados):
                        colunas_encontradas.append(colunas[i])

        if colunas_encontradas:
            selecao_manual = st.multiselect(
                "Desmarque as que não quer remover:",
                options=colunas_encontradas,
                default=colunas_encontradas,
                key="colunas_encontradas"
            )
        else:
            selecao_manual = []

        if st.button("Adicionar à lista de remoção"):
            if selecao_manual:
                indices_a_remover = {colunas.index(nome) + 1 for nome in selecao_manual}
                if "columns_to_remove" not in st.session_state:
                    st.session_state.columns_to_remove = set()
                st.session_state.columns_to_remove.update(indices_a_remover)

            # Limpar apenas se existir
            st.session_state.pop("colunas_encontradas", None)
            st.session_state.pop("palavras_chave", None)

            st.rerun()

    st.divider()

    # Mostrar colunas marcadas para remoção com botão de reposição
    st.markdown("### Colunas selecionadas para remoção:")

    with st.container(border=True):
        if st.session_state.columns_to_remove:
            for idx in sorted(st.session_state.columns_to_remove):
                nome = colunas[idx - 1]
                col1, col2 = st.columns([8, 2])
                with col1:
                    st.markdown(f"- **{idx}**: {nome}")
                with col2:
                    if st.button(f"🔁 Repor", key=f"repor_{idx}"):
                        st.session_state.columns_to_remove.remove(idx)
                        st.success(f"Coluna '{nome}' foi reposta.")
                        st.rerun()
        else:
            st.markdown("*Nenhuma coluna foi selecionada.*")


    col1, col2, col3 = st.columns([1, 9, 1])
    with col1:
        if st.button("⬅️ Voltar", use_container_width=True):
            st.session_state.page = "processo"
            st.rerun()
    with col3:
        if st.button("Avançar ➡️", use_container_width=True):

            # Create New DF
            col_indices_remover = sorted(st.session_state.columns_to_remove)
            colunas_remover = [colunas[i - 1] for i in col_indices_remover]
            df_new = df_original.drop(columns=colunas_remover)
            st.session_state.df_new = df_new
            
            st.session_state.page = "process_groups"
            st.rerun()
def show_process_groups():
    st.title("Definição dos Grupos de Colunas")
    st.write("Indique quantas colunas pertencem a cada grupo.")

    df_new = st.session_state.get("df_new")
    if df_new is None:
        st.error("Nenhum DataFrame carregado.")
        col1, col2 = st.columns([10.5, 1])
        with col1:
            if st.button("⬅️ Voltar"):
                st.session_state.page = "process_col_remover"
                st.rerun()
        st.stop()

    columns = list(df_new.columns)
    total_columns = len(columns)

    col_esq, col_dir = st.columns([1, 2])
    with col_esq:
        with st.container(border=True):
            st.subheader("Definir grupos")  

            default_groups = ["identificacao", "formacoes", "interesses", "disponibilidade", "tipo de ensino"]

            # Inicialização compacta
            st.session_state.setdefault("ordem_grupos", default_groups.copy())
            st.session_state.setdefault("grupos_validacao", {g: {"start": None, "end": None, "overlay": True} for g in default_groups})
            st.session_state.setdefault("grupos_ativos", {g: True for g in default_groups})

            group_order = st.session_state.ordem_grupos

            # Reordenar grupos
            def move_group(index, direction):
                new_order = st.session_state.ordem_grupos.copy()
                new_index = index + direction
                if 0 <= new_index < len(new_order):
                    new_order[index], new_order[new_index] = new_order[new_index], new_order[index]
                    st.session_state.ordem_grupos = new_order
                    st.rerun()

            previous_active_group_end = 0

            for i, group in enumerate(group_order):
                with st.container():
                    # Cabeçalho
                    st.markdown(f"""
                    <div style="background-color: #326d00; padding: 10px 15px; border-radius: 10px; margin-bottom: 10px;">
                        <div style="font-size: 16px; font-weight: bold;">#{i+1} - {group.capitalize()}</div>
                    </div>
                    """, unsafe_allow_html=True)

                    # Controles
                    c1, c2, c3, c4, c5 = st.columns([2, 2, 1, 1, 2])
                    active = st.session_state.grupos_ativos[group]

                    with c1:
                        start = st.number_input("Start", min_value=1, max_value=total_columns, step=1,
                                                key=f"{group}_start", label_visibility="collapsed", disabled=not active)
                    with c2:
                        end = st.number_input("End", min_value=1, max_value=total_columns, step=1,
                                            key=f"{group}_end", label_visibility="collapsed", disabled=not active)
                    with c3:
                        st.button("⬆️", key=f"up_{group}", disabled=(i == 0), on_click=move_group, args=(i, -1))
                    with c4:
                        st.button("⬇️", key=f"down_{group}", disabled=(i == len(group_order) - 1), on_click=move_group, args=(i, 1))
                    with c5:
                        prev = active
                        novo = st.checkbox("Ativo", value=prev, key=f"{group}_ativo", disabled=(group == "identificacao"))
                        if novo != prev:
                            st.session_state.grupos_ativos[group] = novo
                            st.rerun()

                    # Atualizar estado e validar
                    if not active:
                        st.session_state.grupos_validacao[group].update({"start": None, "end": None, "overlay": False})
                        continue

                    overlay = previous_active_group_end and start <= previous_active_group_end
                    st.session_state.grupos_validacao[group].update({"start": start, "end": end, "overlay": overlay})

    # Mensagens de validação visuais (com validação real de sobreposição)
    previous_end = 0
    for group in group_order:
        if not st.session_state.grupos_ativos[group]:
            continue

        start = st.session_state.grupos_validacao[group]["start"]
        end = st.session_state.grupos_validacao[group]["end"]

        if start is None or end is None or start > end:
            st.session_state.grupos_validacao[group]["overlay"] = False  # força False para evitar confusão
            st.error(f"⚠️ Intervalo inválido no grupo '{group}'")
            continue

        overlay = start <= previous_end
        st.session_state.grupos_validacao[group]["overlay"] = overlay

        if overlay:
            st.error(f"⚠️ Sobreposição com grupo anterior no grupo '{group}'")
        else:
            previous_end = end  # só avança se for válido e não sobreposto



    with col_dir:
        with st.container(border=True):
            st.subheader("Visualização das colunas")

            ctrl1, ctrl2, ctrl3 = st.columns([1, 1, 1])
            with ctrl1:
                cols_per_page = st.number_input("Nº de colunas por página", min_value=5, max_value=100, value=12, step=5)
            total_pages = math.ceil(total_columns / cols_per_page)
            with ctrl2:
                current_page = st.number_input("Página", min_value=1, max_value=total_pages, value=1, step=1)
            with ctrl3:
                st.markdown(f"""
                    <div style=\"margin-top:30px; font-weight:bold; font-size:20px\">
                        Total de colunas: {total_columns}
                    </div>""", unsafe_allow_html=True)

            start_idx = (current_page - 1) * cols_per_page
            end_idx = min(start_idx + cols_per_page, total_columns)

            st.markdown(f"**Colunas {start_idx + 1} a {end_idx}**")

            for i in range(start_idx, end_idx):
                col_name = columns[i]
                idx_display = i + 1
                bg_color = "#326d00" if i % 2 == 0 else "#121212"
                st.markdown(f"""
                    <div title=\"{col_name}\" style=\"
                        background-color:{bg_color};
                        padding:6px 10px;
                        border-radius:6px;
                        margin-bottom:4px;
                        font-size:13px;
                        color:white;
                        height:32px;
                        line-height:20px;
                        overflow:hidden;
                        white-space:nowrap;
                        text-overflow:ellipsis;
                    \">
                        <strong>{idx_display}</strong>: {col_name}
                    </div>""", unsafe_allow_html=True)

    # Preview active groups
    st.subheader("Pré-visualização dos grupos definidos")
    valid_groups = []
    for group in group_order:
        if not st.session_state.grupos_ativos.get(group, True):
            continue
        info = st.session_state.grupos_validacao.get(group, {})
        start, end = info.get("start"), info.get("end")
        overlap = info.get("overlay", True)
        if isinstance(start, int) and isinstance(end, int) and 1 <= start <= end <= len(columns) and not overlap:
            group_cols = columns[start - 1:end]
            valid_groups.append((group, start, end, group_cols))

    if len(valid_groups) == 0:
        st.info("Ainda não existe nenhum grupo válido")

    for i in range(0, len(valid_groups), 2):
        row = valid_groups[i:i + 2]
        cols = st.columns(len(row))
        for j, (group, start, end, group_cols) in enumerate(row):
            content = "".join(f"<li>{c}</li>" for c in group_cols)
            html = f"""
            <div style="
                border-radius: 16px;
                padding: 16px;
                background-color: #326d00;
                color: white;
                box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
                max-height: 250px;
                overflow-y: auto;
            ">
                <h4 style="margin-top:0;">{group.capitalize()} ({start}–{end})</h4>
                <ul style="margin-left: 20px; padding-right: 10px;">{content}</ul>
            </div>
            """
            with cols[j]:
                with st.expander(f"{group.capitalize()} ({start}–{end}) - {len(group_cols)} colunas", expanded=True):
                    st.markdown(html, unsafe_allow_html=True)

    # Navigation
    if "show_grupos_invalidos_message_error" not in st.session_state:
        st.session_state.show_grupos_invalidos_message_error = False
    elif st.session_state.show_grupos_invalidos_message_error:
        st.warning("⚠️ Os Grupos não são válidos para avançar para a próxima etapa! ⚠️")

    col1, col2 = st.columns([10.5, 1])
    with col1:
        if st.button("⬅️ Voltar"):
            st.session_state.page = "process_col_remover"
            st.session_state.show_grupos_invalidos_message_error = False
            st.rerun()
    with col2:
        if st.button("Avançar ➡️"):
            all_valid = all(
                g_data.get("start") is not None and
                g_data.get("end") is not None and
                g_data.get("start") <= g_data.get("end") and
                not g_data.get("overlay", False)
                for g, g_data in st.session_state.grupos_validacao.items()
                if st.session_state.grupos_ativos.get(g, True)
            )

            if all_valid:
                group_export = {
                    group: {
                        "start": data["start"],
                        "end": data["end"]
                    }
                    for group, data in st.session_state.grupos_validacao.items()
                    if st.session_state.grupos_ativos.get(group, True)
                    and isinstance(data.get("start"), int)
                    and isinstance(data.get("end"), int)
                    and 1 <= data["start"] <= data["end"] <= len(columns)
                    and not data.get("overlay", True)
                }
                uploaded_file = st.session_state.get("uploaded_file")
                file_name = uploaded_file.name if uploaded_file else None
                if "mdb" in st.session_state and st.session_state.mdb is not None:
                    collection_col_map = st.session_state.mdb["ConfigColMap"]
                    collection_col_map.update_one(
                        {"year": st.session_state.selected_year},
                        {
                            "$set": {
                                "file_path": file_name,
                                "groups": group_export,
                                "registration_date": datetime.now(timezone.utc)
                            }
                        },
                        upsert=True
                    )
                st.session_state.page = "process_map"
                st.session_state.show_grupos_invalidos_message_error = False
            else:
                st.session_state.show_grupos_invalidos_message_error = True
            st.rerun()
def show_process_map():

    # Connection MongoDB Validation
    if not st.session_state.mongo_connected:
        st.error("Imposível reconectar ao MongoDB")
        if st.button("Reconectar"):
            connect_mongo()
            st.rerun()

    # MongoDB Collections
    if st.session_state.get("mongo_connected", False):
        collection_advance_config = st.session_state.mdb["ConfigAdvanced"]
        collection_ren_col = st.session_state.mdb["ConfigRenCol"]
        collection_map_ent = st.session_state.mdb["ConfigMapEnt"]
    else:
        collection_advance_config = None
        collection_ren_col = None
        collection_map_ent = None

    # Nomalized Fields
    critical_fields = []
    non_critical_fields = []
    fields = []
    if st.session_state.mongo_connected:
        doc = collection_advance_config.find_one({"_id": ObjectId("681c76384a332df1948632e2")})
        if doc and isinstance(doc, dict):
            critical_fields = [f["field"] for f in doc.get("identification_fields", []) if f["critical"]]
            non_critical_fields = [f["field"] for f in doc.get("identification_fields", []) if not f["critical"]]
            fields = critical_fields + non_critical_fields

    # Fields Inquiry
    group_dfs = {}
    df_new = st.session_state.get("df_new")
    df_new.columns = [col.lstrip() for col in df_new.columns] # PASSAR ISTO PARA A PÁGINA DE LIMPEZA
    groups = st.session_state.get("grupos_validacao", {})
    for group_name, limites in groups.items():
        start = limites.get("start")
        end = limites.get("end")
        if isinstance(start, int) and isinstance(end, int) and 1 <= start <= end <= len(df_new.columns):
            group_dfs[group_name] = df_new.iloc[:, start - 1:end]


    # Fields Verification Matches
    cols = list(df_new.columns)
    unmatched_critical_fields = []
    unmatched_non_critical_fields = []
    cols_normalized = [normalize_text(c) for c in cols]
    for field in fields:
        field2col = [
            m["original_name"] for m in list(collection_ren_col.find({"new_name": field}, {"_id": 0, "original_name": 1}))
            if normalize_text(m.get("original_name", "")) in cols_normalized
        ]
        if not field2col:
            if field in critical_fields:
                unmatched_critical_fields.append(field)
            elif field in non_critical_fields:
                unmatched_non_critical_fields.append(field)

    # SII Entity Types 
    cur = st.session_state.get("sii_cur")
    if cur:
        cur.execute("SELECT DISTINCT ent_tipo FROM entidades;")
        entity_types_sii = [row[0] for row in cur.fetchall() if row[0] is not None]
    else:
        entity_types_sii = None

    # Entity Type Col Match
    if st.session_state.mongo_connected:
        entity_type_col = collection_ren_col.find_one({"new_name": "tipo_entidade"}, {"_id": 0, "original_name": 1})
        if entity_type_col and "original_name" in entity_type_col:
            entity_type_col = entity_type_col["original_name"]
    else:
        entity_type_col = None

    # Entities Types Survey
    if normalize_text(entity_type_col) in [normalize_text(col) for col in df_new.columns]:
        entities_types_surv = df_new[entity_type_col].dropna().unique().tolist()
    else:
        entities_types_surv = None

    # Main Interface
    st.markdown("## Mapeamentos")
    tab1, tab2, tab3 = st.tabs(["Renomear Colunas", "Mapear tipo de entidades", "Identificar Comentários"])
    with tab1:
        group_names = list(group_dfs.keys())
        selected_group = st.selectbox("Selecionar grupo para renomear", options=group_names)
        df_group = group_dfs[selected_group]
        cols = list(df_group.columns)

        if len(cols) == 0:
            st.warning(f"O grupo '{selected_group}' ainda não está corretamente definido.")
        else:
            # 1ª Secção
            col1, col2 = st.columns(2)
            with col1:
                with st.container(border=True):
                    st.markdown("### Criar ou Atualizar Mapeamento")
                    if selected_group == "identificacao":
                        new_name = st.selectbox("Nome Normalizado", options=fields)
                    else:
                        new_name = st.text_input("Nome Normalizado (campo livre)", placeholder="ex: 'interesse_formacao'")
                    original_name = st.selectbox("Coluna Original do Inquérito", options=cols)
                    if st.button("💾 Guardar Mapeamento"):
                        if new_name and original_name:
                            info = {
                                "original_name": original_name,
                                "new_name": new_name,
                                "critical": False,
                                "registration_date": datetime.now(timezone.utc)
                            }
                            collection_ren_col.update_one(
                                {"original_name": original_name},
                                {"$set": info},
                                upsert=True
                            )
                            st.rerun()
            with col2:
                with st.container(border=True):
                    st.markdown("### Mapeamentos com este Nome Normalizado")
                    if st.session_state.mongo_connected:
                        mappings = list(collection_ren_col.find({"new_name": new_name}, {"_id": 0}))
                        mappings_filtrados = [
                            m for m in mappings
                            if normalize_text(m.get("original_name", "")) in [normalize_text(c) for c in cols]
                        ]
                    else:
                        mappings_filtrados = None

                    if mappings_filtrados:
                        for m in mappings_filtrados:
                            original_name = m.get("original_name", "—")
                            def truncar_texto(texto, limite=70):
                                return texto if len(texto) <= limite else texto[:limite] + "..."

                            original_name_tunc = truncar_texto(original_name)
                            date = m.get("registration_date")
                            if date:
                                if date.tzinfo is None:
                                    date = date.replace(tzinfo=ZoneInfo("UTC"))
                                date_local = date.astimezone(ZoneInfo("Europe/Lisbon"))
                                data_formatada = date_local.strftime("%Y-%m-%d %H:%M")
                            else:
                                data_formatada = "—"

                            st.markdown(f"""
                            <div style='height: 50px;'></div>
                            <p><strong>Nome Inquérito:</strong> <span title="{original_name}">{original_name_tunc}</span></p>
                            <p><strong>Nome Normalizado:</strong> <span title="{new_name}">{new_name}</span></p>
                            <p><strong>Data de Atualização:</strong> <span title="{data_formatada}">{data_formatada}</span></p>
                            <div style='height: 50px;'></div>
                            """, unsafe_allow_html=True)

                    else:
           
                        st.markdown("<div style='height: 75px;'></div>", unsafe_allow_html=True)
                        st.warning("⚠️ Não existe nenhuma correspondência para este campo ⚠️")
      
                        st.markdown("<div style='height: 76px;'></div>", unsafe_allow_html=True)

            # 2ª Secção
            if unmatched_critical_fields:
                st.error(f"❌ Campos críticos sem correspondência no grupo 'identificacao': {', '.join(unmatched_critical_fields)}")
            else:
                st.success("✅ Todos os campos críticos têm correspondência no grupo 'identificacao'.")

            if unmatched_non_critical_fields:
                st.warning(f"⚠️ Campos não críticos sem correspondência no grupo 'identificacao': '{', '.join(unmatched_non_critical_fields)}'")
                st.session_state.mapping_confirm = True
            else:
                st.info("ℹ️ Todos os campos não críticos têm correspondência no grupo 'identificacao'.")


    with tab2:
        if "tipo_entidade" not in unmatched_non_critical_fields:
            with st.form("form_mapeamento_entidade"):
                tipo_entidade_inq = st.selectbox("Nome no inquérito", options=entities_types_surv)
                tipo_entidade_norm = st.selectbox("Nome Normalizado", options=entity_types_sii)
                submitted = st.form_submit_button("💾 Guardar Mapeamento")

                if submitted and collection_map_ent is not None:
                    if not tipo_entidade_inq:
                        st.error("❌ O nome da coluna no inquérito não pode estar vazio.")
                    else:
                        existing = collection_map_ent.find_one({"tipo_entidade_inq": tipo_entidade_inq})
                        if existing:
                            st.info("🔁 Mapeamento atualizado.")
                        else:
                            st.success("✅ Mapeamento criado.")

                        collection_map_ent.update_one(
                            {"tipo_entidade_inq": tipo_entidade_inq},
                            {
                                "$set": {
                                    "tipo_entidade_norm": tipo_entidade_norm,
                                    "data_atualizacao": datetime.now(timezone.utc)
                                }
                            },
                            upsert=True
                        )
        else:
            st.warning("É preciso definir a correspondência na coluna referente ao tipo das entidades primeiro.")

        st.markdown("### Mapeamentos Existentes")
        if collection_map_ent is not None:
            mappings = list(collection_map_ent.find({}, {"_id": 0}))
        else:
            mappings = []

        if mappings:
            df_mapeamentos = pd.DataFrame(mappings).rename(columns={
                "tipo_entidade_inq": "Coluna no Inquérito",
                "tipo_entidade_norm": "Nome Normalizado",
                "data_atualizacao": "Data de Atualização"
            })
            import pytz
            tz = pytz.timezone("Europe/Lisbon")
            df_mapeamentos["Data de Atualização"] = pd.to_datetime(
                df_mapeamentos["Data de Atualização"]
            ).dt.tz_localize('UTC').dt.tz_convert(tz).dt.strftime("%Y-%m-%d %H:%M")
            st.dataframe(df_mapeamentos[["Coluna no Inquérito", "Nome Normalizado", "Data de Atualização"]],
                         use_container_width=True, hide_index=True)
        else:
            st.info("ℹ️ Nenhum mapeamento encontrado.")

    with tab3:
        if "interesses" not in group_names:
            st.warning("Não Existe o grupo 'Interesses' para executar esta funcionalidade.")
        else:
            st.markdown("### Palavras-chave por Tipo de Coluna")

            # === Tentar carregar do Mongo ===
            doc = None
            if st.session_state.get("mongo_connected"):
                try:
                    doc = collection_advance_config.find_one({"_id": ObjectId("682b5773188a7521e801a4e5")})
                except:
                    st.warning("⚠️ Erro ao carregar doc do Mongo. Usar palavras locais.")
                    doc = None

            # === Preparar palavras e estado ===
            persistencia_ativa = bool(doc)
            if persistencia_ativa:
                comment_keys = doc.get("keys", {}).get("comment_keys", [])
                formando_keys = doc.get("keys", {}).get("formando_keys", [])
                default_type = doc.get("default_type", "interesse")
            else:
                if "keys_temporarias" not in st.session_state:
                    st.session_state.keys_temporarias = {
                        "comment_keys": [],
                        "formando_keys": []
                    }
                comment_keys = st.session_state.keys_temporarias["comment_keys"]
                formando_keys = st.session_state.keys_temporarias["formando_keys"]
                default_type = "interesse"

            tipos_chave = {
                "comentário": comment_keys,
                "numero de formandos": formando_keys
            }

            for tipo, lista in tipos_chave.items():
                with st.expander(f"🔧 {tipo.title()} ({len(lista)} palavras)", expanded=False):
                    st.markdown(
                        f"<ul style='margin-left: 10px'>{''.join(f'<li>{kw}</li>' for kw in lista)}</ul>",
                        unsafe_allow_html=True
                    )

                    # Adicionar
                    nova_palavra = st.text_input(f"Adicionar nova palavra a {tipo}", key=f"nova_{tipo}")
                    if st.button(f"➕ Adicionar a {tipo}", key=f"btn_add_{tipo}"):
                        nova_palavra = nova_palavra.strip().lower()
                        if nova_palavra and nova_palavra not in lista:
                            lista.append(nova_palavra)
                            if persistencia_ativa:
                                campo_mongo = "comment_keys" if tipo == "comentário" else "formando_keys"
                                collection_advance_config.update_one(
                                    {"_id": doc["_id"]},
                                    {"$set": {
                                        f"keys.{campo_mongo}": lista,
                                        "last_updated": datetime.now(timezone.utc).isoformat()
                                    }}
                                )
                                st.success("Palavra adicionada no MongoDB.")
                            else:
                                st.session_state.keys_temporarias[f"{'comment_keys' if tipo == 'comentário' else 'formando_keys'}"] = lista
                                st.info("Palavra adicionada temporariamente.")
                            st.rerun()
                        else:
                            st.warning("Palavra vazia ou já existe.")

                    # Remover
                    if lista:
                        palavra_remover = st.selectbox(f"Selecionar palavra para remover de {tipo}", lista, key=f"rem_{tipo}")
                        if st.button(f"❌ Remover de {tipo}", key=f"btn_rem_{tipo}"):
                            lista.remove(palavra_remover)
                            if persistencia_ativa:
                                campo_mongo = "comment_keys" if tipo == "comentário" else "formando_keys"
                                collection_advance_config.update_one(
                                    {"_id": doc["_id"]},
                                    {"$set": {
                                        f"keys.{campo_mongo}": lista,
                                        "last_updated": datetime.now(timezone.utc).isoformat()
                                    }}
                                )
                                st.success("Palavra removida do MongoDB.")
                            else:
                                st.session_state.keys_temporarias[f"{'comment_keys' if tipo == 'comentário' else 'formando_keys'}"] = lista
                                st.info("Palavra removida temporariamente.")
                            st.rerun()

            # === Função para classificar colunas ===
            def classificar_coluna(col):
                nome = normalize_text(col)
                if any(normalize_text(kw) in normalize_text(nome) for kw in comment_keys):
                    return "comentário"
                if any(normalize_text(kw) in normalize_text(nome) for kw in formando_keys):
                    return "numero de formandos"
                return default_type

            # === Obter colunas ===
            df_new = st.session_state.get("df_new")
            if df_new is None or df_new.empty:
                st.warning("DataFrame 'df_new' não está carregado.")
                return

            df_new.columns = [col.lstrip() for col in df_new.columns]
            grupos = st.session_state.get("grupos_validacao", {})
            interesse_pts = grupos.get("interesses", {})
            cols = []

            start, end = interesse_pts.get("start"), interesse_pts.get("end")
            if isinstance(start, int) and isinstance(end, int) and 1 <= start <= end <= len(df_new.columns):
                cols = list(df_new.columns[start - 1:end])

            if cols:
                df_class = pd.DataFrame({
                    "Coluna": cols,
                    "Tipo": [classificar_coluna(col) for col in cols]
                })

                tipos_encontrados = df_class["Tipo"].unique().tolist()
                
                st.markdown("---")
                st.markdown("### Distribuição de Colunas por Tipo")
                colunas_layout = st.columns(3)
        
                for i, tipo in enumerate(tipos_encontrados):
                    with colunas_layout[i % 3]:
                        colunas_do_tipo = df_class[df_class["Tipo"] == tipo]["Coluna"].tolist()
                        total = len(colunas_do_tipo)
                        st.markdown(f"### 🔹 {tipo.title()} ({total})")
                        with st.expander("Ver colunas"):
                            st.markdown(
                                f"<div style='max-height: 200px; overflow-y: auto;'><ul>{''.join(f'<li>{c}</li>' for c in colunas_do_tipo)}</ul></div>",
                                unsafe_allow_html=True
                            )

    # Navigation
    st.markdown("---")

    if "confirm_forward" not in st.session_state:
        st.session_state.confirm_forward = False
    if "invalid_advance" not in st.session_state:
        st.session_state.invalid_advance = False

    if st.session_state.invalid_advance:
        st.warning("Não é possível continuar sem mapear campos críticos.")

    if st.session_state.confirm_forward:
        st.warning("⚠️ Existem campos não críticos sem correspondência. Deseja avançar mesmo assim?")
        col1, col2 = st.columns([9.8, 1])
        with col1:
            if st.button("❌ Cancelar"):
                st.session_state.confirm_forward = False
                st.rerun()
        with col2:
            if st.button("✅ Confirmar"):
                st.session_state.confirm_forward = False
                st.session_state.invalid_advance = False
                st.session_state.page = "process_confirm_page"
                st.rerun()
    else:
        col1, col2 = st.columns([10.5, 1])
        with col1:
            if st.button("⬅️ Voltar"):
                st.session_state.invalid_advance = False
                st.session_state.page = "process_groups"
                st.rerun()
        with col2:
            if st.button("Avançar ➡️"):
                if unmatched_critical_fields:
                    st.session_state.invalid_advance = True
                elif unmatched_non_critical_fields:
                    st.session_state.confirm_forward = True
                else:
                    st.session_state.page = "process_confirm_page"
                st.rerun()              
def show_process_confirm_page():
    df_new = st.session_state.get("df_new")
    if df_new is None:
        st.warning("Dados do processo incompletos.")
        return
    n_rows = df_new.shape[0]
    st.title("Confirmação do Processo ETL")
    st.markdown("Revê os dados após o processamento ETL, incluindo entidades válidas, duplicadas e sem correspondência.")

    mongo_connected = st.session_state.get("mongo_connected", False)

    if not mongo_connected:
        st.warning("⚠️ Modo offline: ligação ao MongoDB indisponível. Algumas funcionalidades estão desativadas.")

    tab1, tab2, tab3 = st.tabs(["Visualização Global", "Revisão Duplicados", "Revisão Entidades sem correspondência"])


    if "etl_result" not in st.session_state:
        if mongo_connected:
            with st.spinner("🚀 A executar o processo ETL..."):
                group_dfs, duplicates_df, no_match_df = run_etl(
                    year=st.session_state.selected_year,
                    df=df_new,
                    mongo_db=st.session_state.mdb,
                    cur_sii=st.session_state.sii_cur
                )
                st.session_state.etl_result = {
                    "group_dfs": group_dfs,
                    "duplicates_df": duplicates_df,
                    "no_match_df": no_match_df
                }
        else:
            group_dfs = {
                "identificacao": df_new.copy()
            }
            duplicates_df = pd.DataFrame(columns=["id_entidade", "nome_entidade"])
            no_match_df = pd.DataFrame(columns=["id_entidade", "nome_entidade"])
            st.session_state.etl_result = {
                "group_dfs": group_dfs,
                "duplicates_df": duplicates_df,
                "no_match_df": no_match_df
            }
    else:
        group_dfs = st.session_state.etl_result["group_dfs"]    
        duplicates_df = st.session_state.etl_result["duplicates_df"]
        no_match_df = st.session_state.etl_result["no_match_df"]

    all_data_df = pd.concat(group_dfs.values(), axis=1)
    total_validas = len(group_dfs["identificacao"])
    total_duplicados = len(duplicates_df)
    total_invalidas = len(no_match_df)

    # === TAB 1 ===
    with tab1:
        st.success(f"Processamento concluído. {total_validas} entidades válidas processadas de {n_rows}.")
        st.info(f"Duplicados: {total_duplicados} | Sem correspondência: {total_invalidas}")

        with st.expander("Pré-visualizar Resultados do ETL", expanded=True):
            st.subheader("Entidades Válidas")
            st.dataframe(all_data_df.astype(str), hide_index=True)
            st.subheader("Duplicados Detectados")
            st.dataframe(duplicates_df.astype(str), hide_index=True)
            st.subheader("Entidades sem Correspondência")
            st.dataframe(no_match_df.astype(str), hide_index=True)

    # === TAB 2 ===
    with tab2:
        st.session_state.setdefault("all_data_df", all_data_df.copy())
        st.session_state.setdefault("duplicates_df", duplicates_df.copy())

        st.markdown("### Substituir Duplicado pela versão alternativa")

        col1, col2, col3 = st.columns([1, 1, 0.5])

        with col1:
            if st.session_state.mongo_connected:
                entidades_duplicadas = st.session_state.duplicates_df["id_entidade"].unique()
            else:
                entidades_duplicadas = []
            entidade_sel = st.selectbox(
                "Seleciona uma entidade duplicada:",
                entidades_duplicadas,
                index=0 if "entidade_sel_backup" not in st.session_state else
                list(entidades_duplicadas).index(st.session_state.entidade_sel_backup)
            )

        if st.session_state.mongo_connected:
            linha_original = st.session_state.all_data_df[
                st.session_state.all_data_df["id_entidade"] == entidade_sel
            ].reset_index(drop=True)

            duplicados_entidade = st.session_state.duplicates_df[
                st.session_state.duplicates_df["id_entidade"] == entidade_sel
            ].reset_index(drop=True)

            st.markdown("**Linha no ETL Final:**")
            st.dataframe(linha_original.astype(str),hide_index=True)

            st.markdown("**Linhas Alternativas (Duplicadas):**")
            for i, row in duplicados_entidade.iterrows():
                st.write(f"**Opção {i+1}**")
                st.dataframe(pd.DataFrame([row]).astype(str), use_container_width=True, hide_index=True)

        with col2:
            if st.session_state.mongo_connected:
                opcao_idx = st.selectbox(
                    "Seleciona a opção a usar como definitiva:",
                    options=duplicados_entidade.index,
                    format_func=lambda i: f"Opção {i+1}"
                )

        with col3:
            st.markdown("<div style='margin-top: 28px;'></div>", unsafe_allow_html=True)
            if st.button("✅ Substituir"):
                linha_substituta = duplicados_entidade.loc[[opcao_idx]]
                st.session_state.entidade_sel_backup = entidade_sel

                st.session_state.all_data_df = st.session_state.all_data_df[
                    st.session_state.all_data_df["id_entidade"] != entidade_sel
                ].reset_index(drop=True)

                st.session_state.duplicates_df = pd.concat(
                    [st.session_state.duplicates_df, linha_original],
                    ignore_index=True
                )

                st.session_state.all_data_df = pd.concat(
                    [st.session_state.all_data_df, linha_substituta],
                    ignore_index=True
                )

                substituta_valores = linha_substituta.astype(str).iloc[0]
                mask = st.session_state.duplicates_df.astype(str).eq(substituta_valores).all(axis=1)
                st.session_state.duplicates_df = st.session_state.duplicates_df[~mask].reset_index(drop=True)
                st.rerun()

    # === TAB 3 ===
    with tab3:
        st.session_state.setdefault("no_match_df", no_match_df.copy())

        if "entidades_sii_df" not in st.session_state and mongo_connected:
            connect_sii()
            if st.session_state.get("sii_cur"):
                cur = st.session_state.sii_cur
                cur.execute("SELECT id_entidades, ent_nome FROM entidades")
                entidades = cur.fetchall()
                entidades_sii_df = pd.DataFrame(entidades, columns=["id_entidade", "ent_nome"])
                st.session_state.entidades_sii_df = entidades_sii_df
            else:
                st.session_state.entidades_sii_df = None

        no_match_df = st.session_state.no_match_df
        all_data_df = st.session_state.all_data_df
        entidades_sii_df = st.session_state.get("entidades_sii_df")

        st.markdown("### Corrigir Entidades sem Correspondência")
        if no_match_df.empty:
            st.warning("Não existem entidades inválidas carregadas.")
        else:
            col1, col2 = st.columns([1.2, 1])

            with col1:
                entidades_sem_id = no_match_df["nome_entidade"].unique()
                entidade_nome_sel = st.selectbox("Entidade sem correspondência:", entidades_sem_id)
                linha_invalida = no_match_df[no_match_df["nome_entidade"] == entidade_nome_sel].reset_index(drop=True)

                st.markdown("**Dados da Entidade Selecionada:**")
                st.dataframe(linha_invalida.astype(str), use_container_width=True,hide_index=True)

                if entidades_sii_df is not None:
                    opcoes_formatadas = {
                        int(row["id_entidade"]): f"{row['ent_nome']} ({row['id_entidade']})"
                        for _, row in entidades_sii_df.iterrows()
                    }
                    id_correto = st.selectbox(
                        "Seleciona o ID correto da BD SII:",
                        options=list(opcoes_formatadas.keys()),
                        format_func=lambda x: opcoes_formatadas[x]
                    )
                else:
                    id_correto = None

                if st.button("✅ Corrigir Entidade", disabled=(not mongo_connected or id_correto is None)):
                    linha_corrigida = linha_invalida.copy()
                    linha_corrigida["id_entidade"] = id_correto

                    st.session_state.all_data_df = pd.concat(
                        [st.session_state.all_data_df, linha_corrigida],
                        ignore_index=True
                    )

                    st.session_state.no_match_df = no_match_df[
                        no_match_df["nome_entidade"] != entidade_nome_sel
                    ].reset_index(drop=True)

                    st.success(f"Entidade '{entidade_nome_sel}' foi corrigida com ID {id_correto}.")
                    st.rerun()

            with col2:
                if entidades_sii_df is not None:
                    st.markdown("**Entidades disponíveis na BD SII:**")
                    st.dataframe(entidades_sii_df.astype(str), use_container_width=True, hide_index=True)
                else:
                    st.error("Falha ao carregar dados da base de dados SII.")

    # === Rodapé ===
    st.markdown("---")
    col1, col2, col3, col4 = st.columns([1, 7, 1.6, 1])

    with col1:
        if st.button("⬅️ Voltar", key="btn_voltar"):
            st.session_state.page = "process_map"
            st.rerun()

    with col2:
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            for sheet_name, df in group_dfs.items():
                df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
            duplicates_df.to_excel(writer, sheet_name="duplicados", index=False)
            no_match_df.to_excel(writer, sheet_name="entidades_invalidas", index=False)
            all_data_df.to_excel(writer, sheet_name="all_data", index=False)
        buffer.seek(0)

        st.download_button(
            label="📥 Download do Excel Final",
            key="download_excel_etl",
            data=buffer,
            file_name=f"ETL_{st.session_state.selected_year}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    with col3:
        if st.button("Exportar para a BD 📦", disabled=not mongo_connected):
            descarregar_dados_para_postgres(group_dfs   )

    with col4:
        if st.button("Concluir ➡️", key="btn_avancar"):
            st.session_state.clear()
            st.session_state.page = "home"
            st.rerun()

# Pages Map
if st.session_state.page == "home":
    show_home()
elif st.session_state.page == "config":
    show_config_page()
elif st.session_state.page == "processo":
    show_processo_page()
elif st.session_state.page == "process_col_remover":
    show_process_col_remover_page()
elif st.session_state.page == "process_groups":
    show_process_groups()
elif st.session_state.page == "process_map":
    show_process_map()
elif st.session_state.page == "process_confirm_page":
    show_process_confirm_page()

# Footer
st.markdown("""
    <div style='position: fixed; bottom: 10px; left: 0; right: 0; text-align: center; color: gray; font-size: 12px;'>
        Versão 2.0 • Desenvolvido por Francisco Rodrigues
    </div>
""", unsafe_allow_html=True)
