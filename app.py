"""
Dashboard IoT para ESP32 + MQ-2.
Recebe telemetria MQTT, guarda histórico em SQLite e envia comandos ao ESP32.

Instalação:
    python -m pip install -r requirements.txt

Execução:
    streamlit run app.py

O ESP32 e este dashboard precisam usar o mesmo broker e os mesmos tópicos.
"""

import json
import sqlite3
import threading
import time
import uuid
from pathlib import Path

import pandas as pd
import paho.mqtt.client as mqtt
import streamlit as st

MQTT_BROKER = "broker.emqx.io"
MQTT_PORT = 1883
TOPICO_BASE = "dashboard/octavio-rodrigo/mq2"
TOPICO_DADOS = f"{TOPICO_BASE}/dados"
TOPICO_CMD = f"{TOPICO_BASE}/comando"
TOPICO_STATUS = f"{TOPICO_BASE}/status"
DB_NAME = Path(__file__).with_name("dados_mq2.db")

st.set_page_config(page_title="Dashboard MQ-2", page_icon="🔥", layout="wide")
st.title("🔥 Dashboard IoT — Sensor de gás MQ-2")
st.caption("ESP32 + MQTT + Streamlit • Leitura ADC relativa (0–4095), não ppm calibrado")


def inicializar_banco():
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS leituras (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                gas INTEGER NOT NULL,
                limiar INTEGER NOT NULL,
                perigo INTEGER NOT NULL,
                led INTEGER NOT NULL,
                modo TEXT NOT NULL,
                intervalo INTEGER NOT NULL DEFAULT 2
            )
        """)
        # Migração simples caso o banco antigo não tenha a coluna intervalo.
        colunas = {row[1] for row in conn.execute("PRAGMA table_info(leituras)")}
        if "intervalo" not in colunas:
            conn.execute("ALTER TABLE leituras ADD COLUMN intervalo INTEGER NOT NULL DEFAULT 2")


def salvar_leitura(dados):
    if "gas" not in dados:
        return
    agora = time.strftime("%Y-%m-%d %H:%M:%S")
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute("""
            INSERT INTO leituras
            (timestamp, gas, limiar, perigo, led, modo, intervalo)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            agora,
            max(0, min(4095, int(dados["gas"]))),
            max(0, min(4095, int(dados.get("limiar", 700)))),
            int(bool(dados.get("perigo", False))),
            int(bool(dados.get("led", False))),
            str(dados.get("modo", "auto")),
            int(dados.get("intervalo", 2)),
        ))


def buscar_dados(limite=200):
    with sqlite3.connect(DB_NAME) as conn:
        df = pd.read_sql_query("""
            SELECT timestamp AS hora, gas, limiar, perigo, led, modo, intervalo
            FROM leituras ORDER BY id DESC LIMIT ?
        """, conn, params=(limite,))
    return df.iloc[::-1].reset_index(drop=True)


@st.cache_resource
def iniciar_mqtt():
    estado = {"broker": False, "dispositivo": "aguardando", "ultima_mensagem": 0.0}
    lock = threading.Lock()

    def on_connect(client, userdata, flags, reason_code, properties=None):
        conectado = getattr(reason_code, "value", reason_code) == 0
        with lock:
            estado["broker"] = bool(conectado)
        if conectado:
            client.subscribe([(TOPICO_DADOS, 1), (TOPICO_STATUS, 1)])
            print("Conectado ao broker MQTT e inscrito nos tópicos.")

    def on_disconnect(client, userdata, disconnect_flags=None, reason_code=None, properties=None):
        with lock:
            estado["broker"] = False

    def on_message(client, userdata, msg):
        try:
            texto = msg.payload.decode("utf-8")
            if msg.topic == TOPICO_STATUS:
                with lock:
                    estado["dispositivo"] = texto.strip()
                    estado["ultima_mensagem"] = time.time()
            elif msg.topic == TOPICO_DADOS:
                dados = json.loads(texto)
                salvar_leitura(dados)
                with lock:
                    estado["dispositivo"] = "online"
                    estado["ultima_mensagem"] = time.time()
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError, sqlite3.Error) as exc:
            print(f"Mensagem MQTT ignorada: {exc}")

    client_id = f"dashboard-mq2-{uuid.uuid4().hex[:8]}"
    try:
        cliente = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            protocol=mqtt.MQTTv311,
        )
    except (AttributeError, TypeError):
        cliente = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)

    cliente.on_connect = on_connect
    cliente.on_disconnect = on_disconnect
    cliente.on_message = on_message
    try:
        cliente.connect_async(MQTT_BROKER, MQTT_PORT, keepalive=60)
        cliente.loop_start()
    except Exception as exc:
        print(f"Não foi possível iniciar MQTT: {exc}")
    return cliente, estado, lock


inicializar_banco()
cliente_mqtt, estado_mqtt, estado_lock = iniciar_mqtt()


def enviar_comando(cmd, valor):
    with estado_lock:
        conectado = estado_mqtt["broker"]
        dispositivo = estado_mqtt["dispositivo"]
    if not conectado:
        st.error("O dashboard ainda não está conectado ao broker MQTT.")
        return
    info = cliente_mqtt.publish(
        TOPICO_CMD, json.dumps({"cmd": cmd, "valor": valor}), qos=1
    )
    if info.rc == mqtt.MQTT_ERR_SUCCESS:
        st.toast(f"Comando enviado: {cmd} = {valor}", icon="📤")
        if dispositivo != "online":
            st.warning("O broker recebeu o comando, mas o ESP32 ainda não confirmou que está online.")
    else:
        st.error(f"Falha ao publicar comando MQTT (código {info.rc}).")


with st.sidebar:
    st.header("🎛️ Controle do ESP32")
    st.caption("Ligar/desligar o LED coloca o dispositivo em modo manual.")
    c1, c2 = st.columns(2)
    if c1.button("💡 Ligar LED", use_container_width=True):
        enviar_comando("led", 1)
    if c2.button("⚫ Desligar LED", use_container_width=True):
        enviar_comando("led", 0)
    if st.button("🤖 Modo automático", use_container_width=True):
        enviar_comando("modo", "auto")

    st.divider()
    novo_limiar = st.slider("Limiar de perigo (ADC)", 0, 4095, 700, step=10)
    if st.button("Aplicar limiar", use_container_width=True):
        enviar_comando("limiar", novo_limiar)

    novo_intervalo = st.slider("Intervalo de telemetria (segundos)", 1, 10, 2)
    if st.button("Aplicar intervalo", use_container_width=True):
        enviar_comando("intervalo", novo_intervalo)

    st.divider()
    if st.button("🧹 Limpar histórico local", use_container_width=True):
        with sqlite3.connect(DB_NAME) as conn:
            conn.execute("DELETE FROM leituras")
        st.rerun()

with estado_lock:
    broker_online = estado_mqtt["broker"]
    dispositivo_status = estado_mqtt["dispositivo"]

c1, c2, c3 = st.columns(3)
c1.metric("Broker MQTT", "Conectado" if broker_online else "Desconectado")
c2.metric("ESP32", dispositivo_status.capitalize())
c3.metric("Tópico de dados", "MQ-2")

@st.fragment(run_every=2)
def painel():
    df = buscar_dados()

    with estado_lock:
        broker_ok = estado_mqtt["broker"]
        dispositivo = estado_mqtt["dispositivo"]

    if not broker_ok:
        st.warning("🔌 Conectando ao broker MQTT. Confira a internet e o endereço do broker.")
    if df.empty:
        st.info("⏳ Aguardando telemetria. Inicie a simulação no Wokwi e confirme a conexão MQTT.")
        return

    ultimo = df.iloc[-1]
    gas = int(ultimo["gas"])
    limiar = int(ultimo["limiar"])
    perigo = bool(ultimo["perigo"])
    idade = max(0, int(time.time() - time.mktime(time.strptime(ultimo["hora"], "%Y-%m-%d %H:%M:%S"))))

    if perigo:
        st.error("🚨 PERIGO: leitura do sensor acima do limiar configurado!")
    else:
        st.success("✅ Leitura abaixo do limiar de perigo.")

    if idade > 15:
        st.warning(f"⚠️ A última leitura foi recebida há {idade} segundos. Verifique se a simulação está ativa.")

    a, b, c, d, e = st.columns(5)
    a.metric("🧪 Leitura ADC", gas, delta=gas-limiar, delta_color="inverse")
    b.metric("📏 Limiar", limiar)
    c.metric("💡 LED", "Ligado" if bool(ultimo["led"]) else "Desligado")
    d.metric("⚙️ Modo", str(ultimo["modo"]).capitalize())
    e.metric("📡 Estado", dispositivo.capitalize())

    st.subheader("📈 Histórico de leitura e limiar")
    st.line_chart(df.set_index("hora")[["gas", "limiar"]])

    em_perigo = int(df["perigo"].sum())
    st.caption(f"Leituras armazenadas: {len(df)} • Leituras em estado de perigo no histórico exibido: {em_perigo}")

    with st.expander("📋 Últimas leituras"):
        tabela = df.tail(20).iloc[::-1].copy()
        tabela["perigo"] = tabela["perigo"].astype(bool)
        tabela["led"] = tabela["led"].astype(bool)
        st.dataframe(tabela, hide_index=True, use_container_width=True)

painel()
