/*
  Projeto IoT MQ-2 + ESP32 + LED + Dashboard Python (Streamlit/MQTT)

  Fiação recomendada para ESP32 com Wi-Fi:
    MQ-2 VCC  -> VIN/5V (confira o módulo usado)
    MQ-2 GND  -> GND
    MQ-2 AOUT -> GPIO 34 (ADC1; recomendado com Wi-Fi)
    LED anodo -> GPIO 2 através de resistor de 220–330 ohms
    LED catodo -> GND

  Importante:
  - O GPIO 34 é entrada apenas e pertence ao ADC1, que funciona com Wi-Fi.
  - Se usar outro pino no circuito Wokwi, altere PINO_SENSOR e a fiação.
  - A leitura ADC é um valor relativo (0–4095), não ppm calibrado.
  - O broker MQTT público não tem autenticação: use um tópico exclusivo em projetos reais.

  Bibliotecas no Wokwi Library Manager:
    PubSubClient
    ArduinoJson
*/

#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>

// Wi-Fi padrão do simulador Wokwi
const char* WIFI_SSID = "Wokwi-GUEST";
const char* WIFI_PASSWORD = "";

// Mesmo broker e tópicos configurados em app.py
const char* MQTT_BROKER = "broker.emqx.io";
const uint16_t MQTT_PORT = 1883;
const char* TOPICO_BASE = "dashboard/octavio-rodrigo/mq2";
const char* TOPICO_DADOS = "dashboard/octavio-rodrigo/mq2/dados";
const char* TOPICO_CMD = "dashboard/octavio-rodrigo/mq2/comando";
const char* TOPICO_STATUS = "dashboard/octavio-rodrigo/mq2/status";

constexpr uint8_t PINO_SENSOR = 34;  // ADC1: funciona com Wi-Fi
constexpr uint8_t PINO_LED = 2;
constexpr uint16_t HISTERESE = 50;
constexpr uint8_t AMOSTRAS = 10;
constexpr uint32_t LEITURA_MS = 250;

WiFiClient wifiClient;
PubSubClient mqtt(wifiClient);

int limiar = 700;
uint8_t intervaloSegundos = 2;
String modo = "auto";        // auto ou manual
bool ledManual = false;
bool perigo = false;
bool ledLigado = false;

uint32_t ultimaLeitura = 0;
uint32_t ultimoEnvio = 0;
bool publicarAgora = true;

int lerGas() {
  uint32_t soma = 0;
  for (uint8_t i = 0; i < AMOSTRAS; i++) {
    soma += analogRead(PINO_SENSOR);
    delay(3);
  }
  return soma / AMOSTRAS;
}

void atualizarPerigo(int gas) {
  bool perigoAnterior = perigo;
  if (!perigo && gas >= limiar) {
    perigo = true;
  } else if (perigo && gas < max(0, limiar - (int)HISTERESE)) {
    perigo = false;
  }
  if (perigo != perigoAnterior) publicarAgora = true;
}

void atualizarLed() {
  ledLigado = (modo == "auto") ? perigo : ledManual;
  digitalWrite(PINO_LED, ledLigado ? HIGH : LOW);
}

void publicarStatus(const char* status) {
  if (mqtt.connected()) {
    mqtt.publish(TOPICO_STATUS, status, true);
  }
}

void receberComando(char* topic, byte* payload, unsigned int length) {
  StaticJsonDocument<192> doc;
  DeserializationError erro = deserializeJson(doc, payload, length);
  if (erro) {
    Serial.println("Comando MQTT ignorado: JSON inválido.");
    return;
  }

  const char* cmd = doc["cmd"] | "";
  JsonVariant valor = doc["valor"];
  bool aceito = false;

  if (strcmp(cmd, "led") == 0 && !valor.isNull()) {
    int v = valor.as<int>();
    if (v == 0 || v == 1) {
      modo = "manual";
      ledManual = (v == 1);
      aceito = true;
    }
  } else if (strcmp(cmd, "modo") == 0 && valor.is<const char*>()) {
    String novoModo = valor.as<String>();
    if (novoModo == "auto" || novoModo == "manual") {
      modo = novoModo;
      aceito = true;
    }
  } else if (strcmp(cmd, "limiar") == 0 && !valor.isNull()) {
    int novoLimiar = valor.as<int>();
    if (novoLimiar >= 0 && novoLimiar <= 4095) {
      limiar = novoLimiar;
      aceito = true;
    }
  } else if (strcmp(cmd, "intervalo") == 0 && !valor.isNull()) {
    int novoIntervalo = valor.as<int>();
    if (novoIntervalo >= 1 && novoIntervalo <= 10) {
      intervaloSegundos = (uint8_t)novoIntervalo;
      aceito = true;
    }
  }

  if (aceito) {
    atualizarLed();
    publicarAgora = true;
    Serial.printf("Comando aplicado: %s\n", cmd);
  } else {
    Serial.printf("Comando inválido/ignorado: %s\n", cmd);
  }
}

void conectarWiFi() {
  if (WiFi.status() == WL_CONNECTED) return;

  Serial.printf("Conectando ao Wi-Fi %s", WIFI_SSID);
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);

  uint32_t inicio = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - inicio < 20000) {
    delay(300);
    Serial.print(".");
  }
  if (WiFi.status() == WL_CONNECTED) {
    Serial.printf("\nWi-Fi conectado. IP: %s\n", WiFi.localIP().toString().c_str());
  } else {
    Serial.println("\nWi-Fi ainda indisponível; nova tentativa em breve.");
  }
}

bool conectarMQTT() {
  if (mqtt.connected()) return true;
  if (WiFi.status() != WL_CONNECTED) return false;

  String clientId = "esp32-mq2-" + String((uint32_t)(ESP.getEfuseMac() & 0xFFFFFFFF), HEX);
  mqtt.setServer(MQTT_BROKER, MQTT_PORT);
  mqtt.setCallback(receberComando);
  mqtt.setKeepAlive(30);
  mqtt.setBufferSize(512);

  Serial.print("Conectando ao MQTT...");
  if (mqtt.connect(clientId.c_str(), TOPICO_STATUS, 1, true, "offline")) {
    Serial.println(" conectado.");
    mqtt.subscribe(TOPICO_CMD, 1);
    publicarStatus("online");
    publicarAgora = true;
    return true;
  }
  Serial.printf(" falhou, estado=%d\n", mqtt.state());
  return false;
}

void publicarDados(int gas) {
  if (!mqtt.connected()) return;

  StaticJsonDocument<256> doc;
  doc["gas"] = gas;
  doc["perigo"] = perigo;
  doc["limiar"] = limiar;
  doc["led"] = ledLigado;
  doc["modo"] = modo;
  doc["intervalo"] = intervaloSegundos;

  char buffer[256];
  serializeJson(doc, buffer, sizeof(buffer));
  if (mqtt.publish(TOPICO_DADOS, buffer)) {
    Serial.printf("Gas Sensor Value: %d | limiar: %d | %s | LED: %s\n",
                  gas, limiar, perigo ? "PERIGO" : "Sem perigo",
                  ledLigado ? "ligado" : "desligado");
  } else {
    Serial.println("Falha ao publicar telemetria MQTT.");
  }
}

void setup() {
  Serial.begin(115200);
  pinMode(PINO_LED, OUTPUT);
  digitalWrite(PINO_LED, LOW);
  analogReadResolution(12);  // 0–4095
  pinMode(PINO_SENSOR, INPUT);
  conectarWiFi();
  conectarMQTT();
}

void loop() {
  uint32_t agora = millis();

  if (WiFi.status() != WL_CONNECTED) {
    conectarWiFi();
  }

  if (!mqtt.connected()) {
    conectarMQTT();
  } else {
    mqtt.loop(); // processa comandos recebidos do dashboard
  }

  if (agora - ultimaLeitura >= LEITURA_MS || publicarAgora) {
    ultimaLeitura = agora;
    int gas = lerGas();
    atualizarPerigo(gas);
    atualizarLed();

    if (mqtt.connected() &&
        (publicarAgora || agora - ultimoEnvio >= (uint32_t)intervaloSegundos * 1000UL)) {
      publicarDados(gas);
      ultimoEnvio = agora;
      publicarAgora = false;
    }
  }

  delay(20);
}
