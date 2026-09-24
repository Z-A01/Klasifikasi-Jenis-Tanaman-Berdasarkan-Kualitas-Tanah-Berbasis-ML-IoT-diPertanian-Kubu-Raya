#include <WiFi.h>
#include <AsyncTCP.h>
#include <ESPAsyncWebServer.h>
#include <ModbusMaster.h>
#include <ArduinoJson.h>
#include "FS.h"
#include "SD.h"
#include "SPI.h"
#include <ThreeWire.h>
#include <RtcDS1302.h>
#include <esp_task_wdt.h>
#include <PubSubClient.h>
#include <WiFiClientSecure.h>

// ============================================================
// VERSION & CONFIGURATION
// ============================================================
#define FIRMWARE_VERSION "1.0"
#define WDT_TIMEOUT_MS    30000

// ============================================================
// PIN DEFINITIONS
// ============================================================
// RS485
#define RXD2   16
#define TXD2   17
#define DE_PIN 25

// SD Card
#define SD_CS   5
#define SD_MOSI 23
#define SD_MISO 19
#define SD_SCK  18

// RTC
#define RTC_CLK 4
#define RTC_DAT 27
#define RTC_RST 14

// Buzzer & Buttons
#define BUZZER_PIN         21
#define BUTTON_PIN         32   // SAVE (Digital)
#define SHARE_BUTTON_PIN   13   // SHARE (Tombol fisik untuk share ke MQTT)

// ============================================================
// WIFI CONFIGURATION
// ============================================================
// AP Mode (Hotspot - SELALU HIDUP)
const char* ap_ssid     = "SoilSensor";
const char* ap_password = "12345678";

// STA Mode (Internet - OPSIONAL, jika ada WiFi)
const char* sta_ssid     = "";   // Isi dengan WiFi Anda
const char* sta_password = "";   // Isi dengan password WiFi Anda

// ============================================================
// MQTT CONFIGURATION (HiveMQ Cloud)
// ============================================================
const char* mqtt_server = ".s1.eu.hivemq.cloud";      // Isi dengan HiveMQ broker URL
const int   mqtt_port   = ;    // Port TLS
const char* mqtt_user   = ""; // isi user broker
const char* mqtt_pass   = ""; // Isi password broker
const char* mqtt_topic_realtime = "";  // Topik untuk data realtime
const char* mqtt_topic_share = "";     // Topik untuk data share (tombol)

// ============================================================
// TIMING CONSTANTS
// ============================================================
const unsigned long WAIT_DURATION      = 300000;  // 5 menit (300 detik)
const unsigned long WS_INTERVAL        = 5000;
const unsigned long MODBUS_INTERVAL    = 1000;
const unsigned long SD_CHECK_INTERVAL  = 60000;
const unsigned long DEBOUNCE_DELAY     = 50;
const unsigned long REALTIME_INTERVAL  = 5000;    // Kirim realtime setiap 5 detik

// ============================================================
// GLOBAL OBJECTS
// ============================================================
// Web Server
AsyncWebServer server(80);
AsyncWebSocket ws("/ws");

// Modbus
HardwareSerial RS485Serial(2);
ModbusMaster node;

// MQTT
WiFiClientSecure espClient;
PubSubClient mqttClient(espClient);

// RTC
ThreeWire myWire(RTC_DAT, RTC_CLK, RTC_RST);
RtcDS1302<ThreeWire> rtc(myWire);

// JSON Document
StaticJsonDocument<8192> jsonDoc;

// ============================================================
// GLOBAL VARIABLES
// ============================================================
// WiFi Status
bool internetAvailable = false;
unsigned long lastMQTTCheck = 0;
unsigned long lastMQTTSend = 0;

// RTC
bool rtcHealthy = false;
RtcDateTime lastGoodTime;
unsigned long lastGoodMillis = 0;
unsigned long lastRtcRead = 0;
String currentTimestamp = "1970-01-01 00:00:00";

// SD Card
bool sdCardReady = false;
bool sdBusy = false;
String csvFileName = "/dataset/dataset.csv";
String currentProjectName = "dataset";
int dataCounter = 0;
int titikCounter = 1;

// Sensor Data
struct SensorData {
    float temperature = 0;
    float moisture = 0;
    float ph = 0;
    uint16_t ec = 0;
    uint16_t nitrogen = 0;
    uint16_t phosphor = 0;
    uint16_t potassium = 0;
    uint16_t salinity = 0;
};

SensorData currentData;
SensorData rawData; // Data mentah dari sensor

// Data Collection State
bool waitingForData = false;
unsigned long dataCollectionStart = 0;
String phaseStatus = "idle";
String buttonType = "";  // "save" atau "share"

// Timing
unsigned long lastWsSend = 0;
unsigned long lastModbusRead = 0;
unsigned long lastSDCheck = 0;
unsigned long lastHeapLog = 0;

// Modbus status
unsigned long lastSuccessfulModbus = 0;
const unsigned long MODBUS_TIMEOUT_MS = 2000;

// Debounce
bool lastButtonState = HIGH;
bool buttonState = HIGH;
unsigned long lastDebounceTime = 0;

bool lastShareButtonState = HIGH;
bool shareButtonState = HIGH;
unsigned long lastShareDebounceTime = 0;

// Flags
bool needRefreshData = false;

// ============================================================
// FUNCTION PROTOTYPES
// ============================================================
void writeLog(String level, String message);
String getTimestamp();
String getTimeOnly();
void initRTC();
void initSDCard();
bool createNewCSVFile(String filename);
void getLastIDFromCSV();
String getProjectList();
void saveDataToCSV(const SensorData& data, int titikId, String label = "Normal");
String readLastData(int count = 10);
int countCSVRows();
void clearAllCSVData();
void readModbusData();
void processSensorData();
void sendSensorData();
void sendCollectionProgress();
void resetRTC();
void beep(int duration);
void beepMultiple(int times, int duration, int delayBetween);
void startDataCollection(String type);
void checkDataCollection();
void handleButtonPress();
void handleShareButtonPress();
void preTransmission();
void postTransmission();
void setupWiFi();
void setupMQTT();
void connectMQTT();
void sendRealtimeData(const SensorData& data);
void sendShareData(const SensorData& data);
void mqttCallback(char* topic, byte* payload, unsigned int length);
void onWebSocketEvent(AsyncWebSocket* server, AsyncWebSocketClient* client, AwsEventType type, void* arg, uint8_t* data, size_t len);
void handleWebSocketMessage(AsyncWebSocketClient* client, const String& message);

// ============================================================
// LOGGING
// ============================================================
void writeLog(String level, String message) {
    Serial.println("📝 [" + level + "]: " + message);
}

// ============================================================
// RTC Functions
// ============================================================
void initRTC() {
    rtc.Begin();

    if (rtc.GetIsWriteProtected()) {
        rtc.SetIsWriteProtected(false);
    }

    if (!rtc.GetIsRunning()) {
        rtc.SetIsRunning(true);
        delay(20);
    }

    RtcDateTime now = rtc.GetDateTime();

    if (!rtc.IsDateTimeValid() || now.Year() < 2020 || now.Year() > 2100) {
        RtcDateTime compiled = RtcDateTime(__DATE__, __TIME__);
        rtc.SetDateTime(compiled);
        delay(50);
        now = rtc.GetDateTime();
    }

    if (rtc.IsDateTimeValid() && now.Year() >= 2020) {
        rtcHealthy = true;
        lastGoodTime = now;
        lastGoodMillis = millis();
        currentTimestamp = getTimestamp();
        Serial.println("[RTC] OK - " + currentTimestamp);
    } else {
        rtcHealthy = false;
        Serial.println("[RTC] Gagal inisialisasi");
    }
}

String getTimestamp() {
    if (millis() - lastRtcRead >= 1000) {
        lastRtcRead = millis();

        RtcDateTime now = rtc.GetDateTime();

        if (now.IsValid() && now.Year() >= 2020 && now.Year() <= 2100) {
            lastGoodTime = now;
            lastGoodMillis = millis();
            rtcHealthy = true;
        }
    }

    unsigned long elapsed = (millis() - lastGoodMillis) / 1000;
    RtcDateTime current = lastGoodTime;
    current += elapsed;

    char buffer[25];
    sprintf(buffer, "%04d-%02d-%02d %02d:%02d:%02d",
            current.Year(), current.Month(), current.Day(),
            current.Hour(), current.Minute(), current.Second());

    currentTimestamp = String(buffer);
    return currentTimestamp;
}

String getTimeOnly() {
    String ts = getTimestamp();
    return (ts.length() >= 19) ? ts.substring(11, 19) : "00:00:00";
}

void resetRTC() {
    RtcDateTime compiled = RtcDateTime(__DATE__, __TIME__);
    rtc.SetIsWriteProtected(false);
    rtc.SetIsRunning(true);
    rtc.SetDateTime(compiled);
    delay(50);

    lastGoodTime = rtc.GetDateTime();
    lastGoodMillis = millis();
    rtcHealthy = lastGoodTime.IsValid();
    currentTimestamp = getTimestamp();

    writeLog("INFO", "RTC reset ke waktu compile");
}

// ============================================================
// RS485 FUNCTIONS
// ============================================================
void preTransmission() { digitalWrite(DE_PIN, HIGH); }
void postTransmission() { digitalWrite(DE_PIN, LOW); }

// ============================================================
// SD CARD FUNCTIONS
// ============================================================
void initSDCard() {
    if (sdBusy) return;
    sdBusy = true;
    writeLog("INFO", "Initializing SD Card...");
    esp_task_wdt_reset();
    
    SPI.begin(SD_SCK, SD_MISO, SD_MOSI, SD_CS);
    
    for (int attempt = 0; attempt < 3; attempt++) {
        esp_task_wdt_reset();
        if (SD.begin(SD_CS, SPI, 4000000) && SD.cardType() != CARD_NONE) {
            sdCardReady = true;
            
            // Create directories
            if (!SD.exists("/dataset")) SD.mkdir("/dataset");
            
            // Initialize or open CSV file
            if (!SD.exists(csvFileName)) {
                createNewCSVFile(csvFileName);
            } else {
                getLastIDFromCSV();
                dataCounter = countCSVRows();
            }
            
            // Initialize log file
            if (!SD.exists("/dataset/system_log.csv")) {
                File f = SD.open("/dataset/system_log.csv", FILE_WRITE);
                if (f) {
                    f.println("Timestamp,Level,Message");
                    f.flush();
                    f.close();
                }
            }
            
            writeLog("INFO", "SD Card ready");
            sdBusy = false;
            return;
        }
        delay(100);
    }
    
    sdCardReady = false;
    sdBusy = false;
    writeLog("ERROR", "SD Card mount failed");
}

bool createNewCSVFile(String filename) {
    if (!sdCardReady || sdBusy) return false;
    sdBusy = true;
    
    if (!filename.endsWith(".csv")) filename += ".csv";
    if (!filename.startsWith("/dataset/")) {
        filename = "/dataset/" + filename.substring(filename.lastIndexOf('/') + 1);
    }
    
    File file = SD.open(filename, FILE_WRITE);
    if (file) {
        file.println("No,ID Titik,Timestamp,pH,N(mg/kg),P(mg/kg),K(mg/kg),EC(uS/cm),Temp(C),Moisture(%),Salinity(ppm),Label");
        file.flush();
        file.close();
        
        csvFileName = filename;
        int lastSlash = filename.lastIndexOf('/');
        int lastDot = filename.lastIndexOf('.');
        currentProjectName = (lastSlash >= 0 && lastDot > lastSlash) ?
                             filename.substring(lastSlash + 1, lastDot) :
                             filename.substring(1, lastDot);
        titikCounter = 1;
        dataCounter = 0;
        writeLog("INFO", "Created: " + filename);
        sdBusy = false;
        return true;
    }
    
    sdBusy = false;
    return false;
}

void getLastIDFromCSV() {
    if (!sdCardReady || sdBusy) {
        titikCounter = 1;
        return;
    }
    sdBusy = true;
    
    File file = SD.open(csvFileName);
    if (!file) {
        titikCounter = 1;
        sdBusy = false;
        return;
    }
    
    String lastID = "";
    if (file.available()) file.readStringUntil('\n'); // Skip header
    
    while (file.available()) {
        String line = file.readStringUntil('\n');
        line.trim();
        if (line.length() > 0) {
            int firstComma = line.indexOf(',');
            int secondComma = line.indexOf(',', firstComma + 1);
            if (firstComma > 0 && secondComma > firstComma) {
                lastID = line.substring(firstComma + 1, secondComma);
            }
        }
    }
    file.close();
    
    titikCounter = (lastID.length() > 1 && lastID.charAt(0) == 'T') ?
                   lastID.substring(1).toInt() + 1 : 1;
    sdBusy = false;
}

String getProjectList() {
    if (!sdCardReady || sdBusy) return "[]";
    sdBusy = true;
    
    File root = SD.open("/dataset");
    if (!root) {
        sdBusy = false;
        return "[]";
    }
    
    String json = "[";
    bool first = true;
    while (true) {
        File entry = root.openNextFile();
        if (!entry) break;
        String name = entry.name();
        if (name.endsWith(".csv") && !name.startsWith("system_log")) {
            if (!first) json += ",";
            first = false;
            String projectName = name.substring(0, name.lastIndexOf('.'));
            json += "{\"name\":\"" + projectName + "\",\"size\":" + String(entry.size()) + "}";
        }
        entry.close();
    }
    root.close();
    json += "]";
    sdBusy = false;
    return json;
}

void saveDataToCSV(const SensorData& data, int titikId, String label) {
    if (!sdCardReady || sdBusy) return;
    sdBusy = true;
    
    dataCounter++;
    String timestamp = getTimestamp();
    
    File file = SD.open(csvFileName, FILE_APPEND);
    if (file) {
        file.println(String(dataCounter) + "," +
                     "T" + String(titikId) + "," +
                     timestamp + "," +
                     String(data.ph, 2) + "," +
                     String(data.nitrogen) + "," +
                     String(data.phosphor) + "," +
                     String(data.potassium) + "," +
                     String(data.ec) + "," +
                     String(data.temperature, 1) + "," +
                     String(data.moisture, 1) + "," +
                     String(data.salinity) + "," +
                     label);
        file.flush();
        file.close();
        writeLog("INFO", "Saved: T" + String(titikId) + " | Moisture: " + String(data.moisture, 1) + "%");
    }
    sdBusy = false;
}

String readLastData(int count) {
    if (!sdCardReady || sdBusy) return "[]";
    sdBusy = true;
    
    File file = SD.open(csvFileName);
    if (!file) {
        sdBusy = false;
        return "[]";
    }
    
    if (file.available()) file.readStringUntil('\n'); // Skip header
    
    String lines[100];
    int lineCount = 0;
    while (file.available() && lineCount < 100) {
        String line = file.readStringUntil('\n');
        line.trim();
        if (line.length() > 0 && !line.startsWith("No")) {
            lines[lineCount++] = line;
        }
    }
    file.close();
    
    String json = "[";
    bool first = true;
    int start = max(0, lineCount - count);
    
    for (int i = start; i < lineCount; i++) {
        if (lines[i].length() == 0) continue;
        if (!first) json += ",";
        first = false;
        
        int commas[11], commaCount = 0, pos = 0;
        while (commaCount < 11 && pos < lines[i].length()) {
            int commaPos = lines[i].indexOf(',', pos);
            if (commaPos == -1) break;
            commas[commaCount++] = commaPos;
            pos = commaPos + 1;
        }
        
        if (commaCount == 11) {
            json += "{\"no\":\"" + lines[i].substring(0, commas[0]) + "\",";
            json += "\"id\":\"" + lines[i].substring(commas[0] + 1, commas[1]) + "\",";
            json += "\"timestamp\":\"" + lines[i].substring(commas[1] + 1, commas[2]) + "\",";
            json += "\"ph\":\"" + lines[i].substring(commas[2] + 1, commas[3]) + "\",";
            json += "\"n\":\"" + lines[i].substring(commas[3] + 1, commas[4]) + "\",";
            json += "\"p\":\"" + lines[i].substring(commas[4] + 1, commas[5]) + "\",";
            json += "\"k\":\"" + lines[i].substring(commas[5] + 1, commas[6]) + "\",";
            json += "\"ec\":\"" + lines[i].substring(commas[6] + 1, commas[7]) + "\",";
            json += "\"temp\":\"" + lines[i].substring(commas[7] + 1, commas[8]) + "\",";
            json += "\"moisture\":\"" + lines[i].substring(commas[8] + 1, commas[9]) + "\",";
            json += "\"salinity\":\"" + lines[i].substring(commas[9] + 1, commas[10]) + "\",";
            json += "\"label\":\"" + lines[i].substring(commas[10] + 1) + "\"}";
        }
    }
    json += "]";
    sdBusy = false;
    return json;
}

int countCSVRows() {
    if (!sdCardReady || sdBusy) return 0;
    sdBusy = true;
    
    File file = SD.open(csvFileName);
    if (!file) {
        sdBusy = false;
        return 0;
    }
    
    int count = 0;
    if (file.available()) file.readStringUntil('\n'); // Skip header
    
    while (file.available()) {
        String line = file.readStringUntil('\n');
        line.trim();
        if (line.length() > 0) count++;
    }
    file.close();
    sdBusy = false;
    return count;
}

void clearAllCSVData() {
    if (!sdCardReady || sdBusy) return;
    sdBusy = true;
    
    if (SD.exists(csvFileName)) {
        SD.remove(csvFileName);
        File file = SD.open(csvFileName, FILE_WRITE);
        if (file) {
            file.println("No,ID Titik,Timestamp,pH,N(mg/kg),P(mg/kg),K(mg/kg),EC(uS/cm),Temp(C),Moisture(%),Salinity(ppm),Label");
            file.flush();
            file.close();
            dataCounter = 0;
            titikCounter = 1;
            writeLog("INFO", "All data cleared from " + csvFileName);
        }
    }
    sdBusy = false;
}

// ============================================================
// BUZZER FUNCTIONS
// ============================================================
void beep(int duration) {
    digitalWrite(BUZZER_PIN, HIGH);
    delay(duration);
    digitalWrite(BUZZER_PIN, LOW);
}

void beepMultiple(int times, int duration, int delayBetween) {
    for (int i = 0; i < times; i++) {
        beep(duration);
        if (i < times - 1) delay(delayBetween);
    }
}

// ============================================================
// MODBUS FUNCTIONS - FIXED
// ============================================================
void readModbusData() {
    uint8_t result = node.readHoldingRegisters(0x0000, 8);
    
    if (result == node.ku8MBSuccess) {
        float temp = node.getResponseBuffer(0) / 10.0f;
        float moist = node.getResponseBuffer(1) / 10.0f;
        uint16_t ec = node.getResponseBuffer(2);
        float ph = node.getResponseBuffer(3) / 100.0f;
        uint16_t n = node.getResponseBuffer(4);
        uint16_t p = node.getResponseBuffer(5);
        uint16_t k = node.getResponseBuffer(6);
        uint16_t sal = node.getResponseBuffer(7);

        // Simpan data mentah SELALU dari sensor (nilai real terbaru)
        rawData.temperature = temp;
        rawData.moisture = moist;
        rawData.ec = ec;
        rawData.ph = ph;
        rawData.nitrogen = n;
        rawData.phosphor = p;
        rawData.potassium = k;
        rawData.salinity = sal;

        Serial.printf("Raw: T=%.1f M=%.1f EC=%u pH=%.2f N=%u P=%u K=%u Sal=%u\n",
                      temp, moist, ec, ph, n, p, k, sal);
        
        // Proses data dengan logika moisture - menggunakan rawData terbaru
        processSensorData();
        
        lastSuccessfulModbus = millis();
    } else {
        // Jika pembacaan gagal, jangan update currentData
        // Biarkan currentData dengan nilai terakhir yang valid
        Serial.println("⚠️ Modbus read failed!");
    }
}

// ============================================================
// PROSES DATA SENSOR DENGAN LOGIKA MOISTURE - FIXED
// ============================================================
void processSensorData() {
    // pH, Temperature, Moisture SELALU nilai real dari sensor
    currentData.temperature = rawData.temperature;
    currentData.moisture = rawData.moisture;
    currentData.ph = rawData.ph;
    
    // Cek moisture dari rawData (nilai real terbaru)
    if (rawData.moisture <= 5.0) {
        // Jika moisture <= 5%, EC, N, P, K, Salinity = 0
        currentData.ec = 0;
        currentData.nitrogen = 0;
        currentData.phosphor = 0;
        currentData.potassium = 0;
        currentData.salinity = 0;
        
        writeLog("DEBUG", "Moisture " + String(rawData.moisture, 1) + "% <= 5% - EC,NPK,Salinity set to 0 (pH,Temp,Moisture real)");
    } else {
        // Jika moisture > 5%, semua nilai menggunakan sensor real
        currentData.ec = rawData.ec;
        currentData.nitrogen = rawData.nitrogen;
        currentData.phosphor = rawData.phosphor;
        currentData.potassium = rawData.potassium;
        currentData.salinity = rawData.salinity;
        
        writeLog("DEBUG", "Moisture " + String(rawData.moisture, 1) + "% > 5% - Using real sensor values for all parameters");
    }
}

// ============================================================
// DATA COLLECTION (SAVE & SHARE - Tunggu 5 menit, ambil 1 data)
// ============================================================
void startDataCollection(String type) {
    if (!waitingForData) {
        beep(200);
        writeLog("INFO", type + " button pressed - Waiting 5 minutes");
        
        waitingForData = true;
        dataCollectionStart = millis();
        phaseStatus = "waiting";
        buttonType = type;
        
        ws.textAll("{\"buttonStatus\":\"waiting\",\"type\":\"" + type + "\",\"waitTime\":" + String(WAIT_DURATION / 1000) + "}");
        sendCollectionProgress();
    }
}

void checkDataCollection() {
    if (!waitingForData) return;
    
    unsigned long elapsed = millis() - dataCollectionStart;
    unsigned long remaining = WAIT_DURATION - elapsed;
    
    // Kirim progress setiap 1 detik
    static unsigned long lastProgressSend = 0;
    if (millis() - lastProgressSend >= 1000) {
        sendCollectionProgress();
        lastProgressSend = millis();
    }
    
    // Jika sudah 5 menit (300 detik)
    if (elapsed >= WAIT_DURATION) {
        waitingForData = false;
        phaseStatus = "complete";
        
        // Baca data sensor
        readModbusData();
        
        // Data sudah diproses di processSensorData()
        
        // Simpan ke SD Card
        saveDataToCSV(currentData, titikCounter, buttonType);
        titikCounter++;
        
        // Kirim ke MQTT
        if (mqttClient.connected()) {
            writeLog("INFO", "📤 Sending " + buttonType + " data to MQTT...");
            sendShareData(currentData);
            beepMultiple(2, 200, 150);
        } else {
            writeLog("WARNING", "⚠️ MQTT not connected, data not sent");
            beepMultiple(3, 100, 50);
        }
        
        ws.textAll("{\"buttonStatus\":\"complete\",\"type\":\"" + buttonType + "\",\"titikId\":" + String(titikCounter - 1) + "}");
        needRefreshData = true;
        sendSensorData();
        sendCollectionProgress();
        
        writeLog("INFO", buttonType + " complete - Data saved to SD and sent to MQTT");
    }
}

void sendCollectionProgress() {
    if (ws.count() == 0) return;
    
    unsigned long elapsed = millis() - dataCollectionStart;
    unsigned long remaining = WAIT_DURATION - elapsed;
    
    int progressPercent = 0;
    if (elapsed < WAIT_DURATION) {
        progressPercent = (elapsed * 100) / WAIT_DURATION;
        if (progressPercent > 99) progressPercent = 99;
    } else {
        progressPercent = 100;
    }
    
    int elapsedSeconds = elapsed / 1000;
    int remainingSeconds = remaining / 1000;
    int totalSeconds = WAIT_DURATION / 1000;
    
    // Format waktu: MM:SS
    char elapsedStr[10], remainingStr[10], totalStr[10];
    sprintf(elapsedStr, "%02d:%02d", elapsedSeconds / 60, elapsedSeconds % 60);
    sprintf(remainingStr, "%02d:%02d", remainingSeconds / 60, remainingSeconds % 60);
    sprintf(totalStr, "%02d:%02d", totalSeconds / 60, totalSeconds % 60);
    
    ws.textAll("{\"collectionProgress\":{"
               "\"phase\":\"" + phaseStatus + "\","
               "\"progress\":" + String(progressPercent) + ","
               "\"elapsed\":\"" + String(elapsedStr) + "\","
               "\"remaining\":\"" + String(remainingStr) + "\","
               "\"total\":\"" + String(totalStr) + "\","
               "\"elapsedSeconds\":" + String(elapsedSeconds) + ","
               "\"remainingSeconds\":" + String(remainingSeconds) + ","
               "\"type\":\"" + buttonType + "\"}}");
}

// ============================================================
// MQTT FUNCTIONS
// ============================================================
void setupMQTT() {
    if (!internetAvailable) {
        writeLog("INFO", "📡 MQTT: Internet tidak tersedia, MQTT dinonaktifkan");
        return;
    }
    
    espClient.setInsecure();
    mqttClient.setServer(mqtt_server, mqtt_port);
    mqttClient.setCallback(mqttCallback);
    mqttClient.setBufferSize(512);
    connectMQTT();
}

void connectMQTT() {
    if (!internetAvailable) {
        return;
    }
    
    while (!mqttClient.connected() && internetAvailable) {
        Serial.print("Connecting HiveMQ...");
        String clientId = "ESP32-Soil-" + String((uint32_t)ESP.getEfuseMac(), HEX);
        
        if (mqttClient.connect(clientId.c_str(), mqtt_user, mqtt_pass)) {
            Serial.println(" Connected!");
            writeLog("INFO", "✅ MQTT Connected!");
        } else {
            Serial.print(" Failed rc=");
            Serial.println(mqttClient.state());
            writeLog("ERROR", "❌ MQTT Failed! State: " + String(mqttClient.state()));
            delay(3000);
        }
    }
}

void sendRealtimeData(const SensorData& data) {
    if (!mqttClient.connected()) {
        connectMQTT();
        if (!mqttClient.connected()) {
            return;
        }
    }
    
    char payload[256];
    char timestamp[25];
    strcpy(timestamp, currentTimestamp.c_str());
    
    snprintf(payload, sizeof(payload),
        "{\"timestamp\":\"%s\",\"moisture\":%.1f,\"temperature\":%.1f,\"ec\":%u,"
        "\"ph\":%.2f,\"nitrogen\":%u,\"phosphor\":%u,"
        "\"potassium\":%u,\"salinity\":%u}",
        timestamp, data.moisture, data.temperature, data.ec,
        data.ph, data.nitrogen, data.phosphor,
        data.potassium, data.salinity
    );
    
    bool ok = mqttClient.publish(mqtt_topic_realtime, payload);
    if (ok) {
        writeLog("INFO", "✅ Realtime sent | Moisture: " + String(data.moisture, 1) + "%");
    } else {
        writeLog("ERROR", "❌ Realtime publish failed!");
    }
}

void sendShareData(const SensorData& data) {
    if (!mqttClient.connected()) {
        connectMQTT();
        if (!mqttClient.connected()) {
            writeLog("ERROR", "Share: MQTT tidak terhubung");
            return;
        }
    }
    
    char payload[256];
    char timestamp[25];
    strcpy(timestamp, currentTimestamp.c_str());
    
    snprintf(payload, sizeof(payload),
        "{\"timestamp\":\"%s\",\"moisture\":%.1f,\"temperature\":%.1f,\"ec\":%u,"
        "\"ph\":%.2f,\"nitrogen\":%u,\"phosphor\":%u,"
        "\"potassium\":%u,\"salinity\":%u}",
        timestamp, data.moisture, data.temperature, data.ec,
        data.ph, data.nitrogen, data.phosphor,
        data.potassium, data.salinity
    );
    
    bool ok = mqttClient.publish(mqtt_topic_share, payload);
    if (ok) {
        writeLog("INFO", "✅ Share data sent | Moisture: " + String(data.moisture, 1) + "%");
    } else {
        writeLog("ERROR", "❌ Share data publish failed!");
    }
}

void mqttCallback(char* topic, byte* payload, unsigned int length) {
    String message;
    for (unsigned int i = 0; i < length; i++) {
        message += (char)payload[i];
    }
    writeLog("DEBUG", "📩 MQTT received: " + message);
}

// ============================================================
// WIFI SETUP - DUAL MODE (AP + STA)
// ============================================================
void setupWiFi() {
    Serial.println("\n📡 Setting up WiFi...");
    
    // AP Mode - SELALU HIDUP
    WiFi.mode(WIFI_AP_STA);
    WiFi.softAP(ap_ssid, ap_password);
    IPAddress apIP = WiFi.softAPIP();
    Serial.println("✅ AP Mode started!");
    Serial.println("   SSID: " + String(ap_ssid));
    Serial.println("   Password: " + String(ap_password));
    Serial.println("   IP: " + apIP.toString());
    writeLog("INFO", "📡 AP IP: " + apIP.toString());
    
    // STA Mode - OPSIONAL (jika ada WiFi)
    if (String(sta_ssid) != "" && String(sta_password) != "") {
        Serial.println("\n📶 Connecting to WiFi (STA Mode)...");
        Serial.print("   SSID: " + String(sta_ssid));
        
        WiFi.begin(sta_ssid, sta_password);
        
        int attempts = 0;
        while (WiFi.status() != WL_CONNECTED && attempts < 20) {
            delay(500);
            Serial.print(".");
            attempts++;
            esp_task_wdt_reset();
        }
        
        if (WiFi.status() == WL_CONNECTED) {
            internetAvailable = true;
            IPAddress staIP = WiFi.localIP();
            Serial.println("\n✅ STA Mode connected!");
            Serial.println("   IP: " + staIP.toString());
            writeLog("INFO", "🌐 STA IP: " + staIP.toString());
            writeLog("INFO", "✅ Internet available!");
        } else {
            internetAvailable = false;
            Serial.println("\n⚠️ STA Mode failed to connect!");
            Serial.println("   Internet tidak tersedia (MQTT tidak aktif)");
            writeLog("WARNING", "⚠️ STA Gagal konek! Internet tidak tersedia");
        }
    } else {
        internetAvailable = false;
        Serial.println("\n⚠️ STA credentials tidak diisi!");
        Serial.println("   Internet tidak tersedia (MQTT tidak aktif)");
        writeLog("INFO", "⚠️ STA credentials kosong, internet tidak tersedia");
    }
}

// ============================================================
// WEBSOCKET FUNCTIONS
// ============================================================
void sendSensorData() {
    if (ws.count() == 0) return;
    
    esp_task_wdt_reset();
    jsonDoc.clear();
    
    // Sensor Data (sudah diproses)
    jsonDoc["temperature"] = currentData.temperature;
    jsonDoc["moisture"] = currentData.moisture;
    jsonDoc["ec"] = currentData.ec;
    jsonDoc["ph"] = currentData.ph;
    jsonDoc["nitrogen"] = currentData.nitrogen;
    jsonDoc["phosphor"] = currentData.phosphor;
    jsonDoc["potassium"] = currentData.potassium;
    jsonDoc["salinity"] = currentData.salinity;
    
    // SD Card Data
    jsonDoc["sdReady"] = sdCardReady;
    jsonDoc["projectName"] = currentProjectName;
    jsonDoc["totalRows"] = countCSVRows();
    jsonDoc["titikCounter"] = titikCounter;
    jsonDoc["csvData"] = readLastData(50);
    jsonDoc["projects"] = getProjectList();
    
    // Other Data    jsonDoc["rtcTime"] = getTimestamp();
    jsonDoc["firmware"] = FIRMWARE_VERSION;
    jsonDoc["freeHeap"] = ESP.getFreeHeap();
    jsonDoc["internetAvailable"] = internetAvailable;
    jsonDoc["mqttConnected"] = mqttClient.connected();
    
    String jsonString;
    serializeJson(jsonDoc, jsonString);
    ws.textAll(jsonString);
    needRefreshData = false;
}

void handleWebSocketMessage(AsyncWebSocketClient* client, const String& message) {
    StaticJsonDocument<512> doc;
    if (deserializeJson(doc, message)) return;
    
    String action = doc["action"];
    
    if (action == "digitalButton") {
        if (doc["state"]) startDataCollection("save");
    } else if (action == "shareButton") {
        if (doc["state"]) startDataCollection("share");
    } else if (action == "createProject") {
        String name = doc["projectName"];
        if (name.length() > 0 && name.length() < 50) {
            bool valid = true;
            for (char c : name) {
                if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                      (c >= '0' && c <= '9') || c == '_' || c == '-')) {
                    valid = false;
                    break;
                }
            }
            if (valid && createNewCSVFile("/dataset/" + name + ".csv")) {
                client->text("{\"projectCreated\":\"" + name + "\"}");
                needRefreshData = true;
                sendSensorData();
            }
        }
    } else if (action == "switchProject") {
        String name = doc["projectName"];
        if (name.length() > 0) {
            String filename = "/dataset/" + name + ".csv";
            if (SD.exists(filename)) {
                csvFileName = filename;
                currentProjectName = name;
                getLastIDFromCSV();
                dataCounter = countCSVRows();
                client->text("{\"projectSwitched\":\"" + name + "\"}");
                needRefreshData = true;
                sendSensorData();
            }
        }
    } else if (action == "listProjects") {
        client->text("{\"projects\":" + getProjectList() + "}");
    } else if (action == "clearAllData") {
        clearAllCSVData();
        needRefreshData = true;
        sendSensorData();
        client->text("{\"dataCleared\":true}");
    }
}

void onWebSocketEvent(AsyncWebSocket* server, AsyncWebSocketClient* client,
                      AwsEventType type, void* arg, uint8_t* data, size_t len) {
    if (type == WS_EVT_CONNECT) {
        writeLog("INFO", "Client #" + String(client->id()) + " connected");
        client->text("{\"sdStatus\":\"" + String(sdCardReady ? "ready" : "not ready") +
                     "\",\"internetAvailable\":" + String(internetAvailable ? "true" : "false") + "}");
        sendSensorData();
    } else if (type == WS_EVT_DATA) {
        AwsFrameInfo* info = (AwsFrameInfo*)arg;
        if (info->final && info->index == 0 && info->len == len && len < 512) {
            char buffer[512];
            memcpy(buffer, data, len);
            buffer[len] = '\0';
            handleWebSocketMessage(client, String(buffer));
        }
    }
}

// ============================================================
// BUTTON HANDLERS
// ============================================================
void handleButtonPress() {
    if (!waitingForData) {
        startDataCollection("save");
    }
}

void handleShareButtonPress() {
    if (!waitingForData) {
        startDataCollection("share");
    }
}

// ============================================================
// HTML PAGE
// ============================================================
const char index_html[] PROGMEM = R"rawliteral(
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Soil Sensor v2</title>
    <style>
        *{margin:0;padding:0;box-sizing:border-box}
        body{font-family:'Segoe UI',sans-serif;background:#1a1a2e;color:#eee;padding:15px}
        .container{max-width:1200px;margin:0 auto}
        .header{background:linear-gradient(135deg,#16213e,#0f3460);padding:15px 20px;border-radius:12px;margin-bottom:15px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px}
        .header h1{font-size:20px;color:#e94560}
        .status-bar{display:flex;flex-wrap:wrap;gap:8px;font-size:12px}
        .status-item{padding:3px 10px;border-radius:12px;background:#16213e;border:1px solid #0f3460}
        .status-item.online{background:#1b5e20;border-color:#2e7d32}
        .status-item.offline{background:#b71c1c;border-color:#c62828}
        .status-item.ready{background:#0d47a1;border-color:#1565c0}
        .status-item.internet-on{background:#1b5e20;border-color:#2e7d32}
        .status-item.internet-off{background:#b71c1c;border-color:#c62828}
        .status-item.mqtt-on{background:#1b5e20;border-color:#2e7d32}
        .status-item.mqtt-off{background:#b71c1c;border-color:#c62828}
        .status-item.waiting{background:#e65100;border-color:#f57c00;animation:blink 1s infinite}
        @keyframes blink{50%{opacity:.5}}
        .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:10px;margin-bottom:15px}
        .card{background:#16213e;padding:12px;border-radius:10px;border-left:3px solid #e94560}
        .card-label{font-size:10px;color:#888;text-transform:uppercase}
        .card-value{font-size:22px;font-weight:700;margin-top:3px}
        .card-unit{font-size:12px;color:#888}
        .controls{background:#16213e;padding:12px 15px;border-radius:12px;margin-bottom:15px;display:flex;flex-wrap:wrap;gap:10px;align-items:center}
        .btn{padding:8px 16px;border:none;border-radius:8px;cursor:pointer;font-size:13px;font-weight:600;transition:.2s;color:#fff}
        .btn-primary{background:#0d47a1}
        .btn-primary:hover{background:#1565c0}
        .btn-share{background:#00695c}
        .btn-share:hover{background:#00897b}
        .btn-success{background:#1b5e20}
        .btn-success:hover{background:#2e7d32}
        .btn-danger{background:#b71c1c}
        .btn-danger:hover{background:#c62828}
        .btn:disabled{opacity:.5;cursor:not-allowed}
        #collectionProgress{background:#16213e;padding:12px 15px;border-radius:12px;margin-bottom:15px;display:none;border:1px solid #0f3460}
        .progress-header{display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;margin-bottom:8px}
        .progress-phase{font-size:14px;font-weight:700;color:#e94560}
        .progress-time{font-size:12px;color:#888}
        .progress-bar-track{width:100%;height:10px;background:#0f3460;border-radius:5px;overflow:hidden;margin-bottom:5px}
        .progress-bar-fill{width:0%;height:100%;background:linear-gradient(90deg,#e94560,#f5a623);transition:width .5s}
        .progress-detail{font-size:11px;color:#888;text-align:center}
        .data-section{background:#16213e;padding:15px;border-radius:12px;margin-bottom:15px}
        .data-header{display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px;margin-bottom:10px}
        .data-header h2{font-size:16px;color:#e94560}
        .sd-info{display:flex;flex-wrap:wrap;gap:15px;padding:10px;background:#0f3460;border-radius:8px;margin-bottom:10px}
        .sd-info div{font-size:13px}
        .sd-info strong{color:#e94560}
        .table-wrapper{overflow-x:auto;max-height:400px;overflow-y:auto;border-radius:8px;border:1px solid #0f3460;margin-top:10px}
        table{width:100%;border-collapse:collapse;font-size:12px}
        th{background:#0f3460;color:#fff;padding:8px 10px;text-align:left;position:sticky;top:0;z-index:10;white-space:nowrap}
        td{padding:6px 10px;border-bottom:1px solid #1a1a2e}
        tr:hover{background:#1a1a2e}
        .btn-sm{padding:3px 8px;font-size:11px}
        .file-manager{background:#16213e;padding:12px 15px;border-radius:12px;margin-top:10px}
        .file-manager .row{display:flex;flex-wrap:wrap;gap:8px;align-items:center}
        .file-manager select{padding:6px 12px;border-radius:6px;background:#0f3460;color:#eee;border:1px solid #1a1a2e;font-size:13px;min-width:140px}
        .file-manager input{padding:6px 12px;border-radius:6px;background:#0f3460;color:#eee;border:1px solid #1a1a2e;font-size:13px;width:130px}
        .alert{padding:8px 12px;border-radius:6px;margin-bottom:10px;font-size:13px}
        .alert-info{background:#0d47a1;color:#fff}
        .alert-success{background:#1b5e20;color:#fff}
        .alert-error{background:#b71c1c;color:#fff}
        .footer{text-align:center;font-size:11px;color:#666;margin-top:15px}
        @media(max-width:600px){.grid{grid-template-columns:1fr 1fr}.card-value{font-size:18px}.header h1{font-size:16px}.controls .btn{font-size:12px;padding:6px 12px}}
    </style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1>🌱 Soil Sensor v2</h1>
        <div class="status-bar">
            <span class="status-item" id="wsStatus">● Connecting</span>
            <span class="status-item" id="sdStatus">SD: --</span>
            <span class="status-item" id="internetStatus">🌐 --</span>
            <span class="status-item" id="mqttStatus">📡 --</span>
            <span class="status-item" id="rtcTime">--:--:--</span>
        </div>
    </div>
    <div id="alertContainer"></div>
    <div class="grid">
        <div class="card"><div class="card-label">Temp</div><div class="card-value"><span id="temp">--</span><span class="card-unit">°C</span></div></div>
        <div class="card"><div class="card-label">Moisture</div><div class="card-value"><span id="moisture">--</span><span class="card-unit">%</span></div></div>
        <div class="card"><div class="card-label">EC</div><div class="card-value"><span id="ec">--</span><span class="card-unit">uS</span></div></div>
        <div class="card"><div class="card-label">pH</div><div class="card-value"><span id="ph">--</span></div></div>
        <div class="card"><div class="card-label">N</div><div class="card-value"><span id="n">--</span><span class="card-unit">mg</span></div></div>
        <div class="card"><div class="card-label">P</div><div class="card-value"><span id="p">--</span><span class="card-unit">mg</span></div></div>
        <div class="card"><div class="card-label">K</div><div class="card-value"><span id="k">--</span><span class="card-unit">mg</span></div></div>
        <div class="card"><div class="card-label">Salinity</div><div class="card-value"><span id="salinity">--</span><span class="card-unit">ppm</span></div></div>
    </div>
    <div id="collectionProgress">
        <div class="progress-header">
            <span class="progress-phase" id="progressPhase">⏳ Menunggu 5 menit...</span>
            <span class="progress-time" id="progressTime">00:00 / 05:00</span>
        </div>
        <div class="progress-bar-track"><div class="progress-bar-fill" id="progressBar"></div></div>
        <div class="progress-detail" id="progressDetail">Menunggu 5 menit... (data akan diambil tepat 5 menit)</div>
    </div>
    <div class="controls">
        <button class="btn btn-share" id="btnShare" onclick="pressShare()">📤 Share ML</button>
        <button class="btn btn-primary" id="btnSave" onclick="pressDigital()">🔘 Save</button>
    </div>
    
    <!-- MICRO SD CARD DATA SECTION -->
    <div class="data-section">
        <div class="data-header">
            <h2>📂 MicroSD Card Data</h2>
        </div>
        
        <!-- SD Card Status -->
        <div class="sd-info">
            <div><strong>File:</strong> <span id="sdFileName">--</span></div>
            <div><strong>Records:</strong> <span id="sdRecordCount">0</span></div>
            <div><strong>Next ID:</strong> <span id="sdNextID">--</span></div>
        </div>
        
        <!-- Tabel Data dari SD Card -->
        <div class="table-wrapper">
            <table>
                <thead>
                    <tr>
                        <th>No</th>
                        <th>ID</th>
                        <th>Time</th>
                        <th>pH</th>
                        <th>N</th>
                        <th>P</th>
                        <th>K</th>
                        <th>EC</th>
                        <th>Temp</th>
                        <th>Moist</th>
                        <th>Sal</th>
                        <th>Label</th>
                    </tr>
                </thead>
                <tbody id="sdTableBody">
                    <tr><td colspan="12" style="text-align:center;padding:20px;">Loading data from SD Card...</td></tr>
                </tbody>
            </table>
        </div>
        
        <!-- Project Manager -->
        <div class="file-manager">
            <div class="row">
                <select id="projectSelect" onchange="switchProject()">
                    <option value="">📂 Select file...</option>
                </select>
                <input type="text" id="newProjectName" placeholder="New file name" onkeypress="if(event.key==='Enter') createProject()">
                <button class="btn btn-primary btn-sm" onclick="createProject()">Create</button>
                <button class="btn btn-success btn-sm" onclick="listProjects()">Refresh</button>
                <button class="btn btn-danger btn-sm" onclick="clearAllData()">🗑️ Delete All</button>
            </div>
        </div>
    </div>
    
    <div class="footer">Firmware v2.1.0 | ESP32 Soil Sensor</div>
</div>
<script>
// ============================================================
// WEBSOCKET
// ============================================================
const ws = new WebSocket(`ws://${location.hostname}/ws`);
let alertTimeout = null, currentTime = "";

ws.onopen = ()=>{
    document.getElementById('wsStatus').className='status-item online';
    document.getElementById('wsStatus').textContent='● Online';
};
ws.onclose = ()=>{
    document.getElementById('wsStatus').className='status-item offline';
    document.getElementById('wsStatus').textContent='● Offline';
    setTimeout(()=>{ if(ws.readyState===WebSocket.CLOSED) location.reload(); },5000);
};

ws.onmessage = (e)=>{
    try{
        const data = JSON.parse(e.data);
        
        // RTC Time
        if(data.rtcTime !== undefined){
            currentTime = data.rtcTime;
            document.getElementById('rtcTime').textContent = currentTime;
        }
        
        // Collection Progress
        if(data.collectionProgress !== undefined) updateCollectionProgress(data.collectionProgress);
        
        // SD Card Data
        if(data.sdReady !== undefined || data.projectName !== undefined || 
           data.totalRows !== undefined || data.csvData !== undefined) {
            updateSDCardData(data);
        }
        
        // Sensor Data
        if(data.temperature !== undefined) updateDashboard(data);
        
        // Internet & MQTT Status
        if(data.internetAvailable !== undefined) updateInternetStatus(data.internetAvailable);
        if(data.mqttConnected !== undefined) updateMQTTStatus(data.mqttConnected);
        
        // Button Status
        if(data.buttonStatus !== undefined){
            const div=document.getElementById('collectionProgress');
            if(data.buttonStatus==='waiting'){
                div.style.display='block';
                document.getElementById('btnSave').disabled=true;
                document.getElementById('btnShare').disabled=true;
            } else if(data.buttonStatus==='complete'){
                setTimeout(()=>{ 
                    div.style.display='none';
                    document.getElementById('btnSave').disabled=false;
                    document.getElementById('btnShare').disabled=false;
                }, 3000);
            }
        }
        
        // Project Created/Switched
        if(data.projectCreated !== undefined){
            showAlert('✅ Created: '+data.projectCreated+'.csv','success');
            document.getElementById('newProjectName').value='';
            listProjects();
        }
        if(data.projectSwitched !== undefined){
            showAlert('✅ Switched to: '+data.projectSwitched+'.csv','success');
            listProjects();
        }
        if(data.dataCleared !== undefined){
            showAlert('🗑️ All data cleared successfully!','success');
            listProjects();
        }
    } catch(e){ console.error(e); }
};

// ============================================================
// SD CARD FUNCTIONS
// ============================================================
function updateSDCardData(data) {
    // Update SD Status
    if (data.sdReady !== undefined) {
        const sdStatusEl = document.getElementById('sdStatus');
        if (data.sdReady) {
            sdStatusEl.className = 'status-item ready';
            sdStatusEl.textContent = 'SD: Ready';
        } else {
            sdStatusEl.className = 'status-item offline';
            sdStatusEl.textContent = 'SD: Error';
        }
    }
    
    // Update file name
    if (data.projectName !== undefined) {
        document.getElementById('sdFileName').textContent = data.projectName + '.csv';
    }
    
    // Update record count
    if (data.totalRows !== undefined) {
        document.getElementById('sdRecordCount').textContent = data.totalRows;
    }
    
    // Update next ID
    if (data.titikCounter !== undefined) {
        document.getElementById('sdNextID').textContent = 'T' + data.titikCounter;
    }
    
    // Update table
    if (data.csvData !== undefined) {
        updateSDTable(data.csvData);
    }
    
    // Update project list
    if (data.projects !== undefined) {
        updateProjectList(data.projects);
    }
}

function updateSDTable(csvData) {
    const tbody = document.getElementById('sdTableBody');
    try {
        const data = JSON.parse(csvData);
        if (!data || data.length === 0) {
            tbody.innerHTML = '<tr><td colspan="12" style="text-align:center;padding:15px;">📭 No data in SD Card</td></tr>';
            return;
        }
        let html = '';
        data.forEach(r => {
            html += `<tr>
                <td>${r.no}</td>
                <td>${r.id}</td>
                <td style="font-size:11px;">${r.timestamp}</td>
                <td>${r.ph}</td>
                <td>${r.n}</td>
                <td>${r.p}</td>
                <td>${r.k}</td>
                <td>${r.ec}</td>
                <td>${r.temp}</td>
                <td>${r.moisture}</td>
                <td>${r.salinity}</td>
                <td>${r.label}</td>
            </tr>`;
        });
        tbody.innerHTML = html;
    } catch(e) {
        console.error('Error parsing CSV data:', e);
        tbody.innerHTML = '<tr><td colspan="12" style="text-align:center;padding:15px;color:#f44336;">❌ Error loading data</td></tr>';
    }
}

function listProjects() {
    ws.send(JSON.stringify({action: 'listProjects'}));
}

function createProject() {
    const name = document.getElementById('newProjectName').value.trim();
    if (!name) {
        showAlert('⚠️ Enter a file name', 'error');
        return;
    }
    if (!/^[a-zA-Z0-9_-]+$/.test(name)) {
        showAlert('⚠️ Use letters, numbers, _, - only', 'error');
        return;
    }
    if (confirm('Create "' + name + '.csv"?')) {
        ws.send(JSON.stringify({action: 'createProject', projectName: name}));
    }
}

function switchProject() {
    const name = document.getElementById('projectSelect').value;
    if (!name) return;
    if (confirm('Switch to "' + name + '.csv"?')) {
        ws.send(JSON.stringify({action: 'switchProject', projectName: name}));
    }
}

function clearAllData() {
    if (confirm('⚠️ Delete ALL data from current file?')) {
        if (confirm('Are you sure? This cannot be undone!')) {
            ws.send(JSON.stringify({action: 'clearAllData'}));
            showAlert('🗑️ Deleting all data...', 'info');
        }
    }
}

function updateProjectList(projectsData) {
    const select = document.getElementById('projectSelect');
    const currentValue = select.value;
    select.innerHTML = '<option value="">📂 Select file...</option>';
    try {
        const data = JSON.parse(projectsData);
        data.forEach(project => {
            const opt = document.createElement('option');
            opt.value = project.name;
            opt.textContent = project.name + ' (' + (project.size/1024).toFixed(1) + ' KB)';
            select.appendChild(opt);
        });
        if (currentValue && data.some(p => p.name === currentValue)) {
            select.value = currentValue;
        }
    } catch(e) {
        console.error('Error parsing projects:', e);
    }
}

// ============================================================
// UI UPDATE FUNCTIONS
// ============================================================
function updateCollectionProgress(p){
    const div=document.getElementById('collectionProgress');
    div.style.display='block';
    
    const phaseEl=document.getElementById('progressPhase');
    const timeEl=document.getElementById('progressTime');
    const barEl=document.getElementById('progressBar');
    const detailEl=document.getElementById('progressDetail');
    
    if(p.phase==='waiting'){
        phaseEl.textContent = '⏳ Menunggu 5 menit... (' + p.type + ')';
        timeEl.textContent = p.elapsed + ' / ' + p.total;
        barEl.style.width = p.progress + '%';
        detailEl.textContent = 'Sisa waktu: ' + p.remaining + ' (data akan diambil tepat 5 menit)';
    } else if(p.phase==='complete'){
        phaseEl.textContent = '✅ Selesai!';
        timeEl.textContent = 'Selesai';
        barEl.style.width = '100%';
        detailEl.textContent = 'Data ' + p.type + ' berhasil disimpan ke SD Card dan dikirim!';
    }
}

function updateInternetStatus(available) {
    const el = document.getElementById('internetStatus');
    if(available) {
        el.className = 'status-item internet-on';
        el.textContent = '🌐 Online';
    } else {
        el.className = 'status-item internet-off';
        el.textContent = '🌐 Offline';
    }
}

function updateMQTTStatus(connected) {
    const el = document.getElementById('mqttStatus');
    if(connected) {
        el.className = 'status-item mqtt-on';
        el.textContent = '📡 Active';
    } else {
        el.className = 'status-item mqtt-off';
        el.textContent = '📡 Inactive';
    }
}

function updateDashboard(d){ 
    document.getElementById('temp').textContent=d.temperature.toFixed(1); 
    document.getElementById('moisture').textContent=d.moisture.toFixed(1); 
    document.getElementById('ec').textContent=d.ec; 
    document.getElementById('ph').textContent=d.ph.toFixed(2); 
    document.getElementById('n').textContent=d.nitrogen; 
    document.getElementById('p').textContent=d.phosphor; 
    document.getElementById('k').textContent=d.potassium; 
    document.getElementById('salinity').textContent=d.salinity;
}

function updateLocalTimeFallback(){ 
    if(!currentTime){ 
        const n=new Date(); 
        document.getElementById('rtcTime').textContent=String(n.getHours()).padStart(2,'0')+':'+String(n.getMinutes()).padStart(2,'0')+':'+String(n.getSeconds()).padStart(2,'0'); 
    } 
}
setInterval(updateLocalTimeFallback,1000);

function showAlert(msg,type){ 
    const c=document.getElementById('alertContainer'); 
    const a=document.createElement('div'); 
    a.className='alert alert-'+type; 
    a.textContent=msg; 
    c.innerHTML=''; 
    c.appendChild(a); 
    if(alertTimeout) clearTimeout(alertTimeout); 
    alertTimeout=setTimeout(()=>{ 
        if(a.parentNode) a.remove(); 
    },3000); 
}

// ============================================================
// BUTTON FUNCTIONS
// ============================================================
function pressDigital(){ 
    const btn=document.getElementById('btnSave');
    if(btn.disabled) return;
    btn.textContent='⏳ Waiting...'; 
    btn.disabled=true; 
    ws.send(JSON.stringify({action:'digitalButton',state:true})); 
    showAlert('🔘 Save button pressed! Waiting 5 minutes...','info'); 
    setTimeout(()=>{ 
        if(!btn.disabled){
            btn.textContent='🔘 Save'; 
            btn.disabled=false; 
        }
    }, 1000); 
}

function pressShare(){
    const btn=document.getElementById('btnShare');
    if(btn.disabled) return;
    btn.textContent='⏳ Waiting...'; 
    btn.disabled=true; 
    ws.send(JSON.stringify({action:'shareButton',state:true})); 
    showAlert('📤 Share button pressed! Waiting 5 minutes...','info'); 
    setTimeout(()=>{ 
        if(!btn.disabled){
            btn.textContent='📤 Share ML'; 
            btn.disabled=false; 
        }
    }, 1000); 
}

// Auto refresh project list every 10 seconds
setInterval(()=>{ 
    if(ws.readyState===WebSocket.OPEN) 
        ws.send(JSON.stringify({action:'listProjects'})); 
}, 10000);

// Initial load
setTimeout(()=>listProjects(), 500);
</script>
</body>
</html>
)rawliteral";

// ============================================================
// SETUP
// ============================================================
void setup() {
    Serial.begin(115200);
    delay(1000);
    
    Serial.println("\n========================================");
    Serial.println("   SOIL SENSOR SYSTEM v" + String(FIRMWARE_VERSION));
    Serial.println("   DUAL MODE WiFi + MQTT");
    Serial.println("========================================\n");
    
    // Watchdog Timer
    esp_task_wdt_config_t wdt_config = {
        .timeout_ms = WDT_TIMEOUT_MS,
        .idle_core_mask = (1 << 0) | (1 << 1),
        .trigger_panic = true
    };
    esp_task_wdt_init(&wdt_config);
    esp_task_wdt_add(NULL);
    esp_task_wdt_reset();
    
    // Initialize pins
    pinMode(BUZZER_PIN, OUTPUT);
    digitalWrite(BUZZER_PIN, LOW);
    pinMode(BUTTON_PIN, INPUT_PULLUP);        // GPIO 32 - SAVE
    pinMode(SHARE_BUTTON_PIN, INPUT_PULLUP);  // GPIO 13 - SHARE
    esp_task_wdt_reset();
    
    // RTC
    initRTC();
    esp_task_wdt_reset();
    
    // RS485
    pinMode(DE_PIN, OUTPUT);
    postTransmission();
    RS485Serial.begin(9600, SERIAL_8N1, RXD2, TXD2);
    node.begin(1, RS485Serial);
    node.preTransmission(preTransmission);
    node.postTransmission(postTransmission);
    esp_task_wdt_reset();
    
    // SD Card
    initSDCard();
    esp_task_wdt_reset();
    
    // WiFi Dual Mode (AP + STA)
    setupWiFi();
    esp_task_wdt_reset();
    
    // MQTT (Hanya jika internet tersedia)
    if (internetAvailable) {
        setupMQTT();
    } else {
        writeLog("INFO", "📡 MQTT: Internet tidak tersedia, MQTT dinonaktifkan");
    }
    esp_task_wdt_reset();
    
    // Web Server
    ws.onEvent(onWebSocketEvent);
    server.addHandler(&ws);
    server.on("/", HTTP_GET, [](AsyncWebServerRequest* request) {
        request->send(200, "text/html", index_html);
    });
    server.begin();
    esp_task_wdt_reset();
    
    writeLog("INFO", "Server started - v" + String(FIRMWARE_VERSION));
    writeLog("INFO", "Heap: " + String(ESP.getFreeHeap()) + " bytes");
    writeLog("INFO", "Internet: " + String(internetAvailable ? "Available" : "Not Available"));
    writeLog("INFO", "MQTT Topics:");
    writeLog("INFO", "  - Realtime: " + String(mqtt_topic_realtime));
    writeLog("INFO", "  - Share: " + String(mqtt_topic_share));
    writeLog("INFO", "Buttons:");
    writeLog("INFO", "  - GPIO 32: SAVE (tunggu 5 menit, ambil 1 data)");
    writeLog("INFO", "  - GPIO 13: SHARE (tunggu 5 menit, ambil 1 data)");
    writeLog("INFO", "Moisture Logic:");
    writeLog("INFO", "  - pH, Temp, Moisture: Always real value");
    writeLog("INFO", "  - If Moisture <= 5%, EC, N, P, K, Salinity = 0");
    writeLog("INFO", "  - If Moisture > 5%, EC, N, P, K, Salinity = real value");
    
    // Startup beeps
    beepMultiple(2, 150, 100);
    delay(500);
    beepMultiple(3, 100, 50);
    
    Serial.println("\n✅ SYSTEM READY!");
    Serial.println("📱 Connect to WiFi: " + String(ap_ssid));
    Serial.println("🔗 Open browser: http://" + WiFi.softAPIP().toString());
    if (internetAvailable) {
        Serial.println("🌐 Internet: Connected!");
        Serial.println("📡 MQTT: " + String(mqttClient.connected() ? "Connected" : "Disconnected"));
        Serial.println("📤 Realtime akan otomatis terkirim setiap 5 detik");
        Serial.println("📤 Share Topic: " + String(mqtt_topic_share));
    } else {
        Serial.println("⚠️ Internet: Not available (MQTT disabled)");
    }
    Serial.println("========================================\n");
}

// ============================================================
// LOOP
// ============================================================
void loop() {
    esp_task_wdt_reset();
    ws.cleanupClients();
    
    unsigned long currentMillis = millis();
    
    // RTC Time Update
    if (currentMillis - lastRtcRead >= 1000) {
        getTimestamp();
        if (ws.count() > 0) {
            ws.textAll("{\"rtcTime\":\"" + currentTimestamp + "\"}");
        }
    }
    
    // Modbus Read
    if (currentMillis - lastModbusRead >= MODBUS_INTERVAL) {
        readModbusData();
        lastModbusRead = currentMillis;
    }
    
    // Button Debounce - GPIO 32 (SAVE)
    bool reading = digitalRead(BUTTON_PIN);
    if (reading != lastButtonState) {
        lastDebounceTime = currentMillis;
    }
    if ((currentMillis - lastDebounceTime) > DEBOUNCE_DELAY) {
        if (reading != buttonState) {
            buttonState = reading;
            if (buttonState == LOW) handleButtonPress();
        }
    }
    lastButtonState = reading;
    
    // Button Debounce - GPIO 13 (SHARE)
    bool shareReading = digitalRead(SHARE_BUTTON_PIN);
    if (shareReading != lastShareButtonState) {
        lastShareDebounceTime = currentMillis;
    }
    if ((currentMillis - lastShareDebounceTime) > DEBOUNCE_DELAY) {
        if (shareReading != shareButtonState) {
            shareButtonState = shareReading;
            if (shareButtonState == LOW) handleShareButtonPress();
        }
    }
    lastShareButtonState = shareReading;
    
    // Check data collection state
    checkDataCollection();
    
    // MQTT Loop & Realtime (hanya jika internet tersedia)
    if (internetAvailable) {
        mqttClient.loop();
        
        // Cek koneksi MQTT setiap 30 detik
        if (currentMillis - lastMQTTCheck >= 30000) {
            if (!mqttClient.connected()) {
                writeLog("WARNING", "MQTT disconnected, reconnecting...");
                connectMQTT();
            }
            lastMQTTCheck = currentMillis;
        }
        
        // Kirim data realtime ke MQTT setiap 5 detik (otomatis)
        if (mqttClient.connected()) {
            if (currentMillis - lastMQTTSend >= REALTIME_INTERVAL) {
                readModbusData();
                sendRealtimeData(currentData);
                lastMQTTSend = currentMillis;
            }
        }
    }
    
    // Send sensor data via WebSocket
    if (currentMillis - lastWsSend >= WS_INTERVAL || needRefreshData) {
        sendSensorData();
        lastWsSend = currentMillis;
        needRefreshData = false;
    }
    
    // SD Card retry
    if (!sdCardReady && (currentMillis - lastSDCheck >= SD_CHECK_INTERVAL)) {
        writeLog("WARNING", "SD Card retry...");
        initSDCard();
        lastSDCheck = currentMillis;
    }
    
    // Heap monitoring
    if (currentMillis - lastHeapLog >= 300000) {
        writeLog("DEBUG", "Heap: " + String(ESP.getFreeHeap()) + " bytes");
        writeLog("DEBUG", "Internet: " + String(internetAvailable ? "Available" : "Not Available"));
        writeLog("DEBUG", "MQTT: " + String(mqttClient.connected() ? "Connected" : "Disconnected"));
        lastHeapLog = currentMillis;
    }
    
    delay(1);
}