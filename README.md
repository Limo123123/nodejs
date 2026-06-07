# 🏙️ Limo Open Source Project (Limazon) - Backend

Willkommen im Backend-Repository des Limo Open Source Projects (Limazon). Dieses Node.js-Backend ist das Herzstück einer massiven, virtuellen Gesellschaft. Es simuliert eine komplette Wirtschaft, soziale Netzwerke, ein Justizsystem, Immobilienmärkte und vieles mehr.

Das System ist auf Performance und Multi-Core-Nutzung ausgelegt (Node.js Cluster Modul) und nutzt Redis für blitzschnelles In-Memory Rate-Limiting und Cross-Worker-Kommunikation.

---

## ✨ Features auf einen Blick

* **Wirtschaft & Krypto:** Dynamische Börse ("Limo Stonks"), Kryptowährungen, Kreditsystem (Schufa), Steuern und ein C2C-Kleinanzeigenmarkt.
* **Soziales & Lifestyle:** Tinda (Dating mit KI-Partnern), Limterest, Heiraten, WG-System & Immobilien-Einrichtung (Limea).
* **Gaming:** Teachermon (TCG Sammelkartenspiel), Tamagotchi-Haustiere (inkl. Friedhof & Wiederbelebung), Casino & Arcade-Highscores.
* **Kriminalität & Politik:** Überfälle, Community-Heists auf die Bank, Gang-Kriege, Wahlen zum Bürgermeister, Gerichtsprozesse (Limo Court) und Petitionen.
* **KI & Automation:** LNN (Limo News Network) generiert automatische Satire-Nachrichten über Spieler-Aktionen via Gemini API. NPCs auf Tinda reagieren via Groq/Llama 3.

---

## 🛠️ Systemvoraussetzungen

Um diesen Server auszuführen, benötigst du folgende Infrastruktur:

* **Node.js** (v18 oder neuer)
* **MongoDB** (Lokal oder Atlas, für alle persistenten Daten)
* **Redis** (Zwingend erforderlich für Rate-Limits und Cluster-Sync)
* **Docker & Nginx** (Optional, aber empfohlen für das Bild-CDN)

---

## 🚀 Installation & Setup

### 1. Repository klonen & Abhängigkeiten installieren
Lade den Code herunter und installiere die Node-Module:

```bash
git clone <deine-repo-url>
cd limazon-backend
npm install
```

*(Hinweis: `sharp` wird für die Bildkomprimierung genutzt. Auf Linux/ARM-Architekturen wie einem Raspberry Pi werden hierfür ggf. die passenden Build-Tools benötigt. Füge bei "deine-repo-url" die passende Limazon URL ein, von der du es kopieren möchtest. Es gibt den Code auf OneDev und auf GitHub. Ich empfehle generell GitHub, außer du möchtest etwas beitragen ).*

### 2. Umgebungsvariablen konfigurieren (`.env`)
Erstelle im Hauptverzeichnis eine Datei namens `secret.env` (oder `.env`) und kopiere die Werte aus der `.env.example` (siehe unten). Passe die Werte an deine Infrastruktur an.

### 3. Server starten
Da das Projekt das Node.js Cluster-Modul nutzt, startet der Server automatisch einen Master-Prozess und für jeden CPU-Kern einen Worker-Prozess.

```bash
# Für die lokale Entwicklung:
npm start

# Oder direkt via Node:
node server.js
```

---

## ⚙️ `.env.example` Vorlage

Erstelle eine `secret.env` und fülle sie mit deinen Daten:

```env
# ==========================================
# LIMO BACKEND - ENVIRONMENT CONFIG
# ==========================================

# --- Webserver ---
PORT=10000
NODE_ENV=development # 'production' für sichere Cookies
FRONTEND_URL=http://127.0.0.1:8080 # Deine Frontend-URL für CORS

# --- Sicherheit ---
SESSION_SECRET=dein_super_geheimes_passwort_hier_einfuegen_123!

# --- MongoDB Datenbank ---
# Variante 1: Komplette URI (empfohlen)
MONGO_URI=mongodb+srv://<user>:<password>@<cluster-url>/?retryWrites=true&w=majority

# Variante 2: Einzelne Credentials (Fallback)
MONGO_USER=admin
MONGO_PASSWORD=secret
MONGO_CLUSTER_ADDRESS=<address>
MONGO_DB_NAME=shop
MONGO_APP_NAME=LimoDB

# --- Redis Server ---
# Wird für API-Rate-Limits und Pub/Sub im CPU-Cluster benötigt
REDIS_URL=redis://127.0.0.1:6379

# --- Künstliche Intelligenz (APIs) ---
# Für die automatischen LNN-Zeitungsartikel
GEMINI_API_KEY=dein_google_gemini_api_key
# Für die Tinda-Chatbots und Familien-Simulationen (Llama 3.1)
GROQ_API_KEY=dein_groq_api_key

# --- Admin & Engine ---
# Kommaseparierte Liste von Usernamen, die den raw /api/admin/engine Endpoint nutzen dürfen
ENGINE_WHITELIST=admin1,admin2
```

---

## 🐳 Deployment (Docker & CDN)

Das Backend enthält einen integrierten Proxy für das Bilder-CDN (`/cdn`). Wenn Nutzer Bilder für Limterest oder Kleinanzeigen hochladen, werden diese vom Backend via `multer` in den Arbeitsspeicher geladen, mit `sharp` extrem komprimiert (WebP) und im Ordner `cdn-data` abgelegt.

Für das produktive Deployment (z.B. per Portainer oder Docker Compose) solltest du einen Nginx-Container direkt daneben laufen lassen, der die statischen Bilder schnell ausliefert.

Das Backend erwartet den Nginx-Container unter `http://limazon-cdn:80`. 

**Ordner-Struktur für Volumes:**
Achte darauf, dass der Ordner `./cdn-data` als Shared Volume zwischen dem Node.js-Backend und dem Nginx-Container eingebunden ist.

---

## 🔒 Cronjobs & Automatisierungen

Das Backend verlässt sich nicht auf externe Cron-Services. Der Master-Prozess (`cluster.isPrimary`) übernimmt alle automatisierten Hintergrundaufgaben, um Race-Conditions mit den Workern zu vermeiden:

* **Minütlich:** Aktien/Krypto-Börse updaten, Auktionen beenden, Pakete zustellen.
* **10-15 Min:** Gebiets-Einnahmen der Gangs verteilen, Jugendamt-Check.
* **Stündlich:** KI-Zeitungsartikel generieren, Datenbank nach Schimpfwörtern scannen.
* **Täglich:** Steuern einziehen, WG-Mieten abbuchen, Kredite pfänden.
* **Wöchentlich (Sonntag 20 Uhr):** Lottoziehung.

## 🤝 Contributing
Wenn du neue API-Routen hinzufügst, denke daran, die Berechtigungen im `ENDPOINT_PERMISSIONS` Mapping (Zeile ca. 650) einzutragen und kritische Aktionen über das `logActivity()` System zu tracken.