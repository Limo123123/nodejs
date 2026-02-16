# Limo Open Source Project (Limazon) - Backend Documentation

Diese Dokumentation beschreibt die Architektur, die Module und die API-Endpunkte des Limo/Limazon Node.js Backends. Das System betreibt eine komplexe virtuelle Wirtschaft, soziale Netzwerke, Minispiele und dynamische Events.

## 🛠 Tech Stack
* **Framework:** Node.js mit Express.js
* **Datenbank:** MongoDB (Native Driver)
* **Authentifizierung:** Cookie-basierte Sessions (`express-session` + `connect-mongo`) + bcrypt
* **Sicherheit:** Custom Rate-Limiting (RAM-basiert), Helmet, CORS
* **KI-Integration:** * Google Gemini (für das Limo News Network - LNN)
  * Ollama (für KI-Chatbots in der Tinda-Erweiterung)

---

## ⚙️ Umgebungsvariablen (`secret.env` / System Env)
Folgende Variablen werden zum Starten des Servers benötigt:
* `PORT`: HTTP Port (Standard: 10000)
* `MONGO_URI` (oder `MONGO_USER`, `MONGO_PASSWORD`, `MONGO_CLUSTER_ADDRESS`): Verbindungsstring zur MongoDB.
* `MONGO_DB_NAME`: Name der Datenbank (Standard: `shop`)
* `SESSION_SECRET`: Geheimer Schlüssel für Cookie-Verschlüsselung.
* `FRONTEND_URL`: URL des produktiven Frontends für CORS.
* `GEMINI_API_KEY`: API-Key für die automatisierte Nachrichten-Generierung.
* `OLLAMA_URL`: URL zur lokalen Ollama-Instanz (für Tinda KI-Chats).

---

## 🗄 Datenbank-Collections
Das Backend nutzt eine Vielzahl an Collections, um die verschiedenen Features zu trennen:
* **Kern-Wirtschaft:** `users`, `products`, `orders`, `userInventories`, `portfolios`, `transactions`
* **Banking & Krypto:** `bankTransactions`, Token-Collections (`tokenCodes`, `tokenTransactions`)
* **Soziales:** `limChats`, `limMessages`, `limUserChatSettings`, `tindaSwipes`, `limterestPins`
* **Community Features:** `ideas`, `auctions`, `dontBlameMePosts`, `humans`, `ratings` (Court System & Human Grades)
* **Spiele & Kriminalität:** `wheels`, `highscores`, `robberyLogs`, `gangs`, `zones`
* **System:** `systemSettings`, `bugReports`, `banned_ips`

---

## 🚀 Kern-Features & Module

### 1. Wirtschaftssystem & Shop
* Dynamische Börse ("Limo Stonks") mit automatischen Preisschwankungen durch Angebot, Nachfrage, Schwerkraft und Chaos.
* Krypto-Börse mit simulierter Volatilität.
* Warenkorbsystem mit Transaktionssicherheit (MongoDB Sessions), um Race-Conditions zu vermeiden.
* Auktionshaus (User-to-User Handel).

### 2. Soziale Netzwerke
* **WhatsLim:** Echtzeit-Chatsystem (Einzel- und Gruppenchats) mit Share-Codes und Rechtesystem (Admin, Kick, Ban).
* **Tinda:** Dating-App-Klon. Zieht zufällige Profile aus der Datenbank. KI-Chatbots (Ollama) antworten bei einem Match.
* **Limterest:** Bilder-Pinnwand (Pinterest-Klon) mit Follower- und Like-System.
* **Don't Blame Me:** Anonyme Beicht-Plattform.

### 3. Kriminalität & Justiz
* **Crime System:** Überfälle auf andere Spieler. Erfolgschance basiert auf Reichtum und Sicherheits-Items (Alarmanlage).
* **The Heist:** Ein Community-Raid auf die Staatskasse. Firewall muss erst gemeinsam gehackt werden.
* **Court System:** Spieler können sich gegenseitig verklagen. Die Community stimmt ab (Schuldig / Unschuldig).
* **Gang Wars:** Spieler können Gangs gründen, sich Upgrades kaufen, Gebiete erobern und gegeneinander in den Krieg ziehen.

### 4. Job Center & Restaurant
* Jobs mit unterschiedlichen Cooldowns und Gehaltsstufen.
* Energie-System: Cooldowns können durch den Kauf von Essen im "Limo's Diner" reduziert werden.

### 5. Automatisierte Background-Jobs (Cron)
* **LNN (Limo News Network):** Gemini liest alle 45 Minuten die neuesten Datenbank-Ereignisse (Käufe, Überfälle, Matches) und schreibt eine satirische Schlagzeile.
* **Tax Collector:** Zieht alle 24 Stunden Steuern bei extrem reichen Spielern ein (außer sie besitzen ein Steuerschutz-Zertifikat).
* **Börsen-Update:** Aktualisiert Aktien- und Kryptokurse jede Minute.
* **Payday (Gangs):** Schüttet alle 10 Minuten Geld an Gangs aus, die Gebiete kontrollieren.

---

## 📡 API Endpoints (Übersicht)

*Die meisten Endpunkte erfordern eine aktive Session (`isAuthenticated` Middleware).*

### Authentifizierung & User
* `POST /api/auth/register` - Neuen Account anlegen.
* `POST /api/auth/login` - Login (inkl. Rate-Limiting & IP-Ban-Check).
* `POST /api/auth/logout` - Session beenden.
* `GET /api/auth/me` - Eigene Userdaten abrufen.
* `GET /api/profile/:username` - Öffentliches Profil & Achievements eines Users abrufen.

### Bank & Wirtschaft
* `GET /api/bank/transactions` - Eigene Überweisungshistorie.
* `POST /api/bank/transfer` - Limo-Dollar oder Tokens an andere senden.
* `POST /api/daily` - Tägliche Belohnung abholen.
* `GET /api/taxes/my-stats` - Steuerstatus einsehen.

### Shop & Inventar
* `GET /api/products` - Lädt alle Produkte (hochoptimiert durch String-Caching).
* `POST /api/purchase` - Checkout-Prozess (Warenkorb).
* `POST /api/products/sell` - Items aus dem Inventar zurückverkaufen.
* `GET /api/inventory` - Eigenes Inventar abrufen.

### Chat (WhatsLim)
* `GET /api/chat/chats` - Alle eigenen Chats abrufen.
* `POST /api/chat/chats/personal` - Neuen Chat via Share-Code starten.
* `POST /api/chat/chats/group` - Gruppe erstellen.
* `GET /api/chat/chats/:chatId/messages` - Nachrichten laden (inkl. Paginierung).
* `POST /api/chat/chats/:chatId/messages` - Nachricht senden.

### Tinda & Limterest
* `GET /api/tinda/stack` - 10 neue Profile für den Swipe-Stapel laden.
* `POST /api/tinda/swipe` - Rechts/Links Swipe (Triggert ggf. KI-Chat).
* `POST /api/limterest/pin` - Neues Bild posten.
* `GET /api/limterest/feed` - Bilder-Feed laden.

### Kriminalität & Gangs
* `POST /api/crime/rob` - Einen anderen User ausrauben.
* `POST /api/heist/hack` - Firewall der Staatskasse schwächen.
* `POST /api/heist/start` - Die Staatskasse ausrauben.
* `GET /api/gangs/dashboard` - Gang-Übersicht & Zonen-Status laden.
* `POST /api/gangs/attack` - Andere Gang angreifen.

### Admin-Panel (`isAdmin` Middleware)
* `POST /api/admin/data-manipulation` - Roher DB-Zugriff über das Frontend (abgesichert durch extra Passwort).
* `POST /api/admin/system/force-tax` - Manuelle Steuereintreibung.
* `POST /api/admin/system/revoke-infinity` - Setzt die Economy zurück.
* `POST /api/admin/news/trigger-ai` - Erzwingt eine neue LNN KI-Nachricht.

---

## 🔒 Sicherheits-Mechanismen
* **Transactions:** Sämtliche Finanz- und Shop-Transaktionen nutzen MongoDB `session.withTransaction()`, um Duplizierungs-Bugs bei schlechtem Internet zu verhindern.
* **Rate Limits:** In-Memory Rate-Limiter für Login-Versuche und globale API-Anfragen.
* **Sanitization:** Striktes Parsen von Eingaben (z.B. Umwandlung von String-Kontoständen in Floats, Limits auf Max-Integers).
* **Caching:** Der Shop `/api/products` wird als vorberechneter JSON-String im RAM gehalten, um tausende Requests pro Sekunde ohne DB-Last zu bedienen.