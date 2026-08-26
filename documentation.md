# 🛠️ Installationsanleitung (Server Setup)

Dieses Backend nutzt fortgeschrittene Features wie Video-Kompression, Caching, ein RAM-basiertes Rate-Limiting und dynamische YouTube-Downloads.

### 1. System-Voraussetzungen
Stelle sicher, dass folgende Pakete auf dem System installiert sind:
*   Node.js (v18 oder neuer empfohlen)
*   MongoDB (Lokal oder als Atlas-Cluster)
*   Redis-Server (Zwingend erforderlich für das Cluster-Pub/Sub, Rate-Limiting und AmongUs-P2P)
*   FFmpeg (Wird für die Thumbnail-Generierung von Videos benötigt)
*   yt-dlp (Für den LimTube YouTube-Import)

Auf Debian/Ubuntu-basierten Systemen:
sudo apt update
sudo apt install redis-server ffmpeg
# yt-dlp immer aktuell über das offizielle Repo laden:
sudo curl -L https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp -o /usr/local/bin/yt-dlp
sudo chmod a+rx /usr/local/bin/yt-dlp

### 2. Projekt klonen & Abhängigkeiten installieren
# In den Projektordner wechseln
npm install

### 3. Umgebungsvariablen (.env oder secret.env)
Erstelle eine `secret.env` Datei im Hauptverzeichnis (oder unter `/etc/secrets/secret.env`) mit folgendem Inhalt:
PORT=10000
MONGO_URI=mongodb+srv://<USER>:<PASSWORT>@<CLUSTER>/shop?retryWrites=true&w=majority
SESSION_SECRET=dein_super_geheimes_session_secret
REDIS_URL=redis://127.0.0.1:6379
FRONTEND_URL=https://deine-frontend-domain.de
GEMINI_API_KEY=dein_gemini_key_fuer_lnn_news
GROQ_API_KEY=dein_groq_key_fuer_npc_chats
ENGINE_WHITELIST=admin1,admin2

Gibt es hier auch als .env.example https://git.slimo.v6.rocks/limazon/nodejs/~files/main/.env.example
Bitte stelle auch Sicher, das du im Server Code "api.limazon.v6.rocks" und "app.limazon.v6.rocks" zu deinen URLs ausstauschst!
### 4. Server starten
Da die Anwendung das Node.js `cluster` Modul nutzt, startet sie automatisch einen Worker-Prozess pro verfügbarem CPU-Kern. Für den dauerhaften Hintergrundbetrieb empfiehlt sich PM2 oder Docker:

# Normaler Start
node server.js

# Mit PM2 (empfohlen für Dauerbetrieb)
pm2 start server.js --name "limazon-api"

By the way: Intern wird für unsere Demo Docker verwendet. Die Docker Dateien stehen dafür hier aber nicht bereit.

---

# 📚 Limazon API Endpoints - Detaillierte Übersicht

> Generelle Hinweise: 
> * Geschützte Endpunkte erfordern ein Session-Cookie (isAuthenticated) oder einen gültigen API-Key im Header (x-api-key / Authorization: Bearer ...).
> * Admin-Routen sind durch ein rollenbasiertes System (isAdmin) geschützt.

## 🔐 1. Authentifizierung, Sicherheit & Account
Verwaltung von Sessions, DSGVO-Daten, Passkeys und API-Schlüsseln.

*   POST /api/auth/register
    *   Registriert einen neuen Benutzer. Benötigt zwingend einen inviteCode. Führt strenge Namensfilter- und Geräte-Bann-Prüfungen durch.
*   POST /api/auth/login
    *   Klassischer Login mit Username/Passwort. Limitiert auf max. 50 Versuche/5 Min. pro IP (via Redis).
*   POST /api/auth/temp-login
    *   Erlaubt Admins die "Impersonation" (Einloggen als anderer User) mithilfe eines 10-Minuten-Codes.
*   GET /api/webauthn/login-options & POST /api/webauthn/login-verify
    *   Passwortloser Login (WebAuthn/Passkeys) mittels Fingerabdruck, Face ID oder Windows Hello.
*   GET /api/account/export
    *   DSGVO-Export. Generiert eine formatierte JSON-Datei mit allen Profil-, Inventar-, Immobilien- und Transaktionsdaten.
*   POST /api/account/api-keys & GET /api/account/api-keys
    *   Erstellt und listet persönliche API-Schlüssel für externe Skripte (werden nur einmal im Klartext gezeigt).
*   DELETE /api/account/me
    *   Der "Rote Knopf". Löscht nach Passwortbestätigung den Account und triggert einen globalen Cleanup (löscht Immobilien, Haustiere, Chats, etc.).

## 🛒 2. Wirtschaft, Logistik & Produktion
Das Herzstück des Systems: Einkaufen, Liefern und Herstellen von Items.

*   GET /api/products
    *   Lädt alle Shop-Produkte. Extrem schnell, da die Antwort als vorgerenderter JSON-String aus dem RAM (globalProductCacheString) kommt.
*   POST /api/purchase
    *   Warenkorb-Checkout. Nutzt strikte MongoDB ACID-Transaktionen, um Item-Duplizierungen zu verhindern. Bucht Geld ab, updatet den globalen Stock und triggert ggf. das "Infinity Money" Achievement.
*   POST /api/products/sell
    *   Verkauft Items zurück an den Shop. Beinhaltet ein Wahrscheinlichkeitssystem (Anti-Wucher-Schutz & Marktsättigung) und setzt bei Fehlschlag einen 60-Sekunden-Cooldown.
*   GET /api/delivery/providers & POST /api/delivery/send
    *   Logistik-System. Wählt zwischen verschiedenen Lieferdiensten (z.B. Limo Prime, DHL, Hermes) mit dynamischen Zeiten/Kosten. Easter Egg: 5% Chance auf die Tiefseebahn.
*   GET /api/3dprint/status & POST /api/3dprint/start
    *   3D-Druck Studio: Spieler mit einem 3D-Drucker können Baupläne (z.B. Mando-Helm, Flexi-Drachen) drucken. Verbraucht Filament und hat Erfolgschancen (Fehlschlag = Spaghetti-Salat).

## 🏦 3. Finanzen, Schufa & Krypto (Limo Exchange)
Das Finanzzentrum. Kredite, Überweisungen und der dynamische Krypto-Markt.

*   POST /api/bank/transfer
    *   Überweist Geld/Tokens an andere Spieler. Der High-Limit-Modus erlaubt gigantische Beträge, zieht aber 1% Gebühr für die Staatskasse ab.
*   GET /api/bank/loan & POST /api/bank/loan/apply / pay
    *   Das Schufa-System. Kreditrahmen und Zinsen basieren auf dem dynamischen Schufa-Score des Users. Ein Cron-Job pfändet automatisch Konten bei Nichtzahlung (inkl. Score-Abzug).
*   GET /api/finance/market & POST /api/finance/trade
    *   Krypto-Markt: Zeigt Kurse (BTC, DOGE, LIMO, VOID) an und wickelt Trades ab. Die Kurse simulieren echte Volatilität und werden alle 30 Sekunden per Redis an alle Worker-Kerne synchronisiert.
*   POST /api/stonks/buy & POST /api/stonks/sell
    *   Kauf/Verkauf von Unternehmensanteilen mit einem Algorithmus, der auf Angebot/Nachfrage (Gravity & Volatility) reagiert. 5-Minuten Trade-Cooldown.

## 📱 4. Social Media, Medien & Kommunikation
Das Limazon "Internet" im Spiel.

*   POST /api/limtube/upload & GET /api/limtube/feed
    *   LimTube: Erlaubt den Video-/Audio-Upload (inkl. automatischer WebP/JPG-Thumbnail-Generierung via FFmpeg). Limitierte Uploads (außer für Prime-User).
*   POST /api/limtube/import-youtube
    *   Lädt Videos von YouTube über yt-dlp direkt auf den eigenen Server herunter und veröffentlicht sie im LimTube-Account des Nutzers.
*   POST /api/limabook/post & GET /api/limabook/feed
    *   Limabook: Das interne Social-Network. Posts mit Text und Bildern (Bilder werden serverseitig in platzsparendes WebP umgewandelt).
*   POST /api/limterest/pin
    *   Limterest: Bilder pinnen und mit Tags versehen (nur über URLs, Base64 blockiert).
*   POST /api/whatslim/start & POST /api/whatslim/group
    *   WhatsLim: 1-zu-1 Chats und Gruppenchats mit anderen Spielern. Easter Egg: Chatte mit dem KI-Bot "Ollama".

## 🦹 5. Kriminalität, Kartell & Gangs
Die düstere Seite von Limazon.

*   POST /api/crime/rob
    *   Versucht, einen Spieler auszurauben. Erfolgschance sinkt bei reichen Opfern oder Alarmanlagen im Inventar. Raid Boss: Das Ausrauben von Admins hat eine 5% Chance, ist aber extrem lukrativ (bei Fail: SEK-Stürmung & 50% Kontenpfändung).
*   POST /api/cartel/apply & POST /api/cartel/buy-stock
    *   Spieler können sich beim Kartell bewerben. Nach Annahme können sie "Limo-Kristalle" im Großhandel einkaufen.
*   POST /api/drugs/use
    *   Konsum von Kristallen. Chance auf massiven Crime-Buff (Cooldown-Reset), Überdosis (Krankenhaus-Rechnung) oder Razzia (SEK stürmt das Haus, User muss auspacken).
*   POST /api/cartel/snitch
    *   Das Verhör-Zimmer. Wird man erwischt, muss man den Namen eines echten Dealers tippen, um seine Strafe zu senken. (Triggert LNN News).
*   POST /api/gangs/create, attack, rent-zone
    *   Gangs gründen, Upgrades (Bunker, Anwalt) kaufen, Kriege gegen andere Gangs führen (Berechnung durch Mitgliederanzahl + Upgrades) und lukrative Zonen (Arcade, Casino, Bank) mieten, die passives Einkommen generieren.

## ❤️ 6. Familie, Haustiere & Standesamt
Das digitale Privatleben.

*   GET /api/tinda/stack & POST /api/tinda/swipe
    *   Die Dating-App. Rechts-Swipes bei verheirateten Spielern triggern mit 5% Chance einen "Paparazzi Skandal" (Strafe + Sofort-Scheidung). Bei einem Match antwortet ein flirty KI-Bot (via Groq API).
*   POST /api/tinda/chat/:chatId/have-child
    *   Ehepartner können ein Kind bekommen. Erzeugt Tamagotchi-Chats für Familie und Kind.
*   POST /api/tinda/child/:chatId/feed
    *   Wenn das Kind nicht gefüttert wird, holt das Jugendamt (Cron-Job) es ab und steckt es in das orphanage (Waisenhaus).
*   POST /api/pets/adopt, feed, pension, resurrect
    *   Adoption von Tieren (Hund bis Dinosaurier). Vernachlässigte Tiere landen auf dem Friedhof. Satanisches Ritual: Für 500 Milliarden können tote Tiere als Zombies wiederbelebt werden.
*   POST /api/standesamt/propose & respond
    *   Erlaubt echten Spielern, einander Ringe zu kaufen und Anträge zu machen (inklusive gemeinsamer WG und Ehekonto).

## 🏠 7. Alltag: Limea (Möbel), Jobs & Schule
Wohnen und Arbeiten.

*   POST /api/realestate/buy
    *   Haus kaufen. Der User zieht automatisch aus alten WGs aus. Landlord-Abonnenten bekommen 20% Rabatt.
*   POST /api/realestate/my-home/layout
    *   Speichert das genaue X/Y/Rotation-Layout von gekauften Limea-Möbeln.
*   POST /api/jobs/work
    *   Geld verdienen. Verdienst skaliert mit dem Job-Level. Chance auf Beförderung (Level-Up) oder lustige Random-Events.
*   POST /api/school/attend
    *   Schul-Simulator. Random-Events (Referat geht viral, Spicken erwischt, Feueralarm). Generiert Noten für das /api/school/zeugnis.

## ⚖️ 8. Gericht, Demokratie & News (LNN)
Community-Steuerung und Abstimmungen.

*   POST /api/court/file & vote
    *   Gerichtsprozesse eröffnen (z.B. bei Scheidungen mit Kindern = Sorgerechtsstreit um 50% des Vermögens). Die Jury (andere Spieler) stimmt ab. Gesichert durch strenge Anti-Smurf-Fingerprints.
*   POST /api/mayor/vote
    *   Wahl des Bürgermeisters aus der Community.
*   POST /api/mayor/taxes & stimulus
    *   Bürgermeister-Funktionen: Steuersatz für den gesamten Server anpassen (0.1% bis 1.5%) oder ein Konjunkturpaket (Geld aus der Staatskasse an alle) verteilen.
*   POST /api/admin/news/trigger-ai (LNN News)
    *   Die KI (Gemini) scannt alle 75 Minuten die Datenbank-Logs (Beichten, Überfälle, Jugendamt) und schreibt eine extrem zynische, automatische Boulevard-Schlagzeile für den Server.

## 🛠️ 9. Admin Engine & System-Wartung
Entwickler-Werkzeuge.

*   POST /api/admin/engine
    *   Der God-Mode. Erlaubt rohe MongoDB-Befehle (find, update, deleteMany) direkt über das Web-Frontend. Abgesichert über eine Collection-Whitelist und die .env ENGINE_WHITELIST.
*   GET /api/admin/health-check & api/system/stats
    *   Liefert Echtzeit-Metriken über RAM-Verbrauch (Heap), CPU-Last pro Kern, Redis-Status und zählt per GitHub-API die "Lines of Code" des Frontends.
*   POST /api/admin/system/normalize & fix-decimals
    *   Data-Sanitizer. Normalisiert korrupte Kontostände, rundet fehlerhafte Floating-Points und repariert alte Bild-URLs in der Datenbank.
*   DELETE /api/account/users/:id
    *   Vernichtet einen User restlos. Löst komplexe Verkettungen (entfernt den User aus fremden WGs, löst Gangs auf, löscht alle Social-Media-Posts).