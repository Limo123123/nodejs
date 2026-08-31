# Limazon Backend - AI Guidelines & Context

Das Backend ist eine Node.js / Express-Anwendung mit MongoDB. Es läuft containerisiert via Docker Compose. Jeder User-Server (Instanz) läuft in einem eigenen Container mit eigener MongoDB-Datenbank, aber demselben Quellcode.

## 1. Dynamische URLs (Multi-Tenancy)
- Schreibe **niemals** festcodierte Domains (wie `app.limazon.v6.rocks` oder `api.limazon...`) in den Code.
- Für Links zum eigenen Backend nutze Request-Header: `const baseUrl = \`\${req.protocol}://\${req.get('host')}\`;`
- Für Links zum Frontend (z. B. CORS, WebAuthn, E-Mails) nutze: `process.env.FRONTEND_URL`.
- Wenn ein Hostname benötigt wird (z. B. WebAuthn rpID), generiere ihn dynamisch: `new URL(process.env.FRONTEND_URL).hostname`.

## 2. Cluster Mode & Cronjobs (ACHTUNG!)
- Der Server nutzt das Node.js `cluster` Modul. Er spannt für jeden CPU-Kern einen Worker auf.
- **Wichtig:** Alle Hintergrund-Aufgaben (`setInterval`, Cronjobs, Discord-Bots, Telegram-Hooks) dürfen **NUR** auf dem Master-Prozess laufen!
- Umschließe solche Jobs immer mit: `if (cluster.isPrimary) { setInterval(...) }`. Tust du das nicht, laufen Jobs x-fach parallel und zerstören die Wirtschaft.

## 3. Datenbank & Transaktionen
- Wir nutzen den nativen `mongodb` Treiber (kein Mongoose).
- Wenn bei Käufen, Verkäufen oder Überweisungen Geld den Besitzer wechselt und Items verschoben werden, MUSS eine MongoDB-Session (`client.startSession()`) und `session.withTransaction()` genutzt werden, um Race-Conditions (Duplication Glitches) zu verhindern.

## 4. Architektur-Regeln
- Passwörter oder API-Keys werden **nicht** hartcodiert.
- Bei der Erstellung von neuen User-Instanzen werden keine Klartext-Passwörter in `.env`-Dateien geschrieben. Das Haupt-Backend klont den Admin-Account via MongoDB direkt in die neue Datenbank (`shop_instanzname`).

## 5. Modul- & Streik-Architektur
Jeder Bereich des Spiels (z.B. Casino, Bank, Tinda) ist als einheitliches Modul definiert. Der Name des Moduls MUSS exakt dem HTML-Dateinamen (ohne `.html`) entsprechen.

Wenn du eine **neue API-Route** für eine neue Seite (z.B. `krankenhaus.html`) erstellst, beachte zwingend diese 3 Schritte:
1. **Türsteher (Middleware):** Alle neuen Routen müssen mit den Middlewares `isModuleEnabled('krankenhaus')` und `isNotOnStrike('krankenhaus')` geschützt werden.
   *Beispiel:* `app.use('/api/krankenhaus', isModuleEnabled('krankenhaus'), isNotOnStrike('krankenhaus'));`
2. **Admin-Panel (`admin.html`):** Der Schalter zum Deaktivieren muss in der `admin.html` (Tab: System) hinzugefügt werden. Die ID der Checkbox muss zwingend `id="mod-krankenhaus"` heißen, und der Key `'krankenhaus'` muss in das Array `allModKeys` eingetragen werden.
3. **Gewerkschaft:** Damit das Modul bestreikt werden kann, muss der Name `'krankenhaus'` im Backend in der Route `app.post('/api/strikes/propose')` in das Array `ALLOWED_MODULES` aufgenommen werden.