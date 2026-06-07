# Limazon API Endpoints - Detaillierte Übersicht

Alle geschützten Endpunkte erfordern ein gültiges Session-Cookie (`isAuthenticated`). Admin-Endpunkte erfordern zusätzlich entsprechende Rollen/Berechtigungen (`isAdmin`).

---

## 🔐 1. Authentifizierung & Account Management
Diese Routen verwalten den Zugriff und die Sicherheit der Benutzerkonten.

* **`POST /api/auth/register`**
  * **Beschreibung:** Registriert einen neuen Benutzer. Prüft auf verbotene Wörter (Wortfilter), gültige Zeichen und checkt, ob das Gerät gebannt ist (`x-device-fingerprint`). Startkapital: $5000 und 10 Tokens.
* **`POST /api/auth/login`**
  * **Beschreibung:** Meldet einen Benutzer an. RAM-basiertes Rate-Limiting blockiert Brute-Force-Angriffe (max. 50 Versuche/5 Min pro IP).
* **`POST /api/auth/temp-login`**
  * **Beschreibung:** Ermöglicht Admins, sich mit einem Einmal-Code in andere Accounts einzuloggen (Impersonation).
* **`POST /api/auth/logout`**
  * **Beschreibung:** Zerstört die Session und löscht das Cookie.
* **`GET /api/auth/me`**
  * **Beschreibung:** Gibt die eigenen, aktuellen Benutzerdaten zurück (Kontostand, Tokens, Admin-Status, Schufa-Score, aktive Kredite).
* **`PATCH /api/account/settings`**
  * **Beschreibung:** Speichert Account-Einstellungen (z.B. Infinity Money Toggle).
* **`PATCH /api/account/password`** & **`PATCH /api/account/username`**
  * **Beschreibung:** Ändert das Passwort oder den Benutzernamen (inkl. Wortfilter-Check).
* **`DELETE /api/account/me`**
  * **Beschreibung:** Löscht den eigenen Account und triggert den restlosen Cleanup aller verknüpften Daten (Häuser, Haustiere, Chats etc.). Erfordert Passwort-Bestätigung.
* **`GET /api/account/export`**
  * **Beschreibung:** DSGVO-konformer Datenexport. Bündelt Profil, Inventar, Immobilien, Haustiere und Transaktionen in eine JSON-Datei.

---

## 🛒 2. Shop, Inventar & Logistik
Das Herzstück der Wirtschaft. Kaufen, Verkaufen und Liefern von Items.

* **`GET /api/products`**
  * **Beschreibung:** Lädt alle Shop-Produkte. Hochoptimiert durch einen im RAM gecachten JSON-String.
* **`GET /api/products/:id/history`**
  * **Beschreibung:** Lädt den Börsen-Verlauf (Preishistorie) eines bestimmten Produkts.
* **`POST /api/purchase`**
  * **Beschreibung:** Checkout für den Warenkorb. Nutzt **MongoDB ACID-Transaktionen**, um Item-Duplizierung bei Lags zu verhindern. Zieht Geld ab und legt Items ins Inventar.
* **`POST /api/products/sell`**
  * **Beschreibung:** Verkauft Items aus dem Inventar zurück an den Shop. Hat ein Wahrscheinlichkeitssystem (Marktsättigung) und einen 60-Sekunden-Cooldown bei Fehlschlägen.
* **`GET /api/inventory`** & **`GET /api/orders`**
  * **Beschreibung:** Zeigt die Besitztümer und die Bestellhistorie des Users.
* **`GET /api/delivery/providers`** & **`POST /api/delivery/send`**
  * **Beschreibung:** Generiert Lieferdienste (z.B. Prime, DHL, Hermes) mit dynamischen Kosten/Zeiten und versendet Items aus dem Inventar an andere Spieler.

---

## 🏦 3. Banking, Schufa & Krypto (Limo Exchange)
Verwaltung der Finanzen, Kredite und Krypto-Wallets.

* **`POST /api/bank/transfer`**
  * **Beschreibung:** Überweist Geld oder Tokens an andere Spieler. Besitzt einen High-Limit-Modus (für Milliardenbeträge) mit 1% Gebühr, die in die Staatskasse fließt.
* **`GET /api/bank/loan`** & **`POST /api/bank/loan/apply`** / **`pay`**
  * **Beschreibung:** Zeigt die aktuellen Schufa-Konditionen an. Ermöglicht das Aufnehmen und Abbezahlen von Krediten. Zinsen und Limits basieren auf dem Schufa-Score.
* **`GET /api/finance/market`**
  * **Beschreibung:** Zeigt die aktuellen Kurse der Limo-Kryptowährungen (LIMO, BTC, DOGE, VOID) an.
* **`POST /api/finance/trade`**
  * **Beschreibung:** Kauft oder verkauft Kryptowährungen basierend auf den dynamischen Echtzeitkursen.

---

## 📈 4. Aktien (Limo Stonks) & Kleinanzeigen
Spieler-gesteuerte Märkte.

* **`POST /api/stonks/buy`** & **`POST /api/stonks/sell`**
  * **Beschreibung:** Kauft/Verkauft "echte" Unternehmensanteile. Hat einen 5-Minuten Trade-Cooldown.
* **`GET /api/classifieds`** & **`POST /api/classifieds`**
  * **Beschreibung:** Lädt und erstellt Kleinanzeigen (Spieler-zu-Spieler Marktplatz).
* **`POST /api/classifieds/:id/buy`**
  * **Beschreibung:** Direktkauf eines Items über ein Treuhand (Escrow)-System.
* **`POST /api/classifieds/:id/chat`** & **`offer`**
  * **Beschreibung:** Startet einen Verhandlungs-Chat mit dem Verkäufer und sendet Preisvorschläge.

---

## 🦹 5. Kriminalität, Gangs & Heists
Der Untergrund von Limazon.

* **`POST /api/crime/rob`**
  * **Beschreibung:** Versucht, einen anderen Spieler auszurauben. Erfolgschance sinkt bei reichen Opfern oder Alarmanlagen. Das Ausrauben von Admins ist ein "Raid Boss" mit extrem hohem Risiko (50% Strafe) aber massiver Beute.
* **`GET /api/heist/info`**, **`POST /api/heist/hack`**, **`POST /api/heist/start`**
  * **Beschreibung:** Community-Raid auf die Staatskasse. Zuerst muss die Firewall gemeinschaftlich gehackt werden, danach kann das Geld gestohlen werden.
* **`POST /api/bounty/place`**
  * **Beschreibung:** Setzt ein Kopfgeld auf einen anderen Spieler aus.
* **`POST /api/gangs/create`**, **`join`**, **`attack`**, **`rent-zone`**
  * **Beschreibung:** Erlaubt das Gründen von Gangs. Gangs können Upgrades kaufen, andere Gangs angreifen und Gebiete (Zones wie Casino, Arcade) für passives Einkommen mieten.

---

## ⚖️ 6. Gericht (Limo Court) & Politik
Das demokratische Rückgrat des Servers.

* **`POST /api/court/file`** & **`POST /api/court/vote`**
  * **Beschreibung:** Eröffnet ein Gerichtsverfahren gegen einen Spieler (z.B. wegen Betrug oder Sorgerechtsstreit). User stimmen als Jury ab. Mit Anti-Smurf (Device-Fingerprint).
* **`GET /api/mayor/election`** & **`POST /api/mayor/vote`**
  * **Beschreibung:** Zeigt aktive Wahlen und lässt das Volk den Bürgermeister wählen.
* **`POST /api/mayor/taxes`**, **`stimulus`**, **`pardon`**
  * **Beschreibung:** Bürgermeister-Exklusiv: Steuern ändern, Konjunkturpakete (Geld für alle) aus der Staatskasse verteilen und laufende Gerichtsfälle begnadigen.
* **`POST /api/petitions/create`** & **`sign`**
  * **Beschreibung:** Spieler können Petitionen starten. Erreichen sie genug Unterschriften, wird die Idee automatisch in die offizielle Ideenbox gepusht.

---

## ❤️ 7. Soziales: Tinda, Familie & Standesamt
Das Dating- und Familien-Netzwerk.

* **`GET /api/tinda/stack`** & **`POST /api/tinda/swipe`**
  * **Beschreibung:** Lädt Profile für die Dating-App. Rechts-Swipes bei verheirateten Spielern können einen "Paparazzi Skandal" auslösen (Strafe + Scheidung). Bei einem Match antwortet ein KI-Bot.
* **`POST /api/tinda/chat/:chatId/marry`** / **`divorce`** / **`have-child`**
  * **Beschreibung:** Ermöglicht das Heiraten von Matches, das Zusammenziehen und das Zeugen von (Adoptiv-)Kindern.
* **`POST /api/tinda/child/:chatId/feed`**
  * **Beschreibung:** Tamagotchi-Mechanik. Wenn Kinder nicht gefüttert werden, holt das Jugendamt sie ab und steckt sie in das Adoptionszentrum (`/api/orphanage/...`).
* **`POST /api/standesamt/propose`** & **`respond`**
  * **Beschreibung:** Erlaubt echten Spielern, einander Ringe zu kaufen und Heiratsanträge zu machen.

---

## 🐶 8. Haustiere (Pets) & Friedhof
Tamagotchi-System für die Spieler.

* **`POST /api/pets/adopt`**, **`feed`**, **`equip`**, **`pension`**
  * **Beschreibung:** Tiere adoptieren und pflegen. `pension` stoppt den Hunger-Timer für Geld.
* **`POST /api/park/toggle`** & **`interact`**
  * **Beschreibung:** Schickt das Tier in den virtuellen Park, wo es mit den Tieren anderer Spieler interagieren kann (generiert lustige Chat-Nachrichten).
* **`POST /api/pets/:id/resurrect`**
  * **Beschreibung:** (Satanskreis) Erweckt ein verhungertes Tier vom Friedhof für $500 Milliarden als Zombie wieder zum Leben.

---

## 🃏 9. Teachermon (Karten-Sammelspiel)
TCG mit Packs, Tausch und PvP.

* **`POST /api/teachermon/pack/buy`** & **`buy-multi`**
  * **Beschreibung:** Zieht 3 (oder 30) zufällige Karten aus dem gewählten Universum (RNG basierend auf Drop-Raten).
* **`POST /api/teachermon/satanic-circle`**
  * **Beschreibung:** Opfert eine bestimmte Anzahl an doppelten Karten (z.B. 50 Common), um eine höherwertige Karte zu beschwören.
* **`POST /api/teachermon/trades/create`** & **`accept`**
  * **Beschreibung:** Tauschbörse. Doppelte Karten können gegen andere getauscht werden.
* **`POST /api/teachermon/battles/create`** & **`accept`**
  * **Beschreibung:** Arena (Auto-Battler). Fordert Spieler in Werten wie "Intelligenz" heraus. Der Gewinner erhält beide Karten.

---

## 🏠 10. Immobilien (Limea & Real Estate)
Wohnen und Einrichten.

* **`POST /api/realestate/buy`** & **`sell`**
  * **Beschreibung:** Kauft ein Haus. Kündigt automatisch alle bestehenden Untermietverträge.
* **`POST /api/realestate/wg/invite`** & **`respond`**
  * **Beschreibung:** Lädt andere Spieler ein, als Mitbewohner einzuziehen (WG-System, Miete wird täglich abgebucht).
* **`POST /api/limea/buy`** & **`POST /api/realestate/my-home/layout`**
  * **Beschreibung:** Kauft Möbel (nach Pixelmaßen) und speichert die genauen Koordinaten (X, Y, Rotation) im Haus-Profil.

---

## 🛠 11. Admin & System
Vollständige Kontrolle über das Spiel. (Nur für `isAdmin`)

* **`GET /api/admin/health-check`** & **`system/stats`**
  * **Beschreibung:** Liefert detaillierte Metriken: RAM-Verbrauch, CPU-Last (pro Kern), DB-Status, Lines of Code.
* **`PUT /api/admin/users/:id`**
  * **Beschreibung:** Kontostände, Tokens, Berechtigungen und den Schufa-Score von Usern manipulieren.
* **`POST /api/admin/news/trigger-ai`**
  * **Beschreibung:** Zwingt Gemini, sofort die letzten DB-Ereignisse zu lesen und einen Zeitungsartikel zu schreiben.
* **`POST /api/admin/engine`**
  * **Beschreibung:** Das "God Mode" Interface. Ermöglicht rohe MongoDB-Befehle (`find`, `updateOne`, `deleteMany`) über das Web-Interface. Abgesichert gegen NoSQL-Injections und Whitelists.
* **`POST /api/admin/users/:id/temp-login-code`**
  * **Beschreibung:** Erzeugt einen 10-minütigen Code, mit dem sich ein Admin in den Account eines normalen Users einloggen kann.