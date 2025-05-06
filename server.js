// server.js
const express = require('express');
const fs = require('fs');
const http = require('http');
const cors = require('cors');
const { MongoClient } = require('mongodb');

const app = express();
const HTTP_PORT = 80; // Oder der gewünschte Port
const PRODUCTS_FILE = 'products.json';
const TIMEZONE = 'Europe/Berlin';

// MongoDB config
const mongoUser = 'git'; // Stelle sicher, dass dies korrekt ist
const mongoPassword = 'c72JfwytnPVD0YHv'; // Stelle sicher, dass dies korrekt ist
const mongoUri = `mongodb+srv://${mongoUser}:${mongoPassword}@limodb.kbacr5r.mongodb.net/?retryWrites=true&w=majority&appName=LimoDB`;
const mongoDbName = 'shop';
const mongoCollectionName = 'products';

app.use(cors());
app.use(express.json());

let productsCollection;

function writeProductsFile(products) {
  fs.writeFileSync(PRODUCTS_FILE, JSON.stringify({ products }, null, 2));
}

function readProductsFile() {
  if (!fs.existsSync(PRODUCTS_FILE)) return { products: [] };
  try {
    const data = fs.readFileSync(PRODUCTS_FILE);
    return JSON.parse(data);
  } catch (error) {
    console.error("Fehler beim Lesen von products.json:", error);
    return { products: [] }; // Gib leere Liste zurück bei Fehler
  }
}

/**
 * Bidirektionaler Sync zwischen local JSON und MongoDB
 * Sorgt dafür, dass MongoDB die Master-Quelle ist und die lokale Datei updated wird.
 */
async function syncLocalAndRemote() {
  try {
    // 1) Lade lokale und remote Produkte
    const localProducts = readProductsFile().products;
    let remoteProducts = await productsCollection.find().toArray();

    // Map für existenz-check
    const remoteMap = new Map(remoteProducts.map(p => [p.id, p]));
    const localMap = new Map(localProducts.map(p => [p.id, p]));

    // 2) Füge lokale Produkte hinzu, die in MongoDB fehlen
    for (const local of localProducts) {
      if (!remoteMap.has(local.id)) {
        // Stelle sicher, dass stock und default_stock gesetzt sind, wenn sie fehlen
        const productToAdd = {
          ...local,
          stock: local.stock ?? 20,
          default_stock: local.default_stock ?? local.stock ?? 20 // Fallback default_stock
        };
        await productsCollection.insertOne(productToAdd);
        console.log(`➕ Lokales Produkt mit ID ${local.id} zu MongoDB hinzugefügt.`);
      }
    }

    // 3) Lösche remote Produkte, die lokal fehlen (Optional, je nach Anforderung, hier nicht gemacht, da MongoDB Master ist)
    // Wenn MongoDB Master ist, sollten lokale Löschungen normalerweise nicht zu Remote-Löschungen führen.

    // 4) Re-fetch remote nach Einfügungen
    remoteProducts = await productsCollection.find().toArray(); // Lade die aktualisierte Liste
    const newRemoteMap = new Map(remoteProducts.map(p => [p.id, p]));

    // 5) Merge-Liste erstellen in Reihenfolge der Remote-Produkte (da MongoDB Master)
    // Dies stellt sicher, dass die lokale Datei den aktuellen Stand aus MongoDB widerspiegelt.
    const merged = remoteProducts.map(p => {
        const mergedItem = { ...p };
        // Stelle sicher, dass Schema-Felder (Stock und Default-Stock) vorhanden sind,
        // falls sie in alten Einträgen fehlen. MongoDB sollte dies idealerweise beim Insert/Update sicherstellen.
        if (mergedItem.stock === undefined) mergedItem.stock = 20;
        if (mergedItem.default_stock === undefined) mergedItem.default_stock = mergedItem.stock; // Default ist der aktuelle Stock, falls default fehlt
        return mergedItem;
    });

    // Sortiere die gemergte Liste nach ID, um Konsistenz zu gewährleisten
    merged.sort((a, b) => a.id - b.id);


    // 6) Schreibe merged Produkte in JSON
    writeProductsFile(merged);
    console.log(`🔄 Lokale products.json auf ${merged.length} Einträge aktualisiert (aus MongoDB geladen).`);

    // 7) Upsert merged in MongoDB ohne _id (Dieser Schritt könnte redundant sein, wenn MongoDB die Master-Quelle ist
    //    und der vorherige Schritt (2) fehlende lokale Produkte hinzufügt.
    //    Behalten wir es vorerst, falls es andere Sync-Fälle abdecken soll.)
    //    *WICHTIG:* Wenn MongoDB die Quelle ist, sollten wir hier nicht einfach alles wieder hochladen.
    //    Ein einfacherer Sync wäre: Lokale Datei laden -> Fehlende lokale Produkte zu Remote hinzufügen -> Remote laden -> Lokale Datei mit Remote überschreiben.
    //    Lassen wir die Upsert-Logik für jetzt weg, da sie potenziell ungewollte Änderungen von der lokalen Datei zurück pusht.
    //    Die vorherige Logik, die lokale Produkte zu Remote hinzufügt (falls sie in remote fehlen), ist zentraler.
    //    Der wichtigere Sync ist, die lokale Datei IMMER aus Remote neu zu erstellen.

    // Neuer Ansatz für 7: Nur sicherstellen, dass alle remote Produkte *mindestens* im lokalen JSON sind.
    // Die aktuelle Step 5+6 Logik (Remote laden, mergen, JSON schreiben) macht genau das.
    // Die Upsert-Schleife unten würde nur nötig sein, wenn wir Änderungen VON der lokalen Datei ZUR MongoDB pushen wollen,
    // was bei stock-Updates über das PATCH-Endpoint nicht der Fall ist und potenziell Konflikte verursacht.
    // Entfernen wir die Upsert-Schleife, da die JSON-Datei jetzt nur ein lokaler Cache ist, der aus MongoDB erstellt wird.
    /*
    for (const prod of merged) {
      const { _id, ...data } = prod; // _id nicht upserten
      await productsCollection.updateOne(
        { id: prod.id },
        { $set: data },
        { upsert: true } // upsert: true fügt ein, falls nicht gefunden (basierend auf { id: prod.id })
      );
    }
    console.log(`🔄 MongoDB auf ${merged.length} Einträge synchronisiert (aus gemergter Liste, primär aus Remote).`);
    */
     console.log(`✅ Synchronisation beendet. MongoDB ist die Quelle für die products.json.`);


  } catch (error) {
    console.error('❌ Fehler während der Synchronisation:', error);
    // Je nach Schwere des Fehlers hier ggf. Prozess beenden oder weiterlaufen lassen
  }
}


async function resetProductStock() {
  try {
    const result = await productsCollection.updateMany({}, [{ $set: { stock: '$default_stock' } }]);
    console.log(`♻️ Lagerbestand auf ${result.modifiedCount} Produkte auf default_stock zurückgesetzt.`);
    // Nach dem Reset die lokale Datei aktualisieren
    await syncLocalAndRemote();
  } catch (error) {
    console.error('❌ Fehler beim Zurücksetzen des Lagerbestands:', error);
  }
}

// Init MongoDB-Verbindung
MongoClient.connect(mongoUri)
  .then(async client => {
    const db = client.db(mongoDbName);
    productsCollection = db.collection(mongoCollectionName);
    console.log('✅ MongoDB verbunden.');

    // Sicherstellen, dass Index auf 'id' existiert für effiziente Suchen/Updates
    await productsCollection.createIndex({ id: 1 }, { unique: true }).catch(console.error);

    await syncLocalAndRemote(); // Initialer Sync beim Start

    // HTTP Server starten
    http.createServer(app).listen(HTTP_PORT, () => {
      console.log(`🌐 HTTP-Server läuft auf Port ${HTTP_PORT}`);
    });
  })
  .catch(err => {
    console.error('❌ MongoDB-Verbindung fehlgeschlagen:', err);
    process.exit(1); // Beende den Prozess bei Verbindungsfehler
  });

// Täglicher Reset um 00:00 Europe/Berlin
// Verwendet setInterval, was problematisch sein kann, wenn die Funktion länger als 1s dauert.
// Eine robustere Methode wäre ein Cronjob oder ein Scheduler-Modul.
// Für dieses Beispiel behalten wir setInterval, aber beachten das Potential für Ungenauigkeiten.
console.log(`⏳ Tägliches Zurücksetzen des Lagerbestands geplant für 00:00 Uhr ${TIMEZONE}.`);
setInterval(() => {
  const now = new Date().toLocaleString('de-DE', { timeZone: TIMEZONE });
  const time = now.split(', ')[1]; // Extrahiere die Zeit (z.B. "00:00:00")
   // Füge eine Toleranz hinzu, falls der Check nicht exakt um 00:00:00 läuft
   // Oder prüfe nur auf die Stunde und Minute
   const date = new Date(new Date().toLocaleString('en-US', { timeZone: TIMEZONE })); // Erzeuge Date Objekt in der Ziel-TZ
   if (date.getHours() === 0 && date.getMinutes() === 0 && date.getSeconds() <= 5) { // Prüfe um Mitternacht +/- 5s
     console.log('⏰ Mitternacht erreicht. Setze Lagerbestand zurück...');
     resetProductStock();
   }
}, 1000); // Prüfe jede Sekunde

// API Endpoints

// GET Produkte
app.get('/api/products', async (req, res) => {
  try {
    const products = await productsCollection.find().toArray();
    // Sicherstellen, dass stock und default_stock immer vorhanden sind, falls DB inkonsistent ist
    const sanitizedProducts = products.map(p => ({
        ...p,
        stock: p.stock ?? 20, // Standardwert 0, wenn stock fehlt
        default_stock: p.default_stock ?? p.stock ?? 0 // Fallback
    }));
    res.json({ products: sanitizedProducts });
  } catch (err) {
    console.error('Fehler beim Abrufen der Produkte:', err);
    res.status(500).json({ error: 'Fehler beim Abrufen der Produkte!' });
  }
});

// POST Neues Produkt hinzufügen
app.post('/api/products', async (req, res) => {
  let { name, image_url, price, stock } = req.body; // stock wird aus dem Body gelesen

  // Basic validation
  if (!name || !image_url || !price) {
      return res.status(400).json({ error: 'Name, Bild-URL und Preis sind erforderlich!' });
  }

  // Price validation
  price = price.trim();
  if (!price.startsWith('$')) price = `$${price}`;
  const numericPrice = parseFloat(price.replace(/[^0-9.]/g, ''));
  if (isNaN(numericPrice) || numericPrice < 0) {
      return res.status(400).json({ error: 'Ungültiger Preisformat oder Wert!' });
  }

  // Stock validation and default
  let initialStock = 20; // Standardwert
  if (stock !== undefined) {
      const parsedStock = parseInt(stock, 10);
      if (!isNaN(parsedStock) && Number.isInteger(parsedStock) && parsedStock >= 0) {
          initialStock = parsedStock;
      } else {
          console.warn(`Ungültiger Stock-Wert im POST-Request für neues Produkt: "${stock}". Verwende Standardwert ${initialStock}.`);
          // Man könnte hier auch einen Fehler zurückgeben, aber ein Fallback ist oft benutzerfreundlicher.
      }
  }

  // Generiere eine eindeutige 6-stellige ID
  let newId;
  let idExists = true;
  // Versuche mehrmals, eine eindeutige ID zu finden
  for (let i = 0; i < 10; i++) {
     newId = Math.floor(100000 + Math.random() * 900000);
     const existing = await productsCollection.findOne({ id: newId });
     if (!existing) {
         idExists = false;
         break;
     }
  }
  if (idExists) {
      console.error('Fehler: Konnte nach mehreren Versuchen keine eindeutige ID generieren.');
      return res.status(500).json({ error: 'Fehler bei der ID-Generierung, bitte versuchen Sie es erneut.' });
  }


  const prod = {
    id: newId,
    name,
    image_url,
    price,
    stock: initialStock, // Initialer Bestand
    default_stock: initialStock // Standard-Bestand ist der initiale Bestand
  };

  try {
    await productsCollection.insertOne(prod);
    // Nach dem Hinzufügen synchronisieren, um die lokale Datei zu aktualisieren
    await syncLocalAndRemote();
    res.status(201).json({ message: 'Produkt hinzugefügt!', product: prod });
  } catch (err) {
    console.error('Fehler beim Hinzufügen des Produkts:', err);
    res.status(500).json({ error: 'Fehler beim Hinzufügen des Produkts!' });
  }
});

// DELETE Produkt löschen
app.delete('/api/products/:id', async (req, res) => {
  const id = parseInt(req.params.id, 10);
  if (!/^\d{6}$/.test(req.params.id)) {
      return res.status(400).json({ error: 'Ungültiges ID-Format! Muss 6 Ziffern sein.' });
  }
  try {
    const result = await productsCollection.deleteOne({ id: id }); // Lösche nach 'id', nicht '_id'
    if (result.deletedCount === 0) {
        return res.status(404).json({ error: 'Produkt nicht gefunden!' });
    }
    // Nach dem Löschen synchronisieren, um die lokale Datei zu aktualisieren
    await syncLocalAndRemote();
    res.json({ message: `Produkt mit ID ${id} gelöscht!` });
  } catch (err) {
    console.error('Fehler beim Löschen des Produkts:', err);
    res.status(500).json({ error: 'Fehler beim Löschen des Produkts!' });
  }
});

// PATCH Lagerbestand aktualisieren (NEU HINZUGEFÜGT)
app.patch('/api/products/:id', async (req, res) => {
  const id = parseInt(req.params.id, 10);
  if (!/^\d{6}$/.test(req.params.id)) {
    return res.status(400).json({ error: 'Ungültiges ID-Format! Muss 6 Ziffern sein.' });
  }

  const { stock } = req.body;

  // Validierung für den Stock-Wert
  if (stock === undefined || typeof stock !== 'number' || !Number.isInteger(stock) || stock < 0) {
      // Überprüfe auch, ob es NaN ist, falls parseInt fehlschlägt (obwohl typeof number das abfangen sollte)
       if (isNaN(stock)) {
           return res.status(400).json({ error: 'Ungültiger Lagerbestandswert (NaN)!' });
       }
      return res.status(400).json({ error: 'Ungültiger Lagerbestandswert! Muss eine nicht-negative Ganzzahl sein.' });
  }

  try {
    const result = await productsCollection.updateOne(
      { id: id }, // Finde das Produkt nach der 'id'
      { $set: { stock: stock } } // Setze nur das 'stock'-Feld
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({ error: `Produkt mit ID ${id} nicht gefunden!` });
    }

    // Nach dem Update des Stocks in der DB MÜSSEN wir syncLocalAndRemote aufrufen,
    // damit die lokale JSON-Datei, die beim nächsten Start geladen wird, korrekt ist.
    // Bei jedem einzelnen Stock-Update über das PATCH-Endpoint syncen ist eventuell viel I/O,
    // aber nötig, wenn die lokale Datei als Fallback dient.
    // EINE ALTERNATIVE wäre: Die lokale Datei nur beim Start aus der DB laden
    // und NICHT nach jeder kleinen Änderung syncen. Das würde die Schreiblast reduzieren.
    // Belassen wir es vorerst beim Sync nach jeder Änderung, da es die Konsistenz von DB/JSON sicherstellt.
     await syncLocalAndRemote();


    // Optional: Lade das aktualisierte Produkt, um es in der Antwort zurückzugeben
    const updatedProduct = await productsCollection.findOne({ id: id });


    res.json({ message: `Lagerbestand für Produkt ${id} aktualisiert.`, product: updatedProduct });

  } catch (err) {
    console.error(`Fehler beim Aktualisieren des Lagerbestands für Produkt ${id}:`, err);
    res.status(500).json({ error: 'Fehler beim Aktualisieren des Lagerbestands!' });
  }
});


// PATCH Lagerbestand zurücksetzen
app.patch('/api/products/reset', async (req, res) => {
  try {
    await resetProductStock();
    res.json({ message: 'Lagerbestand auf Standardwerte zurückgesetzt.' });
  } catch (err) {
    console.error('Fehler beim Zurücksetzen des Lagerbestands:', err);
    res.status(500).json({ error: 'Fehler beim Zurücksetzen des Lagerbestands!' });
  }
});

// POST Manuelle Synchronisation triggern
app.post('/api/products/sync', async (req, res) => {
  try {
    await syncLocalAndRemote();
    res.json({ message: 'Bidirektionaler Sync durchgeführt.' });
  } catch (err) {
    console.error('Fehler beim manuellen Sync:', err);
    res.status(500).json({ error: 'Fehler beim Sync.' });
  }
});

// Standard-Route für unhandled requests
app.use((req, res) => {
    res.status(404).send('Endpoint nicht gefunden');
});