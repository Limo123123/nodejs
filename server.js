// server.js
const express = require('express');
const fs = require('fs');
const http = require('http');
const cors = require('cors');
const { MongoClient } = require('mongodb');

const app = express();
const HTTP_PORT = 80;
const PRODUCTS_FILE = 'products.json';
const TIMEZONE = 'Europe/Berlin';

// MongoDB-Konfiguration
const mongoUser = 'git';
const mongoPassword = 'c72JfwytnPVD0YHv';
const mongoUri = `mongodb+srv://${mongoUser}:${mongoPassword}@limodb.kbacr5r.mongodb.net/?retryWrites=true&w=majority&appName=LimoDB`;
const mongoDbName = 'shop';
const mongoCollectionName = 'products';

app.use(cors());
app.use(express.json());

let productsCollection;

// --- Hilfsfunktionen ---
function writeProductsFile(products) {
  fs.writeFileSync(PRODUCTS_FILE, JSON.stringify({ products }, null, 2));
}

function readProductsFile() {
  if (!fs.existsSync(PRODUCTS_FILE)) return { products: [] };
  return JSON.parse(fs.readFileSync(PRODUCTS_FILE));
}

/**
 * Bidirektionaler Sync:
 * - Fehlt ein Produkt in MongoDB, wird es eingefügt.
 * - Fehlt ein Produkt lokal, wird es aus MongoDB kopiert.
 * - Beide Datensätze werden gemerged, in products.json geschrieben und per upsert in MongoDB zurückgeschrieben.
 */
async function syncLocalAndRemote() {
  const local = readProductsFile().products;
  const remote = await productsCollection.find().toArray();

  const localMap = new Map(local.map(p => [p.id, p]));
  const remoteMap = new Map(remote.map(p => [p.id, p]));

  const merged = [];
  const allIds = new Set([...localMap.keys(), ...remoteMap.keys()]);

  for (const id of allIds) {
    let prod;
    if (localMap.has(id) && remoteMap.has(id)) {
      // Beide haben das Produkt: wir nehmen das Remote-Objekt (DB enthält aktuellste stock-Werte)
      prod = remoteMap.get(id);
    } else if (localMap.has(id)) {
      // Nur lokal vorhanden → ab in die DB
      prod = localMap.get(id);
      await productsCollection.insertOne(prod);
    } else {
      // Nur remote vorhanden → ab in die lokale Liste
      prod = remoteMap.get(id);
    }

    // Schema-Felder sicherstellen
    if (prod.stock === undefined) prod.stock = 20;
    if (prod.default_stock === undefined) prod.default_stock = prod.stock;

    // Upsert zurück in DB, um ggf. Schema-Felder hinzuzufügen
    await productsCollection.updateOne(
      { id: prod.id },
      { $set: prod },
      { upsert: true }
    );

    merged.push(prod);
  }

  // Lokale JSON-Datei auf den neuesten Stand bringen
  writeProductsFile(merged);
  console.log(`🔄 Bidirektionaler Sync abgeschlossen für ${merged.length} Produkt(e).`);
}

// Reset: stock ← default_stock
async function resetProductStock() {
  await productsCollection.updateMany(
    {},
    [{ $set: { stock: "$default_stock" } }]
  );
  console.log("♻️ Lagerbestand auf default_stock zurückgesetzt.");
  await syncLocalAndRemote(); // Backup + DB-Update
}

// --- Server-Init ---
MongoClient.connect(mongoUri, { useUnifiedTopology: true })
  .then(async client => {
    const db = client.db(mongoDbName);
    productsCollection = db.collection(mongoCollectionName);
    console.log("✅ MongoDB verbunden.");

    // 1) Bidirektionaler Sync (lokal ↔ remote)
    await syncLocalAndRemote();

    // 2) HTTP-Server starten
    http.createServer(app).listen(HTTP_PORT, () => {
      console.log(`🌐 HTTP-Server läuft auf Port ${HTTP_PORT}`);
    });
  })
  .catch(err => {
    console.error("❌ MongoDB-Verbindung fehlgeschlagen:", err);
    process.exit(1);
  });

// Täglicher Reset um 00:00 Europe/Berlin
setInterval(() => {
  const now = new Date().toLocaleString('de-DE', { timeZone: TIMEZONE });
  const time = now.split(', ')[1];
  if (time === '00:00:00') {
    resetProductStock();
  }
}, 1000);

// --- API-Endpunkte ---

// Alle Produkte
app.get('/api/products', async (req, res) => {
  try {
    const products = await productsCollection.find().toArray();
    res.json({ products });
  } catch {
    res.status(500).json({ error: "Fehler beim Abrufen!" });
  }
});

// Neues Produkt
app.post('/api/products', async (req, res) => {
  let { name, image_url, price, stock } = req.body;
  if (!name || !image_url || !price) {
    return res.status(400).json({ error: "Alle Felder erforderlich!" });
  }
  price = price.trim();
  if (!price.startsWith('$')) price = `$${price}`;
  const numericPrice = parseFloat(price.replace(/[^0-9.]/g, ''));
  if (isNaN(numericPrice)) {
    return res.status(400).json({ error: "Ungültiger Preis!" });
  }

  const newId = Math.floor(100000 + Math.random() * 900000);
  const prod = {
    id: newId,
    name,
    image_url,
    price,
    stock: stock ?? 20,
    default_stock: stock ?? 20
  };

  try {
    await productsCollection.insertOne(prod);
    await syncLocalAndRemote();
    res.status(201).json({ message: "Produkt hinzugefügt!", product: prod });
  } catch {
    res.status(500).json({ error: "Fehler beim Hinzufügen!" });
  }
});

// Produkt löschen
app.delete('/api/products/:id', async (req, res) => {
  const id = parseInt(req.params.id, 10);
  if (!/^\d{6}$/.test(req.params.id)) {
    return res.status(400).json({ error: "Ungültige ID!" });
  }
  try {
    const result = await productsCollection.deleteOne({ id });
    if (result.deletedCount === 0) {
      return res.status(404).json({ error: "Produkt nicht gefunden!" });
    }
    await syncLocalAndRemote();
    res.json({ message: "Produkt gelöscht!" });
  } catch {
    res.status(500).json({ error: "Fehler beim Löschen!" });
  }
});

// Reset per API
app.patch('/api/products/reset', async (req, res) => {
  try {
    await resetProductStock();
    res.json({ message: "Bestand zurückgesetzt." });
  } catch {
    res.status(500).json({ error: "Fehler beim Zurücksetzen." });
  }
});

// Manueller Sync-Trigger
app.post('/api/products/sync', async (req, res) => {
  try {
    await syncLocalAndRemote();
    res.json({ message: "Bidirektionaler Sync durchgeführt." });
  } catch {
    res.status(500).json({ error: "Fehler beim Sync." });
  }
});
