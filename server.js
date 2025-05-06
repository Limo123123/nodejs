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

// MongoDB Setup
const mongoUser = 'git';
const mongoPassword = 'c72JfwytnPVD0YHv'; // dein Passwort
const mongoUri = `mongodb+srv://${mongoUser}:${mongoPassword}@limodb.kbacr5r.mongodb.net/?retryWrites=true&w=majority&appName=LimoDB`;
const mongoDbName = 'shop';
const mongoCollectionName = 'products';

app.use(cors());
app.use(express.json());

let productsCollection;

// Hilfsfunktionen
function writeProductsFile(products) {
  fs.writeFileSync(PRODUCTS_FILE, JSON.stringify({ products }, null, 2));
}

function readProductsFile() {
  if (!fs.existsSync(PRODUCTS_FILE)) return { products: [] };
  return JSON.parse(fs.readFileSync(PRODUCTS_FILE));
}

async function syncFromMongoToFile() {
  const mongoProducts = await productsCollection.find().toArray();
  writeProductsFile(mongoProducts);
  console.log("📦 Produkte aus MongoDB gesichert.");
}

async function syncFromFileToMongo() {
  const localProducts = readProductsFile().products;
  for (const prod of localProducts) {
    const exists = await productsCollection.findOne({ id: prod.id });
    if (!exists) {
      await productsCollection.insertOne(prod);
      console.log(`➕ Neues Produkt übernommen: ${prod.name}`);
    }
  }
  console.log("🔄 Produkte aus Datei in MongoDB aktualisiert.");
}

// Schema-Upgrade: nur wenn undefined
async function upgradeProductsSchema() {
  const all = await productsCollection.find().toArray();
  for (const p of all) {
    if (p.stock === undefined || p.default_stock === undefined) {
      await productsCollection.updateOne(
        { id: p.id },
        { $set: { stock: 20, default_stock: 20 } }
      );
      console.log(`⬆️ Produkt ${p.id} Schema upgegraded.`);
    }
  }
  console.log("✅ Schema-Upgrade abgeschlossen.");
}

// Reset-Funktion
async function resetProductStock() {
  await productsCollection.updateMany({}, [
    { $set: { stock: "$default_stock" } }
  ]);
  console.log("♻️ Lagerbestand zurückgesetzt!");
  await syncFromMongoToFile();
}

// Initialisierung
MongoClient.connect(mongoUri, { useUnifiedTopology: true })
  .then(async client => {
    const db = client.db(mongoDbName);
    productsCollection = db.collection(mongoCollectionName);
    console.log("✅ MongoDB verbunden.");

    // Erstmal Schema-Upgrade & Sync
    await upgradeProductsSchema();
    await syncFromMongoToFile();
    await syncFromFileToMongo();

    // Starte HTTP-Server
    http.createServer(app).listen(HTTP_PORT, () => {
      console.log(`🌐 HTTP-Server läuft auf Port ${HTTP_PORT}`);
    });
  })
  .catch(err => {
    console.error("❌ MongoDB-Verbindung fehlgeschlagen:", err);
    process.exit(1);
  });

// Automatischer Reset um 00:00 Europe/Berlin
setInterval(() => {
  const now = new Date().toLocaleString('de-DE', { timeZone: TIMEZONE });
  const time = now.split(', ')[1];
  if (time === '00:00:00') {
    resetProductStock();
  }
}, 1000);

// Endpoints

// Alle Produkte lesen
app.get('/api/products', async (req, res) => {
  try {
    const products = await productsCollection.find().toArray();
    res.json({ products });
  } catch (e) {
    res.status(500).json({ error: "Fehler beim Abrufen!" });
  }
});

// Neues Produkt anlegen
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
    await syncFromMongoToFile();
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
    await syncFromMongoToFile();
    res.json({ message: "Produkt gelöscht!" });
  } catch {
    res.status(500).json({ error: "Fehler beim Löschen!" });
  }
});

// Reset via API
app.patch('/api/products/reset', async (req, res) => {
  try {
    await resetProductStock();
    res.json({ message: "Bestand zurückgesetzt." });
  } catch {
    res.status(500).json({ error: "Fehler beim Zurücksetzen." });
  }
});

// Manueller Schema-Upgrade via API
app.post('/api/products/upgrade-schema', async (req, res) => {
  try {
    await upgradeProductsSchema();
    res.json({ message: "Schema-Upgrade durchgeführt." });
  } catch {
    res.status(500).json({ error: "Fehler beim Schema-Upgrade." });
  }
});
