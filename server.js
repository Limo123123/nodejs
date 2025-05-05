const express = require('express');
const fs = require('fs');
const http = require('http');
const https = require('https');
const cors = require('cors');
const path = require('path');
const { MongoClient } = require('mongodb');

// MongoDB Setup
const mongoUser = 'git';
const mongoPassword = 'c72JfwytnPVD0YHv'; // Note that this will only have Access to special databases and Collections
const mongoUri = `mongodb+srv://${mongoUser}:${mongoPassword}@limodb.kbacr5r.mongodb.net/?retryWrites=true&w=majority&appName=LimoDB`;
const mongoDbName = 'shop';
const mongoCollectionName = 'products';

// App Setup
const app = express();
const HTTP_PORT = 80;
const HTTPS_PORT = 443;
const PRODUCTS_FILE = 'products.json';

// Middleware
app.use(cors());
app.use(express.json());

// MongoDB Client
let db, productsCollection;
MongoClient.connect(mongoUri)
    .then(client => {
        db = client.db(mongoDbName);
        productsCollection = db.collection(mongoCollectionName);
        console.log("✅ MongoDB verbunden.");
        syncFromMongoToFile(); // Initial-Backup bei Start
    })
    .catch(err => {
        console.error("❌ MongoDB Fehler:", err);
    });

/* ---------- Hilfsfunktionen ---------- */

// Schreibe Produkte in JSON-Datei
function writeProductsFile(products) {
    fs.writeFileSync(PRODUCTS_FILE, JSON.stringify({ products }, null, 2));
}

// Lese JSON-Datei
function readProductsFile() {
    if (!fs.existsSync(PRODUCTS_FILE)) return { products: [] };
    return JSON.parse(fs.readFileSync(PRODUCTS_FILE));
}

// MongoDB → JSON (Backup bei Start)
async function syncFromMongoToFile() {
    const mongoProducts = await productsCollection.find().toArray();
    writeProductsFile(mongoProducts);
    console.log("📦 Produkte aus MongoDB gesichert.");
}

// JSON → MongoDB (wenn Datei sich ändert)
async function syncFromFileToMongo() {
    const localProducts = readProductsFile().products;

    await productsCollection.deleteMany({});
    if (localProducts.length > 0) {
        await productsCollection.insertMany(localProducts);
    }
    console.log("🔄 Produkte aus Datei in MongoDB aktualisiert.");
}

// Alle Produkte
app.get('/api/products', async (req, res) => {
    try {
        const products = await productsCollection.find().toArray();
        res.json({ products });
    } catch (err) {
        res.status(500).json({ error: "Fehler beim Abrufen!" });
    }
});

// Produkt hinzufügen
app.post('/api/products', async (req, res) => {
    let { name, image_url, price } = req.body;
    if (!name || !image_url || !price) return res.status(400).json({ error: "Alle Felder erforderlich!" });

    price = price.trim();
    if (!price.startsWith('$')) price = `$${price}`;
    const numericPrice = parseFloat(price.replace(/[^0-9.]/g, ''));
    if (isNaN(numericPrice)) return res.status(400).json({ error: "Ungültiger Preis!" });

    const newId = Math.floor(100000 + Math.random() * 900000);
    const product = { id: newId, name, image_url, price };

    try {
        await productsCollection.insertOne(product);
        await syncFromMongoToFile(); // Optional: Datei aktualisieren
        res.status(201).json({ message: "Produkt hinzugefügt!", product });
    } catch (err) {
        res.status(500).json({ error: "Fehler beim Hinzufügen!" });
    }
});

// Produkt löschen
app.delete('/api/products/:id', async (req, res) => {
    const productId = parseInt(req.params.id);
    if (!/^\d{6}$/.test(req.params.id)) return res.status(400).json({ error: "Ungültige ID!" });

    try {
        const result = await productsCollection.deleteOne({ id: productId });
        if (result.deletedCount === 0) return res.status(404).json({ error: "Produkt nicht gefunden!" });

        await syncFromMongoToFile(); // Optional: Datei aktualisieren
        res.json({ message: "Produkt gelöscht!" });
    } catch (err) {
        res.status(500).json({ error: "Fehler beim Löschen!" });
    }
});

/* ---------- Server starten ---------- */
http.createServer(app).listen(HTTP_PORT,_
