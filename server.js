// server.js
const express = require('express');
const fs = require('fs');
const http = require('http');
const cors = require('cors');
const path = require('path');
const { MongoClient } = require('mongodb');

const app = express();
const HTTP_PORT = 80;
const PRODUCTS_FILE = 'products.json';
const TIMEZONE = 'Europe/Berlin';

// MongoDB Setup
const mongoUser = 'git';
const mongoPassword = 'c72JfwytnPVD0YHv';
const mongoUri = `mongodb+srv://${mongoUser}:${mongoPassword}@limodb.kbacr5r.mongodb.net/?retryWrites=true&w=majority&appName=LimoDB`;
const mongoDbName = 'shop';
const mongoCollectionName = 'products';

app.use(cors());
app.use(express.json());

let db, productsCollection;
MongoClient.connect(mongoUri)
    .then(client => {
        db = client.db(mongoDbName);
        productsCollection = db.collection(mongoCollectionName);
        console.log("✅ MongoDB verbunden.");
        upgradeProductsSchema().then(syncFromMongoToFile);
    })
    .catch(err => console.error("❌ MongoDB Fehler:", err));

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
    for (const product of localProducts) {
        const exists = await productsCollection.findOne({ id: product.id });
        if (!exists) {
            await productsCollection.insertOne(product);
            console.log(`➕ Neues Produkt übernommen: ${product.name}`);
        }
    }
    console.log("🔄 Produkte aus Datei in MongoDB aktualisiert.");
}

async function upgradeProductsSchema() {
    const all = await productsCollection.find().toArray();
    for (const p of all) {
        if (!p.stock || !p.default_stock) {
            await productsCollection.updateOne(
                { id: p.id },
                { $set: { stock: 20, default_stock: 20 } }
            );
        }
    }
    console.log("⬆️ Produkte aktualisiert (Schema)");
}

async function resetProductStock() {
    await productsCollection.updateMany({}, [
        { $set: { stock: "$default_stock" } }
    ]);
    console.log("♻️ Lagerbestand zurückgesetzt!");
    syncFromMongoToFile();
}

setInterval(() => {
    const now = new Date().toLocaleString('de-DE', { timeZone: TIMEZONE });
    const time = now.split(', ')[1];
    if (time === '00:00:00') {
        resetProductStock();
    }
}, 1000);

app.get('/api/products', async (req, res) => {
    try {
        const products = await productsCollection.find().toArray();
        res.json({ products });
    } catch {
        res.status(500).json({ error: "Fehler beim Abrufen!" });
    }
});

app.post('/api/products', async (req, res) => {
    let { name, image_url, price, stock } = req.body;
    if (!name || !image_url || !price) return res.status(400).json({ error: "Alle Felder erforderlich!" });

    price = price.trim();
    if (!price.startsWith('$')) price = `$${price}`;
    const numericPrice = parseFloat(price.replace(/[^0-9.]/g, ''));
    if (isNaN(numericPrice)) return res.status(400).json({ error: "Ungültiger Preis!" });

    const newId = Math.floor(100000 + Math.random() * 900000);
    const product = {
        id: newId,
        name,
        image_url,
        price,
        stock: stock ?? 20,
        default_stock: stock ?? 20
    };

    try {
        await productsCollection.insertOne(product);
        await syncFromMongoToFile();
        res.status(201).json({ message: "Produkt hinzugefügt!", product });
    } catch {
        res.status(500).json({ error: "Fehler beim Hinzufügen!" });
    }
});

app.patch('/api/products/reset', async (req, res) => {
    try {
        await resetProductStock();
        res.json({ message: "Bestand zurückgesetzt." });
    } catch {
        res.status(500).json({ error: "Fehler beim Zurücksetzen." });
    }
});

http.createServer(app).listen(HTTP_PORT, () => {
    console.log(`HTTP Server running on port ${HTTP_PORT}`);
});
