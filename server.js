// server.js - Ansatz: MongoDB als einzige Quelle der Wahrheit
const express = require('express');
const fs = require('fs'); // Nur noch für initiales Seed benötigt
const http = require('http');
const cors = require('cors');
const { MongoClient } = require('mongodb');

const app = express();
const HTTP_PORT = process.env.PORT || 80;
const SEED_PRODUCTS_FILE = 'products.json'; // <- UMBENENNEN! Diese Datei enthält die Rohdaten ohne IDs etc.
const TIMEZONE = 'Europe/Berlin'; // Für täglichen Reset

// MongoDB config
const mongoUser = process.env.MONGO_USER || 'git';
const mongoPassword = process.env.MONGO_PASSWORD || 'c72JfwytnPVD0YHv';
const mongoUri = process.env.MONGO_URI || `mongodb+srv://${mongoUser}:${mongoPassword}@limodb.kbacr5r.mongodb.net/?retryWrites=true&w=majority&appName=LimoDB`;
const mongoDbName = 'shop';
const mongoCollectionName = 'products';

app.use(cors());
app.use(express.json());
// In server.js, nach app = express();
app.use(express.json({limit: "50mb"}));
app.use(express.urlencoded({limit: "50mb", extended: true, parameterLimit:50000}));

let productsCollection;

// --- HILFSFUNKTIONEN ---
async function generateUniqueId() {
    let newId;
    let idExists = true;
    let attempts = 0;
    const maxAttempts = 1000;
    while (idExists && attempts < maxAttempts) {
        newId = Math.floor(100000 + Math.random() * 900000);
        try {
            const existingProduct = await productsCollection.findOne({ id: newId }, { projection: { _id: 1 } });
            if (!existingProduct) idExists = false;
        } catch (findErr) {
            console.error(`   ❌ Fehler bei ID-Existenzprüfung ${newId}:`, findErr);
            throw new Error('Datenbankfehler bei ID-Generierung.');
        }
        attempts++;
    }
    if (idExists) throw new Error('Fehler bei der ID-Generierung, zu viele Kollisionen.');
    console.log(`   🔑 Neue eindeutige ID generiert: ${newId}`);
    return newId;
}

// Funktion zum einmaligen Befüllen der DB aus einer JSON (ohne IDs)
async function seedDatabaseFromLocalJson() {
    console.log(`🌱 Versuche DB aus ${SEED_PRODUCTS_FILE} zu befüllen...`);
    if (!fs.existsSync(SEED_PRODUCTS_FILE)) {
        console.warn(`   Datei ${SEED_PRODUCTS_FILE} nicht gefunden. Überspringe Seeding.`);
        return 0; // Gibt 0 zurück, wenn keine Datei gefunden wurde
    }
    let seededCount = 0;
    try {
        const data = fs.readFileSync(SEED_PRODUCTS_FILE, 'utf8');
        const parsedData = JSON.parse(data);
        if (!parsedData || !Array.isArray(parsedData.products)) {
            console.error(`   ${SEED_PRODUCTS_FILE} hat ungültiges Format.`);
            return 0;
        }

        const productsToSeed = [];
        console.log(`   Lese ${parsedData.products.length} Produkte aus Seed-Datei.`);

        for (const prod of parsedData.products) {
            if (!prod || typeof prod.name !== 'string' || !prod.name.trim()) {
                console.warn('   ⚠️ Ignoriere fehlerhaften Eintrag in Seed-Datei:', prod);
                continue;
            }
            try {
                const newId = await generateUniqueId();
                productsToSeed.push({
                    id: newId,
                    name: prod.name.trim(),
                    price: prod.price && typeof prod.price === 'string' ? prod.price.trim() : "$0.00",
                    image_url: prod.image_url && typeof prod.image_url === 'string' ? prod.image_url.trim() : "https://via.placeholder.com/150?text=Kein+Bild",
                    stock: 20, // Standard-Stock für Seed
                    default_stock: 20 // Standard-Default für Seed
                });
            } catch (idError) {
                console.error(`   Fehler bei ID-Generierung für Seed-Produkt ${prod.name}: ${idError.message}`);
                // Breche hier ggf. ab oder mache weiter
                // return -1; // Signalisiert Fehler bei ID-Generierung
            }
        }

        if (productsToSeed.length > 0) {
            console.log(`   Füge ${productsToSeed.length} Produkte in die Datenbank ein...`);
            // Fehler bei insertMany abfangen
            try {
                const insertResult = await productsCollection.insertMany(productsToSeed, { ordered: false }); // ordered:false ist robuster
                seededCount = insertResult.insertedCount;
                console.log(`   ✅ Datenbank erfolgreich mit ${seededCount} Produkten befüllt.`);
            } catch (insertManyError) {
                 console.error('❌ Fehler beim insertMany während des Seedings:', insertManyError);
                 // Versuche zumindest, die Anzahl der erfolgreichen Inserts zu ermitteln, falls verfügbar
                 seededCount = insertManyError.result ? insertManyError.result.nInserted : 0;
                 console.error(`   Nur ${seededCount} von ${productsToSeed.length} Produkten konnten eingefügt werden.`);
                 // Optional: Fehler weiterwerfen, um Serverstart zu verhindern?
                 // throw insertManyError;
            }
        } else {
            console.log('   Keine gültigen Produkte in Seed-Datei gefunden zum Einfügen.');
        }
        return seededCount; // Gibt die Anzahl der tatsächlich eingefügten Produkte zurück

    } catch (error) {
        console.error('❌ Fehler beim Lesen/Parsen der Seed-Datei:', error);
        return -1; // Signalisiert Fehler beim Seeding
    }
}

async function resetProductStock() {
  console.log('♻️ Startet Zurücksetzen des Lagerbestands auf default_stock...');
  try {
    const result = await productsCollection.updateMany(
      { id: { $type: 'number', $gte: 100000 } },
      [ { $set: { stock: { $ifNull: ["$default_stock", 20] } } } ]
        // Alternative mit Typ-Check:
        /*
        [ { $set: { stock: { $cond: {
            if: { $and: [ { $ne: [{$type: "$default_stock"}, "missing"] }, { $in: [{$type:"$default_stock"}, ["int", "long", "double"]] }, { $gte: ["$default_stock", 0] } ] },
            then: "$default_stock", else: 20
        }}}} ]
        */
    );
    console.log(`♻️ Lagerbestand für ${result.modifiedCount} Produkte auf default_stock zurückgesetzt (Matched: ${result.matchedCount}).`);
    // Kein Sync mit JSON mehr nötig
  } catch (error) {
    console.error('❌ Fehler beim Zurücksetzen des Lagerbestands:', error);
    throw error;
  }
}
// --- ENDE HILFSFUNKTIONEN ---


// Init MongoDB-Verbindung
MongoClient.connect(mongoUri)
  .then(async client => {
    const db = client.db(mongoDbName);
    productsCollection = db.collection(mongoCollectionName);
    console.log('✅ MongoDB verbunden.');

    try {
        await productsCollection.createIndex({ id: 1 }, { unique: true });
        console.log('✅ MongoDB Index auf "id" erstellt/existiert.');
    } catch (indexErr) {
        console.error('❌ Fehler beim Erstellen des MongoDB Index:', indexErr);
    }

    // Prüfe, ob die Collection leer ist, um initiales Seeding durchzuführen
    try {
        const count = await productsCollection.countDocuments();
        if (count === 0) {
            console.log('   Datenbank ist leer. Starte initiales Seeding...');
            const seededCount = await seedDatabaseFromLocalJson();
            if (seededCount < 0) {
                 console.error("   Seeding fehlgeschlagen. Server startet trotzdem, aber DB könnte unvollständig sein.");
            } else {
                 console.log(`   Seeding abgeschlossen. ${seededCount} Produkte hinzugefügt.`);
            }
        } else {
            console.log(`   Datenbank enthält bereits ${count} Produkte. Überspringe Seeding.`);
        }
    } catch(err) {
        console.error("   Fehler beim Prüfen/Seeden der DB:", err);
    }

    // Starte HTTP-Server nach DB-Init
    http.createServer(app).listen(HTTP_PORT, () => {
      console.log(`🌐 HTTP-Server läuft auf Port ${HTTP_PORT}`);
    });
  })
  .catch(err => {
    console.error('❌ MongoDB-Verbindung fehlgeschlagen:', err);
    process.exit(1);
  });

// Täglicher Reset
console.log(`⏳ Tägliches Zurücksetzen des Lagerbestands geplant für 00:00 Uhr ${TIMEZONE}.`);
setInterval(() => {
   try {
        const date = new Date(new Date().toLocaleString('en-US', { timeZone: TIMEZONE }));
        // Prüfe auf Mitternacht (Stunde 0, Minute 0) mit kleiner Toleranz
        if (date.getHours() === 0 && date.getMinutes() === 0 && date.getSeconds() <= 15) { 
          console.log('⏰ Mitternacht erreicht. Setze Lagerbestand zurück...');
          resetProductStock(); // Funktion direkt aufrufen
        }
    } catch (timeErr) {
        console.error("Fehler bei der Zeitprüfung für den täglichen Reset:", timeErr);
    }
}, 10000); // Prüfe alle 10 Sekunden


// --- API Endpoints (arbeiten jetzt nur mit DB) ---

// Reihenfolge wichtig: Spezifischere Routen vor allgemeinen mit Parametern
app.patch('/api/products/reset', async (req, res) => {
  console.log('API-Endpoint /api/products/reset aufgerufen.');
  // !!! HIER ADMIN-AUTORISIERUNG EINFÜGEN !!!
  try {
    await resetProductStock();
    res.json({ message: 'Lagerbestand auf Standardwerte zurückgesetzt.' });
  } catch (err) {
    console.error('Fehler beim Zurücksetzen des Lagerbestands via API:', err);
    res.status(500).json({ error: 'Fehler beim Zurücksetzen des Lagerbestands auf dem Server.' });
  }
});

// GET alle Produkte (nur aus DB)
app.get('/api/products', async (req, res) => {
  try {
    // Hole nur Produkte mit gültiger ID aus der DB für die Anzeige
    const products = await productsCollection.find({ id: { $type: 'number', $gte: 100000 } }).sort({ id: 1 }).toArray();
    // Sanitize für Frontend
    const sanitizedProducts = products.map(p => {
        const sanitized = { ...p };
        sanitized.stock = (typeof p.stock === 'number' && p.stock >= 0) ? p.stock : 0;
        sanitized.default_stock = (typeof p.default_stock === 'number' && p.default_stock >= 0) ? p.default_stock : 20;
        delete sanitized._id;
        return sanitized;
    });
    res.json({ products: sanitizedProducts });
  } catch (err) {
    console.error('Fehler beim Abrufen der Produkte:', err);
    res.status(500).json({ error: 'Fehler beim Abrufen der Produkte!' });
  }
});

// POST neues Produkt (nur in DB)
app.post('/api/products', async (req, res) => {
  console.log('POST /api/products erhalten:', req.body);
  let { name, image_url, price, stock } = req.body;

  // --- Input Validierung ---
  if (!name || typeof name !== 'string' || name.trim() === '') return res.status(400).json({ error: 'Produktname ist erforderlich!' });
  if (!image_url || typeof image_url !== 'string' || image_url.trim() === '') return res.status(400).json({ error: 'Bild-URL ist erforderlich!' });
  if (!price || typeof price !== 'string' || price.trim() === '') return res.status(400).json({ error: 'Preis ist erforderlich!' });

  price = price.trim();
  if (!price.startsWith('$')) price = `$${price}`;
  const numericPrice = parseFloat(price.replace(/[^0-9.]/g, ''));
  if (isNaN(numericPrice) || numericPrice < 0) return res.status(400).json({ error: 'Ungültiges Preisformat!' });
  const formattedPrice = `$${numericPrice.toFixed(2)}`;

  let initialStock = 20;
  if (stock !== undefined && stock !== null && stock !== '') {
      const parsedStock = parseInt(stock, 10);
      if (!isNaN(parsedStock) && Number.isInteger(parsedStock) && parsedStock >= 0) initialStock = parsedStock;
  }
  // --- Ende Input Validierung ---

  try {
    const newId = await generateUniqueId();
    const prod = { id: newId, name: name.trim(), image_url: image_url.trim(), price: formattedPrice, stock: initialStock, default_stock: initialStock };
    const insertResult = await productsCollection.insertOne(prod);
    console.log('POST /api/products: Produkt eingefügt mit DB _id:', insertResult.insertedId);
    delete prod._id;
    res.status(201).json({ message: 'Produkt erfolgreich hinzugefügt!', product: prod });
  } catch (err) {
    console.error('POST /api/products: Fehler beim Hinzufügen:', err);
    res.status(500).json({ error: err.message || 'Fehler beim Hinzufügen des Produkts!' });
  }
});

// DELETE Produkt (nur aus DB)
app.delete('/api/products/:id', async (req, res) => {
  console.log('DELETE /api/products/:id für ID:', req.params.id);
  const id = parseInt(req.params.id, 10);
  if (!/^\d{6}$/.test(req.params.id) || isNaN(id)) return res.status(400).json({ error: 'Ungültiges ID-Format!' });

  try {
    const result = await productsCollection.deleteOne({ id: id });
    if (result.deletedCount === 0) return res.status(404).json({ error: 'Produkt nicht gefunden!' });
    console.log(`DELETE /api/products/:id: Produkt mit ID ${id} gelöscht.`);
    res.json({ message: `Produkt mit ID ${id} erfolgreich gelöscht!` });
  } catch (err) {
    console.error(`DELETE /api/products/:id: Fehler (ID ${id}):`, err);
    res.status(500).json({ error: 'Fehler beim Löschen des Produkts!' });
  }
});

// PATCH Stock für einzelnes Produkt (nur in DB) - Optional, wenn benötigt
app.patch('/api/products/:id', async (req, res) => {
  console.log('PATCH /api/products/:id für ID:', req.params.id, 'Body:', req.body);
  const id = parseInt(req.params.id, 10);
  if (!/^\d{6}$/.test(req.params.id) || isNaN(id)) return res.status(400).json({ error: 'Ungültiges ID-Format!' });

  const { stock } = req.body;
  if (stock === undefined || stock === null) return res.status(400).json({ error: 'Lagerbestandswert fehlt!' });
  const parsedStock = parseInt(stock, 10);
  if (isNaN(parsedStock) || !Number.isInteger(parsedStock) || parsedStock < 0) return res.status(400).json({ error: 'Ungültiger Lagerbestandswert!' });

  try {
    const result = await productsCollection.updateOne({ id: id }, { $set: { stock: parsedStock } });
    if (result.matchedCount === 0) return res.status(404).json({ error: `Produkt mit ID ${id} nicht gefunden!` });

    console.log(`PATCH /api/products/:id: Lagerbestand für Produkt ${id} auf ${parsedStock} aktualisiert.`);
    const updatedProduct = await productsCollection.findOne({ id: id });
    delete updatedProduct._id;
    res.json({ message: `Lagerbestand aktualisiert.`, product: updatedProduct });
  } catch (err) {
    console.error(`PATCH /api/products/:id: Fehler (ID ${id}):`, err);
    res.status(500).json({ error: 'Fehler beim Aktualisieren des Lagerbestands!' });
  }
});

// POST Kaufabschluss (prüft und updated nur DB)
app.post('/api/purchase', async (req, res) => {
    console.log('POST /api/purchase erhalten. Warenkorb:', req.body.cart);
    const cart = req.body.cart;
    if (!Array.isArray(cart) || cart.length === 0) return res.status(400).json({ error: 'Warenkorb leer/ungültig.' });

    const errors = [];
    const productChecks = [];

    for (const item of cart) {
        if (!item || typeof item.id !== 'number' || item.id < 100000 || typeof item.quantity !== 'number' || item.quantity <= 0) {
            errors.push(`Ungültiges Produkt im Warenkorb (ID: ${item.id || 'unbekannt'}).`); continue;
        }
        productChecks.push(
            productsCollection.findOne({ id: item.id }).then(product => {
                if (!product) { errors.push(`Produkt "${item.name || item.id}" nicht gefunden.`); return null; }
                const currentStock = (typeof product.stock === 'number' && product.stock >= 0) ? product.stock : 0;
                if (item.quantity > currentStock) { errors.push(`Nicht genügend Bestand für "${product.name || product.id}". Verfügbar: ${currentStock}, benötigt: ${item.quantity}.`); return null; }
                return { id: item.id, quantityToDecrement: item.quantity };
            }).catch(dbError => { // Fehler bei findOne abfangen
                console.error(`Fehler bei DB-Abfrage für Produkt ${item.id}:`, dbError);
                errors.push(`Datenbankfehler bei Prüfung von Produkt ${item.id}.`);
                return null;
            })
        );
    }
    if (errors.length > 0) { // Fehler schon bei der initialen Item-Validierung
         console.error('POST /api/purchase: Validierungsfehler im Warenkorb (Item-Struktur).');
         return res.status(400).json({ error: errors.join('; ') });
     }

    try {
        const results = await Promise.all(productChecks);
        // Sammle ALLE Fehler, die während der Promises aufgetreten sind
        const validationErrors = errors.concat(results.filter(r => r === null).map(() => "Fehler bei Produktprüfung")); // Generische Fehlermeldung oder die aus errors oben verwenden
        if (validationErrors.length > 0) {
            console.error('POST /api/purchase: Bestandsprüfung fehlgeschlagen.');
            return res.status(400).json({ error: errors.join('; ') || "Unbekannter Bestands-/Produktfehler."}); // Gib die spezifischen Fehler aus errors zurück
        }

        const validUpdates = results.filter(r => r !== null);
        console.log('POST /api/purchase: Bestandsprüfung bestanden. Reduziere Bestand für', validUpdates.length, 'Produkttypen.');
        if (validUpdates.length > 0) {
            const bulkOperations = validUpdates.map(update => ({
                updateOne: {
                    filter: { id: update.id, stock: { $gte: update.quantityToDecrement } },
                    update: { $inc: { stock: -update.quantityToDecrement } }
                }
            }));
            const bulkWriteResult = await productsCollection.bulkWrite(bulkOperations);
            if (bulkWriteResult.modifiedCount !== validUpdates.length) {
                console.error('POST /api/purchase: Fehler beim Bulk Write. Race Condition?');
                // Hier wäre ein Rollback notwendig!
                return res.status(500).json({ error: 'Konflikt beim Aktualisieren des Lagerbestands. Bitte erneut versuchen.' });
            }
            console.log(`POST /api/purchase: Bestand für ${bulkWriteResult.modifiedCount} Produkte erfolgreich reduziert.`);
        }
        // Kein Sync mit JSON mehr nötig
        res.json({ message: 'Kauf erfolgreich abgeschlossen!' });
    } catch (err) {
        console.error('POST /api/purchase: Unerwarteter Fehler:', err);
        res.status(500).json({ error: 'Ein unerwarteter Fehler ist beim Kauf aufgetreten.' });
    }
});

// Fallback für unbekannte Routen
app.use((req, res) => {
    res.status(404).send('Endpoint nicht gefunden');
});