// server.js
const express = require('express');
const fs = require('fs');
const http = require('http');
const cors = require('cors');
const { MongoClient } = require('mongodb');

const app = express();
const HTTP_PORT = process.env.PORT || 80;
const PRODUCTS_FILE = 'products.json';
const TIMEZONE = 'Europe/Berlin';

// MongoDB config
const mongoUser = process.env.MONGO_USER || 'git';
const mongoPassword = process.env.MONGO_PASSWORD || 'c72JfwytnPVD0YHv';
const mongoUri = process.env.MONGO_URI || `mongodb+srv://${mongoUser}:${mongoPassword}@limodb.kbacr5r.mongodb.net/?retryWrites=true&w=majority&appName=LimoDB`;
const mongoDbName = 'shop';
const mongoCollectionName = 'products';

app.use(cors());
app.use(express.json());

let productsCollection;

// --- NEUE/ANGEPASSTE FUNKTIONEN ---
function writeProductsFile(products) {
  try {
    // Stelle sicher, dass alle Produkte, die geschrieben werden, die benötigten Felder haben
    const productsToWrite = products.map(p => ({
        id: p.id, // Muss existieren und gültig sein
        name: p.name,
        price: p.price,
        image_url: p.image_url,
        stock: (typeof p.stock === 'number' && Number.isInteger(p.stock) && p.stock >= 0) ? p.stock : 20,
        default_stock: (typeof p.default_stock === 'number' && Number.isInteger(p.default_stock) && p.default_stock >= 0)
                         ? p.default_stock
                         : ((typeof p.stock === 'number' && Number.isInteger(p.stock) && p.stock >= 0) ? p.stock : 20)
    }));
    fs.writeFileSync(PRODUCTS_FILE, JSON.stringify({ products: productsToWrite }, null, 2));
    console.log(`📦 products.json erfolgreich geschrieben mit ${productsToWrite.length} Einträgen.`);
  } catch (error) {
    console.error("❌ Fehler beim Schreiben von products.json:", error);
  }
}

function readProductsFile() {
  if (!fs.existsSync(PRODUCTS_FILE)) {
    console.warn("📁 products.json nicht gefunden. Erstelle leeres Array.");
    return { products: [] };
  }
  try {
    const data = fs.readFileSync(PRODUCTS_FILE, 'utf8');
    const parsedData = JSON.parse(data);
    if (parsedData && Array.isArray(parsedData.products)) {
      console.log(`📁 products.json erfolgreich gelesen mit ${parsedData.products.length} Einträgen (vor weiterer Validierung).`);
      return { products: parsedData.products };
    } else {
      console.error("❌ products.json hat unerwartetes Format. Erstelle leeres Array.");
      return { products: [] };
    }
  } catch (error) {
    console.error("❌ Fehler beim Lesen oder Parsen von products.json:", error);
    return { products: [] };
  }
}

async function generateUniqueId() {
    let newId;
    let idExists = true;
    let attempts = 0;
    const maxAttempts = 1000; // Erhöht für größere Datenmengen

    while (idExists && attempts < maxAttempts) {
        newId = Math.floor(100000 + Math.random() * 900000);
        try {
            // Prüfe nur, ob ein Dokument mit dieser ID existiert, ohne das ganze Dokument zu laden
            const existingProduct = await productsCollection.findOne({ id: newId }, { projection: { _id: 1 } });
            if (!existingProduct) {
                idExists = false;
            }
        } catch (findErr) {
            console.error(`   ❌ Fehler bei der Prüfung auf ID-Existenz ${newId} in generateUniqueId:`, findErr);
            throw new Error('Datenbankfehler bei ID-Generierung.');
        }
        attempts++;
    }
    if (idExists) {
        console.error(`   ❌ Konnte nach ${maxAttempts} Versuchen keine eindeutige ID generieren.`);
        throw new Error('Fehler bei der ID-Generierung, zu viele Kollisionen.');
    }
    console.log(`   🔑 Neue eindeutige ID generiert: ${newId}`);
    return newId;
}

async function insertProductWithUniqueIdRetry(productData, maxRetries = 5) {
    let retries = 0;
    let success = false;
    let currentProductData = { ...productData }; // Kopie für Modifikationen

    // Wenn keine gültige ID übergeben wurde, generiere eine
    if (!currentProductData.id || typeof currentProductData.id !== 'number' || currentProductData.id < 100000) {
        console.warn(`   ⚠️ Produkt "${currentProductData.name}" hat keine gültige ID, generiere eine...`);
        // generateUniqueId prüft schon auf Existenz, aber eine Race Condition ist möglich
        currentProductData.id = await generateUniqueId();
    }

    while (retries < maxRetries && !success) {
        try {
            delete currentProductData._id; // MongoDB generiert _id
            await productsCollection.insertOne(currentProductData);
            console.log(`   ✅ Produkt "${currentProductData.name}" (ID: ${currentProductData.id}) erfolgreich eingefügt.`);
            success = true;
            return currentProductData; // Gib das erfolgreich eingefügte Produkt zurück
        } catch (error) {
            if (error.code === 11000) { // Duplicate key error
                retries++;
                console.warn(`   ⚠️ ID-Kollision für "${currentProductData.name}" mit ID ${currentProductData.id}. Versuch ${retries}/${maxRetries}. Generiere neue ID...`);
                if (retries < maxRetries) {
                    // Generiere eine komplett neue ID für den nächsten Versuch
                    currentProductData.id = await generateUniqueId();
                } else {
                    console.error(`   ❌ Produkt "${currentProductData.name}" konnte nach ${maxRetries} Versuchen nicht eingefügt werden (persistente ID-Kollision).`);
                    throw error; // Fehler nach max. Versuchen weiterwerfen
                }
            } else {
                console.error(`   ❌ Unerwarteter Fehler beim Einfügen von Produkt "${currentProductData.name}" (ID: ${currentProductData.id}):`, error);
                throw error; // Anderen Fehler direkt weiterwerfen
            }
        }
    }
     // Dieser Teil sollte nur erreicht werden, wenn maxRetries erreicht wurde und der letzte Versuch auch fehlschlug
    if (!success) {
       console.error(`   ❌ Konnte Produkt "${productData.name}" nach ${maxRetries} Retries nicht einfügen.`);
       // Werfe den letzten Fehler erneut oder einen generischen Fehler
       throw new Error(`Einfügen von Produkt "${productData.name}" nach ${maxRetries} Versuchen fehlgeschlagen.`);
    }
    // Wird eigentlich nicht erreicht, da return oder throw im Loop passiert
    return null;
}


async function syncLocalAndRemote() {
  console.log('🔄 Startet Anreicherung und Synchronisation lokaler und Remote-Produkte...');
  try {
    let localProductsInput = readProductsFile().products;
    let remoteProducts = await productsCollection.find().toArray();
    const remoteMap = new Map(remoteProducts.map(p => p.id ? [p.id, p] : [null, p]));

    console.log(`   Lokal initial: ${localProductsInput.length} Produkte, Remote initial: ${remoteProducts.length} Produkte.`);

    // Produkte einzeln verarbeiten, um ID-Kollisionen besser zu handhaben
    let productsProcessedCount = 0;
    let productsInsertedCount = 0;
    let productsSkippedCount = 0;

    for (const localProd of localProductsInput) {
        productsProcessedCount++;
        if (!localProd || typeof localProd.name !== 'string' || !localProd.name.trim()) {
            console.warn(`   ⚠️ Ignoriere fehlerhaftes lokales Produkt (Index ${productsProcessedCount}): Name fehlt/ungültig.`);
            productsSkippedCount++;
            continue;
        }

        const localId = localProd.id;
        // Prüfe, ob das Produkt (basierend auf seiner ursprünglichen lokalen ID, falls gültig) bereits remote existiert
        if (localId && typeof localId === 'number' && localId >= 100000 && remoteMap.has(localId)) {
            // console.log(`   ℹ️ Lokales Produkt "${localProd.name}" (ID: ${localId}) existiert bereits remote. Überspringe Insert.`);
            continue; // Produkt existiert schon, nichts zu tun für Insert
        }

        // Wenn Produkt nicht remote existiert oder lokale ID ungültig/fehlend ist, versuche es einzufügen
        const productDataForDb = {
            id: (typeof localId === 'number' && localId >= 100000) ? localId : undefined,
            name: localProd.name.trim(),
            price: localProd.price && typeof localProd.price === 'string' ? localProd.price.trim() : "$0.00",
            image_url: localProd.image_url && typeof localProd.image_url === 'string' ? localProd.image_url.trim() : "https://via.placeholder.com/150?text=Kein+Bild",
            stock: (typeof localProd.stock === 'number' && Number.isInteger(localProd.stock) && localProd.stock >= 0) ? localProd.stock : 20,
            default_stock: (typeof localProd.default_stock === 'number' && Number.isInteger(localProd.default_stock) && localProd.default_stock >= 0)
                             ? localProd.default_stock
                             : ((typeof localProd.stock === 'number' && Number.isInteger(localProd.stock) && localProd.stock >= 0) ? localProd.stock : 20),
        };

        try {
            await insertProductWithUniqueIdRetry(productDataForDb);
            productsInsertedCount++;
        } catch (insertError) {
            // Fehler wurde schon in insertProductWithUniqueIdRetry geloggt
            console.error(`   ❌ Fehler beim finalen Versuch, Produkt (ursprünglich Index ${productsProcessedCount}, Name: "${productDataForDb.name}") einzufügen.`);
            productsSkippedCount++;
            // Fahre mit dem nächsten Produkt fort
        }
    }

    console.log(`   Lokale Produkte verarbeitet: ${productsProcessedCount} gesamt, ${productsInsertedCount} eingefügt/versucht, ${productsSkippedCount} übersprungen/fehlerhaft.`);

    // Hole alle Produkte von MongoDB als "Source of Truth"
    const finalRemoteProducts = await productsCollection.find().toArray();

    const productsForJsonFile = finalRemoteProducts.map(p => {
        if (!p || typeof p.id !== 'number' || !Number.isInteger(p.id) || p.id < 100000) {
            console.warn("   ⚠️ Ignoriere fehlerhaftes Produkt aus DB für JSON (ungültige ID oder fehlt):", p ? p.id : "Produkt ist null/undefined");
            return null;
        }
        return {
            id: p.id,
            name: p.name || "Unbenanntes Produkt",
            price: p.price || "$0.00",
            image_url: p.image_url || "https://via.placeholder.com/150?text=Kein+Bild",
            stock: (typeof p.stock === 'number' && Number.isInteger(p.stock) && p.stock >= 0) ? p.stock : 20,
            default_stock: (typeof p.default_stock === 'number' && Number.isInteger(p.default_stock) && p.default_stock >= 0)
                             ? p.default_stock
                             : ((typeof p.stock === 'number' && Number.isInteger(p.stock) && p.stock >= 0) ? p.stock : 20),
        };
    }).filter(p => p !== null);

    productsForJsonFile.sort((a, b) => a.id - b.id);
    writeProductsFile(productsForJsonFile);
    console.log(`🔄 Lokale products.json auf ${productsForJsonFile.length} Einträge aktualisiert.`);
    console.log(`✅ Synchronisation abgeschlossen.`);

  } catch (error) {
    console.error('❌ Schwerwiegender Fehler während der Synchronisation:', error);
  }
}

async function resetProductStock() {
  console.log('♻️ Startet Zurücksetzen des Lagerbestands auf default_stock...');
  try {
    const result = await productsCollection.updateMany(
      { id: { $type: 'number', $gte: 100000 } },
      [
        {
          $set: {
            stock: {
              $cond: {
                if: {
                  $and: [
                    { $ne: [{ $type: "$default_stock" }, "missing"] },
                    { $in: [{$type: "$default_stock"}, ["int", "long", "double"]] }, // Akzeptiert verschiedene numerische Typen
                    { $gte: ["$default_stock", 0] }
                  ]
                },
                then: "$default_stock",
                else: 20
              }
            }
          }
        }
      ]
    );
    console.log(`♻️ Lagerbestand für ${result.modifiedCount} Produkte auf default_stock zurückgesetzt (Matched: ${result.matchedCount}).`);
    await syncLocalAndRemote(); // Sync nach Reset
  } catch (error) {
    console.error('❌ Fehler beim Zurücksetzen des Lagerbestands:', error);
    throw error; // Fehler weiterwerfen für API Endpoint
  }
}
// --- ENDE NEUE/ANGEPASSTE FUNKTIONEN ---


// Init MongoDB-Verbindung
MongoClient.connect(mongoUri)
  .then(async client => {
    const db = client.db(mongoDbName);
    productsCollection = db.collection(mongoCollectionName);
    console.log('✅ MongoDB verbunden.');

    try {
        await productsCollection.createIndex({ id: 1 }, { unique: true });
        console.log('✅ MongoDB Index auf "id" erfolgreich erstellt oder existiert bereits.');
    } catch (indexErr) {
        console.error('❌ MongoDB Index auf "id" konnte nicht erstellt werden.', indexErr);
    }

    await syncLocalAndRemote(); // Initialer Sync beim Start

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
        if (date.getHours() === 0 && date.getMinutes() === 0 && date.getSeconds() <= 10) { // Toleranz von 10s
          console.log('⏰ Mitternacht erreicht. Setze Lagerbestand zurück...');
          resetProductStock();
        }
    } catch (timeErr) {
        console.error("Fehler bei der Zeitprüfung für den täglichen Reset:", timeErr);
    }
}, 10000); // Prüfe alle 10 Sekunden


// --- API Endpoints ---

// Reihenfolge wichtig: Spezifische Routen vor allgemeinen mit Parametern
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

app.post('/api/products/sync', async (req, res) => {
   console.log('API-Endpoint /api/products/sync aufgerufen.');
   // !!! HIER ADMIN-AUTORISIERUNG EINFÜGEN !!!
  try {
    await syncLocalAndRemote();
    res.json({ message: 'Bidirektionaler Sync manuell durchgeführt.' });
  } catch (err) {
    console.error('Fehler beim manuellen Sync via API:', err);
    res.status(500).json({ error: 'Fehler beim Sync.' });
  }
});

// Allgemeine Produkt-Routen
app.get('/api/products', async (req, res) => {
  try {
    // Hole nur Produkte mit gültiger ID aus der DB für die Anzeige
    const products = await productsCollection.find({ id: { $type: 'number', $gte: 100000 } }).toArray();
    const sanitizedProducts = products.map(p => {
        const sanitizedProduct = { ...p };
        sanitizedProduct.stock = (typeof sanitizedProduct.stock === 'number' && Number.isInteger(sanitizedProduct.stock) && sanitizedProduct.stock >= 0) ? sanitizedProduct.stock : 0;
        sanitizedProduct.default_stock = (typeof sanitizedProduct.default_stock === 'number' && Number.isInteger(sanitizedProduct.default_stock) && sanitizedProduct.default_stock >= 0) ? sanitizedProduct.default_stock : 20;
        delete sanitizedProduct._id;
        return sanitizedProduct;
    });

    sanitizedProducts.sort((a, b) => a.id - b.id);
    res.json({ products: sanitizedProducts });
  } catch (err) {
    console.error('Fehler beim Abrufen der Produkte:', err);
    res.status(500).json({ error: 'Fehler beim Abrufen der Produkte!' });
  }
});

app.post('/api/products', async (req, res) => {
  console.log('POST /api/products erhalten:', req.body);
  let { name, image_url, price, stock } = req.body;

  // ... (Validierungen für name, image_url, price) ...
  if (!name || typeof name !== 'string' || name.trim() === '') return res.status(400).json({ error: 'Produktname ist erforderlich!' });
  if (!image_url || typeof image_url !== 'string' || image_url.trim() === '') return res.status(400).json({ error: 'Bild-URL ist erforderlich!' });
  if (!price || typeof price !== 'string' || price.trim() === '') return res.status(400).json({ error: 'Preis ist erforderlich!' });

  price = price.trim();
  if (!price.startsWith('$')) price = `$${price}`;
  const numericPrice = parseFloat(price.replace(/[^0-9.]/g, ''));
  if (isNaN(numericPrice) || numericPrice < 0) {
      return res.status(400).json({ error: 'Ungültiges Preisformat oder Wert!' });
  }
  const formattedPrice = `$${numericPrice.toFixed(2)}`;

  let initialStock = 20;
  if (stock !== undefined && stock !== null && stock !== '') {
      const parsedStock = parseInt(stock, 10);
      if (!isNaN(parsedStock) && Number.isInteger(parsedStock) && parsedStock >= 0) {
          initialStock = parsedStock;
      }
  }

  try {
    const newId = await generateUniqueId(); // ID generieren
    const prod = {
        id: newId,
        name: name.trim(),
        image_url: image_url.trim(),
        price: formattedPrice,
        stock: initialStock,
        default_stock: initialStock
    };
    // Verwende die Retry-Funktion auch hier für Konsistenz
    const insertedProduct = await insertProductWithUniqueIdRetry(prod, 3); // Weniger Retries hier ok
    console.log('POST /api/products: Produkt erfolgreich in DB eingefügt mit ID:', insertedProduct.id);
    syncLocalAndRemote().catch(err => console.error("Fehler beim Sync nach Produkt-POST:", err));
    res.status(201).json({ message: 'Produkt erfolgreich hinzugefügt!', product: insertedProduct });
  } catch (err) {
    console.error('POST /api/products: Fehler beim Hinzufügen des Produkts:', err);
    res.status(500).json({ error: err.message || 'Fehler beim Hinzufügen des Produkts!' });
  }
});


app.delete('/api/products/:id', async (req, res) => {
  console.log('DELETE /api/products/:id für ID:', req.params.id);
  const id = parseInt(req.params.id, 10);
  if (!/^\d{6}$/.test(req.params.id) || isNaN(id)) {
      return res.status(400).json({ error: 'Ungültiges ID-Format!' });
  }
  try {
    const result = await productsCollection.deleteOne({ id: id });
    if (result.deletedCount === 0) {
        return res.status(404).json({ error: 'Produkt nicht gefunden!' });
    }
    console.log(`DELETE /api/products/:id: Produkt mit ID ${id} gelöscht.`);
    syncLocalAndRemote().catch(err => console.error("Fehler beim Sync nach Produkt-DELETE:", err));
    res.json({ message: `Produkt mit ID ${id} erfolgreich gelöscht!` });
  } catch (err) {
    console.error(`DELETE /api/products/:id: Fehler beim Löschen (ID ${id}):`, err);
    res.status(500).json({ error: 'Fehler beim Löschen des Produkts!' });
  }
});

// Muss nach /api/products/reset stehen!
app.patch('/api/products/:id', async (req, res) => {
  console.log('PATCH /api/products/:id für ID:', req.params.id, 'Body:', req.body);
  const id = parseInt(req.params.id, 10);
  if (!/^\d{6}$/.test(req.params.id) || isNaN(id)) {
      return res.status(400).json({ error: 'Ungültiges ID-Format!' });
  }
  const { stock } = req.body;
  if (stock === undefined || stock === null) {
       return res.status(400).json({ error: 'Lagerbestandswert fehlt!' });
  }
  const parsedStock = parseInt(stock, 10);
  if (isNaN(parsedStock) || !Number.isInteger(parsedStock) || parsedStock < 0) {
      return res.status(400).json({ error: 'Ungültiger Lagerbestandswert!' });
  }

  try {
    const result = await productsCollection.updateOne({ id: id }, { $set: { stock: parsedStock } });
    if (result.matchedCount === 0) {
      return res.status(404).json({ error: `Produkt mit ID ${id} nicht gefunden!` });
    }
    console.log(`PATCH /api/products/:id: Lagerbestand für Produkt ${id} auf ${parsedStock} aktualisiert.`);
    syncLocalAndRemote().catch(err => console.error("Fehler beim Sync nach Produkt-PATCH (Stock):", err));
    const updatedProduct = await productsCollection.findOne({ id: id });
    delete updatedProduct._id;
    res.json({ message: `Lagerbestand aktualisiert.`, product: updatedProduct });
  } catch (err) {
    console.error(`PATCH /api/products/:id: Fehler (ID ${id}):`, err);
    res.status(500).json({ error: 'Fehler beim Aktualisieren des Lagerbestands!' });
  }
});

// Purchase Endpoint
app.post('/api/purchase', async (req, res) => {
    console.log('POST /api/purchase erhalten. Warenkorb:', req.body.cart);
    const cart = req.body.cart;
    if (!Array.isArray(cart) || cart.length === 0) {
        return res.status(400).json({ error: 'Warenkorb ist leer oder ungültig.' });
    }

    const errors = [];
    const productChecks = [];

    for (const item of cart) {
        if (!item || typeof item.id !== 'number' || item.id < 100000 || typeof item.quantity !== 'number' || item.quantity <= 0) {
            errors.push(`Ungültiges Produkt im Warenkorb (ID: ${item.id || 'unbekannt'}).`);
            continue;
        }
        productChecks.push(
            productsCollection.findOne({ id: item.id }).then(product => {
                if (!product) {
                    errors.push(`Produkt "${item.name || item.id}" nicht gefunden.`); return null;
                }
                const currentStock = (typeof product.stock === 'number' && product.stock >= 0) ? product.stock : 0;
                if (item.quantity > currentStock) {
                    errors.push(`Nicht genügend Bestand für "${product.name || product.id}". Verfügbar: ${currentStock}, benötigt: ${item.quantity}.`); return null;
                }
                return { id: item.id, quantityToDecrement: item.quantity };
            })
        );
    }
     if (errors.length > 0) { // Fehler schon bei der initialen Item-Validierung
         console.error('POST /api/purchase: Validierungsfehler im Warenkorb (Item-Struktur).');
         return res.status(400).json({ error: errors.join('; ') });
     }

    try {
        const results = await Promise.all(productChecks);
        const validationErrors = errors.filter(e => e); // Sammle Fehler aus den Promises (und ggf. initiale)
        if (validationErrors.length > 0) {
            console.error('POST /api/purchase: Bestandsprüfung fehlgeschlagen.');
            return res.status(400).json({ error: validationErrors.join('; ') });
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
                return res.status(500).json({ error: 'Konflikt beim Aktualisieren des Lagerbestands. Bitte erneut versuchen.' });
            }
            console.log(`POST /api/purchase: Bestand für ${bulkWriteResult.modifiedCount} Produkte erfolgreich reduziert.`);
        }

        syncLocalAndRemote().catch(syncErr => console.error('POST /api/purchase: Fehler beim Hintergrund-Sync:', syncErr));
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