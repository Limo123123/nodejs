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

function writeProductsFile(products) {
  try {
      fs.writeFileSync(PRODUCTS_FILE, JSON.stringify({ products }, null, 2));
      console.log(`📦 products.json erfolgreich geschrieben mit ${products.length} Einträgen.`);
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
         // Filtere Produkte ohne gültige ID schon beim Laden der lokalen Datei (ID >= 100000)
         const validProducts = parsedData.products.filter(p => p && typeof p.id === 'number' && Number.isInteger(p.id) && p.id >= 100000);
         console.log(`📁 products.json erfolgreich gelesen mit ${validProducts.length} gültigen Einträgen (ignoriert: ${parsedData.products.length - validProducts.length}).`);
         return { products: validProducts };
     } else {
         console.error("❌ products.json hat unerwartetes Format. Erstelle leeres Array.");
         return { products: [] };
     }
  } catch (error) {
    console.error("❌ Fehler beim Lesen oder Parsen von products.json:", error);
    return { products: [] };
  }
}

/**
 * Bidirektionaler Sync zwischen local JSON und MongoDB
 * Sorgt dafür, dass MongoDB die Master-Quelle ist und die lokale Datei updated wird.
 */
async function syncLocalAndRemote() {
  console.log('🔄 Startet bidirektionalen Sync...');
  try {
    const localProducts = readProductsFile().products; // readProductsFile liefert bereits gefilterte, gültige Produkte
    let remoteProducts = await productsCollection.find().toArray();
    // Filtere remote Produkte ohne gültige ID (ID >= 100000)
    const validRemoteProducts = remoteProducts.filter(p => p && typeof p.id === 'number' && Number.isInteger(p.id) && p.id >= 100000);

    console.log(`   Lokal: ${localProducts.length} gültige Produkte, Remote: ${validRemoteProducts.length} gültige Produkte geladen (Ignoriert Remote: ${remoteProducts.length - validRemoteProducts.length}).`);


    // Map für existenz-check nur mit gültigen Remote-Produkten
    const remoteMap = new Map(validRemoteProducts.map(p => [p.id, p]));
    // Map für lokale Produkte (schon gefiltert)
    const localMap = new Map(localProducts.map(p => [p.id, p]));


    // 2) Füge lokale Produkte hinzu, die in MongoDB fehlen (und gültig sind)
    for (const local of localProducts) {
      if (!remoteMap.has(local.id)) { // checkt nur gegen gültige remote IDs
        const productToAdd = {
          ...local,
          // Standardwert 20, wenn lokal fehlt oder ungültig
          stock: (typeof local.stock === 'number' && Number.isInteger(local.stock) && local.stock >= 0) ? local.stock : 20,
          default_stock: (typeof local.default_stock === 'number' && Number.isInteger(local.default_stock) && local.default_stock >= 0)
                           ? local.default_stock
                           : ((typeof local.stock === 'number' && Number.isInteger(local.stock) && local.stock >= 0) ? local.stock : 20)
        };
         delete productToAdd._id; // Entferne _id für die Einfügung

        try {
            await productsCollection.insertOne(productToAdd);
            console.log(`   ➕ Lokales Produkt mit ID ${local.id} zu MongoDB hinzugefügt.`);
        } catch (insertErr) {
            console.error(`   ❌ Fehler beim Einfügen von lokalem Produkt ${local.id} in MongoDB:`, insertErr);
            if (insertErr.code === 11000) {
                 console.warn(`   Produkt mit ID ${local.id} existiert bereits in MongoDB.`);
            } else {
                 // Anderen Fehler loggen
            }
        }
      }
    }

    // 3) Re-fetch remote nach Einfügungen und filtere erneut
    remoteProducts = await productsCollection.find().toArray();
    const finalValidRemoteProducts = remoteProducts.filter(p => p && typeof p.id === 'number' && Number.isInteger(p.id) && p.id >= 100000);
     console.log(`   Remote Produkte nach Einfügungen neu geladen: ${finalValidRemoteProducts.length} gültige Produkte.`);


    // 4) Erstelle gemergte Liste basierend auf gültigen Remote Produkten
    const merged = finalValidRemoteProducts.map(p => {
        const mergedItem = { ...p };
        // Stellen Sie sicher, dass Stock und Default-Stock Zahlen sind, mit 20 als Default wenn Feld fehlt oder ungültig
        mergedItem.stock = (typeof mergedItem.stock === 'number' && Number.isInteger(mergedItem.stock) && mergedItem.stock >= 0) ? mergedItem.stock : 20;
        mergedItem.default_stock = (typeof mergedItem.default_stock === 'number' && Number.isInteger(mergedItem.default_stock) && mergedItem.default_stock >= 0) ? mergedItem.default_stock : mergedItem.stock;
        delete mergedItem._id; // Entferne _id für die lokale Datei
        return mergedItem;
    });


    // Sortiere die gemergte Liste nach ID
    merged.sort((a, b) => a.id - b.id);


    // 5) Schreibe merged Produkte in JSON
    writeProductsFile(merged);
    console.log(`🔄 Lokale products.json auf ${merged.length} Einträge aktualisiert (aus MongoDB geladen).`);

     console.log(`✅ Synchronisation abgeschlossen.`);


  } catch (error) {
    console.error('❌ Fehler während der Synchronisation:', error);
  }
}


async function resetProductStock() {
  console.log('♻️ Startet Zurücksetzen des Lagerbestands auf default_stock...');
  try {
    const result = await productsCollection.updateMany(
        { id: { $type: 'number', $gte: 100000 } }, 
        [{ 
            $set: {
                 stock: {
                     $cond: {
                         if: { $and: [ { $exists: ["$default_stock"] }, { $type: ["$default_stock", "number"] }, { $gte: ["$default_stock", 0] } ] },
                         then: "$default_stock",
                         else: 20 
                     }
                 }
             }
        }]
    );
    console.log(`♻️ Lagerbestand auf ${result.modifiedCount} Produkte auf default_stock zurückgesetzt (Matched: ${result.matchedCount}).`);
    await syncLocalAndRemote(); 
  } catch (error) {
    console.error('❌ Fehler beim Zurücksetzen des Lagerbestands:', error);
    // Wichtig: Fehler hier werfen, damit der aufrufende API-Endpunkt ihn fangen kann
    throw error; 
  }
}


// Init MongoDB-Verbindung
MongoClient.connect(mongoUri)
  .then(async client => {
    const db = client.db(mongoDbName);
    productsCollection = db.collection(mongoCollectionName);
    console.log('✅ MongoDB verbunden.');

    // Sicherstellen, dass Index auf 'id' existiert.
    try {
        // Versuche, den Index zu erstellen. Wenn er existiert und unique ist, passiert nichts.
        // Wichtig: Manuelle Bereinigung der DB von Duplikaten/id: null ist notwendig, BEVOR dieser Code erfolgreich laufen kann.
        await productsCollection.createIndex({ id: 1 }, { unique: true });
        console.log('✅ MongoDB Index auf "id" erfolgreich erstellt oder existiert bereits.');
    } catch (indexErr) {
        console.error('❌ MongoDB Index auf "id" konnte nicht erstellt werden. Stelle sicher, dass keine Duplikate (insbesondere id: null oder fehlende id) in der Collection existieren. Manuelle Bereinigung in MongoDB Atlas nötig!', indexErr);
        // Optional: process.exit(1); // Entkommentieren, um den Prozess bei Index-Fehlern zu beenden
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
        if (date.getHours() === 0 && date.getMinutes() === 0 && date.getSeconds() <= 10) {
          console.log('⏰ Mitternacht erreicht. Setze Lagerbestand zurück...');
          resetProductStock();
        }
    } catch (timeErr) {
        console.error("Fehler bei der Zeitprüfung für den täglichen Reset:", timeErr);
    }
}, 1000);


// API Endpoints

app.get('/api/products', async (req, res) => {
  try {
    const products = await productsCollection.find().toArray();
    // Sicherstellen, dass stock und default_stock immer Zahlen sind, Default 0 für Stock-Anzeige, 20 für Default_stock wenn fehlend
    const sanitizedProducts = products.map(p => {
        // Ignoriere Produkte ohne gültige ID (ID >= 100000)
        if (!p || typeof p.id !== 'number' || !Number.isInteger(p.id) || p.id < 100000) {
            console.warn("Ignoriere fehlerhaftes Produkt in GET-Antwort (ungültige ID oder fehlt):", p);
            return null;
        }
        const sanitizedProduct = { ...p };
        // Aktueller Stock: 0 wenn Feld fehlt oder ungültig
        sanitizedProduct.stock = (typeof sanitizedProduct.stock === 'number' && Number.isInteger(sanitizedProduct.stock) && sanitizedProduct.stock >= 0) ? sanitizedProduct.stock : 0;
        // Default Stock: 20 wenn Feld fehlt oder ungültig, sonst der gespeicherte Wert
        sanitizedProduct.default_stock = (typeof sanitizedProduct.default_stock === 'number' && Number.isInteger(sanitizedProduct.default_stock) && sanitizedProduct.default_stock >= 0) ? sanitizedProduct.default_stock : 20;

        return sanitizedProduct;
    }).filter(p => p !== null); // Filter fehlerhafte Produkte heraus

    // Füge Sortierung hinzu, um konsistente Reihenfolge zu gewährleisten (hilft beim Frontend Polling Vergleich)
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
      } else {
          console.warn(`POST /api/products: Ungültiger Stock-Wert im Request: "${stock}". Verwende Standardwert ${initialStock}.`);
      }
  }

  let newId;
  let idExists = true;
  for (let i = 0; i < 100; i++) {
     newId = Math.floor(100000 + Math.random() * 900000);
     try {
         const existing = await productsCollection.findOne({ id: newId });
         if (!existing) {
             idExists = false;
             break;
         }
     } catch (findErr) {
          console.error(`POST /api/products: Fehler bei der Prüfung auf ID-Existenz ${newId}:`, findErr);
          return res.status(500).json({ error: 'Fehler bei der ID-Generierung (Datenbankprüfung fehlgeschlagen).' });
     }
  }
  if (idExists) {
      console.error('POST /api/products: Konnte nach mehreren Versuchen keine eindeutige ID generieren.');
      return res.status(500).json({ error: 'Fehler bei der ID-Generierung, bitte versuchen Sie es erneut.' });
  }


  const prod = {
    id: newId,
    name: name.trim(),
    image_url: image_url.trim(),
    price: formattedPrice,
    stock: initialStock,
    default_stock: initialStock
  };

  try {
    const insertResult = await productsCollection.insertOne(prod);
    console.log('POST /api/products: Produkt erfolgreich in DB eingefügt:', insertResult.insertedId);
    await syncLocalAndRemote(); // Warte auf Sync, da neues Produkt hinzugefügt wurde
    res.status(201).json({ message: 'Produkt erfolgreich hinzugefügt!', product: prod });
  } catch (err) {
    console.error('POST /api/products: Fehler beim Hinzufügen des Produkts:', err);
    res.status(500).json({ error: 'Fehler beim Hinzufügen des Produkts!' });
  }
});

app.delete('/api/products/:id', async (req, res) => {
  console.log('DELETE /api/products/:id erhalten für ID:', req.params.id);
  const id = parseInt(req.params.id, 10);
  if (!/^\d{6}$/.test(req.params.id) || isNaN(id)) { // Prüfe auch auf NaN nach parseInt
      console.warn('DELETE /api/products/:id: Ungültiges ID-Format oder NaN:', req.params.id);
      return res.status(400).json({ error: 'Ungültiges ID-Format! Muss 6 Ziffern sein.' });
  }

   try {
       const exists = await productsCollection.findOne({ id: id });
       if (!exists) {
           console.warn(`DELETE /api/products/:id: Produkt mit ID ${id} nicht gefunden.`);
           return res.status(404).json({ error: 'Produkt nicht gefunden!' });
       }
   } catch (findErr) {
        console.error(`DELETE /api/products/:id: Fehler bei der Prüfung auf Produkt-Existenz ${id} vor dem Löschen:`, findErr);
        return res.status(500).json({ error: 'Fehler bei der Datenbankprüfung vor dem Löschen.' });
   }

  try {
    const result = await productsCollection.deleteOne({ id: id }); // Lösche nach 'id', nicht '_id'
    console.log(`DELETE /api/products/:id: Produkt mit ID ${id} gelöscht. Ergebnis:`, result);
    await syncLocalAndRemote(); // Warte auf Sync nach dem Löschen
    res.json({ message: `Produkt mit ID ${id} erfolgreich gelöscht!` });
  } catch (err) {
    console.error(`DELETE /api/products/:id: Fehler beim Löschen des Produkts mit ID ${id}:`, err);
    res.status(500).json({ error: 'Fehler beim Löschen des Produkts!' });
  }
});

// PATCH Lagerbestand zurücksetzen (Endpoint beibehalten und wird jetzt genutzt)
app.patch('/api/products/reset', async (req, res) => {
  console.log('API-Endpoint /api/products/reset aufgerufen.');
  // HIER SOLLTE EINE ADMIN-AUTORISIERUNG STATTFINDEN!
  // Z.B. Überprüfung eines Admin-Tokens, Session, etc.
  // Für dieses Beispiel lassen wir es erstmal ohne, aber in Produktion ist das kritisch.
  // if (!isAdmin(req)) { // Hypothetische isAdmin Funktion
  //    return res.status(403).json({ error: "Zugriff verweigert. Nur für Admins." });
  // }

  try {
    await resetProductStock();
    res.json({ message: 'Lagerbestand auf Standardwerte zurückgesetzt.' });
  } catch (err) {
    console.error('Fehler beim Zurücksetzen des Lagerbestands via API:', err);
    res.status(500).json({ error: 'Fehler beim Zurücksetzen des Lagerbestands auf dem Server.' }); // Genauere Fehlermeldung
  }
});

app.patch('/api/products/:id', async (req, res) => {
  console.log('PATCH /api/products/:id erhalten für ID:', req.params.id, 'Body:', req.body);
  const id = parseInt(req.params.id, 10);
  if (!/^\d{6}$/.test(req.params.id) || isNaN(id)) {
      console.warn('PATCH /api/products/:id: Ungültiges ID-Format oder NaN:', req.params.id);
      return res.status(400).json({ error: 'Ungültiges ID-Format! Muss 6 Ziffern sein.' });
  }

  const { stock } = req.body;

  if (stock === undefined || stock === null) {
       console.warn('PATCH /api/products/:id: Lagerbestandswert fehlt im Request Body.');
       return res.status(400).json({ error: 'Lagerbestandswert fehlt im Request Body!' });
  }
  const parsedStock = parseInt(stock, 10);
  if (isNaN(parsedStock) || !Number.isInteger(parsedStock) || parsedStock < 0) {
      console.warn('PATCH /api/products/:id: Ungültiger Lagerbestandswert im Request Body:', stock);
      return res.status(400).json({ error: 'Ungültiger Lagerbestandswert! Muss eine nicht-negative Ganzzahl sein.' });
  }
  const newStock = parsedStock;

  try {
    const result = await productsCollection.updateOne(
      { id: id },
      { $set: { stock: newStock } }
    );

    if (result.matchedCount === 0) {
      console.warn(`PATCH /api/products/:id: Produkt mit ID ${id} nicht gefunden.`);
      return res.status(404).json({ error: `Produkt mit ID ${id} nicht gefunden!` });
    }
     console.log(`PATCH /api/products/:id: Lagerbestand für Produkt ${id} auf ${newStock} aktualisiert. Ergebnis:`, result);

     // Führe Sync im Hintergrund aus und antworte sofort
     syncLocalAndRemote().catch(syncErr => {
         console.error(`PATCH /api/products/:id: Fehler beim Hintergrund-Sync nach Stock-Update für ID ${id}:`, syncErr);
     });

    // Lade das aktualisierte Produkt (optional, für die Antwort)
    const updatedProduct = await productsCollection.findOne({ id: id });


    res.json({ message: `Lagerbestand für Produkt ${id} erfolgreich aktualisiert (Sync läuft im Hintergrund).`, product: updatedProduct });


  } catch (err) {
    console.error(`PATCH /api/products/:id: Fehler beim Aktualisieren des Lagerbestands für Produkt ${id}:`, err);
    res.status(500).json({ error: 'Fehler beim Aktualisieren des Lagerbestands!' });
  }
});


// NEU: POST Endpoint für den Kaufabschluss
app.post('/api/purchase', async (req, res) => {
    console.log('POST /api/purchase erhalten. Warenkorb:', req.body.cart);
    const cart = req.body.cart;

    if (!Array.isArray(cart) || cart.length === 0) {
        console.warn('POST /api/purchase: Leerer oder ungültiger Warenkorb erhalten.');
        return res.status(400).json({ error: 'Warenkorb ist leer oder ungültig.' });
    }

    const updates = [];
    const errors = [];

    // 1. Bestand prüfen (transaktional sicherer wäre eine MongoDB Transaktion, aber für dieses Beispiel reicht dieser Ansatz)
    try {
        for (const item of cart) {
            // Grundlegende Validierung des Warenkorb-Items
            if (!item || typeof item.id !== 'number' || !Number.isInteger(item.id) || item.id < 100000 ||
                typeof item.quantity !== 'number' || !Number.isInteger(item.quantity) || item.quantity <= 0) {
                console.warn('POST /api/purchase: Ungültiges Item im Warenkorb:', item);
                errors.push(`Ungültiges Produkt im Warenkorb gefunden.`);
                continue; // Überspringe dieses Item, aber prüfe den Rest
            }

            const product = await productsCollection.findOne({ id: item.id });

            if (!product) {
                console.warn(`POST /api/purchase: Produkt mit ID ${item.id} im Warenkorb nicht in DB gefunden.`);
                errors.push(`Produkt "${item.name || item.id}" nicht gefunden.`);
                continue;
            }

            const currentStock = (typeof product.stock === 'number' && Number.isInteger(product.stock) && product.stock >= 0) ? product.stock : 0;

            if (item.quantity > currentStock) {
                console.warn(`POST /api/purchase: Nicht genügend Bestand für Produkt ${item.id}. Benötigt: ${item.quantity}, Verfügbar: ${currentStock}`);
                errors.push(`Nicht genügend Bestand für "${product.name || product.id}". Verfügbar: ${currentStock}`);
                // WICHTIG: Wenn ein Artikel nicht gekauft werden kann, schlägt der gesamte Kauf fehl
                // oder du müsstest den Benutzer fragen, ob er den Rest kaufen will.
                // Für jetzt lassen wir den gesamten Kauf fehlschlagen.
                // Breche die Prüfung ab und melde den Fehler
                break; // Beende die Schleife nach dem ersten Fehler
            }

            // Wenn die Prüfung bestanden ist, bereite das Update vor
            updates.push({
                id: item.id,
                quantity: item.quantity
            });
        }

        // Wenn Fehler bei der Prüfung aufgetreten sind, gib Fehler zurück
        if (errors.length > 0) {
             // Gib nur den ersten Fehler zurück oder fasse sie zusammen
             const errorMessage = errors[0]; // Oder errors.join(', ')
             console.error('POST /api/purchase: Bestandsprüfung fehlgeschlagen.');
             return res.status(400).json({ error: errorMessage });
        }

        // 2. Bestand reduzieren (wenn alle Prüfungen bestanden sind)
        console.log('POST /api/purchase: Bestandsprüfung bestanden. Reduziere Bestand...');
        const bulkOperations = updates.map(update => ({
            updateOne: {
                filter: { id: update.id, stock: { $gte: update.quantity } }, // Zusätzlicher Check, dass Stock immer noch ausreicht
                update: { $inc: { stock: -update.quantity } }
            }
        }));

        if (bulkOperations.length > 0) {
             const bulkWriteResult = await productsCollection.bulkWrite(bulkOperations);

             // Prüfe, ob alle Updates erfolgreich waren (matchedCount == modifiedCount == Anzahl der Operationen)
             if (bulkWriteResult.matchedCount !== updates.length || bulkWriteResult.modifiedCount !== updates.length) {
                 console.error('POST /api/purchase: Fehler beim Bulk Write Update. Nicht alle Produkte aktualisiert.');
                 // Dies könnte passieren, wenn der Stock zwischen Prüfung und Update gesunken ist
                 // Hier müsste man komplexere Logik implementieren (z.B. Transaktionen, Rollback)
                 // Für jetzt geben wir einen Fehler zurück
                 return res.status(500).json({ error: 'Fehler beim Aktualisieren des Lagerbestands während des Kaufs. Bitte versuchen Sie es erneut.' });
             }
             console.log(`POST /api/purchase: Bestand für ${bulkWriteResult.modifiedCount} Produkte erfolgreich reduziert.`);

        } else {
             console.warn('POST /api/purchase: Keine Produkte zum Aktualisieren im Warenkorb (nach Filterung/Prüfung).');
        }


        // 3. Sync nach erfolgreichem Kauf
        // Führe Sync im Hintergrund aus
        syncLocalAndRemote().catch(syncErr => {
            console.error('POST /api/purchase: Fehler beim Hintergrund-Sync nach Kauf:', syncErr);
        });


        res.json({ message: 'Kauf erfolgreich abgeschlossen!' });

    } catch (err) {
        console.error('POST /api/purchase: Unerwarteter Fehler während des Kaufs:', err);
        res.status(500).json({ error: 'Ein unerwarteter Fehler ist beim Kauf aufgetreten.' });
    }
});

// POST Manuelle Synchronisation triggern (Endpoint beibehalten)
app.post('/api/products/sync', async (req, res) => {
   console.log('API-Endpoint /api/products/sync aufgerufen.');
  try {
    await syncLocalAndRemote();
    res.json({ message: 'Bidirektionaler Sync manuell durchgeführt.' });
  } catch (err) {
    console.error('Fehler beim manuellen Sync via API:', err);
    res.status(500).json({ error: 'Fehler beim Sync.' });
  }
});


app.use((req, res) => {
    res.status(404).send('Endpoint nicht gefunden');
});