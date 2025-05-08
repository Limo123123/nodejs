// server.js - Finale Version mit Login, Guthaben etc. & angepasstem Placeholder/CORS
const path = require('path');
const fs = require('fs');
const dotenv = require('dotenv');

// Lade Umgebungsvariablen aus secret.env (wenn vorhanden)
const pathToSecretEnv = '/etc/secrets/secret.env';
const localPathToSecretEnv = path.resolve(__dirname, 'secret.env');
let configPath;
if (fs.existsSync(pathToSecretEnv)) { configPath = pathToSecretEnv; console.log(`Lade .env aus Render Secret File: ${configPath}`); }
else if (fs.existsSync(localPathToSecretEnv)) { configPath = localPathToSecretEnv; console.log(`Lade .env aus lokaler Datei: ${configPath}`); }
else { console.warn(`Keine Secret File gefunden. Nutze nur System Env Vars.`); }
if (configPath) { const result = dotenv.config({ path: configPath }); if (result.error) console.error('Fehler Laden Secret File:', result.error); else console.log('Secret File geladen.'); }

// --- Requires ---
const express = require('express');
const http = require('http');
const cors = require('cors');
const { MongoClient, ObjectId } = require('mongodb');
const bcrypt = require('bcrypt');
const session = require('express-session');
const MongoStore = require('connect-mongo');

const app = express();
const HTTP_PORT = process.env.PORT || 80; // PORT wird von Render gesetzt
const SEED_PRODUCTS_FILE = 'products.json'; // Umbenennen! Rohdaten ohne IDs
const TIMEZONE = 'Europe/Berlin';

// --- Konfiguration aus Umgebungsvariablen ---
const mongoUser = process.env.MONGO_USER;
const mongoPassword = process.env.MONGO_PASSWORD;
const mongoUriFromEnv = process.env.MONGO_URI;
const mongoUri = mongoUriFromEnv || (mongoUser && mongoPassword ? `mongodb+srv://${mongoUser}:${mongoPassword}@limodb.kbacr5r.mongodb.net/?retryWrites=true&w=majority&appName=LimoDB` : null);
const mongoDbName = process.env.MONGO_DB_NAME || 'shop';
const productsCollectionName = 'products';
const usersCollectionName = 'users';
const ordersCollectionName = 'orders';
const sessionSecret = process.env.SESSION_SECRET;
const SALT_ROUNDS = 10;
// Frontend URLs aus Umgebungsvariable holen oder Defaults setzen
const frontendProdUrl = process.env.FRONTEND_URL; // z.B. https://mein-limazon.onrender.com
const frontendDevUrl = 'https://limo123123.github.io'; // Dein lokaler Live Server Port

if (!sessionSecret) { console.error('!!! FEHLER: Kein SESSION_SECRET! Server stoppt.'); process.exit(1); }
if (!mongoUri) { console.error('!!! FEHLER: Keine MongoDB URI! Server stoppt.'); process.exit(1); }

// --- Middleware ---
// CORS GANZ OBEN!
const allowedOrigins = [
    process.env.FRONTEND_URL, 
    'http://127.0.0.1:8080',
    'https://limo123123.github.io' 
].filter(Boolean); 

app.use(cors({
    origin: allowedOrigins, // Array direkt übergeben
    credentials: true,
    methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'], 
    allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With', 'Accept', 'Origin'] 
}));

app.use(cors({
    origin: function (origin, callback) {
        // Erlaube Anfragen ohne Origin (Server-zu-Server, Tools) ODER wenn Origin in der Liste ist
        if (!origin || allowedOrigins.includes(origin)) {
            callback(null, true);
        } else {
            console.error(`CORS Fehler: Origin ${origin} nicht erlaubt.`);
            callback(new Error(`Origin ${origin} nicht durch CORS erlaubt`));
        }
    },
    credentials: true
}));

app.use(express.json({ limit: "50mb" }));
app.use(express.urlencoded({ limit: "50mb", extended: true, parameterLimit: 50000 }));
app.use(session({
    secret: sessionSecret,
    resave: true, saveUninitialized: false,
    store: MongoStore.create({ mongoUrl: mongoUri, dbName: mongoDbName, collectionName: 'sessions', ttl: 14 * 24 * 60 * 60 }),
    cookie: {
    secure: false, // Muss true sein für SameSite=None
    httpOnly: true,
    maxAge: 14 * 24 * 60 * 60 * 1000, 
    sameSite: 'none' // TESTWEISE
}
}));

// --- Datenbank Variablen ---
let db;
let productsCollection;
let usersCollection;
let ordersCollection;

// --- Hilfsfunktionen ---
async function generateUniqueId() {
    let newId; let idExists = true; let attempts = 0; const maxAttempts = 1000;
    while (idExists && attempts < maxAttempts) {
        newId = Math.floor(100000 + Math.random() * 900000);
        try { const existing = await productsCollection.findOne({ id: newId }, { projection: { _id: 1 } }); if (!existing) idExists = false; }
        catch (findErr) { console.error(`❌ Fehler ID-Prüfung ${newId}:`, findErr); throw new Error('DB-Fehler ID-Gen.'); } attempts++;
    } if (idExists) throw new Error('Fehler ID-Gen., Kollisionen.'); console.log(`   🔑 Neue ID: ${newId}`); return newId;
}

async function seedDatabaseFromLocalJson() {
    console.log(`🌱 Versuche DB aus ${SEED_PRODUCTS_FILE} zu befüllen...`);
    if (!fs.existsSync(SEED_PRODUCTS_FILE)) { console.warn(`   Datei ${SEED_PRODUCTS_FILE} nicht gefunden.`); return 0; }
    let seededCount = 0;
    try {
        const data = fs.readFileSync(SEED_PRODUCTS_FILE, 'utf8'); const parsedData = JSON.parse(data);
        if (!parsedData || !Array.isArray(parsedData.products)) { console.error(`   ${SEED_PRODUCTS_FILE} ungültig.`); return 0; }
        const productsToSeed = []; console.log(`   Lese ${parsedData.products.length} Produkte aus Seed.`);
        for (const prod of parsedData.products) {
            if (!prod || typeof prod.name !== 'string' || !prod.name.trim()) { console.warn('   ⚠️ Ignoriere fehlerhaften Eintrag in Seed:', prod); continue; }
            try {
                const newId = await generateUniqueId();
                // Verwende via.placeholder.com als Fallback
                const imageUrl = prod.image_url && typeof prod.image_url === 'string' ? prod.image_url.trim() : `https://via.placeholder.com/150x160.png?text=${encodeURIComponent(prod.name)}`;
                productsToSeed.push({ id: newId, name: prod.name.trim(), price: prod.price && typeof prod.price === 'string' ? prod.price.trim() : "$0.00", image_url: imageUrl, stock: 20, default_stock: 20 });
            } catch (idError) { console.error(`   Fehler ID-Gen für Seed ${prod.name}: ${idError.message}`); }
        }
        if (productsToSeed.length > 0) {
            console.log(`   Füge ${productsToSeed.length} Produkte in DB ein...`);
            try { const insertResult = await productsCollection.insertMany(productsToSeed, { ordered: false }); seededCount = insertResult.insertedCount; console.log(`   ✅ DB mit ${seededCount} Produkten befüllt.`); }
            catch (insertManyError) { console.error('❌ Fehler insertMany Seed:', insertManyError); seededCount = insertManyError.result ? insertManyError.result.nInserted : 0; console.error(`   Nur ${seededCount} eingefügt.`); }
        } else { console.log('   Keine gültigen Produkte in Seed-Datei.'); } return seededCount;
    } catch (error) { console.error('❌ Fehler beim Seeden:', error); return -1; }
}

async function resetProductStock() {
  console.log('♻️ Startet Reset auf default_stock...');
  try { const result = await productsCollection.updateMany( { id: { $type: 'number', $gte: 100000 } }, [ { $set: { stock: { $ifNull: ["$default_stock", 20] } } } ]); console.log(`♻️ Bestand für ${result.modifiedCount} Produkte auf Default zurückgesetzt.`); }
  catch (error) { console.error('❌ Fehler beim Reset auf Default:', error); throw error; }
}

async function zeroOutStock() {
  console.warn('!!! ACHTUNG: Setze Lagerbestand ALLER gültigen Produkte auf 0 !!!');
  try { const result = await productsCollection.updateMany( { id: { $type: 'number', $gte: 100000 } }, { $set: { stock: 0 } } ); console.log(`♻️ Bestand für ${result.modifiedCount} Produkte auf 0 zurückgesetzt.`); }
  catch (error) { console.error('❌ Fehler beim Nullsetzen:', error); throw error; }
}

// --- Middleware für Auth/Admin ---
function isAuthenticated(req, res, next) { if (req.session && req.session.userId) return next(); else res.status(401).json({ error: 'Nicht eingeloggt.' }); }
async function isAdmin(req, res, next) { if (!req.session || !req.session.userId) return res.status(401).json({ error: 'Nicht eingeloggt.' }); try { const user = await usersCollection.findOne({ _id: new ObjectId(req.session.userId) }); if (user && user.isAdmin === true) return next(); else res.status(403).json({ error: 'Zugriff verweigert.' }); } catch (err) { res.status(500).json({ error: "Fehler Berechtigungsprüfung."}); } }
// --- ENDE Middleware ---

// --- Init MongoDB-Verbindung ---
MongoClient.connect(mongoUri)
  .then(async client => {
    db = client.db(mongoDbName); productsCollection = db.collection(productsCollectionName); usersCollection = db.collection(usersCollectionName); ordersCollection = db.collection(ordersCollectionName); console.log('✅ MongoDB verbunden & Collections initialisiert.');
    try { await productsCollection.createIndex({ id: 1 }, { unique: true }); await usersCollection.createIndex({ username: 1 }, { unique: true }); await ordersCollection.createIndex({ userId: 1 }); await ordersCollection.createIndex({ date: -1 }); console.log('✅ Indizes erstellt/existieren.'); }
    catch (indexErr) { console.error('❌ Fehler beim Erstellen von Indizes:', indexErr); }
    try { const count = await productsCollection.countDocuments(); if (count === 0) { console.log('   DB leer. Starte Seeding...'); await seedDatabaseFromLocalJson(); } else { console.log(`   DB enthält ${count} Produkte.`); } }
    catch(err) { console.error("   Fehler beim Prüfen/Seeden:", err); }
    http.createServer(app).listen(HTTP_PORT, () => console.log(`🌐 HTTP-Server läuft auf Port ${HTTP_PORT}`));
  })
  .catch(err => { console.error('❌ MongoDB-Verbindung fehlgeschlagen:', err); process.exit(1); });

// --- Tägliche Aufgaben ---
// setInterval(() => { try { const date = new Date(new Date().toLocaleString('en-US', { timeZone: TIMEZONE })); if (date.getHours() === 0 && date.getMinutes() === 0 && date.getSeconds() <= 15) { console.log('⏰ Mitternacht Reset...'); resetProductStock(); } } catch (timeErr) { console.error("Fehler Zeitprüfung Reset:", timeErr); } }, 10000);

// --- API Endpoints ---

// AUTH
app.post('/api/auth/register', async (req, res) => { const { username, password } = req.body; if (!username || !password || username.length < 3 || password.length < 6) { return res.status(400).json({ error: 'User (min 3)/PW (min 6) nötig.' }); } try { const existingUser = await usersCollection.findOne({ username: username.toLowerCase() }); if (existingUser) return res.status(409).json({ error: 'Benutzername vergeben.' }); const hashedPassword = await bcrypt.hash(password, SALT_ROUNDS); const newUser = { username: username.toLowerCase(), password: hashedPassword, balance: 5000.00, isAdmin: false, infinityMoney: false, createdAt: new Date() }; await usersCollection.insertOne(newUser); res.status(201).json({ message: 'Registrierung erfolgreich!' }); } catch (err) { res.status(500).json({ error: 'Fehler bei Registrierung.' }); } });
app.post('/api/auth/login', async (req, res) => { const { username, password, rememberMe } = req.body; if (!username || !password) return res.status(400).json({ error: 'User/PW nötig.' }); try { const user = await usersCollection.findOne({ username: username.toLowerCase() }); if (!user) return res.status(401).json({ error: 'Login ungültig.' }); const match = await bcrypt.compare(password, user.password); if (match) { req.session.userId = user._id.toString(); req.session.username = user.username; req.session.isAdmin = user.isAdmin; if (rememberMe === true) { req.session.cookie.maxAge = 14 * 24 * 60 * 60 * 1000; console.log(`Login: Remember Me aktiv, setze maxAge auf 14 Tage.`); } else { req.session.cookie.maxAge = null; console.log("Login: Remember Me nicht aktiv, setze Session-Cookie."); } res.json({ message: 'Login erfolgreich!', user: { username: user.username, balance: user.balance, isAdmin: user.isAdmin, infinityMoney: user.infinityMoney } }); } else { res.status(401).json({ error: 'Login ungültig.' }); } } catch (err) { res.status(500).json({ error: 'Login Fehler.' }); } });
app.post('/api/auth/logout', (req, res) => { if (req.session) { req.session.destroy(err => { if (err) return res.status(500).json({ error: 'Logout fehlgeschlagen.' }); res.clearCookie('connect.sid'); res.json({ message: 'Logout erfolgreich!' }); }); } else { res.json({ message: 'Keine aktive Session.' }); } });
app.get('/api/auth/me', isAuthenticated, async (req, res) => { try { const user = await usersCollection.findOne({ _id: new ObjectId(req.session.userId) }, { projection: { password: 0 } }); if (!user) return res.status(404).json({ error: 'User nicht gefunden.' }); res.json(user); } catch (err) { res.status(500).json({ error: "Fehler Userdaten." }); } });

// ACCOUNT
app.patch('/api/account/settings', isAuthenticated, async (req, res) => { const { infinityMoney } = req.body; const updateData = {}; if (typeof infinityMoney === 'boolean') updateData.infinityMoney = infinityMoney; if (Object.keys(updateData).length === 0) return res.status(400).json({ error: "Nichts zu ändern." }); try { const result = await usersCollection.updateOne({ _id: new ObjectId(req.session.userId) }, { $set: updateData }); if (result.matchedCount === 0) return res.status(404).json({ error: "User nicht gefunden." }); res.json({ message: "Einstellungen aktualisiert." }); } catch (err) { res.status(500).json({ error: "Fehler Speichern." }); } });

// ORDERS
app.get('/api/orders', isAuthenticated, async (req, res) => { try { const userOrders = await ordersCollection.find({ userId: new ObjectId(req.session.userId) }).sort({ date: -1 }).limit(50).toArray(); res.json({ orders: userOrders }); } catch (err) { res.status(500).json({ error: "Fehler Laden Historie." }); } });

// ADMIN ACTIONS
app.patch('/api/products/reset', isAdmin, async (req, res) => { try { await resetProductStock(); res.json({ message: 'Lager auf Standard zurückgesetzt.' }); } catch (err) { res.status(500).json({ error: 'Fehler beim Reset (Server).' }); }});
app.patch('/api/admin/zero-stock', isAdmin, async (req, res) => { try { await zeroOutStock(); res.json({ message: 'Lager auf 0 zurückgesetzt.' }); } catch (err) { res.status(500).json({ error: 'Fehler beim Nullsetzen (Server).' }); }});

// PRODUCTS
app.get('/api/products', async (req, res) => { try { const prods = await productsCollection.find({ id: { $type: 'number', $gte: 100000 } }).sort({ id: 1 }).toArray(); const sanitized = prods.map(p => { const s = { ...p }; s.stock = (typeof p.stock === 'number' && p.stock >= 0) ? p.stock : 0; s.default_stock = (typeof p.default_stock === 'number' && p.default_stock >= 0) ? p.default_stock : 20; delete s._id; return s; }); res.json({ products: sanitized }); } catch (err) { res.status(500).json({ error: 'Fehler Produkte holen!' }); }});
app.post('/api/products', isAdmin, async (req, res) => { let { name, image_url, price, stock } = req.body; /* Validierung */ if (!name || !image_url || !price) return res.status(400).json({ error: 'Felder fehlen!' }); price = price.trim(); if (!price.startsWith('$')) price = `$${price}`; const numPrice=parseFloat(price.replace(/[^0-9.]/g,'')); if(isNaN(numPrice)||numPrice<0) return res.status(400).json({error:'Preis ungültig!'}); const fmtPrice=`$${numPrice.toFixed(2)}`; let initStock = 20; if(stock!==undefined){const pStock=parseInt(stock,10); if(!isNaN(pStock)&&pStock>=0) initStock=pStock;} try { const newId = await generateUniqueId(); const prod = { id: newId, name: name.trim(), image_url: image_url.trim(), price: fmtPrice, stock: initStock, default_stock: initStock }; await productsCollection.insertOne(prod); delete prod._id; res.status(201).json({ message: 'Produkt hinzugefügt!', product: prod }); } catch (err) { res.status(500).json({ error: err.message || 'Fehler Hinzufügen.' }); } });
app.delete('/api/products/:id', isAdmin, async (req, res) => { const id = parseInt(req.params.id, 10); if (isNaN(id) || id<100000) return res.status(400).json({ error: 'Ungültige ID!' }); try { const result = await productsCollection.deleteOne({ id: id }); if (result.deletedCount === 0) return res.status(404).json({ error: 'Produkt nicht gefunden!' }); res.json({ message: `Produkt ${id} gelöscht!` }); } catch (err) { res.status(500).json({ error: 'Fehler beim Löschen!' }); } });
app.patch('/api/products/:id', isAdmin, async (req, res) => { const id = parseInt(req.params.id, 10); if (isNaN(id) || id<100000) return res.status(400).json({ error: 'Ungültige ID!' }); const { stock } = req.body; if (stock === undefined) return res.status(400).json({ error: 'Stock fehlt!' }); const pStock = parseInt(stock, 10); if (isNaN(pStock) || pStock < 0) return res.status(400).json({ error: 'Ungültiger Stock!' }); try { const result = await productsCollection.updateOne({ id: id }, { $set: { stock: pStock } }); if (result.matchedCount === 0) return res.status(404).json({ error: `Produkt ${id} nicht gefunden!` }); const updated = await productsCollection.findOne({ id: id }); delete updated._id; res.json({ message: `Lager aktualisiert.`, product: updated }); } catch (err) { res.status(500).json({ error: 'Fehler beim Update!' }); } });

// PURCHASE
app.post('/api/purchase', isAuthenticated, async (req, res) => {
    console.log(`POST /api/purchase von User ${req.session.username} | Warenkorb:`, req.body.cart); const cart = req.body.cart; if (!Array.isArray(cart) || cart.length === 0) return res.status(400).json({ error: 'Warenkorb leer/ungültig.' });
    const userId = new ObjectId(req.session.userId); let user; let totalOrderValue = 0; const errors = []; const productChecks = []; const productDataForOrder = [];
    try {
        user = await usersCollection.findOne({ _id: userId }); if (!user) return res.status(401).json({ error: "Benutzer nicht gefunden." });
        for (const item of cart) { if (!item || typeof item.id !== 'number' || item.id < 100000 || typeof item.quantity !== 'number' || item.quantity <= 0) { errors.push(`Ungültiges Item.`); continue; } productChecks.push( productsCollection.findOne({ id: item.id }).then(pDb => { if (!pDb) { errors.push(`"${item.name || item.id}" nicht gefunden.`); return null; } const stockDb = (typeof pDb.stock === 'number' && pDb.stock >= 0) ? pDb.stock : 0; if (item.quantity > stockDb) { errors.push(`"${pDb.name}": Nur ${stockDb} da.`); return null; } const price = parseFloat((pDb.price || "$0").replace(/[^0-9.]/g,''))||0; totalOrderValue += price * item.quantity; productDataForOrder.push({ productId: pDb.id, name: pDb.name, quantity: item.quantity, price: price, image_url: pDb.image_url }); return { id: item.id, quantityToDecrement: item.quantity }; }).catch(e => { errors.push(`DB-Fehler: ${item.id}`); return null; }) ); }
        if (errors.length > 0 && productChecks.length === 0) return res.status(400).json({ error: errors.join('; ') }); // Nur initiale Fehler
        const results = await Promise.all(productChecks); const validationErrors = errors.concat(results.filter(r => r === null).map(() => "Produktprüfung fehlgeschlagen")); if (validationErrors.length > 0) return res.status(400).json({ error: errors.join('; ') || "Bestandsfehler."});
        const currentBalance = user.balance || 0; if (!user.infinityMoney && currentBalance < totalOrderValue) return res.status(400).json({ error: `Guthaben zu gering. ($${totalOrderValue.toFixed(2)} benötigt)` });
        console.log(`Kauf OK. User: ${user.username}, Total: ${totalOrderValue.toFixed(2)}`); const validUpdates = results.filter(r => r !== null);
        if (validUpdates.length > 0) {
            const bulkProductOps = validUpdates.map(upd => ({ updateOne: { filter: { id: upd.id, stock: { $gte: upd.quantityToDecrement } }, update: { $inc: { stock: -upd.quantityToDecrement } } } })); const productUpdateResult = await productsCollection.bulkWrite(bulkProductOps);
            if (productUpdateResult.modifiedCount !== validUpdates.length) { console.error('Fehler Produkt-Stock Bulk Write!'); return res.status(500).json({ error: 'Konflikt bei Bestandsaktualisierung.' }); }
            console.log(`   -> Bestand für ${productUpdateResult.modifiedCount} Produkte reduziert.`); if (!user.infinityMoney) { const balanceUpdateResult = await usersCollection.updateOne({ _id: userId, balance: { $gte: totalOrderValue } }, { $inc: { balance: -totalOrderValue } }); if (balanceUpdateResult.modifiedCount !== 1) { console.error('Fehler Guthabenabzug!'); return res.status(500).json({ error: 'Konflikt bei Guthaben.' }); } console.log(`   -> Guthaben reduziert.`); } else { console.log(`   -> Guthaben nicht reduziert (Inf).`); }
            try { const order = { userId: userId, username: user.username, date: new Date(), items: productDataForOrder, total: totalOrderValue }; await ordersCollection.insertOne(order); console.log(`   -> Bestellung ${order._id} gespeichert.`); } catch (orderError) { console.error("Fehler Speicher Bestellung:", orderError); }
        }
        res.json({ message: 'Kauf erfolgreich abgeschlossen!' });
    } catch (err) { console.error('POST /api/purchase Fehler:', err); res.status(500).json({ error: 'Unerwarteter Kauffehler.' }); }
});


// Fallback für unbekannte Routen
app.use((req, res) => { res.status(404).send('Endpoint nicht gefunden'); });