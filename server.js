// server.js - Mit explizitem req.session.save() im Login
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
app.set('trust proxy', 1); // WICHTIG für secure cookies hinter Proxy

const HTTP_PORT = process.env.PORT || 80;
const SEED_PRODUCTS_FILE = 'products.json';
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
const frontendProdUrl = process.env.FRONTEND_URL; // z.B. https://limo123123.github.io
const frontendDevUrl = 'http://127.0.0.1:8080'; // Dein lokaler Live Server Port

if (!sessionSecret) { console.error('!!! FEHLER: Kein SESSION_SECRET! Server stoppt.'); process.exit(1); }
if (!mongoUri) { console.error('!!! FEHLER: Keine MongoDB URI! Server stoppt.'); process.exit(1); }

// --- Middleware ---
const allowedOrigins = [frontendDevUrl];
if (frontendProdUrl) { allowedOrigins.push(frontendProdUrl); }
console.log("Erlaubte CORS Origins:", allowedOrigins);

app.use(cors({
    origin: function (origin, callback) {
        if (!origin || allowedOrigins.includes(origin)) { callback(null, true); }
        else { console.error(`CORS Fehler: Origin ${origin} nicht erlaubt.`); callback(new Error(`Origin ${origin} nicht durch CORS erlaubt`)); }
    },
    credentials: true
}));

app.use(express.json({ limit: "50mb" }));
app.use(express.urlencoded({ limit: "50mb", extended: true, parameterLimit: 50000 }));
app.use(session({
    secret: sessionSecret,
    resave: false, // Bleibt false, da saveUninitialized auf false ist und wir save() manuell nutzen können
    saveUninitialized: false,
    store: MongoStore.create({ mongoUrl: mongoUri, dbName: mongoDbName, collectionName: 'sessions', ttl: 14 * 24 * 60 * 60 }),
    cookie: {
        secure: true, // Für Cross-Site mit SameSite=None und generell in Produktion
        httpOnly: true,
        maxAge: 14 * 24 * 60 * 60 * 1000,
        sameSite: 'none', // Notwendig für Cross-Origin Cookies mit Credentials
        path: '/',
    }
}));

// --- Datenbank Variablen ---
let db; let productsCollection; let usersCollection; let ordersCollection;

// --- Hilfsfunktionen ---
async function generateUniqueId() { let newId; let idExists = true; let attempts = 0; const maxAttempts = 1000; while (idExists && attempts < maxAttempts) { newId = Math.floor(100000 + Math.random() * 900000); try { const existing = await productsCollection.findOne({ id: newId }, { projection: { _id: 1 } }); if (!existing) idExists = false; } catch (findErr) { console.error(`❌ Fehler ID-Prüfung ${newId}:`, findErr); throw new Error('DB-Fehler ID-Gen.'); } attempts++; } if (idExists) throw new Error('Fehler ID-Gen., Kollisionen.'); return newId; }
async function seedDatabaseFromLocalJson() { console.log(`🌱 Seeding aus ${SEED_PRODUCTS_FILE}...`); if (!fs.existsSync(SEED_PRODUCTS_FILE)) { console.warn(`   Datei ${SEED_PRODUCTS_FILE} fehlt.`); return 0; } let seededCount = 0; try { const data = fs.readFileSync(SEED_PRODUCTS_FILE, 'utf8'); const parsedData = JSON.parse(data); if (!parsedData || !Array.isArray(parsedData.products)) { console.error(`   ${SEED_PRODUCTS_FILE} ungültig.`); return 0; } const productsToSeed = []; console.log(`   Lese ${parsedData.products.length} Produkte.`); for (const prod of parsedData.products) { if (!prod || typeof prod.name !== 'string' || !prod.name.trim()) { continue; } try { const newId = await generateUniqueId(); productsToSeed.push({ id: newId, name: prod.name.trim(), price: prod.price || "$0.00", image_url: prod.image_url || `https://via.placeholder.com/150?text=${encodeURIComponent(prod.name)}`, stock: 20, default_stock: 20 }); } catch (idError) { console.error(`   ID-Gen Fehler für ${prod.name}: ${idError.message}`); } } if (productsToSeed.length > 0) { console.log(`   Füge ${productsToSeed.length} Produkte in DB...`); try { const ir = await productsCollection.insertMany(productsToSeed, { ordered: false }); seededCount = ir.insertedCount; console.log(`   ✅ DB mit ${seededCount} Produkten befüllt.`); } catch (imErr) { console.error('❌ Fehler insertMany Seed:', imErr); seededCount = imErr.result ? imErr.result.nInserted : 0; console.error(`   Nur ${seededCount} eingefügt.`); } } else { console.log('   Keine Produkte in Seed-Datei.'); } return seededCount; } catch (error) { console.error('❌ Fehler beim Seeden:', error); return -1; } }
async function resetProductStock() { console.log('♻️ Reset auf default_stock...'); try { const r = await productsCollection.updateMany( { id: { $gte: 100000 } }, [ { $set: { stock: { $ifNull: ["$default_stock", 20] } } } ]); console.log(`♻️ Bestand für ${r.modifiedCount} Produkte auf Default.`); } catch (e) { console.error('❌ Fehler Reset Default:', e); throw e; } }
async function zeroOutStock() { console.warn('!!! ACHTUNG: Setze Stock ALLER Produkte auf 0 !!!'); try { const r = await productsCollection.updateMany( { id: { $gte: 100000 } }, { $set: { stock: 0 } } ); console.log(`♻️ Bestand für ${r.modifiedCount} Produkte auf 0.`); } catch (e) { console.error('❌ Fehler Nullsetzen:', e); throw e; } }

// --- Middleware für Auth/Admin ---
function isAuthenticated(req, res, next) { if (req.session && req.session.userId) return next(); else res.status(401).json({ error: 'Nicht eingeloggt.' }); }
async function isAdmin(req, res, next) { if (!req.session || !req.session.userId) return res.status(401).json({ error: 'Nicht eingeloggt.' }); try { const user = await usersCollection.findOne({ _id: new ObjectId(req.session.userId) }); if (user && user.isAdmin === true) return next(); else res.status(403).json({ error: 'Zugriff verweigert.' }); } catch (err) { res.status(500).json({ error: "Fehler Berechtigungsprüfung."}); } }

// --- Init MongoDB-Verbindung ---
MongoClient.connect(mongoUri)
  .then(async client => {
    db = client.db(mongoDbName); productsCollection = db.collection(productsCollectionName); usersCollection = db.collection(usersCollectionName); ordersCollection = db.collection(ordersCollectionName); console.log('✅ MongoDB verbunden.');
    try { await productsCollection.createIndex({ id: 1 }, { unique: true }); await usersCollection.createIndex({ username: 1 }, { unique: true }); await ordersCollection.createIndex({ userId: 1 }); await ordersCollection.createIndex({ date: -1 }); console.log('✅ Indizes erstellt.'); }
    catch (indexErr) { console.error('❌ Fehler Indexerstellung:', indexErr); }
    try { const count = await productsCollection.countDocuments(); if (count === 0) { console.log('   DB leer. Starte Seeding...'); await seedDatabaseFromLocalJson(); } else { console.log(`   DB enthält ${count} Produkte.`); } }
    catch(err) { console.error("   Fehler Prüfen/Seeden:", err); }
    http.createServer(app).listen(HTTP_PORT, () => console.log(`🌐 HTTP-Server läuft auf Port ${HTTP_PORT}`));
  })
  .catch(err => { console.error('❌ MongoDB-Verbindung fehlgeschlagen:', err); process.exit(1); });

// --- Tägliche Aufgaben ---
// setInterval(() => { /* ... (auskommentiert) ... */ }, 10000);

// --- API Endpoints ---

// AUTH
app.post('/api/auth/register', async (req, res) => { const { username, password } = req.body; if (!username || !password || username.length < 3 || password.length < 6) { return res.status(400).json({ error: 'User (min 3)/PW (min 6) nötig.' }); } try { const existingUser = await usersCollection.findOne({ username: username.toLowerCase() }); if (existingUser) return res.status(409).json({ error: 'Benutzername vergeben.' }); const hashedPassword = await bcrypt.hash(password, SALT_ROUNDS); const newUser = { username: username.toLowerCase(), password: hashedPassword, balance: 5000.00, isAdmin: false, infinityMoney: false, createdAt: new Date() }; await usersCollection.insertOne(newUser); res.status(201).json({ message: 'Registrierung erfolgreich!' }); } catch (err) { res.status(500).json({ error: 'Fehler bei Registrierung.' }); } });

app.post('/api/auth/login', async (req, res) => {
    const { username, password, rememberMe } = req.body;
    if (!username || !password) return res.status(400).json({ error: 'User/PW nötig.' });
    try {
        const user = await usersCollection.findOne({ username: username.toLowerCase() });
        if (!user) return res.status(401).json({ error: 'Login ungültig (User nicht gefunden).' });
        const match = await bcrypt.compare(password, user.password);
        if (match) {
            req.session.userId = user._id.toString(); 
            req.session.username = user.username;     
            req.session.isAdmin = user.isAdmin;       
            if (rememberMe === true) { req.session.cookie.maxAge = 14 * 24 * 60 * 60 * 1000; } 
            else { req.session.cookie.maxAge = null; /* Session-Cookie */ }
            
            // Explizites Speichern der Session
            req.session.save(err => {
                if (err) {
                    console.error("Fehler beim Speichern der Session nach Login:", err);
                    return res.status(500).json({ error: 'Fehler beim Erstellen der Session.' });
                }
                console.log(`Login erfolgreich für: ${user.username}. Session gespeichert. Cookie MaxAge: ${req.session.cookie.maxAge}`);
                res.json({ message: 'Login erfolgreich!', user: { username: user.username, balance: user.balance, isAdmin: user.isAdmin, infinityMoney: user.infinityMoney } });
            });
        } else { 
            res.status(401).json({ error: 'Login ungültig (PW falsch).' }); 
        }
    } catch (err) { 
        console.error("Kritischer Fehler im Login Endpoint:", err);
        res.status(500).json({ error: 'Login Fehler (Server).' }); 
    }
});

app.post('/api/auth/logout', (req, res) => { if (req.session) { const username = req.session.username; req.session.destroy(err => { if (err) return res.status(500).json({ error: 'Logout fehlgeschlagen.' }); res.clearCookie('connect.sid'); console.log(`Logout für ${username} erfolgreich.`); res.json({ message: 'Logout erfolgreich!' }); }); } else { res.json({ message: 'Keine aktive Session.' }); } });
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
app.post('/api/products', isAdmin, async (req, res) => { let { name, image_url, price, stock } = req.body; if (!name || !image_url || !price) return res.status(400).json({ error: 'Felder fehlen!' }); price = price.trim(); if (!price.startsWith('$')) price = `$${price}`; const numPrice=parseFloat(price.replace(/[^0-9.]/g,'')); if(isNaN(numPrice)||numPrice<0) return res.status(400).json({error:'Preis ungültig!'}); const fmtPrice=`$${numPrice.toFixed(2)}`; let initStock = 20; if(stock!==undefined){const pStock=parseInt(stock,10); if(!isNaN(pStock)&&pStock>=0) initStock=pStock;} try { const newId = await generateUniqueId(); const prod = { id: newId, name: name.trim(), image_url: image_url.trim(), price: fmtPrice, stock: initStock, default_stock: initStock }; await productsCollection.insertOne(prod); delete prod._id; res.status(201).json({ message: 'Produkt hinzugefügt!', product: prod }); } catch (err) { res.status(500).json({ error: err.message || 'Fehler Hinzufügen.' }); } });
app.delete('/api/products/:id', isAdmin, async (req, res) => { const id = parseInt(req.params.id, 10); if (isNaN(id) || id<100000) return res.status(400).json({ error: 'Ungültige ID!' }); try { const result = await productsCollection.deleteOne({ id: id }); if (result.deletedCount === 0) return res.status(404).json({ error: 'Produkt nicht gefunden!' }); res.json({ message: `Produkt ${id} gelöscht!` }); } catch (err) { res.status(500).json({ error: 'Fehler beim Löschen!' }); } });
app.patch('/api/products/:id', isAdmin, async (req, res) => { const id = parseInt(req.params.id, 10); if (isNaN(id) || id<100000) return res.status(400).json({ error: 'Ungültige ID!' }); const { stock } = req.body; if (stock === undefined) return res.status(400).json({ error: 'Stock fehlt!' }); const pStock = parseInt(stock, 10); if (isNaN(pStock) || pStock < 0) return res.status(400).json({ error: 'Ungültiger Stock!' }); try { const result = await productsCollection.updateOne({ id: id }, { $set: { stock: pStock } }); if (result.matchedCount === 0) return res.status(404).json({ error: `Produkt ${id} nicht gefunden!` }); const updated = await productsCollection.findOne({ id: id }); delete updated._id; res.json({ message: `Lager aktualisiert.`, product: updated }); } catch (err) { res.status(500).json({ error: 'Fehler beim Update!' }); } });

// PURCHASE
app.post('/api/purchase', isAuthenticated, async (req, res) => {
    console.log(`POST /api/purchase von User ${req.session.username} | Warenkorb:`, req.body.cart); const cart = req.body.cart; if (!Array.isArray(cart) || cart.length === 0) return res.status(400).json({ error: 'Warenkorb leer/ungültig.' });
    const userId = new ObjectId(req.session.userId); let user; let totalOrderValue = 0; const errors = []; const productChecks = []; const productDataForOrder = [];
    try {
        user = await usersCollection.findOne({ _id: userId }); if (!user) return res.status(401).json({ error: "User nicht gefunden." });
        for (const item of cart) { if (!item || typeof item.id !== 'number' || item.id < 100000 || typeof item.quantity !== 'number' || item.quantity <= 0) { errors.push(`Ungültiges Item.`); continue; } productChecks.push( productsCollection.findOne({ id: item.id }).then(pDb => { if (!pDb) { errors.push(`"${item.name || item.id}" nicht gefunden.`); return null; } const stockDb = (typeof pDb.stock === 'number' && pDb.stock >= 0) ? pDb.stock : 0; if (item.quantity > stockDb) { errors.push(`"${pDb.name}": Nur ${stockDb} da.`); return null; } const price = parseFloat((pDb.price || "$0").replace(/[^0-9.]/g,''))||0; totalOrderValue += price * item.quantity; productDataForOrder.push({ productId: pDb.id, name: pDb.name, quantity: item.quantity, price: price, image_url: pDb.image_url }); return { id: item.id, quantityToDecrement: item.quantity }; }).catch(e => { errors.push(`DB-Fehler: ${item.id}`); return null; }) ); }
        if (errors.length > 0 && productChecks.length === 0) return res.status(400).json({ error: errors.join('; ') });
        const results = await Promise.all(productChecks); const validationErrorsFromPromises = results.filter(r => r === null).map((r,idx) => errors[idx] || "Produktprüfung fehlgeschlagen"); const allErrors = errors.filter(e => e !== null && e !== undefined && !validationErrorsFromPromises.includes(e) ).concat(validationErrorsFromPromises); if (allErrors.length > 0) return res.status(400).json({ error: allErrors.join('; ') });
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

const SELL_COOLDOWN_SECONDS = 60; // 1 Minute Cooldown

app.post('/api/products/sell', isAuthenticated, async (req, res) => {
    const { productId, sellPrice, quantity } = req.body;
    const userId = new ObjectId(req.session.userId);

    if (typeof productId !== 'number' || productId < 100000 || 
        typeof sellPrice !== 'number' || sellPrice < 0 ||
        typeof quantity !== 'number' || quantity <= 0 || !Number.isInteger(quantity)) {
        return res.status(400).json({ error: 'Ungültige Eingabe: Produkt-ID, Verkaufspreis und Menge (mind. 1) erforderlich.' });
    }

    try {
        const user = await usersCollection.findOne({ _id: userId });
        if (!user) return res.status(404).json({ error: 'Benutzer nicht gefunden.' });

        const product = await productsCollection.findOne({ id: productId });
        if (!product) return res.status(404).json({ error: 'Produkt nicht gefunden.' });

        // Cooldown-Prüfung
        const cooldowns = user.productSellCooldowns || {};
        const lastAttemptTimestamp = cooldowns[productId];
        if (lastAttemptTimestamp) {
            const cooldownEnds = new Date(lastAttemptTimestamp).getTime() + (SELL_COOLDOWN_SECONDS * 1000);
            if (Date.now() < cooldownEnds) {
                const timeLeft = Math.ceil((cooldownEnds - Date.now()) / 1000);
                return res.status(429).json({ error: `Cooldown aktiv. Bitte warte noch ${timeLeft} Sekunden, bevor du dieses Produkt erneut verkaufst.` });
            }
        }

        // Verkaufswahrscheinlichkeit berechnen (Beispiel-Logik)
        // Preis aus String "$X.XX" in Zahl umwandeln
        const originalPriceNumeric = parseFloat((product.price || "$0").replace(/[^0-9.]/g, '')) || 1; // Mindestens 1, um Division durch 0 zu vermeiden
        let probability = 1.0; // 100%

        if (sellPrice > originalPriceNumeric) {
            // Wenn Verkaufspreis höher, Wahrscheinlichkeit sinkt.
            // Je höher der Aufschlag, desto geringer die Wahrscheinlichkeit.
            // Beispiel: 2x Preis = 50% Chance, 3x Preis = 33% Chance etc. (vereinfacht)
            // Man könnte es komplexer machen (z.B. exponentiell fallend)
            probability = originalPriceNumeric / sellPrice;
            probability = Math.max(0.05, Math.min(1.0, probability)); // Min 5% Chance, Max 100%
        } else if (sellPrice < originalPriceNumeric * 0.5) { // Wenn Preis sehr niedrig
            probability = 1.0; // Hohe Chance bei sehr niedrigem Preis
        }
        // Man könnte hier auch product.stock berücksichtigen: Je mehr Stock, desto geringer die System-Kaufbereitschaft

        const wasSold = Math.random() < probability;
        let message = "";

        if (wasSold) {
            const earnings = sellPrice * quantity;
            
            // Guthaben aktualisieren (wenn nicht infinityMoney)
            if (!user.infinityMoney) {
                await usersCollection.updateOne({ _id: userId }, { $inc: { balance: earnings } });
            }
            // Produktbestand auf dem Server erhöhen
            await productsCollection.updateOne({ id: productId }, { $inc: { stock: quantity } });
            
            message = `Erfolgreich ${quantity}x "${product.name}" für $${sellPrice.toFixed(2)}/Stk. verkauft! Du erhältst $${earnings.toFixed(2)}. Neuer Bestand: ${product.stock + quantity}.`;
            console.log(`User ${user.username} verkaufte ${quantity}x Produkt ${productId} für ${earnings}. Probability: ${probability.toFixed(2)}`);
            
            // Cooldown-Timestamp für dieses Produkt entfernen (oder erfolgreich-Timestamp setzen)
            delete cooldowns[productId]; 
            await usersCollection.updateOne({ _id: userId }, { $set: { productSellCooldowns: cooldowns } });

            res.json({ success: true, message: message, newStock: product.stock + quantity, earnings: earnings, probability: probability });
        } else {
            message = `Leider wurde dein Angebot für "${product.name}" (Verkaufspreis: $${sellPrice.toFixed(2)}) diesmal nicht angenommen (Chance war ca. ${(probability * 100).toFixed(0)}%). Versuche es in ${SELL_COOLDOWN_SECONDS} Sekunden erneut.`;
            console.log(`User ${user.username} Verkaufsversuch für ${quantity}x Produkt ${productId} fehlgeschlagen. Probability: ${probability.toFixed(2)}`);
            
            // Cooldown setzen
            cooldowns[productId] = new Date().toISOString();
            await usersCollection.updateOne({ _id: userId }, { $set: { productSellCooldowns: cooldowns } });

            res.status(400).json({ success: false, error: message, probability: probability }); // 400 oder einen anderen passenden Status
        }

    } catch (err) {
        console.error("Fehler beim /api/products/sell:", err);
        res.status(500).json({ error: "Serverfehler beim Verkaufsversuch." });
    }
});

// Fallback für unbekannte Routen
app.use((req, res) => { res.status(404).send('Endpoint nicht gefunden'); });