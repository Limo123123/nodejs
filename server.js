// server.js - Mit Login, Guthaben, Inventar-Basis, Verkauf & Infinity Money Logik
// NEU: Glücksrad-Funktionen und Token-System (Überarbeitet)
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
const { v4: uuidv4 } = require('uuid'); // Für eindeutige Share-Codes etc.

const SELL_COOLDOWN_SECONDS = 59;
const SELL_COOLDOWN_SECONDS_SHOW = 60;

const app = express();
app.set('trust proxy', 1);

const HTTP_PORT = process.env.PORT || 80;
const SEED_PRODUCTS_FILE = 'products.json';
const TIMEZONE = 'Europe/Berlin';

// --- Konfiguration aus Umgebungsvariablen ---
const mongoUser = process.env.MONGO_USER;
const mongoPassword = process.env.MONGO_PASSWORD;
const mongoUriFromEnv = process.env.MONGO_URI;
const mongoClusterAddress = process.env.MONGO_CLUSTER_ADDRESS || "limodb.kbacr5r.mongodb.net";
const mongoAppName = process.env.MONGO_APP_NAME || "LimoDB";
const mongoUri = mongoUriFromEnv || (mongoUser && mongoPassword ? `mongodb+srv://${mongoUser}:${mongoPassword}@${mongoClusterAddress}/?retryWrites=true&w=majority&appName=${mongoAppName}` : null);
const mongoDbName = process.env.MONGO_DB_NAME || 'shop';
const productsCollectionName = 'products';
const usersCollectionName = 'users';
const ordersCollectionName = 'orders';
const inventoriesCollectionName = 'userInventories';
const wheelsCollectionName = 'wheels';
const tokenCodesCollectionName = 'tokenCodes';
const tokenTransactionsCollectionName = 'tokenTransactions';
const sessionSecret = process.env.SESSION_SECRET;
const SALT_ROUNDS = 10;
const frontendProdUrl = process.env.FRONTEND_URL;
const frontendDevUrlHttp = 'http://127.0.0.1:8080';
const frontendDevUrlHttps = 'https://127.0.0.1:8080';

// --- Glücksrad & Token Konstanten ---
const DEFAULT_STARTING_TOKENS = 10;
const DEFAULT_WHEEL_CREATION_COST_TOKENS = 5; // Standardkosten, falls ein Rad kostenpflichtig ist
const DOLLAR_TO_TOKEN_RATE = 100;
const TOKEN_TO_DOLLAR_RATE = 0.008;

if (!sessionSecret) { console.error('!!! FEHLER: Kein SESSION_SECRET! Server stoppt.'); process.exit(1); }
if (!mongoUri) { console.error('!!! FEHLER: Keine MongoDB URI! Server stoppt.'); process.exit(1); }

// --- Middleware ---
const allowedOrigins = [frontendDevUrlHttp, frontendDevUrlHttps];
if (frontendProdUrl) { allowedOrigins.push(frontendProdUrl); }
console.log("Erlaubte CORS Origins:", allowedOrigins);
app.use(cors({ origin: function (origin, callback) { if (!origin || allowedOrigins.includes(origin)) { callback(null, true); } else { console.error(`CORS Fehler: Origin ${origin} nicht erlaubt.`); callback(new Error(`Origin ${origin} nicht durch CORS erlaubt`)); } }, credentials: true }));
app.use(express.json({ limit: "50mb" }));
app.use(express.urlencoded({ limit: "50mb", extended: true, parameterLimit: 50000 }));
app.use(session({ secret: sessionSecret, resave: false, saveUninitialized: false, store: MongoStore.create({ mongoUrl: mongoUri, dbName: mongoDbName, collectionName: 'sessions', ttl: 14 * 24 * 60 * 60 }), cookie: { secure: process.env.NODE_ENV === 'production', httpOnly: true, maxAge: 14 * 24 * 60 * 60 * 1000, sameSite: 'none' } }));

// --- Datenbank Variablen ---
let db;
let productsCollection;
let usersCollection;
let ordersCollection;
let inventoriesCollection;
let wheelsCollection;
let tokenCodesCollection;
let tokenTransactionsCollection;

// --- Hilfsfunktionen ---
async function generateUniqueId(collection = productsCollection) { let newId; let idExists = true; let attempts = 0; const maxAttempts = 1000; while (idExists && attempts < maxAttempts) { newId = Math.floor(100000 + Math.random() * 900000); try { const existing = await collection.findOne({ id: newId }, { projection: { _id: 1 } }); if (!existing) idExists = false; } catch (findErr) { console.error(`❌ Fehler ID-Prüfung ${newId}:`, findErr); throw new Error('DB-Fehler ID-Gen.'); } attempts++; } if (idExists) throw new Error('Fehler ID-Gen., Kollisionen.'); return newId; }
async function seedDatabaseFromLocalJson() { console.log(`🌱 Seeding aus ${SEED_PRODUCTS_FILE}...`); if (!fs.existsSync(SEED_PRODUCTS_FILE)) { console.warn(`   Datei ${SEED_PRODUCTS_FILE} fehlt.`); return 0; } let seededCount = 0; try { const data = fs.readFileSync(SEED_PRODUCTS_FILE, 'utf8'); const parsedData = JSON.parse(data); if (!parsedData || !Array.isArray(parsedData.products)) { console.error(`   ${SEED_PRODUCTS_FILE} ungültig.`); return 0; } const productsToSeed = []; console.log(`   Lese ${parsedData.products.length} Produkte.`); for (const prod of parsedData.products) { if (!prod || typeof prod.name !== 'string' || !prod.name.trim()) { continue; } try { const newId = await generateUniqueId(productsCollection); productsToSeed.push({ id: newId, name: prod.name.trim(), price: prod.price && typeof prod.price === 'string' ? prod.price.trim() : "$0.00", image_url: prod.image_url && typeof prod.image_url === 'string' ? prod.image_url.trim() : `https://via.placeholder.com/150x160.png?text=${encodeURIComponent(prod.name)}`, stock: 20, default_stock: 20 }); } catch (idError) { console.error(`   ID-Gen Fehler für ${prod.name}: ${idError.message}`); } } if (productsToSeed.length > 0) { console.log(`   Füge ${productsToSeed.length} Produkte in DB...`); try { const ir = await productsCollection.insertMany(productsToSeed, { ordered: false }); seededCount = ir.insertedCount; console.log(`   ✅ DB mit ${seededCount} Produkten befüllt.`); } catch (imErr) { console.error('❌ Fehler insertMany Seed:', imErr); seededCount = imErr.result ? imErr.result.nInserted : 0; console.error(`   Nur ${seededCount} eingefügt.`); } } else { console.log('   Keine Produkte in Seed-Datei.'); } return seededCount; } catch (error) { console.error('❌ Fehler beim Seeden:', error); return -1; } }
async function resetProductStock() { console.log('♻️ Reset auf default_stock...'); try { const r = await productsCollection.updateMany({ id: { $type: 'number', $gte: 100000 } }, [{ $set: { stock: { $ifNull: ["$default_stock", 20] } } }]); console.log(`♻️ Bestand für ${r.modifiedCount} Produkte auf Default.`); } catch (e) { console.error('❌ Fehler Reset Default:', e); throw e; } }
async function zeroOutStock() { console.warn('!!! ACHTUNG: Setze Stock ALLER Produkte auf 0 !!!'); try { const r = await productsCollection.updateMany({ id: { $type: 'number', $gte: 100000 } }, { $set: { stock: 0 } }); console.log(`♻️ Bestand für ${r.modifiedCount} Produkte auf 0.`); } catch (e) { console.error('❌ Fehler Nullsetzen:', e); throw e; } }

async function generateUniqueShareCode() {
    let code;
    let exists = true;
    while (exists) {
        code = uuidv4().substr(0, 8).toUpperCase();
        const existingWheel = await wheelsCollection.findOne({ shareCode: code }, { projection: { _id: 1 } });
        if (!existingWheel) {
            exists = false;
        }
    }
    return code;
}

async function generateUniqueTokenRedeemCode() {
    let code;
    let exists = true;
    const prefix = "LMTKN-";
    while (exists) {
        code = prefix + Math.random().toString(36).substring(2, 7).toUpperCase() + "-" + Math.random().toString(36).substring(2, 7).toUpperCase();
        const existingCode = await tokenCodesCollection.findOne({ code: code }, { projection: { _id: 1 } });
        if (!existingCode) {
            exists = false;
        }
    }
    return code;
}

async function seedDefaultPublicWheel() {
    try {
        const existingPublicWheels = await wheelsCollection.countDocuments({ isPublic: true, creatorUsername: "System" });
        if (existingPublicWheels === 0) {
            console.log('Keine öffentlichen System-Glücksräder gefunden. Erstelle ein Beispielrad...');
            const defaultWheel = {
                creatorId: null,
                creatorUsername: "System",
                name: "Tägliches Spaßrad",
                description: "Drehe und schau, was du heute bekommst! (Keine echten Token-Gewinne)",
                isPublic: true,
                segments: [ // Segmente geben primär Text aus
                    { text: "Super!", color: "#4CAF50", value: "Super!", valueType: "text_prize" },
                    { text: "Niete", color: "#F44336", value: "Niete", valueType: "text_prize" },
                    { text: "Versuch's nochmal", color: "#2196F3", value: "Versuch's nochmal!", valueType: "text_prize" },
                    { text: "Freispiel", color: "#FFEB3B", value: "Freispiel", valueType: "free_spin" },
                    { text: "Toller Preis!", color: "#9C27B0", value: "Toller Preis!", valueType: "text_prize" },
                    { text: "Glück gehabt!", color: "#FF9800", value: "Glück gehabt!", valueType: "text_prize" },
                    { text: "Fast...", color: "#795548", value: "Fast...", valueType: "text_prize" },
                    { text: "Schade :(", color: "#607D8B", value: "Schade :(", valueType: "text_prize" },
                ],
                spinCost: 1, // Beispiel: Kostet 1 Token pro Dreh, kann auch 0 sein
                creationCostPaid: 0,
                shareCode: await generateUniqueShareCode(),
                totalSpins: 0,
                createdAt: new Date(),
                updatedAt: new Date()
            };
            await wheelsCollection.insertOne(defaultWheel);
            console.log('✅ Beispiel-Glücksrad (System) erstellt.');
        } else {
            console.log(`   ${existingPublicWheels} öffentliche System-Glücksräder bereits vorhanden.`);
        }
    } catch (error) {
        console.error("❌ Fehler beim Erstellen des Beispiel-Glücksrads:", error);
    }
}

async function logTokenTransaction(userId, type, amount, balanceBefore, balanceAfter, description, relatedWheelId = null, relatedCodeId = null) {
    if (!tokenTransactionsCollection) {
        console.warn("Token Transaktionslogging ist nicht aktiviert (Collection nicht initialisiert).");
        return;
    }
    try {
        const logEntry = {
            userId,
            type,
            amount,
            balanceBefore: parseFloat(balanceBefore.toFixed(4)), // Sichern, dass es Zahlen sind
            balanceAfter: parseFloat(balanceAfter.toFixed(4)),
            description,
            timestamp: new Date()
        };
        if (relatedWheelId) logEntry.relatedWheelId = relatedWheelId;
        if (relatedCodeId) logEntry.relatedCodeId = relatedCodeId;

        await tokenTransactionsCollection.insertOne(logEntry);
        console.log(`Token-Log: User ${userId}, Typ ${type}, Betrag ${amount}, Von ${balanceBefore} zu ${balanceAfter}. Desc: ${description}`);
    } catch (err) {
        console.error("Fehler beim Loggen der Token-Transaktion:", err);
    }
}

// --- Middleware für Auth/Admin ---
function isAuthenticated(req, res, next) { if (req.session && req.session.userId) return next(); else res.status(401).json({ error: 'Nicht eingeloggt. Bitte zuerst anmelden.' }); }
async function isAdmin(req, res, next) { if (!req.session || !req.session.userId) return res.status(401).json({ error: 'Nicht eingeloggt.' }); try { const user = await usersCollection.findOne({ _id: new ObjectId(req.session.userId) }); if (user && user.isAdmin === true) return next(); else res.status(403).json({ error: 'Zugriff verweigert. Nur für Admins.' }); } catch (err) { console.error("Fehler Admin-Prüfung:", err); res.status(500).json({ error: "Fehler Berechtigungsprüfung." }); } }

// --- Init MongoDB-Verbindung ---
MongoClient.connect(mongoUri)
    .then(async client => {
        db = client.db(mongoDbName);
        productsCollection = db.collection(productsCollectionName);
        usersCollection = db.collection(usersCollectionName);
        ordersCollection = db.collection(ordersCollectionName);
        inventoriesCollection = db.collection(inventoriesCollectionName);
        wheelsCollection = db.collection(wheelsCollectionName);
        tokenCodesCollection = db.collection(tokenCodesCollectionName);
        tokenTransactionsCollection = db.collection(tokenTransactionsCollectionName);

        console.log('✅ MongoDB verbunden & Collections initialisiert.');
        try {
            await productsCollection.createIndex({ id: 1 }, { unique: true });
            await usersCollection.createIndex({ username: 1 }, { unique: true });
            await usersCollection.createIndex({ tokens: 1 });
            await ordersCollection.createIndex({ userId: 1 });
            await ordersCollection.createIndex({ date: -1 });
            await inventoriesCollection.createIndex({ userId: 1, productId: 1 }, { unique: true });
            await inventoriesCollection.createIndex({ userId: 1 });
            await wheelsCollection.createIndex({ creatorId: 1 });
            await wheelsCollection.createIndex({ isPublic: 1 });
            await wheelsCollection.createIndex({ shareCode: 1 }, { unique: true, sparse: true });
            await tokenCodesCollection.createIndex({ code: 1 }, { unique: true });
            await tokenCodesCollection.createIndex({ redeemedByUserId: 1 });
            if (tokenTransactionsCollection) {
                await tokenTransactionsCollection.createIndex({ userId: 1 });
                await tokenTransactionsCollection.createIndex({ type: 1 });
                await tokenTransactionsCollection.createIndex({ timestamp: -1 });
            }
            console.log('✅ Indizes erstellt.');
        }
        catch (indexErr) { console.error('❌ Fehler Indexerstellung:', indexErr); }

        try {
            const count = await productsCollection.countDocuments();
            if (count === 0) { console.log('   DB (products) leer. Starte Seeding...'); await seedDatabaseFromLocalJson(); }
            else { console.log(`   DB (products) enthält ${count} Produkte.`); }
        }
        catch (err) { console.error("   Fehler Prüfen/Seeden:", err); }

        await seedDefaultPublicWheel();

        http.createServer(app).listen(HTTP_PORT, () => console.log(`🌐 HTTP-Server läuft auf Port ${HTTP_PORT}`));
    })
    .catch(err => { console.error('❌ MongoDB-Verbindung fehlgeschlagen:', err); process.exit(1); });


// --- API Endpoints ---

// AUTH
app.post('/api/auth/register', async (req, res) => {
    const { username, password } = req.body;
    if (!username || !password || typeof username !== 'string' || typeof password !== 'string' || username.length < 3 || password.length < 6) {
        return res.status(400).json({ error: 'User (min 3)/PW (min 6) nötig.' });
    }
    try {
        const existingUser = await usersCollection.findOne({ username: username.toLowerCase() });
        if (existingUser) return res.status(409).json({ error: 'Benutzername vergeben.' });
        const hashedPassword = await bcrypt.hash(password, SALT_ROUNDS);
        const newUser = {
            username: username.toLowerCase(),
            password: hashedPassword,
            balance: 5000.00,
            tokens: DEFAULT_STARTING_TOKENS,
            isAdmin: false,
            infinityMoney: false,
            unlockedInfinityMoney: false,
            createdAt: new Date(),
            productSellCooldowns: {}
        };
        await usersCollection.insertOne(newUser);
        console.log(`User ${username.toLowerCase()} registriert mit ${DEFAULT_STARTING_TOKENS} Tokens.`);
        res.status(201).json({ message: 'Registrierung erfolgreich!' });
    } catch (err) {
        console.error("Register Fehler:", err);
        res.status(500).json({ error: 'Fehler bei Registrierung.' });
    }
});

app.post('/api/auth/login', async (req, res) => {
    const { username, password, rememberMe } = req.body;
    if (!username || !password) return res.status(400).json({ error: 'User/PW nötig.' });
    try {
        const user = await usersCollection.findOne({ username: username.toLowerCase() });
        if (!user) return res.status(401).json({ error: 'Login ungültig.' });
        const match = await bcrypt.compare(password, user.password);
        if (match) {
            req.session.userId = user._id.toString();
            req.session.username = user.username;
            req.session.isAdmin = user.isAdmin || false;
            if (rememberMe === true) {
                req.session.cookie.maxAge = 14 * 24 * 60 * 60 * 1000;
            } else {
                req.session.cookie.expires = false;
                req.session.cookie.maxAge = null;
            }
            req.session.save(err => {
                if (err) { console.error("Fehler Session Speichern:", err); return res.status(500).json({ error: 'Fehler Session.' }); }
                const effectiveInfinityMoney = user.isAdmin ? true : (user.infinityMoney || false);
                console.log(`User ${user.username} eingeloggt.`);
                res.json({
                    message: 'Login erfolgreich!',
                    user: {
                        userId: user._id.toString(),
                        username: user.username,
                        balance: user.balance,
                        tokens: user.tokens || 0,
                        isAdmin: user.isAdmin || false,
                        infinityMoney: effectiveInfinityMoney,
                        unlockedInfinityMoney: user.unlockedInfinityMoney || false,
                        productSellCooldowns: user.productSellCooldowns || {}
                    }
                });
            });
        } else {
            res.status(401).json({ error: 'Login ungültig.' });
        }
    } catch (err) {
        console.error("Login Fehler:", err);
        res.status(500).json({ error: 'Login Fehler (Server).' });
    }
});

app.post('/api/auth/logout', (req, res) => {  if (req.session) { const username = req.session.username; req.session.destroy(err => { if (err) return res.status(500).json({ error: 'Logout fehlgeschlagen.' }); res.clearCookie('connect.sid'); console.log(`Logout für ${username}.`); res.json({ message: 'Logout erfolgreich!' }); }); } else { res.json({ message: 'Keine aktive Session.' }); } });

app.get('/api/auth/me', isAuthenticated, async (req, res) => {
    try {
        const user = await usersCollection.findOne({ _id: new ObjectId(req.session.userId) }, { projection: { password: 0 } });
        if (!user) return res.status(404).json({ error: 'User nicht gefunden.' });
        const effectiveInfinityMoney = user.isAdmin ? true : (user.infinityMoney || false);
        res.json({ ...user, tokens: user.tokens || 0, infinityMoney: effectiveInfinityMoney, unlockedInfinityMoney: user.unlockedInfinityMoney || false, productSellCooldowns: user.productSellCooldowns || {} });
    } catch (err) {
        console.error("Fehler /api/auth/me:", err);
        res.status(500).json({ error: "Fehler Userdaten." });
    }
});


// ACCOUNT
app.patch('/api/account/settings', isAuthenticated, async (req, res) => { const { infinityMoney } = req.body; const userId = new ObjectId(req.session.userId); const updateData = {}; let message = "Einstellungen aktualisiert."; try { const user = await usersCollection.findOne({ _id: userId }); if (!user) return res.status(404).json({ error: "Benutzer nicht gefunden." }); if (typeof infinityMoney === 'boolean' && !user.isAdmin) { if (user.unlockedInfinityMoney) { updateData.infinityMoney = infinityMoney; } else { return res.status(403).json({ error: "Infinity Money noch nicht freigeschaltet." }); } } if (Object.keys(updateData).length > 0) { await usersCollection.updateOne({ _id: userId }, { $set: updateData }); console.log(`Acc-Settings für ${user.username} geändert: ${JSON.stringify(updateData)}`);} const updatedUser = await usersCollection.findOne({ _id: userId }, { projection: { password: 0 } }); const effectiveInfinityMoney = updatedUser.isAdmin ? true : (updatedUser.infinityMoney || false); res.json({ message: message, user: { ...updatedUser, tokens: updatedUser.tokens || 0, infinityMoney: effectiveInfinityMoney, productSellCooldowns: updatedUser.productSellCooldowns || {} } }); } catch (err) { console.error("Fehler Account Settings:", err); res.status(500).json({ error: "Fehler Speichern." }); } });

// ORDERS
app.get('/api/orders', isAuthenticated, async (req, res) => { try { const userOrders = await ordersCollection.find({ userId: new ObjectId(req.session.userId) }).sort({ date: -1 }).limit(50).toArray(); res.json({ orders: userOrders }); } catch (err) { console.error("Fehler /api/orders:", err); res.status(500).json({ error: "Fehler Laden Historie." }); } });

// INVENTORY
app.get('/api/inventory', isAuthenticated, async (req, res) => { const userId = new ObjectId(req.session.userId); try { const userInv = await inventoriesCollection.find({ userId: userId, quantityOwned: { $gt: 0 } }).toArray(); const prodIds = userInv.map(item => item.productId); const prodDetails = await productsCollection.find({ id: { $in: prodIds } }, { projection: { name: 1, image_url: 1, price: 1, id: 1, _id: 0 } }).toArray(); const prodMap = new Map(prodDetails.map(p => [p.id, p])); const populated = userInv.map(item => ({ ...item, productDetails: prodMap.get(item.productId) || { name: "Unbekannt", image_url: "", price: "$0.00" } })); res.json({ inventory: populated }); } catch (err) { console.error("Fehler /api/inventory:", err); res.status(500).json({ error: "Fehler Laden Inventar." }); } });

// ADMIN ACTIONS
app.patch('/api/products/reset', isAdmin, async (req, res) => { try { await resetProductStock(); console.log("Admin Action: Product Stock Reset durch", req.session.username); res.json({ message: 'Lager auf Standard zurückgesetzt.' }); } catch (err) { console.error("Fehler /api/products/reset:", err); res.status(500).json({ error: 'Fehler beim Reset (Server).' }); } });
app.patch('/api/admin/zero-stock', isAdmin, async (req, res) => { try { await zeroOutStock(); console.log("Admin Action: Zero Stock durch", req.session.username); res.json({ message: 'Lager auf 0 zurückgesetzt.' }); } catch (err) { console.error("Fehler /api/admin/zero-stock:", err); res.status(500).json({ error: 'Fehler beim Nullsetzen (Server).' }); } });

// PRODUCTS
app.get('/api/products', async (req, res) => { try { const prods = await productsCollection.find({ id: { $type: 'number', $gte: 100000 } }).sort({ id: 1 }).toArray(); const sanitized = prods.map(p => { const s = { ...p }; s.stock = (typeof p.stock === 'number' && p.stock >= 0) ? p.stock : 0; s.default_stock = (typeof p.default_stock === 'number' && p.default_stock >= 0) ? p.default_stock : 20; delete s._id; return s; }); res.json({ products: sanitized }); } catch (err) { console.error("Fehler /api/products (GET):", err); res.status(500).json({ error: 'Fehler Produkte holen!' }); } });
app.post('/api/products', isAdmin, async (req, res) => { let { name, image_url, price, stock } = req.body; if (!name || !image_url || !price) return res.status(400).json({ error: 'Felder fehlen!' }); price = price.trim(); if (!price.startsWith('$')) price = `$${price}`; const numPrice = parseFloat(price.replace(/[^0-9.]/g, '')); if (isNaN(numPrice) || numPrice < 0) return res.status(400).json({ error: 'Preis ungültig!' }); const fmtPrice = `$${numPrice.toFixed(2)}`; let initStock = 20; if (stock !== undefined) { const pStock = parseInt(stock, 10); if (!isNaN(pStock) && pStock >= 0) initStock = pStock; } try { const newId = await generateUniqueId(productsCollection); const prod = { id: newId, name: name.trim(), image_url: image_url.trim(), price: fmtPrice, stock: initStock, default_stock: initStock }; await productsCollection.insertOne(prod); delete prod._id; console.log(`Admin ${req.session.username} hat Produkt ${prod.name} (ID: ${newId}) hinzugefügt.`); res.status(201).json({ message: 'Produkt hinzugefügt!', product: prod }); } catch (err) { console.error("Fehler /api/products (POST):", err); res.status(500).json({ error: err.message || 'Fehler Hinzufügen.' }); } });
app.delete('/api/products/:id', isAdmin, async (req, res) => { const id = parseInt(req.params.id, 10); if (isNaN(id) || id < 100000) return res.status(400).json({ error: 'Ungültige ID!' }); try { await inventoriesCollection.deleteMany({ productId: id }); const result = await productsCollection.deleteOne({ id: id }); if (result.deletedCount === 0) return res.status(404).json({ error: 'Produkt nicht gefunden!' }); console.log(`Admin ${req.session.username} hat Produkt ID ${id} gelöscht.`); res.json({ message: `Produkt ${id} und Inventareinträge gelöscht!` }); } catch (err) { console.error("Fehler /api/products (DELETE):", err); res.status(500).json({ error: 'Fehler beim Löschen!' }); } });
app.patch('/api/products/:id', isAdmin, async (req, res) => { const id = parseInt(req.params.id, 10); if (isNaN(id) || id < 100000) return res.status(400).json({ error: 'Ungültige ID!' }); const { stock } = req.body; if (stock === undefined) return res.status(400).json({ error: 'Stock fehlt!' }); const pStock = parseInt(stock, 10); if (isNaN(pStock) || pStock < 0) return res.status(400).json({ error: 'Ungültiger Stock!' }); try { const result = await productsCollection.updateOne({ id: id }, { $set: { stock: pStock } }); if (result.matchedCount === 0) return res.status(404).json({ error: `Produkt ${id} nicht gefunden!` }); const updated = await productsCollection.findOne({ id: id }); delete updated._id; console.log(`Admin ${req.session.username} hat Stock von Produkt ID ${id} auf ${pStock} geändert.`); res.json({ message: `Lager aktualisiert.`, product: updated }); } catch (err) { console.error("Fehler /api/products (PATCH):", err); res.status(500).json({ error: 'Fehler beim Update!' }); } });

// PURCHASE (Vereinfacht: Keine automatische Token-Pack-Generierung mehr hier)
app.post('/api/purchase', isAuthenticated, async (req, res) => {
    console.log(`POST /api/purchase von User ${req.session.username} | Warenkorb:`, req.body.cart);
    const cart = req.body.cart;
    if (!Array.isArray(cart) || cart.length === 0) return res.status(400).json({ error: 'Warenkorb leer/ungültig.' });

    const userId = new ObjectId(req.session.userId);
    let user;
    let totalOrderValue = 0;
    const errors = [];
    const productChecks = [];
    const productDataForOrder = [];
    const inventoryOps = [];
    let newUnlockOccurred = false;

    try {
        user = await usersCollection.findOne({ _id: userId });
        if (!user) return res.status(401).json({ error: "User nicht gefunden." });
        const isInfinityMoneyActiveForPurchase = user.isAdmin || user.infinityMoney;

        for (const item of cart) {
            if (!item || typeof item.id !== 'number' || item.id < 100000 || typeof item.quantity !== 'number' || item.quantity <= 0) {
                errors.push(`Ungültiges Item.`);
                continue;
            }
            productChecks.push(
                productsCollection.findOne({ id: item.id }).then(pDb => {
                    if (!pDb) { errors.push(`"${item.name || item.id}" nicht gefunden.`); return null; }
                    
                    // Token Packs werden nicht mehr hier generiert, sondern von Admins.
                    // Falls es ein Produkt gäbe, das als "Token Pack" im Shop ist, würde es wie ein normales Produkt behandelt.
                    // Es ist besser, Token-Packs komplett aus dem normalen Kaufprozess zu entfernen und nur über Admin-generierte Codes verfügbar zu machen.

                    const stockDb = (typeof pDb.stock === 'number' && pDb.stock >= 0) ? pDb.stock : 0;
                    if (item.quantity > stockDb) { errors.push(`"${pDb.name}": Nur ${stockDb} da.`); return null; }
                    const price = parseFloat((pDb.price || "$0").replace(/[^0-9.]/g, '')) || 0;
                    totalOrderValue += price * item.quantity;
                    productDataForOrder.push({ productId: pDb.id, name: pDb.name, quantity: item.quantity, price: price, image_url: pDb.image_url });
                    inventoryOps.push({ updateOne: { filter: { userId: userId, productId: pDb.id }, update: { $inc: { quantityOwned: item.quantity }, $set: { lastAcquiredPrice: price } }, upsert: true } });
                    return { id: item.id, quantityToDecrement: item.quantity, priceAtPurchase: price };
                }).catch(e => { errors.push(`DB-Fehler: ${item.id}`); console.error("Product check error:", e); return null; })
            );
        }
        
        if (errors.length > 0 && productChecks.length === 0) return res.status(400).json({ error: errors.join('; ') });
        const results = await Promise.all(productChecks);
        const validationErrorsFromPromises = results.filter(r => r === null).map((r, idx) => errors[idx] || "Produktprüfung fehlgeschlagen");
        const allErrors = errors.filter(e => e && !validationErrorsFromPromises.includes(e)).concat(validationErrorsFromPromises);
        if (allErrors.length > 0) return res.status(400).json({ error: allErrors.join('; ') });

        const currentBalance = user.balance || 0;
        if (!isInfinityMoneyActiveForPurchase && currentBalance < totalOrderValue) return res.status(400).json({ error: `Guthaben zu gering. ($${totalOrderValue.toFixed(2)} benötigt)` });

        const validUpdates = results.filter(r => r !== null);
        if (validUpdates.length > 0) {
            const bulkProductOps = validUpdates.map(upd => ({ updateOne: { filter: { id: upd.id, stock: { $gte: upd.quantityToDecrement } }, update: { $inc: { stock: -upd.quantityToDecrement } } } }));
            const productUpdateResult = await productsCollection.bulkWrite(bulkProductOps);
            if (productUpdateResult.modifiedCount !== validUpdates.length) { console.error('Fehler Produkt-Stock Bulk Write!'); return res.status(500).json({ error: 'Konflikt bei Bestandsaktualisierung.' }); }
        }
        if (inventoryOps.length > 0) { await inventoriesCollection.bulkWrite(inventoryOps); }

        if (!isInfinityMoneyActiveForPurchase) {
            const balanceUpdateResult = await usersCollection.updateOne({ _id: userId, balance: { $gte: totalOrderValue } }, { $inc: { balance: -totalOrderValue } });
            if (balanceUpdateResult.modifiedCount !== 1 && totalOrderValue > 0) { console.error('Fehler Guthabenabzug!'); return res.status(500).json({ error: 'Konflikt bei Guthaben.' }); }
        }

        if (!user.unlockedInfinityMoney && !user.isAdmin) {
            const allShopProds = await productsCollection.find({ id: { $gte: 100000 } }, { projection: { price: 1, _id: 0 } }).sort({ price: -1 }).limit(1).toArray();
            let maxPriceInShop = 0; if (allShopProds.length > 0) maxPriceInShop = parseFloat((allShopProds[0].price || "$0").replace(/[^0-9.]/g, '')) || 0;
            for (const boughtItemData of productDataForOrder) { if (boughtItemData.price >= maxPriceInShop && maxPriceInShop > 0.01) { await usersCollection.updateOne({ _id: userId }, { $set: { unlockedInfinityMoney: true } }); newUnlockOccurred = true; break; } }
        }
        try { const order = { userId: userId, username: user.username, date: new Date(), items: productDataForOrder, total: totalOrderValue }; await ordersCollection.insertOne(order); }
        catch (orderError) { console.error("Fehler Speicher Bestellung:", orderError); }

        const finalUser = await usersCollection.findOne({ _id: userId }, { projection: { password: 0 } });
        const effectiveInfinityMoneyFinal = finalUser.isAdmin ? true : (finalUser.infinityMoney || false);
        let purchaseMessage = `Kauf erfolgreich!`;
        if (newUnlockOccurred) purchaseMessage += ' Glückwunsch, Infinity Money freigeschaltet!';
        console.log(`User ${user.username} hat Einkauf für $${totalOrderValue.toFixed(2)} getätigt.`);
        res.json({ message: purchaseMessage, user: { ...finalUser, tokens: finalUser.tokens || 0, infinityMoney: effectiveInfinityMoneyFinal, productSellCooldowns: finalUser.productSellCooldowns || {} } });
    } catch (err) {
        console.error('POST /api/purchase Fehler:', err);
        res.status(500).json({ error: 'Unerwarteter Kauffehler.' });
    }
});


// SELL Product
app.post('/api/products/sell', isAuthenticated, async (req, res) => { const { productId, sellPrice, quantity } = req.body; const userId = new ObjectId(req.session.userId); if (typeof productId !== 'number' || productId < 100000 || typeof sellPrice !== 'number' || sellPrice <= 0 || typeof quantity !== 'number' || quantity <= 0 || !Number.isInteger(quantity)) { return res.status(400).json({ error: 'Ungültige Eingabe.' }); } try { let user = await usersCollection.findOne({ _id: userId }); if (!user) return res.status(404).json({ error: 'User nicht gefunden.' }); const productToSell = await productsCollection.findOne({ id: productId }); if (!productToSell) return res.status(404).json({ error: 'Produkt nicht im Katalog.' }); const inventoryItem = await inventoriesCollection.findOne({ userId: userId, productId: productId }); if (!inventoryItem || inventoryItem.quantityOwned < quantity) { return res.status(400).json({ error: `Du besitzt nicht ${quantity}x "${productToSell.name}". Bestand: ${inventoryItem ? inventoryItem.quantityOwned : 0}.` }); } let cooldowns = user.productSellCooldowns || {}; const lastAttemptCooldownEndISO = cooldowns[productId.toString()]; if (lastAttemptCooldownEndISO) { const cooldownEndTime = new Date(lastAttemptCooldownEndISO).getTime(); if (Date.now() < cooldownEndTime) { const timeLeft = Math.ceil((cooldownEndTime - Date.now()) / 1000); return res.status(429).json({ success: false, error: `Cooldown: Warte ${timeLeft}s.`, cooldownActiveForProduct: productId, cooldownEndsAt: lastAttemptCooldownEndISO, productSellCooldowns: cooldowns }); } else { delete cooldowns[productId.toString()]; await usersCollection.updateOne({ _id: userId }, { $set: { productSellCooldowns: cooldowns } }); } } const originalPrice = parseFloat((productToSell.price || "$0").replace(/[^0-9.]/g, '')) || 1; let probability = 1.0; if (sellPrice > originalPrice) probability = originalPrice / sellPrice; else if (sellPrice < originalPrice * 0.5) probability = 1.0; const currentGlobalStock = productToSell.stock || 0; const defaultGlobalStock = productToSell.default_stock || 20; if (currentGlobalStock > defaultGlobalStock * 2.5) probability *= 0.1; else if (currentGlobalStock > defaultGlobalStock * 1.8) probability *= 0.5; else if (currentGlobalStock > defaultGlobalStock * 1.2) probability *= 0.8; probability = Math.max(0.01, Math.min(1.0, probability)); const wasSold = Math.random() < probability; let responseMessage = ""; if (wasSold) { const earnings = sellPrice * quantity; await inventoriesCollection.updateOne({ userId: userId, productId: productId, quantityOwned: { $gte: quantity } }, { $inc: { quantityOwned: -quantity } }); await productsCollection.updateOne({ id: productId }, { $inc: { stock: quantity } }); if (!user.isAdmin && !user.infinityMoney) await usersCollection.updateOne({ _id: userId }, { $inc: { balance: earnings } }); else console.log(`   -> Guthaben für ${user.username} nicht erhöht (Infinity/Admin).`); responseMessage = `Erfolgreich ${quantity}x "${productToSell.name}" für $${sellPrice.toFixed(2)}/Stk. verkauft! Erlös: $${earnings.toFixed(2)}.`; delete cooldowns[productId.toString()]; await usersCollection.updateOne({ _id: userId }, { $set: { productSellCooldowns: cooldowns } }); user = await usersCollection.findOne({ _id: userId }, { projection: { password: 0 } }); const effectiveInfinityMoneyFinal = user.isAdmin ? true : (user.infinityMoney || false); console.log(`User ${user.username} hat ${quantity}x ${productToSell.name} verkauft für $${earnings.toFixed(2)}.`); res.json({ success: true, message: responseMessage, earnings: earnings, probability: probability, user: { ...user, tokens: user.tokens || 0, infinityMoney: effectiveInfinityMoneyFinal, productSellCooldowns: user.productSellCooldowns || {} } }); } else { responseMessage = `Angebot für "${productToSell.name}" nicht angenommen (Chance ca. ${(probability * 100).toFixed(0)}%).`; const cooldownEndTime = new Date(Date.now() + SELL_COOLDOWN_SECONDS * 1000); cooldowns[productId.toString()] = cooldownEndTime.toISOString(); await usersCollection.updateOne({ _id: userId }, { $set: { productSellCooldowns: cooldowns } }); console.log(`User ${user.username} Verkaufsversuch für ${productToSell.name} fehlgeschlagen. Cooldown gesetzt.`); res.status(429).json({ success: false, error: `${responseMessage} Cooldown: ${SELL_COOLDOWN_SECONDS_SHOW}s.`, probability: probability, cooldownActiveForProduct: productId, cooldownEndsAt: cooldownEndTime.toISOString(), productSellCooldowns: cooldowns }); } } catch (err) { console.error("Fehler /api/products/sell:", err); res.status(500).json({ error: "Serverfehler Verkaufsversuch." }); } });


// --- TOKEN ENDPOINTS ---
app.post('/api/tokens/convert-dollars-to-tokens', isAuthenticated, async (req, res) => {
    const { dollarAmount } = req.body;
    const userId = new ObjectId(req.session.userId);

    if (typeof dollarAmount !== 'number' || dollarAmount <= 0) {
        return res.status(400).json({ error: "Ungültiger Betrag." });
    }

    try {
        const user = await usersCollection.findOne({ _id: userId });
        if (!user) return res.status(404).json({ error: "Benutzer nicht gefunden." });

        if (user.balance < dollarAmount) {
            return res.status(400).json({ error: "Nicht genügend Guthaben (Dollar)." });
        }

        const tokensToReceive = Math.floor(dollarAmount * DOLLAR_TO_TOKEN_RATE);
        const currentTokenBalance = user.tokens || 0;
        const currentUserBalance = user.balance;

        await usersCollection.updateOne(
            { _id: userId },
            { $inc: { balance: -dollarAmount, tokens: tokensToReceive } }
        );
        
        await logTokenTransaction(userId, "dollar_conversion_to_token", tokensToReceive, currentTokenBalance, currentTokenBalance + tokensToReceive, `Converted $${dollarAmount.toFixed(2)} to ${tokensToReceive} tokens. Dollar balance from $${currentUserBalance.toFixed(2)} to $${(currentUserBalance - dollarAmount).toFixed(2)}`);

        const updatedUser = await usersCollection.findOne({ _id: userId }, { projection: { password: 0 } });
        res.json({
            message: `${dollarAmount.toFixed(2)}$ erfolgreich in ${tokensToReceive} Tokens umgewandelt.`,
            user: { ...updatedUser, tokens: updatedUser.tokens || 0 }
        });

    } catch (err) {
        console.error("Fehler /api/tokens/convert-dollars-to-tokens:", err);
        res.status(500).json({ error: "Serverfehler bei der Umwandlung." });
    }
});

app.post('/api/tokens/convert-tokens-to-dollars', isAuthenticated, async (req, res) => {
    const { tokenAmount } = req.body;
    const userId = new ObjectId(req.session.userId);

    if (typeof tokenAmount !== 'number' || tokenAmount <= 0 || !Number.isInteger(tokenAmount)) {
        return res.status(400).json({ error: "Ungültige Anzahl an Tokens (muss eine positive Ganzzahl sein)." });
    }

    try {
        const user = await usersCollection.findOne({ _id: userId });
        if (!user) return res.status(404).json({ error: "Benutzer nicht gefunden." });

        if ((user.tokens || 0) < tokenAmount) {
            return res.status(400).json({ error: "Nicht genügend Tokens." });
        }

        const dollarsToReceive = parseFloat((tokenAmount * TOKEN_TO_DOLLAR_RATE).toFixed(2));
        const currentTokenBalance = user.tokens || 0;
        const currentUserBalance = user.balance;


        await usersCollection.updateOne(
            { _id: userId },
            { $inc: { tokens: -tokenAmount, balance: dollarsToReceive } }
        );
        
        await logTokenTransaction(userId, "token_conversion_to_dollar", -tokenAmount, currentTokenBalance, currentTokenBalance - tokenAmount, `Converted ${tokenAmount} tokens to $${dollarsToReceive.toFixed(2)}. Dollar balance from $${currentUserBalance.toFixed(2)} to $${(currentUserBalance + dollarsToReceive).toFixed(2)}`);

        const updatedUser = await usersCollection.findOne({ _id: userId }, { projection: { password: 0 } });
        res.json({
            message: `${tokenAmount} Tokens erfolgreich in ${dollarsToReceive.toFixed(2)}$ umgewandelt.`,
            user: { ...updatedUser, tokens: updatedUser.tokens || 0 }
        });

    } catch (err) {
        console.error("Fehler /api/tokens/convert-tokens-to-dollars:", err);
        res.status(500).json({ error: "Serverfehler bei der Umwandlung." });
    }
});

app.post('/api/tokens/redeem', isAuthenticated, async (req, res) => {
    const { code } = req.body;
    const userId = new ObjectId(req.session.userId);

    if (!code || typeof code !== 'string') {
        return res.status(400).json({ error: "Token-Code erforderlich." });
    }

    try {
        const tokenCode = await tokenCodesCollection.findOne({ code: code.trim() });

        if (!tokenCode) {
            return res.status(404).json({ error: "Token-Code ungültig." });
        }
        if (tokenCode.isRedeemed) {
            return res.status(400).json({ error: "Token-Code bereits eingelöst." });
        }

        const user = await usersCollection.findOne({ _id: userId });
        if (!user) return res.status(404).json({ error: "Benutzer nicht gefunden." });
        
        const currentTokenBalance = user.tokens || 0;

        await tokenCodesCollection.updateOne(
            { _id: tokenCode._id },
            { $set: { isRedeemed: true, redeemedByUserId: userId, redeemedAt: new Date() } }
        );
        await usersCollection.updateOne(
            { _id: userId },
            { $inc: { tokens: tokenCode.tokenAmount } }
        );
        
        await logTokenTransaction(userId, "redeem_code", tokenCode.tokenAmount, currentTokenBalance, currentTokenBalance + tokenCode.tokenAmount, `Redeemed code ${code} for ${tokenCode.tokenAmount} tokens.`, null, tokenCode._id);

        const updatedUser = await usersCollection.findOne({ _id: userId }, { projection: { password: 0 } });
        res.json({
            message: `Code erfolgreich eingelöst! ${tokenCode.tokenAmount} Tokens gutgeschrieben.`,
            user: { ...updatedUser, tokens: updatedUser.tokens || 0 }
        });

    } catch (err) {
        console.error("Fehler /api/tokens/redeem:", err);
        res.status(500).json({ error: "Serverfehler beim Einlösen des Codes." });
    }
});

app.post('/api/admin/generate-token-code', isAdmin, async (req, res) => {
    const { tokenAmount, count = 1 } = req.body;
    if (typeof tokenAmount !== 'number' || tokenAmount <= 0 || !Number.isInteger(tokenAmount)) {
        return res.status(400).json({ error: "Ungültiger Token-Betrag (muss positive Ganzzahl sein)." });
    }
    if (typeof count !== 'number' || count <= 0 || count > 100 || !Number.isInteger(count)) {
        return res.status(400).json({ error: "Ungültige Anzahl (1-100, Ganzzahl)." });
    }

    try {
        const generatedCodes = [];
        for (let i = 0; i < count; i++) {
            const uniqueCode = await generateUniqueTokenRedeemCode();
            await tokenCodesCollection.insertOne({
                code: uniqueCode,
                tokenAmount: tokenAmount,
                isRedeemed: false,
                createdAt: new Date(),
                generatedByAdminId: new ObjectId(req.session.userId) // Loggen welcher Admin es war
            });
            generatedCodes.push({code: uniqueCode, amount: tokenAmount});
        }
        console.log(`Admin ${req.session.username} hat ${count} Token-Code(s) mit je ${tokenAmount} Tokens generiert.`);
        res.status(201).json({ message: `${count} Token-Code(s) mit je ${tokenAmount} Tokens generiert.`, codes: generatedCodes });
    } catch (err) {
        console.error("Fehler /api/admin/generate-token-code:", err);
        res.status(500).json({ error: "Fehler bei der Code-Generierung." });
    }
});

// Zeigt dem User nur Codes, die spezifisch für ihn generiert wurden (falls diese Logik je implementiert wird)
// Aktuell werden Codes allgemein generiert. Hier könnte man alle nicht eingelösten Codes für Admins anzeigen.
app.get('/api/tokens/my-codes', isAuthenticated, async (req, res) => {
    // Diese Route ist aktuell weniger sinnvoll, da Codes nicht direkt einem User zugeordnet werden beim Generieren durch Admin.
    // Man könnte sie umfunktionieren, um z.B. eingelöste Codes des Users anzuzeigen.
    // Fürs Erste, eine leere Liste, da `generatedForUserId` nicht mehr primär genutzt wird.
    res.json({ codes: [], message: "Diese Funktion ist derzeit nicht für Endbenutzer-Codes vorgesehen." });
});


// --- GLÜCKSRAD (WHEEL) ENDPOINTS ---
app.post('/api/wheels', isAuthenticated, async (req, res) => {
    let { name, description, isPublic, segments, spinCost, creationCost } = req.body; // 'creationCost' statt 'requiresCreationCost'
    const userId = new ObjectId(req.session.userId);
    const username = req.session.username;

    if (!name || typeof name !== 'string' || name.length < 3 || name.length > 50) {
        return res.status(400).json({ error: "Name des Glücksrads ungültig (3-50 Zeichen)." });
    }
    if (!Array.isArray(segments) || segments.length < 2 || segments.length > 20) {
        return res.status(400).json({ error: "Segmente ungültig (Min 2, Max 20)." });
    }
    for (const seg of segments) {
        if (!seg.text || typeof seg.text !== 'string' || seg.text.length === 0 || seg.text.length > 30) {
            return res.status(400).json({ error: `Segment Text ungültig: "${seg.text}" (1-30 Zeichen).` });
        }
        if (!seg.color || !/^#[0-9A-F]{6}$/i.test(seg.color)) {
            return res.status(400).json({ error: `Segment Farbe ungültig für "${seg.text}". Muss Hex-Code sein (z.B. #FF0000).` });
        }
        // valueType ist nun optionaler, da primär Textpreise. 'free_spin' ist ein spezieller valueType.
        if (seg.valueType && !["text_prize", "free_spin"].includes(seg.valueType)) {
             return res.status(400).json({ error: `Ungültiger valueType "${seg.valueType}" für Segment "${seg.text}". Erlaubt: text_prize, free_spin.` });
        }
        if (!seg.valueType) seg.valueType = "text_prize"; // Default
        if (!seg.value) seg.value = seg.text; // Default value ist der Text selbst
    }
    if (typeof spinCost !== 'number' || spinCost < 0 || !Number.isInteger(spinCost)) {
        return res.status(400).json({ error: "Drehkosten ungültig (Mindestens 0, Ganzzahl)." });
    }
    // creationCost ist nun der Betrag, kann 0 sein.
    if (typeof creationCost !== 'number' || creationCost < 0 || !Number.isInteger(creationCost)) {
        creationCost = 0; // Default auf 0, falls ungültig oder nicht vorhanden
    }


    try {
        const user = await usersCollection.findOne({ _id: userId });
        const currentTokenBalance = user.tokens || 0;

        if (creationCost > 0) {
            if (currentTokenBalance < creationCost) {
                return res.status(400).json({ error: `Nicht genügend Tokens (${creationCost}) für die Erstellung des Glücksrads. Du hast ${currentTokenBalance}.` });
            }
            await usersCollection.updateOne({ _id: userId }, { $inc: { tokens: -creationCost } });
            await logTokenTransaction(userId, "wheel_creation_cost", -creationCost, currentTokenBalance, currentTokenBalance - creationCost, `Paid ${creationCost} tokens for creating wheel '${name}'.`);
        }
        
        const shareCode = await generateUniqueShareCode();
        const newWheel = {
            creatorId: userId,
            creatorUsername: username,
            name,
            description: description || "",
            isPublic: !!isPublic,
            segments,
            spinCost,
            creationCostPaid: creationCost, // Speichert die tatsächlichen Kosten
            shareCode,
            totalSpins: 0,
            createdAt: new Date(),
            updatedAt: new Date()
        };

        const result = await wheelsCollection.insertOne(newWheel);
        console.log(`User ${username} hat Glücksrad '${name}' erstellt (ID: ${result.insertedId}). Kosten: ${creationCost} Tokens.`);

        const updatedUser = await usersCollection.findOne({ _id: userId }, { projection: { password: 0 } });

        res.status(201).json({ message: "Glücksrad erfolgreich erstellt!", wheel: newWheel, user: { ...updatedUser, tokens: updatedUser.tokens || 0 } });

    } catch (err) {
        console.error("Fehler /api/wheels (POST):", err);
        res.status(500).json({ error: "Serverfehler beim Erstellen des Glücksrads." });
    }
});

app.get('/api/wheels/public', async (req, res) => {
    try {
        const publicWheels = await wheelsCollection.find({ isPublic: true }).sort({ createdAt: -1 }).limit(50).project({ segments: 0 }).toArray(); // Segmente nicht in der Liste
        res.json({ wheels: publicWheels });
    } catch (err) {
        console.error("Fehler /api/wheels/public:", err);
        res.status(500).json({ error: "Fehler beim Laden öffentlicher Glücksräder." });
    }
});

app.get('/api/wheels/my', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    try {
        const myWheels = await wheelsCollection.find({ creatorId: userId }).sort({ createdAt: -1 }).limit(50).project({ segments: 0 }).toArray(); // Segmente nicht in der Liste
        res.json({ wheels: myWheels });
    } catch (err) {
        console.error("Fehler /api/wheels/my:", err);
        res.status(500).json({ error: "Fehler beim Laden meiner Glücksräder." });
    }
});

app.get('/api/wheels/:id', async (req, res) => {
    const wheelId = req.params.id;
    if (!ObjectId.isValid(wheelId)) {
        return res.status(400).json({ error: "Ungültige Glücksrad-ID." });
    }
    try {
        const wheel = await wheelsCollection.findOne({ _id: new ObjectId(wheelId) });
        if (!wheel) {
            return res.status(404).json({ error: "Glücksrad nicht gefunden." });
        }
        if (!wheel.isPublic) {
            if (!req.session || !req.session.userId || req.session.userId !== wheel.creatorId.toString()) {
                return res.status(403).json({ error: "Zugriff auf privates Glücksrad verweigert oder nutze den Share-Code." });
            }
        }
        res.json({ wheel }); // Segmente werden hier mitgeliefert
    } catch (err) {
        console.error(`Fehler /api/wheels/${wheelId}:`, err);
        res.status(500).json({ error: "Fehler beim Laden des Glücksrads." });
    }
});

app.get('/api/wheels/shared/:shareCode', async (req, res) => {
    const { shareCode } = req.params;
    if (!shareCode || typeof shareCode !== 'string') {
        return res.status(400).json({ error: "Ungültiger Share-Code." });
    }
    try {
        const wheel = await wheelsCollection.findOne({ shareCode: shareCode });
        if (!wheel) {
            return res.status(404).json({ error: "Kein Glücksrad mit diesem Code gefunden." });
        }
        res.json({ wheel }); // Segmente werden hier mitgeliefert
    } catch (err) {
        console.error(`Fehler /api/wheels/shared/${shareCode}:`, err);
        res.status(500).json({ error: "Fehler beim Laden des geteilten Glücksrads." });
    }
});


app.post('/api/wheels/:id/spin', isAuthenticated, async (req, res) => {
    const wheelId = req.params.id;
    const userId = new ObjectId(req.session.userId);

    if (!ObjectId.isValid(wheelId)) {
        return res.status(400).json({ error: "Ungültige Glücksrad-ID." });
    }

    try {
        const wheel = await wheelsCollection.findOne({ _id: new ObjectId(wheelId) });
        if (!wheel) {
            return res.status(404).json({ error: "Glücksrad nicht gefunden." });
        }

        const user = await usersCollection.findOne({ _id: userId });
        if (!user) return res.status(404).json({ error: "Benutzer nicht gefunden." });

        const currentTokenBalance = user.tokens || 0;

        if (currentTokenBalance < wheel.spinCost) {
            return res.status(400).json({ error: `Nicht genügend Tokens. Benötigt: ${wheel.spinCost}, Vorhanden: ${currentTokenBalance}.` });
        }
        
        if (wheel.spinCost > 0) {
            await usersCollection.updateOne({ _id: userId }, { $inc: { tokens: -wheel.spinCost } });
            await logTokenTransaction(userId, "spin_cost", -wheel.spinCost, currentTokenBalance, currentTokenBalance - wheel.spinCost, `Spin cost for wheel '${wheel.name}' (ID: ${wheel._id}).`, wheel._id);
        }

        const winningSegmentIndex = Math.floor(Math.random() * wheel.segments.length);
        const winningSegment = wheel.segments[winningSegmentIndex];

        let prizeMessage = `Du hast gewonnen: ${winningSegment.text}!`; // Standard-Nachricht
        // Keine direkten Token-Gewinne mehr vom Rad!

        if (winningSegment.valueType === "free_spin") {
            prizeMessage = `Du hast gewonnen: ${winningSegment.text}! Dein Einsatz von ${wheel.spinCost} Token(s) wird dir gutgeschrieben.`;
            if (wheel.spinCost > 0) {
                 await usersCollection.updateOne({ _id: userId }, { $inc: { tokens: wheel.spinCost } });
                 // Logge die Rückerstattung
                 const balanceAfterCost = currentTokenBalance - wheel.spinCost; // Balance nach Abzug der Kosten
                 await logTokenTransaction(userId, "free_spin_refund", wheel.spinCost, balanceAfterCost, balanceAfterCost + wheel.spinCost, `Refund for free spin on wheel '${wheel.name}'.`, wheel._id);
            }
        }
        // valueType "text_prize" oder default ist bereits abgedeckt.
        // Andere valueTypes (z.B. "item") könnten hier noch implementiert werden.

        await wheelsCollection.updateOne({ _id: wheel._id }, { $inc: { totalSpins: 1 }, $set: {updatedAt: new Date()} });
        console.log(`User ${user.username} hat Rad '${wheel.name}' gedreht. Ergebnis: ${winningSegment.text}`);

        const updatedUser = await usersCollection.findOne({ _id: userId }, { projection: { password: 0 } });

        res.json({
            message: prizeMessage,
            winningSegment: winningSegment,
            winningSegmentIndex: winningSegmentIndex,
            user: { ...updatedUser, tokens: updatedUser.tokens || 0 }
        });

    } catch (err) {
        console.error(`Fehler /api/wheels/${wheelId}/spin:`, err);
        res.status(500).json({ error: "Serverfehler beim Drehen des Glücksrads." });
    }
});

app.delete('/api/wheels/:id', isAuthenticated, async (req, res) => {
    const wheelId = req.params.id;
    const userId = new ObjectId(req.session.userId);

    if (!ObjectId.isValid(wheelId)) {
        return res.status(400).json({ error: "Ungültige Glücksrad-ID." });
    }

    try {
        const wheel = await wheelsCollection.findOne({ _id: new ObjectId(wheelId) });
        if (!wheel) {
            return res.status(404).json({ error: "Glücksrad nicht gefunden." });
        }

        const user = await usersCollection.findOne({ _id: userId });
        if (wheel.creatorId.toString() !== userId.toString() && !(user && user.isAdmin)) {
            return res.status(403).json({ error: "Du bist nicht berechtigt, dieses Glücksrad zu löschen." });
        }

        await wheelsCollection.deleteOne({ _id: new ObjectId(wheelId) });
        console.log(`User ${req.session.username} (Admin: ${user.isAdmin}) hat Glücksrad '${wheel.name}' (ID: ${wheelId}) gelöscht.`);
        res.json({ message: "Glücksrad erfolgreich gelöscht." });

    } catch (err) {
        console.error(`Fehler /api/wheels/${wheelId} (DELETE):`, err);
        res.status(500).json({ error: "Serverfehler beim Löschen des Glücksrads." });
    }
});


// Fallback für unbekannte Routen
app.use((req, res) => { res.status(404).send('Endpoint nicht gefunden'); });