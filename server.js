// server.js - Vollständiges Backend für Limazon
// Inklusive: Auth, Produkte, Inventar, Kauf (mit Token-Karten), Token-System, Glücksrad-Backend

const path = require('path');
const fs = require('fs');
const dotenv = require('dotenv');

// Lade Umgebungsvariablen aus secret.env (wenn vorhanden)
const pathToSecretEnv = '/etc/secrets/secret.env'; // Für Render
const localPathToSecretEnv = path.resolve(__dirname, 'secret.env'); // Für lokale Entwicklung
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
const { v4: uuidv4 } = require('uuid');
const axios = require('axios'); // Hinzufügen für HTTP-Anfragen 

const SELL_COOLDOWN_SECONDS = 59;
const SELL_COOLDOWN_SECONDS_SHOW = 60;
const LOG_PREFIX_SERVER = "[Limazon BE]";

const app = express();
app.set('trust proxy', 1);

const HTTP_PORT = process.env.PORT || 10000;
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
const DEFAULT_WHEEL_CREATION_COST_TOKENS = 5;
const DOLLAR_TO_TOKEN_RATE = 0.004; // $1 gibt 0.004 Tokens (1 Token ~ $250, ähnlich teuerster Karte)
const TOKEN_TO_DOLLAR_RATE = 200;    // 1 Token gibt $200 zurück

if (!sessionSecret) { console.error(`${LOG_PREFIX_SERVER} !!! FEHLER: Kein SESSION_SECRET in Umgebungsvariablen! Server stoppt.`); process.exit(1); }
if (!mongoUri) { console.error(`${LOG_PREFIX_SERVER} !!! FEHLER: Keine MongoDB URI (MONGO_URI oder User/PW/Cluster) in Umgebungsvariablen! Server stoppt.`); process.exit(1); }

// --- Middleware ---
const allowedOrigins = [frontendDevUrlHttp, frontendDevUrlHttps];
if (frontendProdUrl) { allowedOrigins.push(frontendProdUrl); }
console.log(`${LOG_PREFIX_SERVER} Erlaubte CORS Origins:`, allowedOrigins);

app.use(cors({
    origin: function (origin, callback) {
        if (!origin || allowedOrigins.includes(origin)) {
            callback(null, true);
        } else {
            console.error(`${LOG_PREFIX_SERVER} CORS Fehler: Origin ${origin} nicht erlaubt.`);
            callback(new Error(`Origin ${origin} nicht durch CORS erlaubt`));
        }
    },
    credentials: true
}));
app.use(express.json({ limit: "50mb" }));
app.use(express.urlencoded({ limit: "50mb", extended: true, parameterLimit: 50000 }));
app.use(session({
    secret: sessionSecret,
    resave: false,
    saveUninitialized: false,
    store: MongoStore.create({ mongoUrl: mongoUri, dbName: mongoDbName, collectionName: 'sessions', ttl: 14 * 24 * 60 * 60 }),
    cookie: {
        secure: process.env.NODE_ENV === 'production',
        httpOnly: true,
        maxAge: 14 * 24 * 60 * 60 * 1000,
        sameSite: process.env.NODE_ENV === 'production' ? 'none' : 'lax'
    }
}));

// --- Datenbank Variablen ---
let db;
let productsCollection, usersCollection, ordersCollection, inventoriesCollection;
let wheelsCollection, tokenCodesCollection, tokenTransactionsCollection;

// --- Hilfsfunktionen ---
async function generateUniqueId(collection = productsCollection, prefix = '') {
    let newIdValue; // Renamed to avoid conflict with 'id' parameter if any
    let idExists = true;
    let attempts = 0;
    const maxAttempts = 1000;
    while (idExists && attempts < maxAttempts) {
        newIdValue = prefix + Math.floor(100000 + Math.random() * 900000);
        try {
            const existing = await collection.findOne({ id: newIdValue }, { projection: { _id: 1 } });
            if (!existing) {
                idExists = false;
            }
        } catch (findErr) {
            console.error(`${LOG_PREFIX_SERVER} ❌ Fehler bei ID-Prüfung für ${newIdValue} in Collection ${collection.collectionName}:`, findErr);
            throw new Error('Datenbankfehler bei ID-Generierung.');
        }
        attempts++;
    }
    if (idExists) {
        throw new Error('Fehler bei ID-Generierung nach maximalen Versuchen (Kollisionen).');
    }
    return newIdValue;
}

async function seedDatabaseFromLocalJson() {
    console.log(`${LOG_PREFIX_SERVER} 🌱 Seeding von regulären Produkten aus ${SEED_PRODUCTS_FILE}...`);
    if (!fs.existsSync(SEED_PRODUCTS_FILE)) {
        console.warn(`${LOG_PREFIX_SERVER}    Datei ${SEED_PRODUCTS_FILE} für Produkt-Seeding nicht gefunden.`);
        return 0;
    }
    let seededCount = 0;
    try {
        const data = fs.readFileSync(SEED_PRODUCTS_FILE, 'utf8');
        const parsedData = JSON.parse(data);
        if (!parsedData || !Array.isArray(parsedData.products)) {
            console.error(`${LOG_PREFIX_SERVER}    Format von ${SEED_PRODUCTS_FILE} ist ungültig.`);
            return 0;
        }
        const productsToSeed = [];
        console.log(`${LOG_PREFIX_SERVER}    Lese ${parsedData.products.length} Produkte aus JSON.`);
        for (const prod of parsedData.products) {
            if (!prod || typeof prod.name !== 'string' || !prod.name.trim()) {
                continue;
            }
            try {
                let productId = prod.id;
                if (!productId || await productsCollection.findOne({ id: productId })) {
                    productId = await generateUniqueId(productsCollection);
                }
                productsToSeed.push({
                    id: productId,
                    name: prod.name.trim(),
                    price: prod.price && typeof prod.price === 'string' ? prod.price.trim() : "$0.00",
                    image_url: prod.image_url && typeof prod.image_url === 'string' ? prod.image_url.trim() : `https://via.placeholder.com/150x160.png?text=${encodeURIComponent(prod.name)}`,
                    stock: prod.stock !== undefined ? parseInt(prod.stock) : 20,
                    default_stock: prod.default_stock !== undefined ? parseInt(prod.default_stock) : 20,
                    isTokenCard: false
                });
            } catch (idError) {
                console.error(`${LOG_PREFIX_SERVER}    ID-Generierungsfehler für Produkt ${prod.name}: ${idError.message}`);
            }
        }

        if (productsToSeed.length > 0) {
            console.log(`${LOG_PREFIX_SERVER}    Füge ${productsToSeed.length} neue Produkte in die Datenbank ein...`);
            try {
                const insertResult = await productsCollection.insertMany(productsToSeed, { ordered: false });
                seededCount = insertResult.insertedCount;
                console.log(`${LOG_PREFIX_SERVER}    ✅ Datenbank mit ${seededCount} regulären Produkten befüllt/aktualisiert.`);
            } catch (insertManyErr) {
                console.error(`${LOG_PREFIX_SERVER} ❌ Fehler beim insertMany für Produkt-Seeding:`, insertManyErr.message);
                seededCount = insertManyErr.result ? insertManyErr.result.nInserted : 0;
                 if (seededCount > 0) console.error(`${LOG_PREFIX_SERVER}    Trotz Fehler wurden ${seededCount} Produkte eingefügt.`);
            }
        } else {
            console.log(`${LOG_PREFIX_SERVER}    Keine neuen regulären Produkte zum Seeden aus Datei ${SEED_PRODUCTS_FILE}.`);
        }
        return seededCount;
    } catch (error) {
        console.error(`${LOG_PREFIX_SERVER} ❌ Schwerwiegender Fehler beim Produkt-Seeding:`, error);
        return -1;
    }
}

async function resetProductStock() {
    console.log(`${LOG_PREFIX_SERVER} ♻️ Setze Lagerbestand regulärer Produkte auf Standard zurück...`);
    try {
        const result = await productsCollection.updateMany(
            { isTokenCard: { $ne: true } },
            [{ $set: { stock: { $ifNull: ["$default_stock", 20] } } }]
        );
        console.log(`${LOG_PREFIX_SERVER} ♻️ Lagerbestand für ${result.modifiedCount} reguläre Produkte auf Standard zurückgesetzt.`);
    } catch (e) {
        console.error(`${LOG_PREFIX_SERVER} ❌ Fehler beim Zurücksetzen des Lagerbestands:`, e);
        throw e;
    }
}

async function zeroOutStock() {
    console.warn(`${LOG_PREFIX_SERVER} !!! ACHTUNG: Setze Lagerbestand ALLER regulären Produkte auf 0 !!!`);
    try {
        const result = await productsCollection.updateMany(
            { isTokenCard: { $ne: true } },
            { $set: { stock: 0 } }
        );
        console.log(`${LOG_PREFIX_SERVER} ♻️ Lagerbestand für ${result.modifiedCount} reguläre Produkte auf 0 gesetzt.`);
    } catch (e) {
        console.error(`${LOG_PREFIX_SERVER} ❌ Fehler beim Nullsetzen des Lagerbestands:`, e);
        throw e;
    }
}

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
            console.log(`${LOG_PREFIX_SERVER} Keine öffentlichen System-Glücksräder gefunden. Erstelle ein Beispielrad...`);
            const defaultWheel = {
                creatorId: null,
                creatorUsername: "System",
                name: "Tägliches Spaßrad",
                description: "Drehe und schau, was du heute bekommst! (Keine echten Token-Gewinne, nur Textpreise)",
                isPublic: true,
                segments: [
                    { text: "Super!", color: "#4CAF50", value: "Super!", valueType: "text_prize" },
                    { text: "Niete", color: "#F44336", value: "Niete", valueType: "text_prize" },
                    { text: "Versuch's nochmal", color: "#2196F3", value: "Versuch's nochmal!", valueType: "text_prize" },
                    { text: "Freispiel", color: "#FFEB3B", value: "Freispiel", valueType: "free_spin" },
                    { text: "Toller Preis!", color: "#9C27B0", value: "Toller Preis!", valueType: "text_prize" },
                    { text: "Glück gehabt!", color: "#FF9800", value: "Glück gehabt!", valueType: "text_prize" },
                    { text: "Fast...", color: "#795548", value: "Fast...", valueType: "text_prize" },
                    { text: "Schade :(", color: "#607D8B", value: "Schade :(", valueType: "text_prize" },
                ],
                spinCost: 1,
                creationCostPaid: 0,
                shareCode: await generateUniqueShareCode(),
                totalSpins: 0,
                createdAt: new Date(),
                updatedAt: new Date()
            };
            await wheelsCollection.insertOne(defaultWheel);
            console.log(`${LOG_PREFIX_SERVER} ✅ Beispiel-Glücksrad (System) erstellt.`);
        } else {
            console.log(`${LOG_PREFIX_SERVER}    ${existingPublicWheels} öffentliche System-Glücksräder bereits vorhanden.`);
        }
    } catch (error) {
        console.error(`${LOG_PREFIX_SERVER} ❌ Fehler beim Erstellen des Beispiel-Glücksrads:`, error);
    }
}

async function logTokenTransaction(userId, type, amount, balanceBefore, balanceAfter, description, relatedWheelId = null, relatedCodeId = null) {
    if (!tokenTransactionsCollection) {
        console.warn(`${LOG_PREFIX_SERVER} Token Transaktionslogging ist nicht aktiviert (Collection 'tokenTransactions' nicht initialisiert).`);
        return;
    }
    try {
        const logEntry = {
            userId, type, amount,
            balanceBefore: parseFloat(balanceBefore.toFixed(4)),
            balanceAfter: parseFloat(balanceAfter.toFixed(4)),
            description, timestamp: new Date()
        };
        if (relatedWheelId) logEntry.relatedWheelId = relatedWheelId;
        if (relatedCodeId) logEntry.relatedCodeId = relatedCodeId;
        await tokenTransactionsCollection.insertOne(logEntry);
        console.log(`${LOG_PREFIX_SERVER} Token-Log: User ${userId}, Typ ${type}, Betrag ${amount}, Von ${balanceBefore.toFixed(2)} zu ${balanceAfter.toFixed(2)}. Desc: ${description}`);
    } catch (err) {
        console.error(`${LOG_PREFIX_SERVER} Fehler beim Loggen der Token-Transaktion:`, err);
    }
}

async function seedTokenCardProducts() {
    const tokenCardsData = [
        { id: 900010, name: "Token Guthabenkarte 10", price: "$10,000.00", tokenValue: 10, image_url_suffix: "10_Tokens" },
        { id: 900050, name: "Token Guthabenkarte 50", price: "$25,000.00", tokenValue: 50, image_url_suffix: "50_Tokens" },
        { id: 900100, name: "Token Guthabenkarte 100", price: "$48,000.00", tokenValue: 100, image_url_suffix: "100_Tokens" },
        { id: 900500, name: "Token Guthabenkarte 500", price: "$150,000.00", tokenValue: 500, image_url_suffix: "500_Tokens" },
        { id: 901000, name: "Token Guthabenkarte 1000", price: "$240,000.00", tokenValue: 1000, image_url_suffix: "1000_Tokens" },
    ];
    let seededCount = 0;
    for (const card of tokenCardsData) {
        try {
            const existingCard = await productsCollection.findOne({ id: card.id });
            if (!existingCard) {
                await productsCollection.insertOne({
                    id: card.id, name: card.name, price: card.price,
                    image_url: `https://via.placeholder.com/150x160.png?text=${encodeURIComponent(card.image_url_suffix)}`,
                    stock: 99999, default_stock: 99999, isTokenCard: true, tokenValue: card.tokenValue
                });
                console.log(`${LOG_PREFIX_SERVER} 🌱 Token-Karte "${card.name}" geseedet.`);
                seededCount++;
            }
        } catch (err) {
            if (err.code !== 11000) { console.error(`${LOG_PREFIX_SERVER} ❌ Fehler beim Seeden der Token-Karte ${card.name}:`, err); }
        }
    }
    if(seededCount > 0) console.log(`${LOG_PREFIX_SERVER} ✅ ${seededCount} Token-Karten Produkte erfolgreich geseedet.`);
    else console.log(`${LOG_PREFIX_SERVER}    Keine neuen Token-Karten Produkte zu seeden (oder bereits vorhanden).`);
}

// --- Middleware für Authentifizierung und Admin-Rechte ---
function isAuthenticated(req, res, next) {
    if (req.session && req.session.userId) {
        return next();
    } else {
        console.warn(`${LOG_PREFIX_SERVER} isAuthenticated: Zugriff verweigert (nicht eingeloggt) für Pfad ${req.originalUrl}. Session ID: ${req.sessionID}`);
        res.status(401).json({ error: 'Nicht eingeloggt. Bitte zuerst anmelden.' });
    }
}

async function isAdmin(req, res, next) {
    if (!req.session || !req.session.userId) {
        console.warn(`${LOG_PREFIX_SERVER} isAdmin: Zugriff verweigert (nicht eingeloggt) für Pfad ${req.originalUrl}.`);
        return res.status(401).json({ error: 'Nicht eingeloggt.' });
    }
    try {
        const user = await usersCollection.findOne({ _id: new ObjectId(req.session.userId) });
        if (user && user.isAdmin === true) {
            return next();
        } else {
            console.warn(`${LOG_PREFIX_SERVER} isAdmin: Zugriff verweigert (keine Admin-Rechte) für User ${req.session.username} auf Pfad ${req.originalUrl}.`);
            res.status(403).json({ error: 'Zugriff verweigert. Nur für Admins.' });
        }
    } catch (err) {
        console.error(`${LOG_PREFIX_SERVER} Fehler bei Admin-Prüfung für User ${req.session.username}:`, err);
        res.status(500).json({ error: "Fehler bei der Überprüfung der Berechtigungen." });
    }
}

// --- Init MongoDB-Verbindung und Serverstart ---
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
        console.log(`${LOG_PREFIX_SERVER} ✅ MongoDB erfolgreich verbunden und Collections initialisiert.`);
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
            await tokenCodesCollection.createIndex({ generatedForUserId: 1, isRedeemed: 1 });
            if (tokenTransactionsCollection) {
                await tokenTransactionsCollection.createIndex({ userId: 1 });
                await tokenTransactionsCollection.createIndex({ type: 1 });
                await tokenTransactionsCollection.createIndex({ timestamp: -1 });
            }
            console.log(`${LOG_PREFIX_SERVER} ✅ Alle Indizes erfolgreich erstellt oder bereits vorhanden.`);
        }
        catch (indexErr) { console.error(`${LOG_PREFIX_SERVER} ❌ Fehler bei der Indexerstellung:`, indexErr); }
        try {
            const regularProductCount = await productsCollection.countDocuments({ isTokenCard: { $ne: true } });
            if (regularProductCount === 0) {
                console.log(`${LOG_PREFIX_SERVER}    Datenbank (reguläre Produkte) ist leer. Starte initiales Seeding...`);
                await seedDatabaseFromLocalJson();
            } else {
                console.log(`${LOG_PREFIX_SERVER}    Datenbank enthält bereits ${regularProductCount} reguläre Produkte.`);
            }
        } catch (seedErr) { console.error(`${LOG_PREFIX_SERVER}    Fehler beim Überprüfen/Seeden regulärer Produkte:`, seedErr); }
        await seedTokenCardProducts();
        await seedDefaultPublicWheel();
        http.createServer(app).listen(HTTP_PORT, () => {
            console.log(`${LOG_PREFIX_SERVER} 🌐 HTTP-Server läuft auf Port ${HTTP_PORT}`);
        });
    })
    .catch(err => { console.error(`${LOG_PREFIX_SERVER} ❌ Kritischer Fehler: MongoDB-Verbindung fehlgeschlagen:`, err); process.exit(1); });

// === API ENDPOINTS ===

// AUTH
app.post('/api/auth/register', async (req, res) => {
    const { username, password } = req.body;
    console.log(`${LOG_PREFIX_SERVER} Registrierungsversuch für User: ${username ? username.substring(0,3)+"***" : "LEER"}`);
    if (!username || !password || typeof username !== 'string' || typeof password !== 'string' || username.length < 3 || username.length > 30 || password.length < 6) {
        return res.status(400).json({ error: 'Benutzername (3-30 Zeichen) und Passwort (min 6 Zeichen) erforderlich.' });
    }
    try {
        const existingUser = await usersCollection.findOne({ username: username.toLowerCase() });
        if (existingUser) {
            console.warn(`${LOG_PREFIX_SERVER} Registrierung fehlgeschlagen: Benutzername ${username.toLowerCase()} bereits vergeben.`);
            return res.status(409).json({ error: 'Benutzername bereits vergeben.' });
        }
        const hashedPassword = await bcrypt.hash(password, SALT_ROUNDS);
        const newUser = {
            username: username.toLowerCase(), password: hashedPassword, balance: 5000.00, tokens: DEFAULT_STARTING_TOKENS,
            isAdmin: false, infinityMoney: false, unlockedInfinityMoney: false, createdAt: new Date(), productSellCooldowns: {}
        };
        await usersCollection.insertOne(newUser);
        console.log(`${LOG_PREFIX_SERVER} User ${username.toLowerCase()} erfolgreich registriert mit ${DEFAULT_STARTING_TOKENS} Tokens.`);
        res.status(201).json({ message: 'Registrierung erfolgreich!' });
    } catch (err) {
        console.error(`${LOG_PREFIX_SERVER} Fehler bei Registrierung für User ${username}:`, err);
        res.status(500).json({ error: 'Fehler bei der Registrierung auf dem Server.' });
    }
});

app.post('/api/auth/login', async (req, res) => {
    const { username, password, rememberMe } = req.body;
    console.log(`${LOG_PREFIX_SERVER} Login-Versuch für User: ${username ? username.substring(0,3)+"***" : "LEER"}`);
    if (!username || !password) return res.status(400).json({ error: 'Benutzername und Passwort erforderlich.' });
    try {
        const user = await usersCollection.findOne({ username: username.toLowerCase() });
        if (!user) {
            console.warn(`${LOG_PREFIX_SERVER} Login fehlgeschlagen: User ${username.toLowerCase()} nicht gefunden.`);
            return res.status(401).json({ error: 'Ungültige Anmeldedaten.' });
        }
        const match = await bcrypt.compare(password, user.password);
        if (match) {
            req.session.userId = user._id.toString();
            req.session.username = user.username;
            req.session.isAdmin = user.isAdmin || false;
            if (rememberMe === true) req.session.cookie.maxAge = 14 * 24 * 60 * 60 * 1000;
            else { req.session.cookie.expires = false; req.session.cookie.maxAge = null; }
            req.session.save(err => {
                if (err) { console.error(`${LOG_PREFIX_SERVER} Fehler Speichern Session Login ${user.username}:`, err); return res.status(500).json({ error: 'Fehler Session.' }); }
                console.log(`${LOG_PREFIX_SERVER} User ${user.username} eingeloggt. Session ID: ${req.session.id}, Admin: ${req.session.isAdmin}`);
                const effectiveInfinityMoney = user.isAdmin ? true : (user.infinityMoney || false);
                res.json({ message: 'Login erfolgreich!', user: { userId: user._id.toString(), username: user.username, balance: user.balance, tokens: user.tokens || 0, isAdmin: user.isAdmin || false, infinityMoney: effectiveInfinityMoney, unlockedInfinityMoney: user.unlockedInfinityMoney || false, productSellCooldowns: user.productSellCooldowns || {} }});
            });
        } else {
            console.warn(`${LOG_PREFIX_SERVER} Login fehlgeschlagen: Falsches PW für ${username.toLowerCase()}.`);
            res.status(401).json({ error: 'Ungültige Anmeldedaten.' });
        }
    } catch (err) {
        console.error(`${LOG_PREFIX_SERVER} Serverfehler Login ${username}:`, err);
        res.status(500).json({ error: 'Serverfehler beim Login.' });
    }
});

app.post('/api/auth/logout', (req, res) => {
    const username = req.session.username || 'Unbek. User'; const sessionId = req.session.id;
    console.log(`${LOG_PREFIX_SERVER} Logout User: ${username}. Session ID: ${sessionId}`);
    if (req.session) {
        req.session.destroy(err => {
            if (err) { console.error(`${LOG_PREFIX_SERVER} Logout fehlgeschlagen ${username} (Sess ${sessionId}):`, err); return res.status(500).json({ error: 'Logout fehlgeschlagen.' }); }
            res.clearCookie('connect.sid', { path: '/', domain: process.env.NODE_ENV === 'production' && frontendProdUrl ? new URL(frontendProdUrl).hostname : undefined });
            console.log(`${LOG_PREFIX_SERVER} User ${username} (ehem. Sess ${sessionId}) ausgeloggt.`);
            res.json({ message: 'Logout erfolgreich!' });
        });
    } else { console.log(`${LOG_PREFIX_SERVER} Logout: Keine aktive Session.`); res.json({ message: 'Keine aktive Session.' }); }
});

app.get('/api/auth/me', isAuthenticated, async (req, res) => {
    console.log(`${LOG_PREFIX_SERVER} /api/auth/me User: ${req.session.username}, Session ID: ${req.session.id}`);
    try {
        const user = await usersCollection.findOne({ _id: new ObjectId(req.session.userId) }, { projection: { password: 0 } });
        if (!user) { console.error(`${LOG_PREFIX_SERVER} /api/auth/me: User ${req.session.userId} nicht in DB! Zerstöre Session.`); req.session.destroy(() => {}); return res.status(404).json({ error: 'Benutzer nicht gefunden.' }); }
        const effectiveInfinityMoney = user.isAdmin ? true : (user.infinityMoney || false);
        res.json({ userId: user._id.toString(), username: user.username, balance: user.balance, tokens: user.tokens || 0, isAdmin: user.isAdmin || false, infinityMoney: effectiveInfinityMoney, unlockedInfinityMoney: user.unlockedInfinityMoney || false, productSellCooldowns: user.productSellCooldowns || {} });
    } catch (err) { console.error(`${LOG_PREFIX_SERVER} Fehler /api/auth/me ${req.session.username}:`, err); res.status(500).json({ error: "Fehler Abruf Benutzerdaten." }); }
});

// ACCOUNT
app.patch('/api/account/settings', isAuthenticated, async (req, res) => {
    const { infinityMoney } = req.body; const userId = new ObjectId(req.session.userId); const updateData = {}; let message = "Einstellungen aktualisiert.";
    console.log(`${LOG_PREFIX_SERVER} User ${req.session.username} Acc-Settings:`, req.body);
    try {
        const user = await usersCollection.findOne({ _id: userId });
        if (!user) return res.status(404).json({ error: "Benutzer nicht gefunden." });
        if (typeof infinityMoney === 'boolean' && !user.isAdmin) {
            if (user.unlockedInfinityMoney) { updateData.infinityMoney = infinityMoney; console.log(`${LOG_PREFIX_SERVER} User ${req.session.username} infinityMoney -> ${infinityMoney}.`); }
            else { console.warn(`${LOG_PREFIX_SERVER} User ${req.session.username} infinityMoney ohne Freischaltung.`); return res.status(403).json({ error: "Infinity Money nicht freigeschaltet." }); }
        }
        if (Object.keys(updateData).length > 0) await usersCollection.updateOne({ _id: userId }, { $set: updateData });
        else message = "Keine Änderungen.";
        const updatedUser = await usersCollection.findOne({ _id: userId }, { projection: { password: 0 } });
        const effectiveInfinityMoney = updatedUser.isAdmin ? true : (updatedUser.infinityMoney || false);
        res.json({ message: message, user: { ...updatedUser, tokens: updatedUser.tokens || 0, infinityMoney: effectiveInfinityMoney, productSellCooldowns: updatedUser.productSellCooldowns || {} }});
    } catch (err) { console.error(`${LOG_PREFIX_SERVER} Fehler Acc-Settings ${req.session.username}:`, err); res.status(500).json({ error: "Fehler Speichern Einstellungen." }); }
});

// ORDERS
app.get('/api/orders', isAuthenticated, async (req, res) => {
    console.log(`${LOG_PREFIX_SERVER} /api/orders User: ${req.session.username}`);
    try {
        const userOrders = await ordersCollection.find({ userId: new ObjectId(req.session.userId) }).sort({ date: -1 }).limit(50).toArray();
        res.json({ orders: userOrders });
    } catch (err) { console.error(`${LOG_PREFIX_SERVER} Fehler Laden Bestellhistorie ${req.session.username}:`, err); res.status(500).json({ error: "Fehler Laden Bestellhistorie." }); }
});

// INVENTORY
app.get('/api/inventory', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    console.log(`${LOG_PREFIX_SERVER} /api/inventory User: ${req.session.username}`);
    try {
        const userInvItems = await inventoriesCollection.find({ userId: userId, quantityOwned: { $gt: 0 } }).toArray();
        const prodIds = userInvItems.map(item => item.productId);
        const prodDetails = await productsCollection.find({ id: { $in: prodIds }, isTokenCard: { $ne: true } }, { projection: { name: 1, image_url: 1, price: 1, id: 1, _id: 0 } }).toArray();
        const prodMap = new Map(prodDetails.map(p => [p.id, p]));
        const populatedInv = userInvItems.filter(item => prodMap.has(item.productId)).map(item => ({ ...item, productDetails: prodMap.get(item.productId) }));
        res.json({ inventory: populatedInv });
    } catch (err) { console.error(`${LOG_PREFIX_SERVER} Fehler Laden Inventar ${req.session.username}:`, err); res.status(500).json({ error: "Fehler Laden Inventar." }); }
});

// ADMIN ACTIONS
app.patch('/api/products/reset', isAdmin, async (req, res) => {
    console.log(`${LOG_PREFIX_SERVER} Admin ${req.session.username} führt Lager Reset aus.`);
    try { await resetProductStock(); res.json({ message: 'Lagerbestand regulärer Produkte auf Standard zurückgesetzt.' }); }
    catch (err) { console.error(`${LOG_PREFIX_SERVER} Admin Reset Fehler:`, err); res.status(500).json({ error: 'Fehler beim Reset des Lagerbestands.' }); }
});
app.patch('/api/admin/zero-stock', isAdmin, async (req, res) => {
    console.log(`${LOG_PREFIX_SERVER} Admin ${req.session.username} führt Lager Nullen aus.`);
    try { await zeroOutStock(); res.json({ message: 'Lagerbestand regulärer Produkte auf 0 gesetzt.' }); }
    catch (err) { console.error(`${LOG_PREFIX_SERVER} Admin Zero Stock Fehler:`, err); res.status(500).json({ error: 'Fehler beim Nullsetzen des Lagerbestands.' }); }
});
app.post('/api/admin/generate-token-code', isAdmin, async (req, res) => {
    const { tokenAmount, count = 1 } = req.body;
    console.log(`${LOG_PREFIX_SERVER} Admin ${req.session.username} generiert Token Codes: Amount ${tokenAmount}, Count ${count}`);
    if (typeof tokenAmount !== 'number' || tokenAmount <= 0 || !Number.isInteger(tokenAmount)) return res.status(400).json({ error: "Ungültiger Token-Betrag (positive Ganzzahl)." });
    if (typeof count !== 'number' || count <= 0 || count > 100 || !Number.isInteger(count)) return res.status(400).json({ error: "Ungültige Anzahl (1-100, Ganzzahl)." });
    try {
        const generatedCodes = [];
        for (let i = 0; i < count; i++) {
            const uniqueCode = await generateUniqueTokenRedeemCode();
            await tokenCodesCollection.insertOne({ code: uniqueCode, tokenAmount: tokenAmount, isRedeemed: false, createdAt: new Date(), generatedByAdminId: new ObjectId(req.session.userId) });
            generatedCodes.push({code: uniqueCode, amount: tokenAmount});
        }
        console.log(`${LOG_PREFIX_SERVER} Admin ${req.session.username} hat ${count} Codes mit je ${tokenAmount} Tokens generiert.`);
        res.status(201).json({ message: `${count} Token-Code(s) mit je ${tokenAmount} Tokens erfolgreich generiert.`, codes: generatedCodes });
    } catch (err) { console.error(`${LOG_PREFIX_SERVER} Admin Fehler Code-Generierung:`, err); res.status(500).json({ error: "Fehler bei der Code-Generierung." }); }
});

// PRODUCTS
app.get('/api/products', async (req, res) => {
    try {
        const filter = { $or: [ { id: { $type: 'number', $gte: 100000 }, isTokenCard: { $ne: true } }, { isTokenCard: true } ] };
        const prods = await productsCollection.find(filter).sort({ id: 1 }).toArray();
        const sanitized = prods.map(p => { const s = { ...p }; s.stock = (typeof p.stock === 'number' && p.stock >= 0) ? p.stock : 0; s.default_stock = (typeof p.default_stock === 'number' && p.default_stock >= 0) ? p.default_stock : (p.isTokenCard ? 99999 : 20) ; delete s._id; return s; });
        res.json({ products: sanitized });
    } catch (err) { console.error(`${LOG_PREFIX_SERVER} Fehler Abruf Produkte:`, err); res.status(500).json({ error: 'Fehler Abruf Produktliste.' }); }
});
app.post('/api/products', isAdmin, async (req, res) => {
    let { name, image_url, price, stock, isTokenCard, tokenValue } = req.body;
    console.log(`${LOG_PREFIX_SERVER} Admin ${req.session.username} fügt Produkt hinzu:`, {name, price, stock, isTokenCard, tokenValue});
    if (!name || typeof name !== 'string' || !name.trim() || !price) return res.status(400).json({ error: 'Name und Preis erforderlich.' });
    name = name.trim(); price = price.trim(); if (!price.startsWith('$')) price = `$${price}`;
    const numPrice = parseFloat(price.replace(/[^0-9.]/g, '')); if (isNaN(numPrice) || numPrice < 0) return res.status(400).json({ error: 'Ungültiger Preis.' });
    const fmtPrice = `$${numPrice.toFixed(2)}`; let initStock = 20;
    if (stock !== undefined) { const pStock = parseInt(stock, 10); if (!isNaN(pStock) && pStock >= 0) initStock = pStock; }
    const crTokenCard = !!isTokenCard; let cardTokenVal = 0;
    if (crTokenCard) { initStock = 99999; cardTokenVal = parseInt(tokenValue, 10); if (isNaN(cardTokenVal) || cardTokenVal <= 0) return res.status(400).json({ error: 'Ungültiger Token-Wert.'});}
    try {
        const newId = await generateUniqueId(productsCollection, crTokenCard ? 'TC' : '');
        const newProd = { id: newId, name: name, image_url: image_url ? image_url.trim() : `https://via.placeholder.com/150x160.png?text=${encodeURIComponent(name)}`, price: fmtPrice, stock: initStock, default_stock: initStock, isTokenCard: crTokenCard, };
        if (crTokenCard) newProd.tokenValue = cardTokenVal;
        await productsCollection.insertOne(newProd);
        console.log(`${LOG_PREFIX_SERVER} Admin ${req.session.username} Produkt "${name}" (ID: ${newId}) hinzugefügt.`);
        delete newProd._id; res.status(201).json({ message: 'Produkt hinzugefügt!', product: newProd });
    } catch (err) { console.error(`${LOG_PREFIX_SERVER} Admin Fehler Produkt Hinzufügen:`, err); res.status(500).json({ error: err.message || 'Fehler Hinzufügen.' }); }
});
app.delete('/api/products/:id', isAdmin, async (req, res) => {
    const prodId = parseInt(req.params.id, 10);
    console.log(`${LOG_PREFIX_SERVER} Admin ${req.session.username} löscht Produkt ID ${prodId}.`);
    if (isNaN(prodId)) return res.status(400).json({ error: 'Ungültige Produkt-ID.' });
    try {
        const invDelRes = await inventoriesCollection.deleteMany({ productId: prodId });
        console.log(`${LOG_PREFIX_SERVER} ${invDelRes.deletedCount} Inventareinträge für Produkt ${prodId} gelöscht.`);
        const result = await productsCollection.deleteOne({ id: prodId });
        if (result.deletedCount === 0) return res.status(404).json({ error: 'Produkt nicht gefunden.' });
        console.log(`${LOG_PREFIX_SERVER} Produkt ${prodId} von Admin ${req.session.username} gelöscht.`);
        res.json({ message: `Produkt ${prodId} und Inventareinträge gelöscht.` });
    } catch (err) { console.error(`${LOG_PREFIX_SERVER} Admin Fehler Produkt Löschen ${prodId}:`, err); res.status(500).json({ error: 'Fehler Löschen Produkt.' }); }
});
app.patch('/api/products/:id', isAdmin, async (req, res) => {
    const prodId = parseInt(req.params.id, 10);
    console.log(`${LOG_PREFIX_SERVER} Admin ${req.session.username} Stock-Update Produkt ID ${prodId}:`, req.body);
    if (isNaN(prodId)) return res.status(400).json({ error: 'Ungültige Produkt-ID.' });
    const { stock } = req.body; if (stock === undefined) return res.status(400).json({ error: 'Stock-Wert fehlt.' });
    const pStock = parseInt(stock, 10); if (isNaN(pStock) || pStock < 0) return res.status(400).json({ error: 'Ungültiger Stock-Wert.' });
    try {
        const prodToUpd = await productsCollection.findOne({ id: prodId });
        if (!prodToUpd) return res.status(404).json({ error: `Produkt ${prodId} nicht gefunden.` });
        if (prodToUpd.isTokenCard) return res.status(400).json({ error: 'Stock von Token-Karten nicht manuell änderbar.'});
        const result = await productsCollection.updateOne({ id: prodId }, { $set: { stock: pStock } });
        if (result.matchedCount === 0) return res.status(404).json({ error: `Produkt ${prodId} nicht gefunden (Update).` });
        const updatedProd = await productsCollection.findOne({ id: prodId }); delete updatedProd._id;
        console.log(`${LOG_PREFIX_SERVER} Stock Produkt ${prodId} von Admin ${req.session.username} auf ${pStock} aktualisiert.`);
        res.json({ message: `Lagerbestand Produkt ${prodId} aktualisiert.`, product: updatedProd });
    } catch (err) { console.error(`${LOG_PREFIX_SERVER} Admin Fehler Stock-Update ${prodId}:`, err); res.status(500).json({ error: 'Fehler Stock-Update.' }); }
});

// PURCHASE
app.post('/api/purchase', isAuthenticated, async (req, res) => {
    console.log(`${LOG_PREFIX_SERVER} POST /api/purchase von User ${req.session.username} | Warenkorb:`, req.body.cart);
    const cart = req.body.cart;
    if (!Array.isArray(cart) || cart.length === 0) return res.status(400).json({ error: 'Warenkorb leer/ungültig.' });
    const userId = new ObjectId(req.session.userId); let user; let totalOrderValue = 0; const errors = []; const productChecks = [];
    const productDataForOrder = []; const inventoryOps = []; const tokenCodeGenerationTasks = []; let newUnlockOccurred = false;
    try {
        user = await usersCollection.findOne({ _id: userId });
        if (!user) throw new Error("Benutzer nicht gefunden.");
        const isInfinityMoneyActiveForPurchase = user.isAdmin || user.infinityMoney;
        for (const item of cart) {
            if (!item || typeof item.id !== 'number' || typeof item.quantity !== 'number' || item.quantity <= 0) { errors.push(`Ungültiges Item.`); continue; }
            productChecks.push( productsCollection.findOne({ id: item.id }).then(async pDb => {
                if (!pDb) { errors.push(`Produkt "${item.name || item.id}" nicht gefunden.`); return null; }
                const price = parseFloat((pDb.price || "$0").replace(/[^0-9.]/g, '')) || 0; totalOrderValue += price * item.quantity;
                if (pDb.isTokenCard && pDb.tokenValue > 0) {
                    for (let i = 0; i < item.quantity; i++) { tokenCodeGenerationTasks.push({ tokenAmount: pDb.tokenValue, limazonProductId: pDb.id, generatedForUserId: userId, originalPricePaid: price }); }
                    productDataForOrder.push({ productId: pDb.id, name: pDb.name, quantity: item.quantity, price: price, image_url: pDb.image_url, isTokenCardPurchase: true });
                    return { id: item.id, quantityToDecrement: 0, priceAtPurchase: price, isTokenCard: true, name: pDb.name };
                } else {
                    const stockDb = (typeof pDb.stock === 'number' && pDb.stock >= 0) ? pDb.stock : 0;
                    if (item.quantity > stockDb) { errors.push(`"${pDb.name}": Nur ${stockDb} Stk. verfügbar.`); return null; }
                    productDataForOrder.push({ productId: pDb.id, name: pDb.name, quantity: item.quantity, price: price, image_url: pDb.image_url });
                    inventoryOps.push({ updateOne: { filter: { userId: userId, productId: pDb.id }, update: { $inc: { quantityOwned: item.quantity }, $set: { lastAcquiredPrice: price } }, upsert: true }});
                    return { id: item.id, quantityToDecrement: item.quantity, priceAtPurchase: price, name: pDb.name };
                }
            }).catch(e => { errors.push(`DB-Fehler Produktprüfung: ${item.id}`); console.error(`${LOG_PREFIX_SERVER} Product check error:`, e); return null; }) );
        }
        if (errors.length > 0 && productChecks.length === 0) throw new Error(errors.join('; '));
        const results = await Promise.all(productChecks);
        const validationErrorsFromPromises = results.filter(r => r === null).map((r, idx) => errors[idx] || `Produktprüfung Item ${idx + 1} fehlgeschlagen.`);
        const allErrors = errors.filter(e => e && !validationErrorsFromPromises.includes(e)).concat(validationErrorsFromPromises);
        if (allErrors.length > 0) { console.warn(`${LOG_PREFIX_SERVER} Validierungsfehler Kauf ${user.username}:`, allErrors); throw new Error(allErrors.join('; '));}
        const currentBalance = user.balance || 0;
        if (!isInfinityMoneyActiveForPurchase && currentBalance < totalOrderValue) throw new Error(`Guthaben zu gering. $${totalOrderValue.toFixed(2)} benötigt, du hast $${currentBalance.toFixed(2)}.`);
        const validRegProdUpds = results.filter(r => r !== null && !r.isTokenCard && r.quantityToDecrement > 0);
        if (validRegProdUpds.length > 0) {
            const bulkProdOps = validRegProdUpds.map(upd => ({ updateOne: { filter: { id: upd.id, stock: { $gte: upd.quantityToDecrement } }, update: { $inc: { stock: -upd.quantityToDecrement } } } }));
            const prodUpdRes = await productsCollection.bulkWrite(bulkProdOps);
            if (prodUpdRes.modifiedCount !== validRegProdUpds.length) { console.error(`${LOG_PREFIX_SERVER} Fehler Prod-Stock Bulk Write! Erw: ${validRegProdUpds.length}, Mod: ${prodUpdRes.modifiedCount}`); throw new Error('Konflikt Bestandsaktualisierung.'); }
        }
        if (inventoryOps.length > 0) await inventoriesCollection.bulkWrite(inventoryOps);
        const genCodesUserMsg = [];
        if (tokenCodeGenerationTasks.length > 0) {
            const codesToIns = [];
            for (const task of tokenCodeGenerationTasks) { const uniqueCode = await generateUniqueTokenRedeemCode(); codesToIns.push({ code: uniqueCode, tokenAmount: task.tokenAmount, isRedeemed: false, createdAt: new Date(), limazonProductId: task.limazonProductId, generatedForUserId: task.generatedForUserId, originalPricePaid: task.originalPricePaid }); genCodesUserMsg.push(uniqueCode); }
            if (codesToIns.length > 0) { await tokenCodesCollection.insertMany(codesToIns); console.log(`${LOG_PREFIX_SERVER} ${codesToIns.length} Token Codes generiert für User ${userId}.`); }
        }
        if (!isInfinityMoneyActiveForPurchase && totalOrderValue > 0) {
            const balUpdRes = await usersCollection.updateOne({ _id: userId, balance: { $gte: totalOrderValue } }, { $inc: { balance: -totalOrderValue } });
            if (balUpdRes.modifiedCount !== 1) { console.error(`${LOG_PREFIX_SERVER} Kritischer Fehler Guthabenabzug ${userId}! Soll ${totalOrderValue}.`); throw new Error('Kritischer Fehler Guthabenabzug.');}
        }
        if (!user.unlockedInfinityMoney && !user.isAdmin) { const regItemsForUnlock = productDataForOrder.filter(item => !item.isTokenCardPurchase); if (regItemsForUnlock.length > 0) { const allShopProds = await productsCollection.find({ id: { $gte: 100000 }, isTokenCard: { $ne: true } }, { projection: { price: 1, _id: 0 } }).sort({ price: -1 }).limit(1).toArray(); let maxPriceInShop = 0; if (allShopProds.length > 0) maxPriceInShop = parseFloat((allShopProds[0].price || "$0").replace(/[^0-9.]/g, '')) || 0; for (const boughtItemData of regItemsForUnlock) { if (boughtItemData.price >= maxPriceInShop && maxPriceInShop > 0.01) { await usersCollection.updateOne({ _id: userId }, { $set: { unlockedInfinityMoney: true } }); newUnlockOccurred = true; console.log(`${LOG_PREFIX_SERVER} Infinity Money User ${userId} freigeschaltet.`); break; }}}}
        try { const order = { userId: userId, username: user.username, date: new Date(), items: productDataForOrder, total: totalOrderValue }; await ordersCollection.insertOne(order); } catch (orderError) { console.error(`${LOG_PREFIX_SERVER} Fehler Speicher Bestellung ${userId}:`, orderError); }
        const finalUser = await usersCollection.findOne({ _id: userId }, { projection: { password: 0 } });
        const effInfMonFinal = finalUser.isAdmin ? true : (finalUser.infinityMoney || false);
        let purMessage = `Kauf erfolgreich!`;
        if (genCodesUserMsg.length > 0) purMessage += ` ${genCodesUserMsg.length} Token Guthabencode(s) generiert. Siehe "Meine Token Codes".`;
        if (newUnlockOccurred) purMessage += ' Glückwunsch, Infinity Money freigeschaltet!';
        console.log(`${LOG_PREFIX_SERVER} User ${user.username} Einkauf $${totalOrderValue.toFixed(2)}. ${genCodesUserMsg.length} Token Codes.`);
        res.json({ message: purMessage, user: { ...finalUser, tokens: finalUser.tokens || 0, infinityMoney: effInfMonFinal, productSellCooldowns: finalUser.productSellCooldowns || {} }});
    } catch (err) { console.error(`${LOG_PREFIX_SERVER} POST /api/purchase Fehler User ${req.session.username}:`, err.message); if (err.message.startsWith("Guthaben zu gering") || err.message.startsWith("Konflikt Bestandsaktualisierung")) return res.status(400).json({ error: err.message }); res.status(500).json({ error: 'Unerwarteter Kauffehler.' }); }
});

// SELL Product
app.post('/api/products/sell', isAuthenticated, async (req, res) => {
    const { productId, sellPrice, quantity } = req.body; const userId = new ObjectId(req.session.userId);
    console.log(`${LOG_PREFIX_SERVER} User ${req.session.username} verkauft Produkt ${productId}. Menge: ${quantity}, Preis: ${sellPrice}`);
    if (typeof productId !== 'number' || typeof sellPrice !== 'number' || sellPrice <= 0 || typeof quantity !== 'number' || quantity <= 0 || !Number.isInteger(quantity)) return res.status(400).json({ error: 'Ungültige Eingabe Verkauf.' });
    try {
        let user = await usersCollection.findOne({ _id: userId }); if (!user) return res.status(404).json({ error: 'Benutzer nicht gefunden.' });
        const prodToSell = await productsCollection.findOne({ id: productId, isTokenCard: { $ne: true } }); if (!prodToSell) return res.status(404).json({ error: 'Produkt nicht verkaufbar.' });
        const invItem = await inventoriesCollection.findOne({ userId: userId, productId: productId }); if (!invItem || invItem.quantityOwned < quantity) return res.status(400).json({ error: `Nicht ${quantity}x "${prodToSell.name}" im Bestand. Aktuell: ${invItem ? invItem.quantityOwned : 0}.` });
        let cooldowns = user.productSellCooldowns || {}; const lastAttCDISO = cooldowns[productId.toString()];
        if (lastAttCDISO) { const cdEndTime = new Date(lastAttCDISO).getTime(); if (Date.now() < cdEndTime) { const timeLeft = Math.ceil((cdEndTime - Date.now()) / 1000); return res.status(429).json({ success: false, error: `Cooldown aktiv: Warte ${timeLeft}s.`, cooldownActiveForProduct: productId, cooldownEndsAt: lastAttCDISO, productSellCooldowns: cooldowns }); } else { delete cooldowns[productId.toString()]; await usersCollection.updateOne({ _id: userId }, { $set: { productSellCooldowns: cooldowns } }); }}
        const origPrice = parseFloat((prodToSell.price || "$0").replace(/[^0-9.]/g, '')) || 1; let prob = 1.0;
        if (sellPrice > origPrice) prob = origPrice / sellPrice; else if (sellPrice < origPrice * 0.5) prob = 1.0;
        const globStock = prodToSell.stock || 0; const defGlobStock = prodToSell.default_stock || 20;
        if (globStock > defGlobStock * 2.5) prob *= 0.1; else if (globStock > defGlobStock * 1.8) prob *= 0.5; else if (globStock > defGlobStock * 1.2) prob *= 0.8;
        prob = Math.max(0.01, Math.min(1.0, prob)); const wasSold = Math.random() < prob; let respMsg = "";
        if (wasSold) {
            const earnings = parseFloat((sellPrice * quantity).toFixed(2));
            await inventoriesCollection.updateOne({ userId: userId, productId: productId, quantityOwned: { $gte: quantity } }, { $inc: { quantityOwned: -quantity } });
            await productsCollection.updateOne({ id: productId }, { $inc: { stock: quantity } });
            if (!user.isAdmin && !user.infinityMoney) await usersCollection.updateOne({ _id: userId }, { $inc: { balance: earnings } }); else console.log(`${LOG_PREFIX_SERVER} -> Guthaben ${user.username} nicht erhöht (Admin/Inf).`);
            respMsg = `Erfolgreich ${quantity}x "${prodToSell.name}" für $${sellPrice.toFixed(2)}/Stk. verkauft! Erlös: $${earnings.toFixed(2)}.`;
            delete cooldowns[productId.toString()]; await usersCollection.updateOne({ _id: userId }, { $set: { productSellCooldowns: cooldowns } });
            user = await usersCollection.findOne({ _id: userId }, { projection: { password: 0 } }); const effInfMonFinal = user.isAdmin ? true : (user.infinityMoney || false);
            console.log(`${LOG_PREFIX_SERVER} User ${user.username} verkaufte ${quantity}x ${prodToSell.name}. Erlös: $${earnings.toFixed(2)}.`);
            res.json({ success: true, message: respMsg, earnings: earnings, probability: prob, user: { ...user, tokens: user.tokens || 0, infinityMoney: effInfMonFinal, productSellCooldowns: user.productSellCooldowns || {} } });
        } else {
            respMsg = `Angebot für "${prodToSell.name}" nicht angenommen (Chance ca. ${(prob * 100).toFixed(0)}%).`;
            const cdEndTime = new Date(Date.now() + SELL_COOLDOWN_SECONDS * 1000); cooldowns[productId.toString()] = cdEndTime.toISOString();
            await usersCollection.updateOne({ _id: userId }, { $set: { productSellCooldowns: cooldowns } });
            console.log(`${LOG_PREFIX_SERVER} User ${req.session.username} Verkaufsversuch ${prodToSell.name} fehlgeschlagen. Cooldown bis ${cdEndTime.toISOString()}.`);
            res.status(429).json({ success: false, error: `${respMsg} Cooldown: ${SELL_COOLDOWN_SECONDS_SHOW}s.`, probability: prob, cooldownActiveForProduct: productId, cooldownEndsAt: cdEndTime.toISOString(), productSellCooldowns: cooldowns });
        }
    } catch (err) { console.error(`${LOG_PREFIX_SERVER} Fehler /api/products/sell User ${req.session.username}:`, err); res.status(500).json({ error: "Serverfehler Verkaufsversuch." }); }
});

// TOKEN Endpoints
app.post('/api/tokens/convert-dollars-to-tokens', isAuthenticated, async (req, res) => {
    const { dollarAmount } = req.body; const userId = new ObjectId(req.session.userId);
    console.log(`${LOG_PREFIX_SERVER} User ${req.session.username} wandelt $${dollarAmount} in Tokens.`);
    if (typeof dollarAmount !== 'number' || dollarAmount <= 0) return res.status(400).json({ error: "Ungültiger Betrag." });
    try {
        const user = await usersCollection.findOne({ _id: userId }); if (!user) return res.status(404).json({ error: "Benutzer nicht gefunden." });
        if (user.balance < dollarAmount) return res.status(400).json({ error: `Nicht genügend Guthaben. Benötigt: $${dollarAmount.toFixed(2)}, Vorhanden: $${user.balance.toFixed(2)}` });
        const tokensToReceive = Math.floor(dollarAmount * DOLLAR_TO_TOKEN_RATE); const balBeforeTokens = user.tokens || 0;
        await usersCollection.updateOne({ _id: userId }, { $inc: { balance: -dollarAmount, tokens: tokensToReceive } });
        await logTokenTransaction(userId, "dollar_conversion_to_token", tokensToReceive, balBeforeTokens, balBeforeTokens + tokensToReceive, `Converted $${dollarAmount.toFixed(2)} to ${tokensToReceive} tokens.`);
        const updatedUser = await usersCollection.findOne({ _id: userId }, { projection: { password: 0 } });
        res.json({ message: `$${dollarAmount.toFixed(2)} erfolgreich in ${tokensToReceive} Tokens umgewandelt.`, user: { ...updatedUser, tokens: updatedUser.tokens || 0 }});
    } catch (err) { console.error(`${LOG_PREFIX_SERVER} Fehler Dollar zu Token ${req.session.username}:`, err); res.status(500).json({ error: "Serverfehler Umwandlung." }); }
});
app.post('/api/tokens/convert-tokens-to-dollars', isAuthenticated, async (req, res) => {
    const { tokenAmount } = req.body; const userId = new ObjectId(req.session.userId);
    console.log(`${LOG_PREFIX_SERVER} User ${req.session.username} wandelt ${tokenAmount} Tokens in Dollar.`);
    if (typeof tokenAmount !== 'number' || tokenAmount <= 0 || !Number.isInteger(tokenAmount)) return res.status(400).json({ error: "Ungültige Token-Anzahl (positive Ganzzahl)." });
    try {
        const user = await usersCollection.findOne({ _id: userId }); if (!user) return res.status(404).json({ error: "Benutzer nicht gefunden." });
        if ((user.tokens || 0) < tokenAmount) return res.status(400).json({ error: `Nicht genügend Tokens. Benötigt: ${tokenAmount}, Vorhanden: ${user.tokens || 0}` });
        const dollarsToReceive = parseFloat((tokenAmount * TOKEN_TO_DOLLAR_RATE).toFixed(2)); const balBeforeTokens = user.tokens || 0;
        await usersCollection.updateOne({ _id: userId }, { $inc: { tokens: -tokenAmount, balance: dollarsToReceive } });
        await logTokenTransaction(userId, "token_conversion_to_dollar", -tokenAmount, balBeforeTokens, balBeforeTokens - tokenAmount, `Converted ${tokenAmount} tokens to $${dollarsToReceive.toFixed(2)}.`);
        const updatedUser = await usersCollection.findOne({ _id: userId }, { projection: { password: 0 } });
        res.json({ message: `${tokenAmount} Tokens erfolgreich in $${dollarsToReceive.toFixed(2)} umgewandelt.`, user: { ...updatedUser, tokens: updatedUser.tokens || 0 }});
    } catch (err) { console.error(`${LOG_PREFIX_SERVER} Fehler Token zu Dollar ${req.session.username}:`, err); res.status(500).json({ error: "Serverfehler Umwandlung." }); }
});
app.post('/api/tokens/redeem', isAuthenticated, async (req, res) => {
    const { code } = req.body; const userId = new ObjectId(req.session.userId);
    console.log(`${LOG_PREFIX_SERVER} User ${req.session.username} löst Token-Code ein: ${code ? code.substring(0,10)+"..." : "LEER"}`);
    if (!code || typeof code !== 'string') return res.status(400).json({ error: "Token-Code erforderlich." });
    try {
        const tokenCode = await tokenCodesCollection.findOne({ code: code.trim() });
        if (!tokenCode) return res.status(404).json({ error: "Token-Code ungültig." });
        if (tokenCode.isRedeemed) return res.status(400).json({ error: `Token-Code bereits am ${new Date(tokenCode.redeemedAt).toLocaleString('de-DE')} eingelöst.` });
        const user = await usersCollection.findOne({ _id: userId }); if (!user) return res.status(404).json({ error: "Benutzer nicht gefunden." });
        const balBeforeTokens = user.tokens || 0;
        await tokenCodesCollection.updateOne({ _id: tokenCode._id }, { $set: { isRedeemed: true, redeemedByUserId: userId, redeemedAt: new Date() } });
        await usersCollection.updateOne({ _id: userId }, { $inc: { tokens: tokenCode.tokenAmount } });
        await logTokenTransaction(userId, "redeem_code", tokenCode.tokenAmount, balBeforeTokens, balBeforeTokens + tokenCode.tokenAmount, `Redeemed code ${code} for ${tokenCode.tokenAmount} tokens.`, null, tokenCode._id);
        const updatedUser = await usersCollection.findOne({ _id: userId }, { projection: { password: 0 } });
        console.log(`${LOG_PREFIX_SERVER} User ${req.session.username} Code ${code} eingelöst für ${tokenCode.tokenAmount} Tokens.`);
        res.json({ message: `Code erfolgreich eingelöst! ${tokenCode.tokenAmount} Tokens gutgeschrieben.`, user: { ...updatedUser, tokens: updatedUser.tokens || 0 }});
    } catch (err) { console.error(`${LOG_PREFIX_SERVER} Fehler Token-Code Einlösen ${req.session.username}:`, err); res.status(500).json({ error: "Serverfehler Einlösen Code." }); }
});
app.get('/api/tokens/my-codes', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    console.log(`${LOG_PREFIX_SERVER} /api/tokens/my-codes User ${req.session.username}`);
    try {
        const codes = await tokenCodesCollection.find({ generatedForUserId: userId, isRedeemed: false }, { projection: { code: 1, tokenAmount: 1, createdAt: 1, limazonProductId: 1, _id: 0 } }).sort({ createdAt: -1 }).limit(100).toArray();
        const prodIds = [...new Set(codes.map(c => c.limazonProductId).filter(id => id != null))];
        let prodDetailsMap = new Map();
        if (prodIds.length > 0) { const cardProds = await productsCollection.find({ id: { $in: prodIds } }, { projection: {id: 1, name: 1, _id:0 }}).toArray(); prodDetailsMap = new Map(cardProds.map(p => [p.id, p.name])); }
        const populatedCodes = codes.map(c => ({ ...c, productName: prodDetailsMap.get(c.limazonProductId) || "Token Guthaben" }));
        console.log(`${LOG_PREFIX_SERVER} User ${req.session.username} hat ${populatedCodes.length} ungenutzte gekaufte Token Codes.`);
        res.json({ codes: populatedCodes });
    } catch (err) { console.error(`${LOG_PREFIX_SERVER} Fehler /api/tokens/my-codes User ${req.session.username}:`, err); res.status(500).json({ error: "Fehler Abruf gekaufte Token Codes." }); }
});

// GLÜCKSRAD Endpoints
app.post('/api/wheels', isAuthenticated, async (req, res) => {
    let { name, description, isPublic, segments, spinCost, creationCost } = req.body;
    const userId = new ObjectId(req.session.userId); const username = req.session.username;
    console.log(`${LOG_PREFIX_SERVER} User ${username} erstellt Glücksrad: ${name}`);
    if (!name || typeof name !== 'string' || name.length < 3 || name.length > 50) return res.status(400).json({ error: "Name (3-50 Zeichen)." });
    if (!Array.isArray(segments) || segments.length < 2 || segments.length > 50) return res.status(400).json({ error: "Segmente (Min 2, Max 50)." });
    for (const seg of segments) {
        if (!seg.text || typeof seg.text !== 'string' || seg.text.length === 0 || seg.text.length > 30) return res.status(400).json({ error: `Segment Text (1-30 Z.): "${seg.text}".` });
        if (!seg.color || !/^#[0-9A-F]{6}$/i.test(seg.color)) return res.status(400).json({ error: `Segment Farbe ungültig: "${seg.text}". Hex-Code nötig.` });
        if (seg.valueType && !["text_prize", "free_spin"].includes(seg.valueType)) return res.status(400).json({ error: `Ungültiger valueType "${seg.valueType}". Erlaubt: text_prize, free_spin.` });
        if (!seg.valueType) seg.valueType = "text_prize"; if (!seg.value) seg.value = seg.text;
    }
    if (typeof spinCost !== 'number' || spinCost < 0 || !Number.isInteger(spinCost)) return res.status(400).json({ error: "Drehkosten (Min 0, Ganzzahl)." });
    if (typeof creationCost !== 'number' || creationCost < 0 || !Number.isInteger(creationCost)) creationCost = 0;
    try {
        const user = await usersCollection.findOne({ _id: userId }); const balBeforeTokens = user.tokens || 0;
        if (creationCost > 0) {
            if (balBeforeTokens < creationCost) return res.status(400).json({ error: `Nicht genug Tokens (${creationCost}) für Erstellung. Du hast ${balBeforeTokens}.` });
            await usersCollection.updateOne({ _id: userId }, { $inc: { tokens: -creationCost } });
            await logTokenTransaction(userId, "wheel_creation_cost", -creationCost, balBeforeTokens, balBeforeTokens - creationCost, `Paid ${creationCost} tokens for creating wheel '${name}'.`);
        }
        const shareCode = await generateUniqueShareCode();
        const newWheel = { creatorId: userId, creatorUsername: username, name, description: description || "", isPublic: !!isPublic, segments, spinCost, creationCostPaid: creationCost, shareCode, totalSpins: 0, createdAt: new Date(), updatedAt: new Date() };
        const result = await wheelsCollection.insertOne(newWheel);
        console.log(`${LOG_PREFIX_SERVER} User ${username} erstellte Rad '${name}' (ID: ${result.insertedId}). Kosten: ${creationCost} Tokens.`);
        const updatedUser = await usersCollection.findOne({ _id: userId }, { projection: { password: 0 } });
        res.status(201).json({ message: "Glücksrad erstellt!", wheel: newWheel, user: { ...updatedUser, tokens: updatedUser.tokens || 0 } });
    } catch (err) { console.error(`${LOG_PREFIX_SERVER} Fehler /api/wheels (POST) User ${username}:`, err); res.status(500).json({ error: "Serverfehler Erstellung Glücksrad." }); }
});
app.get('/api/wheels/public', async (req, res) => {
    console.log(`${LOG_PREFIX_SERVER} /api/wheels/public aufgerufen.`);
    try { const publicWheels = await wheelsCollection.find({ isPublic: true }).sort({ createdAt: -1 }).limit(50).project({ segments: 0 }).toArray(); res.json({ wheels: publicWheels }); }
    catch (err) { console.error(`${LOG_PREFIX_SERVER} Fehler /api/wheels/public:`, err); res.status(500).json({ error: "Fehler Laden öffentl. Glücksräder." }); }
});
app.get('/api/wheels/my', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    console.log(`${LOG_PREFIX_SERVER} /api/wheels/my User: ${req.session.username}`);
    try { const myWheels = await wheelsCollection.find({ creatorId: userId }).sort({ createdAt: -1 }).limit(50).project({ segments: 0 }).toArray(); res.json({ wheels: myWheels }); }
    catch (err) { console.error(`${LOG_PREFIX_SERVER} Fehler /api/wheels/my User ${req.session.username}:`, err); res.status(500).json({ error: "Fehler Laden meiner Glücksräder." }); }
});
app.get('/api/wheels/:id', async (req, res) => {
    const wheelIdStr = req.params.id; console.log(`${LOG_PREFIX_SERVER} /api/wheels/:id aufgerufen für ID: ${wheelIdStr}`);
    if (!ObjectId.isValid(wheelIdStr)) return res.status(400).json({ error: "Ungültige Glücksrad-ID." });
    const wheelId = new ObjectId(wheelIdStr);
    try {
        const wheel = await wheelsCollection.findOne({ _id: wheelId });
        if (!wheel) return res.status(404).json({ error: "Glücksrad nicht gefunden." });
        if (!wheel.isPublic && (!req.session || !req.session.userId || req.session.userId !== wheel.creatorId.toString())) {
            console.warn(`${LOG_PREFIX_SERVER} Zugriff auf privates Rad ${wheelIdStr} verweigert. Anfrager: ${req.session ? req.session.username : "Gast"}`);
            return res.status(403).json({ error: "Zugriff auf privates Glücksrad verweigert oder nutze den Share-Code." });
        }
        res.json({ wheel });
    } catch (err) { console.error(`${LOG_PREFIX_SERVER} Fehler /api/wheels/${wheelIdStr}:`, err); res.status(500).json({ error: "Fehler Laden Glücksrad." }); }
});
app.put('/api/wheels/:id', isAuthenticated, async (req, res) => {
    const wheelIdStr = req.params.id;
    const userId = new ObjectId(req.session.userId);
    const username = req.session.username;
    console.log(`${LOG_PREFIX_SERVER} User ${username} versucht, Rad ID: ${wheelIdStr} zu aktualisieren.`);

    if (!ObjectId.isValid(wheelIdStr)) {
        return res.status(400).json({ error: "Ungültige Glücksrad-ID." });
    }
    const wheelId = new ObjectId(wheelIdStr);

    // Die Daten, die vom Frontend zum Aktualisieren gesendet werden
    const { name, description, isPublic, segments, spinCost } = req.body;

    // ---- Start: Validierung der Eingabedaten (ähnlich wie bei POST) ----
    if (name !== undefined && (typeof name !== 'string' || name.length < 3 || name.length > 50)) {
        return res.status(400).json({ error: "Name (3-50 Zeichen)." });
    }
    if (segments !== undefined && (!Array.isArray(segments) || segments.length < 2 || segments.length > 50)) {
        return res.status(400).json({ error: "Segmente (Min 2, Max 50)." });
    }
    if (segments !== undefined) {
        for (const seg of segments) {
            if (!seg.text || typeof seg.text !== 'string' || seg.text.length === 0 || seg.text.length > 30) {
                return res.status(400).json({ error: `Segment Text (1-30 Z.): "${seg.text}".` });
            }
            if (!seg.color || !/^#[0-9A-F]{6}$/i.test(seg.color)) {
                return res.status(400).json({ error: `Segment Farbe ungültig: "${seg.text}". Hex-Code nötig.` });
            }
            // valueType und value sollten auch hier validiert oder gesetzt werden, falls Teil des Updates
            if (seg.valueType && !["text_prize", "free_spin"].includes(seg.valueType)) return res.status(400).json({ error: `Ungültiger valueType "${seg.valueType}". Erlaubt: text_prize, free_spin.` });
            if (!seg.valueType) seg.valueType = "text_prize"; if (!seg.value) seg.value = seg.text;
        }
    }
    if (spinCost !== undefined && (typeof spinCost !== 'number' || spinCost < 0 || !Number.isInteger(spinCost))) {
        return res.status(400).json({ error: "Drehkosten (Min 0, Ganzzahl)." });
    }
    // creationCost wird beim Update normalerweise nicht geändert.
    // ---- Ende: Validierung ----

    try {
        const wheelToUpdate = await wheelsCollection.findOne({ _id: wheelId });

        if (!wheelToUpdate) {
            return res.status(404).json({ error: "Glücksrad zum Aktualisieren nicht gefunden." });
        }

        // Berechtigungsprüfung: Nur der Ersteller oder ein Admin darf bearbeiten
        const currentUser = await usersCollection.findOne({ _id: userId });
        if (wheelToUpdate.creatorId.toString() !== userId.toString() && !(currentUser && currentUser.isAdmin)) {
            console.warn(`${LOG_PREFIX_SERVER} User ${username} nicht berechtigt Rad ${wheelIdStr} zu aktualisieren.`);
            return res.status(403).json({ error: "Nicht berechtigt, dieses Glücksrad zu bearbeiten." });
        }

        // Erstelle das Update-Objekt nur mit den Feldern, die auch gesendet wurden
        const updateFields = {};
        if (name !== undefined) updateFields.name = name;
        if (description !== undefined) updateFields.description = description;
        if (isPublic !== undefined) updateFields.isPublic = !!isPublic;
        if (segments !== undefined) updateFields.segments = segments;
        if (spinCost !== undefined) updateFields.spinCost = spinCost;
        updateFields.updatedAt = new Date(); // Immer das Update-Datum setzen

        if (Object.keys(updateFields).length === 1 && updateFields.updatedAt) { // Nur updatedAt würde bedeuten, es gibt nichts zu ändern
            return res.status(400).json({ error: "Keine Daten zum Aktualisieren gesendet." });
        }

        const result = await wheelsCollection.updateOne(
            { _id: wheelId },
            { $set: updateFields }
        );

        if (result.matchedCount === 0) {
            // Sollte durch die wheelToUpdate-Prüfung oben eigentlich nicht passieren
            return res.status(404).json({ error: "Glücksrad nicht gefunden während Update-Versuch." });
        }
        if (result.modifiedCount === 0 && result.matchedCount === 1) {
             console.log(`${LOG_PREFIX_SERVER} Rad ID ${wheelIdStr} wurde nicht geändert (gleiche Daten).`);
            // Kein Fehler, aber es wurden keine Daten geändert (vielleicht waren sie identisch)
            // Sende trotzdem das (unveränderte) Rad zurück oder eine entsprechende Nachricht
        }

        const updatedWheel = await wheelsCollection.findOne({ _id: wheelId });
        console.log(`${LOG_PREFIX_SERVER} User ${username} aktualisierte Rad '${updatedWheel.name}' (ID: ${wheelIdStr}).`);
        
        // Sende aktuelle User-Daten (insbesondere Tokens) zurück
        const updatedUser = await usersCollection.findOne({ _id: userId }, { projection: { password: 0 } });

        res.json({
            message: "Glücksrad erfolgreich aktualisiert!",
            wheel: updatedWheel,
            user: { ...updatedUser, tokens: updatedUser.tokens || 0 } // Wichtig für das UI Token Update
        });

    } catch (err) {
        console.error(`${LOG_PREFIX_SERVER} Fehler /api/wheels/${wheelIdStr} (PUT) User ${username}:`, err);
        res.status(500).json({ error: "Serverfehler beim Aktualisieren des Glücksrads." });
    }
});
app.get('/api/wheels/shared/:shareCode', async (req, res) => {
    const { shareCode } = req.params; console.log(`${LOG_PREFIX_SERVER} /api/wheels/shared/:shareCode aufgerufen für Code: ${shareCode}`);
    if (!shareCode || typeof shareCode !== 'string') return res.status(400).json({ error: "Ungültiger Share-Code." });
    try {
        const wheel = await wheelsCollection.findOne({ shareCode: shareCode });
        if (!wheel) return res.status(404).json({ error: "Kein Glücksrad mit diesem Code gefunden." });
        res.json({ wheel });
    } catch (err) { console.error(`${LOG_PREFIX_SERVER} Fehler /api/wheels/shared/${shareCode}:`, err); res.status(500).json({ error: "Fehler Laden geteiltes Glücksrad." }); }
});
app.post('/api/wheels/:id/spin', isAuthenticated, async (req, res) => {
    const wheelIdStr = req.params.id; const userId = new ObjectId(req.session.userId);
    console.log(`${LOG_PREFIX_SERVER} User ${req.session.username} dreht Rad ID: ${wheelIdStr}`);
    if (!ObjectId.isValid(wheelIdStr)) return res.status(400).json({ error: "Ungültige Glücksrad-ID." });
    const wheelId = new ObjectId(wheelIdStr);
    try {
        const wheel = await wheelsCollection.findOne({ _id: wheelId }); if (!wheel) return res.status(404).json({ error: "Glücksrad nicht gefunden." });
        const user = await usersCollection.findOne({ _id: userId }); if (!user) return res.status(404).json({ error: "Benutzer nicht gefunden." });
        const balBeforeTokens = user.tokens || 0;
        if (balBeforeTokens < wheel.spinCost) return res.status(400).json({ error: `Nicht genug Tokens. Benötigt: ${wheel.spinCost}, Vorhanden: ${balBeforeTokens}.` });
        if (wheel.spinCost > 0) {
            await usersCollection.updateOne({ _id: userId }, { $inc: { tokens: -wheel.spinCost } });
            await logTokenTransaction(userId, "spin_cost", -wheel.spinCost, balBeforeTokens, balBeforeTokens - wheel.spinCost, `Spin cost for wheel '${wheel.name}' (ID: ${wheel._id}).`, wheel._id);
        }
        const winningSegmentIndex = Math.floor(Math.random() * wheel.segments.length); const winningSegment = wheel.segments[winningSegmentIndex];
        let prizeMessage = `Du hast gewonnen: ${winningSegment.text}!`;
        if (winningSegment.valueType === "free_spin") {
            prizeMessage = `Du hast gewonnen: ${winningSegment.text}! Dein Einsatz von ${wheel.spinCost} Token(s) wird dir gutgeschrieben.`;
            if (wheel.spinCost > 0) {
                 await usersCollection.updateOne({ _id: userId }, { $inc: { tokens: wheel.spinCost } });
                 await logTokenTransaction(userId, "free_spin_refund", wheel.spinCost, balBeforeTokens - wheel.spinCost, balBeforeTokens, `Refund for free spin on wheel '${wheel.name}'.`, wheel._id);
            }
        }
        await wheelsCollection.updateOne({ _id: wheel._id }, { $inc: { totalSpins: 1 }, $set: {updatedAt: new Date()} });
        console.log(`${LOG_PREFIX_SERVER} User ${user.username} Rad '${wheel.name}' gedreht. Ergebnis: ${winningSegment.text}`);
        const updatedUser = await usersCollection.findOne({ _id: userId }, { projection: { password: 0 } });
        res.json({ message: prizeMessage, winningSegment: winningSegment, winningSegmentIndex: winningSegmentIndex, user: { ...updatedUser, tokens: updatedUser.tokens || 0 }});
    } catch (err) { console.error(`${LOG_PREFIX_SERVER} Fehler /api/wheels/${wheelIdStr}/spin User ${req.session.username}:`, err); res.status(500).json({ error: "Serverfehler Drehen Glücksrad." }); }
});
app.delete('/api/wheels/:id', isAuthenticated, async (req, res) => {
    const wheelIdStr = req.params.id; const userId = new ObjectId(req.session.userId);
    console.log(`${LOG_PREFIX_SERVER} User ${req.session.username} löscht Rad ID: ${wheelIdStr}`);
    if (!ObjectId.isValid(wheelIdStr)) return res.status(400).json({ error: "Ungültige Glücksrad-ID." });
    const wheelId = new ObjectId(wheelIdStr);
    try {
        const wheel = await wheelsCollection.findOne({ _id: wheelId }); if (!wheel) return res.status(404).json({ error: "Glücksrad nicht gefunden." });
        const user = await usersCollection.findOne({ _id: userId });
        if (wheel.creatorId.toString() !== userId.toString() && !(user && user.isAdmin)) { console.warn(`${LOG_PREFIX_SERVER} User ${req.session.username} nicht berechtigt Rad ${wheelIdStr} zu löschen.`); return res.status(403).json({ error: "Nicht berechtigt, dieses Glücksrad zu löschen." });}
        await wheelsCollection.deleteOne({ _id: wheelId });
        console.log(`${LOG_PREFIX_SERVER} User ${req.session.username} (Admin: ${user.isAdmin}) löschte Rad '${wheel.name}' (ID: ${wheelIdStr}).`);
        res.json({ message: "Glücksrad erfolgreich gelöscht." });
    } catch (err) { console.error(`${LOG_PREFIX_SERVER} Fehler /api/wheels/${wheelIdStr} (DELETE) User ${req.session.username}:`, err); res.status(500).json({ error: "Serverfehler Löschen Glücksrad." }); }
});

// In server.js

// Stelle sicher, dass deine Collection-Namen als Konstanten verfügbar sind,
// wenn du sie unten in collectionsTextSearchFields verwendest.
// Beispiele:
// const productsCollectionName = 'products';
// const usersCollectionName = 'users';
// ...

// === BEGINN ADMIN DATA MANIPULATION ENDPUNKT LOGIK ===
async function adminDataManipulationEndpoint(req, res) {
    const {
        adminUsername,
        adminPassword,
        oneDevUrl,
        oneDevAdminUsername,
        oneDevAdminPassword,
        collectionName,
        operation,
        query,      // Die vom Frontend gesendete Query (kann null/leer sein)
        document,
        documents,
        update,
        options,
        pipeline,
        searchTerm  // Neuer optionaler Parameter für einfache Textsuche
    } = req.body;

    const logPrefixAdminData = `${LOG_PREFIX_SERVER} [AdminDataManipulation] Session User: ${req.session.username} ->`;
    const currentUserFromSession = req.session.username;

    // --- STUFE 2: Lokale Admin-Credentials (aus dem Body) prüfen ---
    console.log(`${logPrefixAdminData} Starte Stufe 2: Lokale Admin Re-Authentifizierung für '${adminUsername || "FEHLEND"}' durch '${currentUserFromSession}'.`);
    if (!adminUsername || !adminPassword) {
        console.warn(`${logPrefixAdminData} Stufe 2 ABGELEHNT: Lokale Admin-Anmeldedaten (adminUsername, adminPassword) im Body fehlen.`);
        return res.status(401).json({ error: 'Stufe 2: Lokale Admin-Anmeldedaten (adminUsername, adminPassword) im Request-Body erforderlich.', stage: 2 });
    }
    try {
        if (!usersCollection) throw new Error("usersCollection nicht initialisiert in adminDataManipulationEndpoint");
        const localAdminForReAuth = await usersCollection.findOne({ username: adminUsername.toLowerCase() });
        if (!localAdminForReAuth) {
            console.warn(`${logPrefixAdminData} Stufe 2 ABGELEHNT: Lokaler Admin-User '${adminUsername}' für Re-Authentifizierung nicht gefunden.`);
            return res.status(401).json({ error: 'Stufe 2: Ungültige lokale Admin-Anmeldedaten.', stage: 2 });
        }
        const passwordMatch = await bcrypt.compare(adminPassword, localAdminForReAuth.password);
        if (!passwordMatch) {
            console.warn(`${logPrefixAdminData} Stufe 2 ABGELEHNT: Falsches Passwort für lokalen Admin-User '${adminUsername}'.`);
            return res.status(401).json({ error: 'Stufe 2: Ungültige lokale Admin-Anmeldedaten.', stage: 2 });
        }
        if (localAdminForReAuth._id.toString() !== req.session.userId || !localAdminForReAuth.isAdmin) {
            console.warn(`${logPrefixAdminData} Stufe 2 ABGELEHNT: Diskrepanz zwischen Session-User und re-authentifiziertem lokalen Admin oder fehlende Admin-Rechte.`);
            return res.status(403).json({ error: 'Stufe 2: Re-Authentifizierung fehlgeschlagen oder nicht autorisiert.', stage: 2 });
        }
        console.log(`${logPrefixAdminData} Stufe 2 ERFOLGREICH: Lokale Admin Re-Authentifizierung für '${adminUsername}' durch '${currentUserFromSession}'.`);
    } catch (reAuthError) {
        console.error(`${logPrefixAdminData} Stufe 2 FEHLER bei lokaler Admin Re-Authentifizierung:`, reAuthError);
        return res.status(500).json({ error: 'Serverfehler bei der lokalen Admin-Re-Authentifizierung.', stage: 2, details: reAuthError.message });
    }

    // --- STUFE 3: OneDev Admin-Credentials prüfen ---
    console.log(`${logPrefixAdminData} Starte Stufe 3: OneDev Admin Authentifizierung für OneDev-User '${oneDevAdminUsername || "FEHLEND"}' (URL: ${oneDevUrl || "FEHLEND"}).`);
    if (!oneDevUrl || !oneDevAdminUsername || !oneDevAdminPassword) {
        console.warn(`${logPrefixAdminData} Stufe 3 ABGELEHNT: OneDev-Anmeldedaten (oneDevUrl, oneDevAdminUsername, oneDevAdminPassword) im Body fehlen.`);
        return res.status(401).json({ error: 'Stufe 3: OneDev-Anmeldedaten (oneDevUrl, oneDevAdminUsername, oneDevAdminPassword) im Request-Body erforderlich.', stage: 3 });
    }
    if (!oneDevUrl.startsWith('http://') && !oneDevUrl.startsWith('https://')) {
        console.warn(`${logPrefixAdminData} Stufe 3 ABGELEHNT: Ungültige OneDev URL: ${oneDevUrl}`);
        return res.status(400).json({ error: 'Stufe 3: Ungültiges Format für oneDevUrl (muss mit http:// oder https:// beginnen).', stage: 3 });
    }
    try {
        let oneDevApiUserMeEndpoint;
        const oneDevBaseUrl = oneDevUrl.replace(/\/$/, '');
        if (oneDevBaseUrl === "http://cause-radio.gl.at.ply.gg:43894" || (oneDevBaseUrl.startsWith('http://') && oneDevBaseUrl.includes('ply.gg')) ) { // Genauer für ply.gg oder allgemeiner für HTTP
            oneDevApiUserMeEndpoint = `${oneDevBaseUrl}/~api/users/me`;
            console.log(`${logPrefixAdminData} Stufe 3: Nutze OneDev Pfad (vermutlich für HTTP-Tunnel/ply.gg): ${oneDevApiUserMeEndpoint}`);
        } else {
            oneDevApiUserMeEndpoint = `${oneDevBaseUrl}/api/users/me`;
            console.log(`${logPrefixAdminData} Stufe 3: Nutze Standard OneDev Pfad: ${oneDevApiUserMeEndpoint}`);
        }
        const response = await axios.get(oneDevApiUserMeEndpoint, {
            auth: { username: oneDevAdminUsername, password: oneDevAdminPassword },
            timeout: 15000
        });
        const oneDevUser = response.data;
        if (oneDevAdminUsername.toLowerCase() !== "admin" || !oneDevUser || typeof oneDevUser.name !== 'string' || oneDevUser.name !== oneDevAdminUsername) {
            console.warn(`${logPrefixAdminData} Stufe 3 ABGELEHNT: OneDev-User ist nicht der erwartete 'admin' oder API-Antwort passt nicht. Angegeben: '${oneDevAdminUsername}', API-Antwort-Name: '${oneDevUser && oneDevUser.name ? oneDevUser.name : "N/A"}'. OneDev User Objekt:`, oneDevUser);
            return res.status(403).json({ error: `Stufe 3: Authentifizierung fehlgeschlagen oder der angegebene OneDev-Benutzer ('${oneDevAdminUsername}') ist nicht der erwartete Administrator ('admin'). Überprüfen Sie die OneDev API-Antwort.`, stage: 3, debug_onedev_response_name: oneDevUser ? oneDevUser.name : null });
        }
        console.log(`${logPrefixAdminData} Stufe 3 ERFOLGREICH: OneDev-User '${oneDevAdminUsername}' erfolgreich als 'admin' auf ${oneDevUrl} authentifiziert.`);
    } catch (oneDevError) {
        if (oneDevError.response) {
            console.error(`${logPrefixAdminData} Stufe 3 FEHLER: OneDev API Fehler (Status ${oneDevError.response.status}):`, typeof oneDevError.response.data === 'string' ? oneDevError.response.data.substring(0, 500) + "..." : oneDevError.response.data);
            const details = (typeof oneDevError.response.data === 'object' ? oneDevError.response.data : `HTML-Antwort oder nicht-JSON-Fehler (Status: ${oneDevError.response.status})`);
            if (oneDevError.response.status === 401) return res.status(401).json({ error: 'Stufe 3: Ungültige OneDev-Anmeldedaten.', stage: 3, details });
            return res.status(oneDevError.response.status || 500).json({ error: 'Stufe 3: Fehler bei der Kommunikation mit dem OneDev-Server.', stage: 3, details });
        } else if (oneDevError.request) {
            console.error(`${logPrefixAdminData} Stufe 3 FEHLER: Keine Antwort vom OneDev-Server:`, oneDevError.message);
            return res.status(503).json({ error: 'Stufe 3: OneDev-Server nicht erreichbar.', stage: 3, details: oneDevError.message });
        } else {
            console.error(`${logPrefixAdminData} Stufe 3 FEHLER: Fehler beim Vorbereiten der OneDev-Anfrage:`, oneDevError.message);
            return res.status(500).json({ error: 'Stufe 3: Interner Fehler beim Versuch, OneDev zu kontaktieren.', stage: 3, details: oneDevError.message });
        }
    }

    // --- DATENBANKOPERATION ---
    if (!collectionName || !operation) {
        return res.status(400).json({ error: '`collectionName` und `operation` sind erforderlich.' });
    }

    // Verwende deine definierten Collection-Namen Konstanten hier
    const allowedCollections = [
        productsCollectionName, usersCollectionName, ordersCollectionName,
        inventoriesCollectionName, wheelsCollectionName, tokenCodesCollectionName,
        tokenTransactionsCollectionName, 'sessions'
    ];
    if (!allowedCollections.includes(collectionName)) {
        console.warn(`${logPrefixAdminData} Zugriff auf nicht erlaubte Collection: ${collectionName}`);
        return res.status(400).json({ error: `Zugriff auf Collection '${collectionName}' nicht erlaubt.` });
    }

    if (!db) {
        console.error(`${logPrefixAdminData} Datenbank (db) nicht initialisiert!`);
        return res.status(500).json({ error: "Server-Konfigurationsfehler: Datenbank nicht bereit." });
    }
    const currentDbCollection = db.collection(collectionName);
    let dbResult;

    const sanitizeQueryIds = (q) => {
        if (!q || typeof q !== 'object') return {}; // Immer ein Objekt zurückgeben, auch wenn q null, undefined oder kein Objekt ist
        const sanitized = { ...q }; // Kopie erstellen
        // _id Konvertierung
        if (sanitized._id && typeof sanitized._id === 'string' && ObjectId.isValid(sanitized._id)) {
            sanitized._id = new ObjectId(sanitized._id);
        }
        // userId Konvertierung für bestimmte Collections
        if (sanitized.userId && typeof sanitized.userId === 'string' && ObjectId.isValid(sanitized.userId)) {
            if ([ordersCollectionName, inventoriesCollectionName, wheelsCollectionName, tokenCodesCollectionName, tokenTransactionsCollectionName].includes(collectionName)) {
                 sanitized.userId = new ObjectId(sanitized.userId);
            }
        }
        // Numerische IDs (wie in 'products') bleiben Zahlen und werden nicht konvertiert,
        // außer sie kommen als String an und müssen geparst werden (hier nicht implementiert, Frontend sollte Zahlen senden).
        return sanitized;
    };

    let finalQuery = sanitizeQueryIds(query); // Beginne mit der vom User gesendeten Query
    const sanitizedOptions = options && typeof options === 'object' ? { ...options } : {}; // Optionen sicher kopieren

    // Serverseitige `searchTerm` Logik für 'find' Operation
    if (operation === 'find' && searchTerm && typeof searchTerm === 'string' && searchTerm.trim() !== '') {
        const searchTermCleaned = searchTerm.trim();
        // Definiere, welche Felder pro Collection durchsucht werden sollen
        const collectionsTextSearchFields = {
            [productsCollectionName]: ['name', 'description'], // id ist numerisch, RegEx darauf ist schwierig
            [usersCollectionName]: ['username', 'fullName', 'email'], // Email auch durchsuchen
            [ordersCollectionName]: ['username', 'items.name'], // In verschachtelten Feldern suchen
            [wheelsCollectionName]: ['name', 'description', 'creatorUsername'],
            // Füge hier weitere Collections und deren relevante Text-Suchfelder hinzu
        };
        const fieldsToSearch = collectionsTextSearchFields[collectionName];

        if (fieldsToSearch && fieldsToSearch.length > 0) {
            // Wenn die `finalQuery` vom User leer ist oder nur Optionen enthält,
            // dann wende den searchTerm an. Sonst hat die User-Query Vorrang.
            if (Object.keys(finalQuery).length === 0) {
                const orConditions = fieldsToSearch.map(field => {
                    // Für numerische ID-Felder, die als Text gesucht werden (z.B. Produkt-ID)
                    if (field === 'id' && collectionName === productsCollectionName && !isNaN(parseInt(searchTermCleaned))) {
                        return { [field]: parseInt(searchTermCleaned) };
                    }
                    // Für normale Textfelder
                    return { [field]: { $regex: searchTermCleaned, $options: 'i' } }; // Case-insensitive RegEx
                });
                finalQuery = { $or: orConditions };
                console.log(`${logPrefixAdminData} Nutze serverseitige Textsuche für: "${searchTermCleaned}" auf Feldern ${fieldsToSearch.join(', ')}.`);
            } else {
                console.log(`${logPrefixAdminData} Spezifische Query vom User vorhanden, serverseitiger searchTerm wird ignoriert.`);
            }
        } else {
            console.log(`${logPrefixAdminData} Keine Standard-Suchfelder für Collection '${collectionName}' für searchTerm definiert. Suchbegriff ignoriert.`);
        }
    }


    if (operation === 'find' && !sanitizedOptions.limit) { // Default-Limit für find, falls nicht gesetzt
        sanitizedOptions.limit = 100;
    }
    if (collectionName === usersCollectionName && (operation === 'find' || operation === 'findOne')) { // Passwort-Schutz
        if (!sanitizedOptions.projection) sanitizedOptions.projection = { password: 0 };
        else if (sanitizedOptions.projection.password === undefined) sanitizedOptions.projection.password = 0;
    }

    console.log(`${logPrefixAdminData} Führe Datenbankoperation aus: ${operation} auf Collection: ${collectionName}. Query: ${JSON.stringify(finalQuery)} Options: ${JSON.stringify(sanitizedOptions)}`);
    try {
        switch (operation) {
            case 'findOne':
                if (Object.keys(finalQuery).length === 0) return res.status(400).json({ error: '`query` (nicht leer) ist für `findOne` erforderlich.' });
                dbResult = await currentDbCollection.findOne(finalQuery, sanitizedOptions);
                break;
            case 'find':
                dbResult = await currentDbCollection.find(finalQuery, sanitizedOptions).toArray();
                break;
            case 'insertOne':
                if (!document || typeof document !== 'object' || Object.keys(document).length === 0) return res.status(400).json({ error: '`document` (nicht leeres Objekt) ist für `insertOne` erforderlich.' });
                dbResult = await currentDbCollection.insertOne(document, sanitizedOptions);
                break;
            case 'insertMany':
                if (!documents || !Array.isArray(documents) || documents.length === 0) return res.status(400).json({ error: '`documents` (nicht leeres Array) ist für `insertMany` erforderlich.' });
                dbResult = await currentDbCollection.insertMany(documents, sanitizedOptions);
                break;
            case 'updateOne':
            case 'updateMany':
                if (Object.keys(finalQuery).length === 0) return res.status(400).json({ error: `\`query\` (nicht leer) ist für \`${operation}\` erforderlich.` });
                if (!update || typeof update !== 'object' || Object.keys(update).length === 0) return res.status(400).json({ error: `\`update\` (nicht leeres Objekt) ist für \`${operation}\` erforderlich.` });
                dbResult = await currentDbCollection[operation](finalQuery, update, sanitizedOptions);
                break;
            case 'deleteOne':
            case 'deleteMany':
                if (Object.keys(finalQuery).length === 0) return res.status(400).json({ error: `\`query\` (nicht leer) ist für \`${operation}\` erforderlich.` });
                dbResult = await currentDbCollection[operation](finalQuery, sanitizedOptions);
                break;
            case 'countDocuments':
                dbResult = await currentDbCollection.countDocuments(finalQuery, sanitizedOptions);
                break;
            case 'aggregate':
                if (!pipeline || !Array.isArray(pipeline) || pipeline.length === 0) return res.status(400).json({ error: '`pipeline` (nicht leeres Array) ist für `aggregate` erforderlich.' });
                dbResult = await currentDbCollection.aggregate(pipeline, sanitizedOptions).toArray();
                break;
            default:
                console.warn(`${logPrefixAdminData} Unbekannte Datenbankoperation: ${operation}`);
                return res.status(400).json({ error: `Unbekannte Datenbankoperation: ${operation}` });
        }
        console.log(`${logPrefixAdminData} Datenbankoperation ${operation} erfolgreich ausgeführt.`);
        res.json({ success: true, operation, collectionName, result: dbResult });

    } catch (dbError) {
        console.error(`${logPrefixAdminData} Fehler bei Datenbankoperation '${operation}' auf '${collectionName}':`, dbError);
        // Unterscheide zwischen "nicht gefunden" und echten Fehlern, falls gewünscht (für bessere UX)
        if (dbError.message.toLowerCase().includes("not found") && (operation === 'findOne' || operation === 'updateOne' || operation === 'deleteOne')) {
             // Hier könnte man auch das dbResult von update/delete prüfen (matchedCount, deletedCount)
            return res.status(404).json({ error: `Dokument nicht gefunden für Operation '${operation}'.`, details: dbError.message });
        }
        res.status(500).json({ error: `Fehler bei der Datenbankoperation '${operation}' auf Collection '${collectionName}'.`, details: dbError.message });
    }
}
app.post('/api/admin/data-manipulation', isAuthenticated, isAdmin, adminDataManipulationEndpoint);
// Fallback für unbekannte Routen
app.use((req, res) => {
    console.warn(`${LOG_PREFIX_SERVER} Unbekannter Endpoint aufgerufen: ${req.method} ${req.originalUrl} von IP ${req.ip}`);
    res.status(404).send('Endpoint nicht gefunden');
});