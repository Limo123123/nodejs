// server.js - Full Backend for Limo Open Source Project and all of the components of it
const path = require('path');
const fs = require('fs');
const dotenv = require('dotenv');
const os = require('os');
const helmet = require('helmet');
const multer = require('multer');
const sharp = require('sharp');
sharp.concurrency(1);
const { createProxyMiddleware } = require('http-proxy-middleware');
const { createClient } = require('redis');

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
const { ok } = require('assert');
const LOG_PREFIX_CHAT = "[WhatsLim API]";
const CHAT_COLLECTION_NAME = 'limChats';
const MESSAGE_COLLECTION_NAME = 'limMessages';
const USER_CHAT_SETTINGS_COLLECTION_NAME = 'limUserChatSettings';
const cluster = require('cluster');
const numCPUs = require('os').cpus().length;

const SELL_COOLDOWN_SECONDS = 59;
const SELL_COOLDOWN_SECONDS_SHOW = 60;
const LOG_PREFIX_SERVER = "[Limazon BACKEND]";

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
const frontendDevUrlHttps = 'https://wl.limazon.v6.rocks';
const PRICE_UPDATE_INTERVAL_MS = 5 * 60 * 1000; // 5 Minuten
const PRICE_VOLATILITY_FACTOR = 0.005; // Wie stark Preise reagieren
const MINIMUM_PRODUCT_PRICE = 1.00; // Minimaler Preis für ein Produkt
const compression = require('compression');
const CACHE_DIR = path.resolve(__dirname, 'cache');
const PRODUCTS_CACHE_FILE = path.resolve(CACHE_DIR, 'products_cache.json');
let globalProductCache = [];
const limterestCollectionName = 'limterestPins';

// --- Glücksrad & Token Konstanten ---
const DEFAULT_STARTING_TOKENS = 10;
const DEFAULT_WHEEL_CREATION_COST_TOKENS = 5;
const DOLLAR_TO_TOKEN_RATE = 0.004; // $1 gibt 0.004 Tokens (1 Token ~ $250, ähnlich teuerster Karte)
const TOKEN_TO_DOLLAR_RATE = 200;    // 1 Token gibt $200 zurück

if (!sessionSecret) { console.error(`${LOG_PREFIX_SERVER} !!! FEHLER: Kein SESSION_SECRET in Umgebungsvariablen! Server stoppt.`); process.exit(1); }
if (!mongoUri) { console.error(`${LOG_PREFIX_SERVER} !!! FEHLER: Keine MongoDB URI (MONGO_URI oder User/PW/Cluster) in Umgebungsvariablen! Server stoppt.`); process.exit(1); }

// --- Middleware ---
const allowedOrigins = [
    frontendDevUrlHttp,
    frontendDevUrlHttps,
    'https://raspberrypi.tail75d81e.ts.net:8443',
    'https://api.limazon.v6.rocks',
    'https://limohub.app',
];
if (frontendProdUrl) { allowedOrigins.push(frontendProdUrl); }
console.log(`${LOG_PREFIX_SERVER} Erlaubte CORS Origins:`, allowedOrigins);

app.use(cors({
    origin: function (origin, callback) {
        // origin === 'null' fängt die OAuth-Redirects ab
        const isAllowed = !origin || 
            origin === 'null' || 
            allowedOrigins.includes(origin) ||
            (origin && origin.endsWith('.scf.usercontent.goog'));

        if (isAllowed) {
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

app.use(compression());
app.use(helmet({
    contentSecurityPolicy: false, // Falls du Probleme mit Bildern/Scripts von extern hast, deaktiviere CSP erstmal
    crossOriginResourcePolicy: { policy: "cross-origin" } // Wichtig für deine CORS Konfiguration
}));
app.use('/api/', globalApiRateLimit); // Schützt alle API-Routen

// --- Datenbank Variablen ---
let db;
let productsCollection, usersCollection, ordersCollection, inventoriesCollection;
let wheelsCollection, tokenCodesCollection, tokenTransactionsCollection;
let limChatsCollection, limMessagesCollection, limUserChatSettingsCollection;
let ideasCollection;
let auctionsCollection;
let portfoliosCollection, transactionsCollection;
let dontBlameMeCollection;
let humansCollection, ratingsCollection, criteriaCollection, categoriesCollection;
let bankTransactionsCollection;
let newsCollection;
const authCodesCollectionName = 'authCodes';
let authCodesCollection;
let robberyLogsCollection;
let client;
let highscoresCollection;
let tindaSwipesCollection;
let bugReportsCollection;
let systemSettingsCollection;
let restaurantOrdersCollection;
let limterestCollection;
let teachermonCardsCollection;
let teachermonInvCollection;
let teachermonTradesCollection;
let teachermonBattlesCollection;
let cachedTeachermonCards = null;
let teachermonUniversesCollection;
let propertiesCollection, ownedPropertiesCollection;
let propertyInvitesCollection;
let petsCollection;
let petCemeteryCollection;
let limeaLayoutsCollection;
let tindaFamiliesCollection;
let mailsCollection;
let proposalsCollection;
let deliveriesCollection;

// =========================================================
// === CDN & BILDER UPLOAD SYSTEM ===
// =========================================================

// Ordner für das Shared Volume erstellen, falls nicht existent
const CDN_DIR = path.resolve(__dirname, 'cdn-data');
if (!fs.existsSync(CDN_DIR)) fs.mkdirSync(CDN_DIR);

// Multer Setup: Bilder im Arbeitsspeicher behalten, um sie direkt zu komprimieren
const storage = multer.memoryStorage();
const upload = multer({
    storage: storage,
    limits: { fileSize: 10 * 1024 * 1024 } // Max 10MB vor der Kompression
});

// Proxy: Alles was auf /cdn/ geht, leiten wir intern an den Nginx Container weiter
app.use('/cdn', createProxyMiddleware({
    target: 'http://limazon-cdn:80', // Name des Containers in der docker-compose
    changeOrigin: true,
    pathRewrite: { '^/cdn': '' }, // Entfernt /cdn aus dem Pfad für Nginx
}));

// API: Bild Hochladen & Komprimieren (WebP)
app.post('/api/cdn/upload', isAuthenticated, upload.single('image'), async (req, res) => {
    if (!req.file) return res.status(400).json({ error: 'Kein Bild empfangen.' });

    try {
        // Generiere einzigartigen Namen
        const filename = `img_${Date.now()}_${Math.floor(Math.random() * 1000)}.webp`;
        const filepath = path.join(CDN_DIR, filename);

        // Bild mit Sharp extrem komprimieren und als WebP speichern
        await sharp(req.file.buffer)
            .rotate() // Fixt auf dem Kopf stehende Handy-Fotos
            .resize({ width: 800, withoutEnlargement: true })
            .webp({ quality: 75, effort: 4 }) // 'effort: 4' ist ein guter Kompromiss aus Speed und RAM
            .toFile(filepath);

        // URL zurückgeben
        const fileUrl = `https://api.limazon.v6.rocks/cdn/${filename}`;

        console.log(`${LOG_PREFIX_SERVER} 🖼️ Neues Bild hochgeladen: ${filename}`);
        res.json({ message: 'Upload erfolgreich!', url: fileUrl, filename: filename });

    } catch (err) {
        console.error(`${LOG_PREFIX_SERVER} Fehler beim Bild-Upload:`, err);
        res.status(500).json({ error: 'Bildverarbeitung fehlgeschlagen.' });
    }
});

// API: Alle meine Bilder anzeigen (Galerie)
app.get('/api/cdn/list', isAuthenticated, (req, res) => {
    fs.readdir(CDN_DIR, (err, files) => {
        if (err) return res.status(500).json({ error: "Konnte Dateien nicht lesen." });

        const baseUrl = `https://api.limazon.v6.rocks/cdn/`;
        // Nur .webp Dateien nehmen und URLs zusammenbauen
        const images = files.filter(f => f.endsWith('.webp')).map(f => ({
            filename: f,
            url: baseUrl + f,
            // (Optional könnte man noch auslesen, von wem das Bild ist, 
            // aber für den Start zeigen wir einfach alle Server-Bilder an)
        }));

        // Da Nginx sehr schnell ist, können wir ruhig alle listen (oder auf 50 limitieren)
        res.json({ images: images.reverse().slice(0, 100) });
    });
});

// API: Bild löschen (Nur für Admins)
app.delete('/api/cdn/delete/:filename', isAuthenticated, isAdmin, (req, res) => {
    const filename = req.params.filename;

    // 1. Sicherheits-Check: Path Traversal verhindern (damit niemand "/../../etc/passwd" löscht)
    if (!filename || filename.includes('/') || filename.includes('\\') || filename.includes('..')) {
        return res.status(400).json({ error: 'Ungültiger Dateiname.' });
    }

    const filepath = path.join(CDN_DIR, filename);

    // 2. Datei löschen
    fs.unlink(filepath, (err) => {
        if (err) {
            // Wenn die Datei schon weg ist, ist das auch okay
            if (err.code === 'ENOENT') {
                return res.status(404).json({ error: 'Datei nicht gefunden.' });
            }
            console.error(`${LOG_PREFIX_SERVER} Fehler beim Löschen von ${filename}:`, err);
            return res.status(500).json({ error: 'Fehler beim Löschen der Datei.' });
        }

        console.log(`${LOG_PREFIX_SERVER} 🗑️ Bild gelöscht: ${filename} von Admin ${req.session.username}`);
        res.json({ message: 'Bild erfolgreich gelöscht.' });
    });
});

// ==============================================================================
// === NEU: AUTOMATISIERTE SICHERHEITS- & REPARATURFUNKTIONEN ====================
// ==============================================================================
const LOG_PREFIX_SECURITY = "[Security Check]";

/**
 * Findet Benutzer, deren Kontostand fälschlicherweise als String gespeichert ist, 
 * und konvertiert ihn in eine Zahl.
 */
async function fixStringBalances() {
    try {
        console.log(`${LOG_PREFIX_SECURITY} Suche nach Kontoständen, die als String gespeichert sind...`);
        const usersWithBadBalance = await usersCollection.find({ balance: { $type: "string" } }).toArray();

        if (usersWithBadBalance.length === 0) {
            console.log(`${LOG_PREFIX_SECURITY} ✅ Alle Kontostände haben den korrekten Datentyp (Zahl).`);
            return { message: "Keine fehlerhaften Kontostände (String) gefunden.", modifiedCount: 0 };
        }

        console.warn(`${LOG_PREFIX_SECURITY} ❗ ${usersWithBadBalance.length} Benutzer mit String-Kontostand gefunden. Starte Reparatur...`);
        const bulkOps = usersWithBadBalance.map(user => ({
            updateOne: {
                filter: { _id: user._id },
                update: { $set: { balance: parseFloat(String(user.balance).replace(/[^0-9.]/g, '')) || 0 } }
            }
        }));
        const result = await usersCollection.bulkWrite(bulkOps);
        console.log(`${LOG_PREFIX_SECURITY} ✅ Reparatur abgeschlossen. ${result.modifiedCount} Kontostände korrigiert.`);
        return { message: `${result.modifiedCount} Kontostände wurden korrigiert.`, modifiedCount: result.modifiedCount };
    } catch (err) {
        console.error(`${LOG_PREFIX_SECURITY} ❌ FEHLER bei der Kontostand-Typ-Reparatur:`, err);
        return { error: "Serverfehler bei der Reparatur von String-Kontoständen." };
    }
}

/**
 * Konvertiert reguläre Produkte in das neue Börsenformat, falls noch nicht geschehen.
 * Dies ist hauptsächlich eine Migrationsaufgabe.
 */
async function convertProductsToStocks() {
    try {
        console.log(`${LOG_PREFIX_SECURITY} Suche nach Produkten, die noch nicht in das Börsenformat konvertiert wurden...`);
        const productsToConvert = await productsCollection.find({
            isTokenCard: { $ne: true },
            currentPrice: { $exists: false }
        }).toArray();

        if (productsToConvert.length === 0) {
            console.log(`${LOG_PREFIX_SECURITY} ✅ Alle Produkte sind bereits im Börsenformat.`);
            return { message: "Keine Produkte zur Konvertierung gefunden.", modifiedCount: 0 };
        }

        console.log(`${LOG_PREFIX_SECURITY} ❗ ${productsToConvert.length} Produkte werden in das Börsenformat konvertiert...`);
        const bulkOps = productsToConvert.map(prod => {
            const priceAsNumber = parseFloat(String(prod.price).replace(/[^0-9.]/g, '')) || 100.0;
            return {
                updateOne: {
                    filter: { _id: prod._id },
                    update: {
                        $set: {
                            currentPrice: priceAsNumber,
                            basePrice: priceAsNumber,
                            priceHistory: [{ price: priceAsNumber, timestamp: new Date() }],
                            buysLastInterval: 0,
                            sellsLastInterval: 0
                        },
                        $unset: { price: "" }
                    }
                }
            };
        });
        const result = await productsCollection.bulkWrite(bulkOps);
        console.log(`${LOG_PREFIX_SECURITY} ✅ Konvertierung abgeschlossen. ${result.modifiedCount} Produkte umgewandelt.`);
        return { message: `${result.modifiedCount} Produkte wurden konvertiert.`, modifiedCount: result.modifiedCount };
    } catch (err) {
        console.error(`${LOG_PREFIX_SECURITY} ❌ FEHLER bei der Produkt-Konvertierung:`, err);
        return { error: "Serverfehler bei der Produkt-Konvertierung." };
    }
}

/**
 * Findet Benutzer mit extrem hohen oder fehlerhaften Kontoständen (Geld & Tokens)
 * und setzt sie auf einen sicheren Maximalwert zurück (100 Billionen).
 * Verhindert "e+" Notation und Layout-Fehler.
 */
async function normalizeExtremeBalances() {
    try {
        // Das harte Limit, das du wolltest (100 Billionen)
        const SAFE_MAX = 100000000000000;

        console.log(`${LOG_PREFIX_SECURITY} Prüfe auf Werte über ${SAFE_MAX} (oder 'Infinity')...`);

        // Finde User, die entweder zu viel Geld ODER zu viele Tokens haben
        // Oder deren Werte "Infinity" sind (MongoDB speichert Infinity manchmal als speziellen Wert)
        const usersToFix = await usersCollection.find({
            $or: [
                { balance: { $gt: SAFE_MAX } },
                { balance: Infinity },
                { tokens: { $gt: SAFE_MAX } },
                { tokens: Infinity }
            ]
        }).toArray();

        if (usersToFix.length === 0) {
            // console.log(`${LOG_PREFIX_SECURITY} ✅ Keine extremen Werte gefunden.`);
            return { message: "Werte normal.", modifiedCount: 0 };
        }

        console.warn(`${LOG_PREFIX_SECURITY} ❗ ${usersToFix.length} Benutzer mit unrealistischen Werten gefunden. Normalisiere...`);

        const bulkOps = usersToFix.map(user => {
            const updates = {};

            // Prüfe Geld
            if (user.balance > SAFE_MAX || user.balance === Infinity) {
                updates.balance = SAFE_MAX;
            }

            // Prüfe Tokens
            if (user.tokens > SAFE_MAX || user.tokens === Infinity) {
                updates.tokens = SAFE_MAX;
            }

            return {
                updateOne: {
                    filter: { _id: user._id },
                    update: { $set: updates }
                }
            };
        });

        const result = await usersCollection.bulkWrite(bulkOps);
        console.log(`${LOG_PREFIX_SECURITY} ✅ Normalisierung abgeschlossen. ${result.modifiedCount} User korrigiert.`);

        return { message: `${result.modifiedCount} Kontostände/Tokens wurden auf das Limit gesetzt.`, modifiedCount: result.modifiedCount };

    } catch (err) {
        console.error(`${LOG_PREFIX_SECURITY} ❌ FEHLER bei der Normalisierung:`, err);
        return { error: "Serverfehler bei der Normalisierung." };
    }
}

/**
 * Führt alle automatisierten Sicherheits- und Reparatur-Checks aus.
 */
async function runAutomatedSecurityChecks() {
    console.log(`${LOG_PREFIX_SECURITY} Starte automatische Datenintegritäts-Prüfung...`);
    try {
        // Reihenfolge ist wichtig: Zuerst Strings fixen, dann Werte normalisieren.
        await fixStringBalances();
        await convertProductsToStocks();
        await normalizeExtremeBalances();
        console.log(`${LOG_PREFIX_SECURITY} Automatische Prüfung abgeschlossen.`);
    } catch (error) {
        console.error(`${LOG_PREFIX_SECURITY} ❌ Ein kritischer Fehler ist während der automatischen Prüfung aufgetreten:`, error);
    }
}


// --- Hilfsfunktionen ---
async function generateUniqueUserShareCode() {
    let code;
    let exists = true;
    while (exists) {
        code = `U-${uuidv4().substr(0, 6).toUpperCase()}`; // Kürzer für User
        const existingUser = await usersCollection.findOne({ userShareCode: code }, { projection: { _id: 1 } });
        if (!existingUser) {
            exists = false;
        }
    }
    return code;
}

async function generateUniqueGroupShareCode() {
    let code;
    let exists = true;
    while (exists) {
        code = `G-${uuidv4().substr(0, 8).toUpperCase()}`; // Etwas länger für Gruppen
        const existingGroup = await limChatsCollection.findOne({ groupShareCode: code }, { projection: { _id: 1 } });
        if (!existingGroup) {
            exists = false;
        }
    }
    return code;
}

async function generateUniqueId(collection = productsCollection) { // Der 'prefix'-Parameter wurde hier entfernt, da er nicht benötigt wird.
    let newIdValue;
    let idExists = true;
    let attempts = 0;
    const maxAttempts = 1000;

    while (idExists && attempts < maxAttempts) {
        // Generiere direkt eine 6-stellige Zahl zwischen 100.000 und 999.999
        newIdValue = Math.floor(100000 + Math.random() * 900000); // Dies ist eine reine Zahl!

        try {
            // Prüfe, ob diese generierte NUMERISCHE ID bereits in der Collection existiert.
            // Da 'id' in der DB Int32 ist, ist dies ein direkter Zahlenvergleich.
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

    // Gib die generierte ZAHL zurück. Sie ist Int32-kompatibel und kann direkt verwendet werden.
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
    if (seededCount > 0) console.log(`${LOG_PREFIX_SERVER} ✅ ${seededCount} Token-Karten Produkte erfolgreich geseedet.`);
    else console.log(`${LOG_PREFIX_SERVER}    Keine neuen Token-Karten Produkte zu seeden (oder bereits vorhanden).`);
}

// Hilfsfunktion: Geld auf 2 Nachkommastellen runden (kaufmännisch)
function roundMoney(amount) {
    return Math.round((amount + Number.EPSILON) * 100) / 100;
}

// Neue, schnelle Funktion (ohne DB Check)
function generateFastTokenCode() {
    const p1 = Math.random().toString(36).substring(2, 7).toUpperCase();
    const p2 = Math.random().toString(36).substring(2, 7).toUpperCase();
    return `LMTKN-${p1}-${p2}`;
}

// =========================================================
// === GLOBAL API RATE LIMITER (RAM BASED) ===
// =========================================================
const apiRequestCounts = new Map();
const API_WINDOW_MS = 60 * 1000; // 1 Minute Zeitfenster
const API_MAX_REQS = 300;        // Max 300 Requests pro Minute pro IP

async function globalApiRateLimit(req, res, next) {
    if (req.headers['x-bot-bypass'] === 'limo-god-mode') {
        return next();
    }

    const ip = req.headers['x-forwarded-for']?.split(',')[0] || req.socket.remoteAddress;
    const redisKey = `rate_limit:global:${ip}`;

    try {
        // Erhöht den Zähler für diese IP um 1
        const currentCount = await global.redisPub.incr(redisKey);

        // Wenn es der erste Request in diesem Zeitfenster ist, setze den Ablauf-Timer (60 Sekunden)
        if (currentCount === 1) {
            await global.redisPub.expire(redisKey, 60);
        }

        if (currentCount > 300) { // Max 300 Requests pro Minute
            return res.status(429).json({ error: "Zu viele Anfragen. Bitte warte einen Moment." });
        }

        next();
    } catch (err) {
        // Fallback: Wenn Redis kurzzeitig hängt, Request trotzdem durchlassen (besser als Server-Downtime)
        console.error(`${LOG_PREFIX_SERVER} Redis Rate-Limit Fehler:`, err.message);
        next();
    }
}

// =========================================================
// === CACHING SYSTEM (LOCAL JSON + RAM + PRE-COMPUTED STRING) ===
// =========================================================

// NEU: Der fertig berechnete JSON-String für ultimativen Speed
let globalProductCacheString = '{"products":[]}';

async function initCacheSystem() {
    console.log(`${LOG_PREFIX_SERVER} 🚀 Initialisiere Cache System...`);

    // 1. Cache Ordner erstellen
    if (!fs.existsSync(CACHE_DIR)) {
        fs.mkdirSync(CACHE_DIR);
    }

    // 2. Initiales Laden (Versuch)
    if (fs.existsSync(PRODUCTS_CACHE_FILE)) {
        try {
            const rawData = fs.readFileSync(PRODUCTS_CACHE_FILE, 'utf8');
            globalProductCache = JSON.parse(rawData);
            // Auch den String initial setzen!
            globalProductCacheString = JSON.stringify({ products: globalProductCache });
            console.log(`${LOG_PREFIX_SERVER} ⚡ Cache aus Datei geladen.`);
        } catch (err) {
            console.warn(`${LOG_PREFIX_SERVER} ⚠️ Cache-Datei fehlerhaft, lade neu aus DB.`);
        }
    }

    // 3. Sofortiges Update aus der DB
    await refreshProductCache();
}

async function seedProperties() {
    const houses = [
        { id: 'carton', name: 'Pappkarton', price: 100, maxRoommates: 0, rent: 0, protection: 0.02, energyBonus: 1.0, img: '📦', desc: 'Wenigstens wird man nicht nass.' },
        { id: 'trailer', name: 'Altes Wohnmobil', price: 25000, maxRoommates: 1, rent: 250, protection: 0.10, energyBonus: 1.1, img: '🚐', desc: 'Eng, aber dein eigener Herr.' },
        { id: 'treehouse', name: 'Baumhaus', price: 45000, maxRoommates: 1, rent: 400, protection: 0.15, energyBonus: 1.1, img: '🌳', desc: 'Natur pur, aber es zieht ein bisschen.' },
        { id: 'apartment', name: 'Stadt-Appartement', price: 120000, maxRoommates: 2, rent: 800, protection: 0.25, energyBonus: 1.2, img: '🏢', desc: 'Mitten im Geschehen.' },
        { id: 'suburb', name: 'Einfamilienhaus', price: 650000, maxRoommates: 4, rent: 2500, protection: 0.45, energyBonus: 1.3, img: '🏡', desc: 'Ruhige Lage, viel Platz.' },
        { id: 'mansion', name: 'Limo-Villa', price: 4500000, maxRoommates: 5, rent: 10000, protection: 0.70, energyBonus: 1.5, img: '🏰', desc: 'Luxus pur.' },
        { id: 'bunker', name: 'Atomschutz-Bunker', price: 12000000, maxRoommates: 6, rent: 15000, protection: 0.98, energyBonus: 1.2, img: '🛡️', desc: 'Sicherer geht es nicht. Platz für 6 Prepper.' },
        { id: 'penthouse', name: 'Sky-Penthouse', price: 25000000, maxRoommates: 6, rent: 45000, protection: 0.90, energyBonus: 2.0, img: '💎', desc: 'Luxus für 6 Personen über den Wolken.' }
    ];
    
    // Upsert stellt sicher, dass neue Häuser (Baumhaus) eingefügt werden, ohne alte zu killen!
    for (const h of houses) {
        await propertiesCollection.updateOne({ id: h.id }, { $set: h }, { upsert: true });
    }
    console.log(`${LOG_PREFIX_SERVER} 🏠 Immobilienmarkt aktualisiert.`);
}

async function refreshProductCache() {
    try {
        // Hole ALLE Produkte aus der DB (ohne History für Speed)
        // Sortieren nach ID sorgt für konsistente Reihenfolge
        const prods = await productsCollection.find({}, {
            projection: { priceHistory: 0 }
        }).sort({ id: 1 }).toArray();

        // Datenbereinigung & Preis-Logik (wie gehabt)
        const sanitized = prods.map(p => {
            const s = { ...p };

            // Preis Logik
            let stablePriceVal = 0;
            if (p.basePrice !== undefined && p.basePrice !== null) {
                stablePriceVal = parseFloat(p.basePrice);
            } else {
                stablePriceVal = parseFloat((p.price || "0").toString().replace(/[^0-9.]/g, '')) || 0;
            }
            s.price = `$${stablePriceVal.toFixed(2)}`;

            // Börsen Preis
            let volatilePriceVal = stablePriceVal;
            if (p.currentPrice !== undefined && p.currentPrice !== null) {
                volatilePriceVal = parseFloat(p.currentPrice);
            }
            s.currentPrice = volatilePriceVal;

            // Rest
            s.stock = (typeof p.stock === 'number' && p.stock >= 0) ? p.stock : 0;
            s.default_stock = (typeof p.default_stock === 'number' && p.default_stock >= 0) ? p.default_stock : (p.isTokenCard ? 99999 : 20);

            // _id entfernen spart Speicher und Bandbreite beim Senden
            delete s._id;
            return s;
        });

        // 1. Update RAM Objekt (für interne Logik wie Käufe)
        globalProductCache = sanitized;

        // 2. Update RAM String (HIER IST DER PERFORMANCE TRICK)
        // Wir berechnen das JSON EINMAL hier, statt 1000x pro Sekunde bei jedem Request.
        globalProductCacheString = JSON.stringify({ products: sanitized });

        // 3. Update Datei (Asynchron, Fehler ignorieren wir hier, damit Server nicht crasht)
        fs.writeFile(PRODUCTS_CACHE_FILE, JSON.stringify(sanitized), (err) => {
            if (err) console.error("Cache-Write Error:", err);
        });

        // Trigger für Smart Polling (Frontend merkt: "Ah, neue Daten!")
        updateDataVersion('products');

        // console.log(`${LOG_PREFIX_SERVER} ♻️ Produkt-Cache aktualisiert (${sanitized.length} Items).`);
        return sanitized;
    } catch (err) {
        console.error(`${LOG_PREFIX_SERVER} ❌ Fehler beim Refreshing des Product Caches:`, err);
        return [];
    }
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

// 1. ZENTRALE RECHTE-LISTE (Einfach hier deine Endpunkte und Rechte mappen)
const AVAILABLE_PERMISSIONS = {
    'manage_products': { name: 'Shop & Produkte', desc: 'Produkte erstellen, bearbeiten, löschen und Lagerbestände ändern.' },
    'manage_tokens': { name: 'Token-Generierung', desc: 'Erlaubt das Generieren von neuen Token-Guthabencodes.' },
    'manage_users': { name: 'Nutzerverwaltung (Basis)', desc: 'Nutzerdaten anpassen (Geld, Tokens) und Geldstrafen verhängen.' },
    'manage_users_critical': { name: 'Nutzerverwaltung (Kritisch)', desc: 'Achtung: Erlaubt das Löschen, Bannen, Rollen-Zuweisung und Passwort-Zurücksetzen von Nutzern.' },
    'manage_news': { name: 'LNN News', desc: 'Manuelle News posten, AI-Trigger ausführen und Artikel löschen.' },
    'manage_ideas': { name: 'Ideenbox Moderation', desc: 'Ideen-Status ändern, löschen und Nutzer für die Ideenbox sperren/entsperren.' },
    'manage_bugs': { name: 'Bug Bounty Moderation', desc: 'Bug-Reports einsehen, Status ändern und Delta-Coins vergeben.' }, // NEU
    'manage_teachermon': { name: 'Teachermon Karten', desc: 'Teachermon Karten erstellen und aus dem Spiel entfernen.' },
    'manage_universes': { name: 'Teachermon Universen', desc: 'Neue Universen anlegen und verwalten.' },
    'manage_human_grades': { name: 'Human Grades Daten', desc: 'Personen, Kategorien und Kriterien anlegen oder löschen.' },
    'manage_human_ratings': { name: 'Human Grades Moderation', desc: 'Bewertungen von Nutzern einsehen und gezielt löschen.' },
    'manage_cdn': { name: 'Bilder & CDN', desc: 'Hochgeladene Bilder vom Server löschen.' },
    'manage_limea': { name: 'Limea Moderation', desc: 'Limea Layouts aus dem Community Store löschen.' }, // NEU
    'manage_economy': { name: 'Wirtschaftskontrolle', desc: 'Steuer-Razzia erzwingen, Vermögen kappen und Infinity-Money entziehen.' },
    'manage_chats': { name: 'Chat Inspektor', desc: 'Tinda-Chats und private Nachrichten lesen sowie als Admin Systemnachrichten senden.' },
    'system_maintenance': { name: 'System & Wartung', desc: 'Health-Check, Stats, Reports abrufen und System-Reparaturen durchführen.' },
    'super_admin': { name: 'Super Admin (Engine)', desc: 'Gefährlich: Voller, ungefilterter Zugriff auf die MongoDB-Engine.' }
};

// 2. MAPPING: WELCHER ENDPOINT BRAUCHT WELCHES RECHT
const ENDPOINT_PERMISSIONS = {
    // --- Shop & Produkte ---
    'GET /api/admin/products': 'manage_products',
    'POST /api/admin/products': 'manage_products',
    'PATCH /api/products/:id': 'manage_products',
    'DELETE /api/admin/products/:id': 'manage_products',
    'DELETE /api/products/:id': 'manage_products', // Alte Route
    'PATCH /api/products/reset': 'manage_products',
    'PATCH /api/admin/zero-stock': 'manage_products',
    
    // --- Tokens ---
    'POST /api/admin/generate-token-code': 'manage_tokens',

    // --- User Management ---
    'GET /api/admin/users': 'manage_users',
    'PUT /api/admin/users/:id': 'manage_users',
    'POST /api/admin/users/:id/fine': 'manage_users',
    'POST /api/admin/users/:id/reset-pw': 'manage_users_critical',
    'DELETE /api/admin/users/:id': 'manage_users_critical',
    'POST /api/admin/banUser': 'manage_users_critical', 
    'GET /api/admin/roles': 'manage_users_critical', // NEU
    'GET /api/admin/permissions': 'manage_users_critical', // NEU

    // --- LNN News ---
    'POST /api/admin/news': 'manage_news',
    'POST /api/admin/news/trigger-ai': 'manage_news',
    'DELETE /api/admin/news/:id': 'manage_news',

    // --- Ideenbox ---
    'PATCH /api/ideas/:id/status': 'manage_ideas',
    'DELETE /api/ideas/:id': 'manage_ideas',
    'POST /api/admin/ideas/ban-user': 'manage_ideas',
    'POST /api/admin/ideas/unban-user': 'manage_ideas',

    // --- Bug Bounty --- // NEU HINZUGEFÜGT
    'GET /api/admin/bugs': 'manage_bugs',
    'POST /api/admin/bugs/:id/resolve': 'manage_bugs',

    // --- Teachermon ---
    'POST /api/teachermon/admin/cards': 'manage_teachermon',
    'DELETE /api/teachermon/admin/cards/:id': 'manage_teachermon',
    'POST /api/teachermon/admin/universes': 'manage_universes',
    'DELETE /api/teachermon/admin/universes/:id': 'manage_universes',

    // --- Human Grades ---
    'POST /api/human/admin/categories': 'manage_human_grades',
    'POST /api/human/admin/criteria': 'manage_human_grades',
    'POST /api/human/admin/humans': 'manage_human_grades',
    'PUT /api/human/admin/humans/:id': 'manage_human_grades',
    'DELETE /api/human/admin/humans/:id': 'manage_human_grades',
    'POST /api/human/admin/reset-defaults': 'manage_human_grades',
    'GET /api/human/admin/raters': 'manage_human_ratings',
    'GET /api/human/admin/raters/:userId': 'manage_human_ratings',
    'DELETE /api/human/admin/ratings/:id': 'manage_human_ratings',

    // --- Limea --- // NEU HINZUGEFÜGT
    'DELETE /api/limea/admin/layouts/:id': 'manage_limea',

    // --- Wirtschaft ---
    'POST /api/admin/system/force-tax': 'manage_economy',
    'POST /api/admin/system/revoke-infinity': 'manage_economy',
    'POST /api/admin/system/reset-rich-users': 'manage_economy',

    // --- Chat Inspektor ---
    'GET /api/admin/chat/tinda-conversations': 'manage_chats',
    'GET /api/admin/chat/messages': 'manage_chats',
    'POST /api/admin/chat/send': 'manage_chats',

    // --- CDN / Bilder ---
    'DELETE /api/cdn/delete/:filename': 'manage_cdn',

    // --- System & Wartung ---
    'GET /api/admin/health-check': 'system_maintenance',
    'GET /api/admin/system/stats': 'system_maintenance', // NEU
    'GET /api/admin/system/report': 'system_maintenance', // NEU
    'POST /api/admin/fix-balances': 'system_maintenance',
    'POST /api/admin/convert-products-to-stocks': 'system_maintenance',
    'POST /api/admin/normalize-balances': 'system_maintenance',
    'POST /api/admin/system/normalize': 'system_maintenance',
    'POST /api/admin/system/fix-images': 'system_maintenance',
    'POST /api/admin/system/fix-decimals': 'system_maintenance',

    // --- Admin Engine ---
    'POST /api/admin/engine': 'super_admin'
};

// 3. VORGEFERTIGTE GRUPPEN (ROLES)
const PREDEFINED_ROLES = {
    'admin': { 
        name: 'Administrator',
        desc: 'Hat vollen Zugriff auf alles.',
        permissions: ['ALL']
    },
    'moderator': {
        name: 'Moderator',
        desc: 'Kümmert sich um die Community, Chats und Strafen.',
        permissions: [
            'manage_users', 
            'manage_news', 
            'manage_ideas', 
            'manage_bugs',
            'manage_human_grades', 
            'manage_human_ratings', 
            'manage_chats'
        ]
    },
    'shop_manager': {
        name: 'Shop & Content Manager',
        desc: 'Verwaltet den Shop, Items, Limea und Teachermon-Karten.',
        permissions: [
            'manage_products',
            'manage_teachermon',
            'manage_universes',
            'manage_limea', // Hab ich dem Shop Manager gegeben
            'manage_cdn'
        ]
    },
    'user': {
        name: 'Standard User',
        desc: 'Normaler Spieler. Keine Admin-Rechte.',
        permissions: [] // Hat Zugriff auf keinen einzigen Admin-Endpoint
    }
};

// 2. DIE NEUE (SMARTE) isAdmin MIDDLEWARE
async function isAdmin(req, res, next) {
    if (!req.session || !req.session.userId) {
        return res.status(401).json({ error: 'Nicht eingeloggt.' });
    }

    try {
        const user = await usersCollection.findOne({ _id: new ObjectId(req.session.userId) });
        if (!user) return res.status(401).json({ error: 'User nicht gefunden.' });

        // A) Legacy-Support: Hat der User noch das alte "isAdmin: true" Feld?
        if (user.isAdmin === true) {
            return next();
        }

        // B) Finde heraus, welchen Endpoint er aufrufen will
        const routePath = req.route ? req.route.path : req.path;
        const routeKey = `${req.method} ${routePath}`;
        const requiredPermission = ENDPOINT_PERMISSIONS[routeKey];

        // Wenn der User gar keine Rolle in der DB hat, ist er ein normaler 'user'
        const userRole = user.role || 'user';

        // C) Prüfung 1: Ist er in einer VORGEFERTIGTEN Gruppe?
        if (PREDEFINED_ROLES[userRole]) {
            const rolePerms = PREDEFINED_ROLES[userRole].permissions;
            
            // Wenn die Gruppe 'ALL' hat (z.B. Admin), darf er sofort durch
            if (rolePerms.includes('ALL')) return next();
            
            // Wenn die Gruppe das spezifische Recht hat, darf er durch
            if (requiredPermission && rolePerms.includes(requiredPermission)) {
                return next();
            }
        }

        // D) Prüfung 2: Hat er die Rolle 'custom' (Spezialanfertigung)?
        if (userRole === 'custom' && user.permissions) {
            if (requiredPermission && user.permissions.includes(requiredPermission)) {
                return next();
            }
        }

        // E) Rauswurf: Weder Admin, noch passende Gruppe, noch passendes Custom-Recht
        console.warn(`${LOG_PREFIX_SERVER} ⛔ Zugriff verweigert für User ${req.session.username} (Rolle: ${userRole}) auf ${req.originalUrl}`);
        res.status(403).json({ error: 'Zugriff verweigert. Fehlende Berechtigungen.' });

    } catch (err) {
        console.error(`${LOG_PREFIX_SERVER} Fehler bei Admin-Prüfung:`, err);
        res.status(500).json({ error: "Fehler bei der Überprüfung der Berechtigungen." });
    }
}

// --- Init MongoDB-Verbindung und Serverstart ---
MongoClient.connect(mongoUri)
    .then(async mongoClient => {
        client = mongoClient; // Client global speichern für Transaktionen
        db = client.db(mongoDbName);

        // --- 1. Collections Initialisieren ---
        productsCollection = db.collection(productsCollectionName);
        usersCollection = db.collection(usersCollectionName);
        ordersCollection = db.collection(ordersCollectionName);
        inventoriesCollection = db.collection(inventoriesCollectionName);
        wheelsCollection = db.collection(wheelsCollectionName);
        tokenCodesCollection = db.collection(tokenCodesCollectionName);
        tokenTransactionsCollection = db.collection(tokenTransactionsCollectionName);
        limChatsCollection = db.collection(CHAT_COLLECTION_NAME);
        limMessagesCollection = db.collection(MESSAGE_COLLECTION_NAME);
        limUserChatSettingsCollection = db.collection(USER_CHAT_SETTINGS_COLLECTION_NAME);
        auctionsCollection = db.collection('auctions');
        dontBlameMeCollection = db.collection('dontBlameMePosts');
        portfoliosCollection = db.collection('portfolios');
        transactionsCollection = db.collection('transactions');
        ideasCollection = db.collection('ideas');
        newsCollection = db.collection('news');
        robberyLogsCollection = db.collection('robberyLogs');
        highscoresCollection = db.collection('highscores');
        bugReportsCollection = db.collection('bugReports');
        systemSettingsCollection = db.collection('systemSettings');
        humansCollection = db.collection('humans');
        ratingsCollection = db.collection('ratings');
        criteriaCollection = db.collection('criteria');
        categoriesCollection = db.collection('categories');
        tindaSwipesCollection = db.collection('tindaSwipes');
        restaurantOrdersCollection = db.collection('restaurantOrders');
        limterestCollection = db.collection(limterestCollectionName);
        teachermonCardsCollection = db.collection('teachermonCards');
        teachermonInvCollection = db.collection('teachermonInventories');
        teachermonTradesCollection = db.collection('teachermonTrades');
        teachermonBattlesCollection = db.collection('teachermonBattles');
		teachermonUniversesCollection = db.collection('teachermonUniverses');
		propertiesCollection = db.collection('properties');
		ownedPropertiesCollection = db.collection('ownedProperties');
		propertyInvitesCollection = db.collection('propertyInvites');
		petsCollection = db.collection('pets');
        petCemeteryCollection = db.collection('petCemetery');
		limeaLayoutsCollection = db.collection('limeaLayouts');
		tindaFamiliesCollection = db.collection('tindaFamilies');
		mailsCollection = db.collection('mails');
		proposalsCollection = db.collection('proposals');
		deliveriesCollection = db.collection('deliveries');

        authCodesCollection = db.collection(authCodesCollectionName);

        bankTransactionsCollection = db.collection('bankTransactions');
        console.log(`${LOG_PREFIX_SERVER} ✅ MongoDB verbunden & alle Collections initialisiert.`);
        // --- 2. Indizes & Reparaturen ---
        try {
            try {
                await ratingsCollection.dropIndex("teacherId_1_userId_1");
                console.log(`${LOG_PREFIX_SERVER} ♻️ Alter Index 'teacherId_1_userId_1' erfolgreich entfernt.`);
            } catch (e) { /* Index existiert nicht mehr, alles gut */ }

            await ratingsCollection.createIndex({ humanId: 1, userId: 1 }, { unique: true });
            await humansCollection.createIndex({ id: 1 }, { unique: true, sparse: true });
            await criteriaCollection.createIndex({ id: 1 }, { unique: true });
            await categoriesCollection.createIndex({ id: 1 }, { unique: true });
            await initCacheSystem();
            await usersCollection.createIndex({ userShareCode: 1 }, { unique: true, sparse: true });
            await limChatsCollection.createIndex({ participants: 1 });
            await limChatsCollection.createIndex({ type: 1 });
            await limChatsCollection.createIndex({ groupShareCode: 1 }, { unique: true, sparse: true });
            await limChatsCollection.createIndex({ ownerId: 1 });
            await limChatsCollection.createIndex({ updatedAt: -1 });
            await limMessagesCollection.createIndex({ chatId: 1, timestamp: -1 });
            await limMessagesCollection.createIndex({ senderId: 1 });
            await limMessagesCollection.createIndex({ content: "text" });
            await limUserChatSettingsCollection.createIndex({ userId: 1, chatId: 1 }, { unique: true });
            await auctionsCollection.createIndex({ status: 1, endTime: 1 });
            await portfoliosCollection.createIndex({ userId: 1, productId: 1 }, { unique: true });
            await transactionsCollection.createIndex({ userId: 1 });
            await transactionsCollection.createIndex({ productId: 1, timestamp: -1 });
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
            await ideasCollection.createIndex({ status: 1, createdAt: -1 });
            await ideasCollection.createIndex({ submitterId: 1 });
            await usersCollection.createIndex({ isBannedFromIdeaBox: 1 });
            await tokenCodesCollection.createIndex({ code: 1 }, { unique: true });
            await tokenCodesCollection.createIndex({ redeemedByUserId: 1 });
            await tokenCodesCollection.createIndex({ generatedForUserId: 1, isRedeemed: 1 });
            await highscoresCollection.createIndex({ game: 1, score: -1 });
            await limterestCollection.createIndex({ tags: 1 });
            await teachermonInvCollection.createIndex({ userId: 1, cardId: 1 }, { unique: true });
            await ratingsCollection.createIndex({ userId: 1 }); // Wichtig, da bisher nur { humanId: 1, userId: 1 } existiert
            await dontBlameMeCollection.createIndex({ userId: 1 });
            await auctionsCollection.createIndex({ sellerId: 1 });
			await seedTeachermonUniverses();
			await seedProperties();
			await mailsCollection.createIndex({ userId: 1, createdAt: -1 });

            if (tokenTransactionsCollection) {
                await tokenTransactionsCollection.createIndex({ userId: 1 });
                await tokenTransactionsCollection.createIndex({ type: 1 });
                await tokenTransactionsCollection.createIndex({ timestamp: -1 });
            }
            await dontBlameMeCollection.createIndex(
                { "createdAt": 1 },
                { expireAfterSeconds: 72 * 60 * 60 }
            );
            await authCodesCollection.createIndex({ "createdAt": 1 }, { expireAfterSeconds: 300 });

            // --- AUTO-DELETE (TTL) INDIZES ---
            // Löscht Logs automatisch nach einer bestimmten Zeit

            // Bank-Historie: 90 Tage aufheben
            await bankTransactionsCollection.createIndex(
                { "timestamp": 1 },
                { expireAfterSeconds: 90 * 24 * 60 * 60 }
            );

            // Raub-Logs: 30 Tage aufheben (interessiert später niemanden mehr)
            if (robberyLogsCollection) {
                await robberyLogsCollection.createIndex(
                    { "timestamp": 1 },
                    { expireAfterSeconds: 30 * 24 * 60 * 60 }
                );
            }

            // Token-Logs: 60 Tage aufheben
            if (tokenTransactionsCollection) {
                await tokenTransactionsCollection.createIndex(
                    { "timestamp": 1 },
                    { expireAfterSeconds: 60 * 24 * 60 * 60 }
                );
            }

            // Nachrichten: Optional, z.B. nach 1 Jahr löschen, wenn der Chat zu voll wird
            await limMessagesCollection.createIndex(
                { "timestamp": 1 },
                { expireAfterSeconds: 365 * 24 * 60 * 60 }
            );

            console.log(`${LOG_PREFIX_SERVER} ♻️ Auto-Delete (TTL) Indizes geprüft.`);

            await seedTeachermonCards();

            console.log(`${LOG_PREFIX_SERVER} ✅ Alle Indizes erfolgreich geprüft/erstellt.`);
        } catch (indexErr) {
            console.error(`${LOG_PREFIX_SERVER} ❌ Fehler bei der Indexerstellung:`, indexErr);
        }

        // --- 3. Seeding (Datenbank befüllen) ---
        try {
            const regularProductCount = await productsCollection.countDocuments({ isTokenCard: { $ne: true } });
            if (regularProductCount === 0) {
                console.log(`${LOG_PREFIX_SERVER}    Datenbank (reguläre Produkte) ist leer. Starte initiales Seeding...`);
                await seedDatabaseFromLocalJson();
            }
        } catch (seedErr) { console.error(`${LOG_PREFIX_SERVER}    Fehler beim Produkt-Seeding:`, seedErr); }

        await seedTokenCardProducts();
        await seedDefaultPublicWheel();

        // WICHTIG: Human Grades Defaults laden (hier an der richtigen Stelle!)
        await seedHumanGradesDefaults();

        // --- 4. Automatisierte Checks & Jobs ---
        console.log(`${LOG_PREFIX_SERVER} 🚀 Führe initiale Datenintegritäts-Prüfung aus...`);
        await runAutomatedSecurityChecks();

        const SECURITY_CHECK_INTERVAL_MS = 60 * 60 * 1000;
        if (cluster.isPrimary) {
            setInterval(runAutomatedSecurityChecks, SECURITY_CHECK_INTERVAL_MS);
            console.log(`${LOG_PREFIX_SERVER} ⏰ Automatische Sicherheits-Prüfung aktiv (Nur Master).`);
        }

        // AUKTION-ENDE-JOB
        if (cluster.isPrimary) {
            setInterval(async () => {
                // console.log(`${LOG_PREFIX_SERVER} [AuctionJob] Prüfe...`);
                const now = new Date();
                try {
                    const expiredAuctions = await auctionsCollection.find({ status: 'active', endTime: { $lte: now } }).toArray();
                    if (expiredAuctions.length > 0) {
                        console.log(`${LOG_PREFIX_SERVER} [AuctionJob] ${expiredAuctions.length} Auktion(en) beendet.`);
                        for (const auction of expiredAuctions) {
                            if (auction.highestBidderId) {
                                await inventoriesCollection.updateOne({ userId: auction.highestBidderId, productId: auction.productId }, { $inc: { quantityOwned: auction.quantity } }, { upsert: true });
                                await usersCollection.updateOne({ _id: auction.sellerId }, { $inc: { balance: auction.currentBid } });
                                await auctionsCollection.updateOne({ _id: auction._id }, { $set: { status: 'ended_sold' } });
                                console.log(`${LOG_PREFIX_SERVER} [AuctionJob] Auktion ${auction._id} verkauft.`);
                            } else {
                                await inventoriesCollection.updateOne({ userId: auction.sellerId, productId: auction.productId }, { $inc: { quantityOwned: auction.quantity } }, { upsert: true });
                                await auctionsCollection.updateOne({ _id: auction._id }, { $set: { status: 'ended_unsold' } });
                                console.log(`${LOG_PREFIX_SERVER} [AuctionJob] Auktion ${auction._id} nicht verkauft (Rückgabe).`);
                            }
                        }
                    }
                } catch (err) { console.error(`${LOG_PREFIX_SERVER} [AuctionJob] Fehler:`, err); }
            }, 60000);
        }

        // =========================================================
        // === BÖRSEN-JOB (Hybrid: User + Chaos + Gravity + LIMITS) ===
        // =========================================================
        const PRICE_UPDATE_INTERVAL_MS = 60000; // 60 Sekunden

        const MAX_STOCK_PRICE = 100000.00;

        if (cluster.isPrimary) {
            setInterval(async () => {
                const now = new Date();
                try {
                    const stocksToUpdate = await productsCollection.find({ isTokenCard: { $ne: true } }).toArray();
                    if (stocksToUpdate.length === 0) return;

                    const bulkOps = stocksToUpdate.map(stock => {
                        let basePrice = stock.basePrice;
                        if (!basePrice) {
                            basePrice = parseFloat((stock.price || "10").replace(/[^0-9.]/g, '')) || 10;
                        }

                        let currentPrice = stock.currentPrice || basePrice;
                        const buys = stock.buysLastInterval || 0;
                        const sells = stock.sellsLastInterval || 0;
                        const netDemand = buys - sells;

                        const impactFactor = 0.001;
                        const userImpact = currentPrice * (netDemand * impactFactor);
                        const volatility = 0.02;
                        const randomChange = currentPrice * (Math.random() * volatility * 2 - volatility);
                        const reversionStrength = 0.05;
                        const gravityPull = (basePrice - currentPrice) * reversionStrength;

                        let newPrice = currentPrice + userImpact + randomChange + gravityPull;

                        if (newPrice < 0.10) newPrice = 0.10;
                        if (newPrice > MAX_STOCK_PRICE) newPrice = MAX_STOCK_PRICE;

                        return {
                            updateOne: {
                                filter: { _id: stock._id },
                                update: {
                                    $set: {
                                        currentPrice: parseFloat(newPrice.toFixed(2)),
                                        basePrice: basePrice,
                                        buysLastInterval: 0,
                                        sellsLastInterval: 0
                                    },
                                    $push: {
                                        priceHistory: {
                                            $each: [{ price: parseFloat(newPrice.toFixed(2)), timestamp: now }],
                                            $slice: -30
                                        }
                                    }
                                }
                            }
                        };
                    });

                    if (bulkOps.length > 0) {
                        await productsCollection.bulkWrite(bulkOps);

                        await refreshProductCache(); // Der Master aktualisiert seinen Cache

                        // Den Workern Bescheid sagen, dass sie updaten sollen
                        if (global.redisPub) {
                            global.redisPub.publish('sync-product-cache', 'update');
                        }
                    }

                } catch (err) {
                    console.error(`${LOG_PREFIX_SERVER} [StockMarketJob] Fehler:`, err);
                }
            }, PRICE_UPDATE_INTERVAL_MS);
        }

        // --- 5. REDIS INIT & HTTP CLUSTER START ---

        // Redis Funkgeräte aufbauen
        const redisUrl = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
        const redisPub = createClient({ url: redisUrl });
        const redisSub = redisPub.duplicate();

        Promise.all([redisPub.connect(), redisSub.connect()]).then(() => {
            console.log(`${LOG_PREFIX_SERVER} 📡 Redis verbunden!`);
            global.redisPub = redisPub; // Global machen für die updateDataVersion Funktion

            // Hören, ob ein anderer Worker ein Update ruft
            redisSub.subscribe('sync-version', (key) => {
                if (dataVersions[key]) dataVersions[key] = Date.now();
            });

            redisSub.subscribe('sync-product-cache', async () => {
                await refreshProductCache();
            });

            // NEU: Teachermon-Cache auf allen Kernen leeren
            redisSub.subscribe('sync-teachermon-cache', () => {
                cachedTeachermonCards = null;
                // Optionaler Log, um zu sehen, dass alle Kerne gehorchen:
                console.log(`${LOG_PREFIX_SERVER} ♻️ Teachermon-Cache auf Worker ${process.pid} geleert.`);
            });

            // CLUSTER LOGIK
            if (cluster.isPrimary) {
                console.log(`${LOG_PREFIX_SERVER} 👑 Master-Prozess ${process.pid} leitet die Server-Farm.`);
                console.log(`${LOG_PREFIX_SERVER} 🚀 Starte ${numCPUs} Worker-Kerne...`);

                // Für jeden CPU-Kern einen Worker starten
                for (let i = 0; i < numCPUs; i++) {
                    cluster.fork();
                }

                // Worker neu starten, falls einer crasht
                cluster.on('exit', (worker, code, signal) => {
                    console.error(`${LOG_PREFIX_SERVER} ⚠️ Worker ${worker.process.pid} abgestürzt. Starte Ersatz...`);
                    cluster.fork();
                });

            } else {
                // 👷 WORKER-PROZESS: Startet den eigentlichen Express-Server
                http.createServer(app).listen(HTTP_PORT, '::', () => {
                    console.log(`${LOG_PREFIX_SERVER} 🌐 Worker ${process.pid} ist online auf Port ${HTTP_PORT}`);
                });
            }
        }).catch(err => {
            console.error(`${LOG_PREFIX_SERVER} ❌ Redis-Verbindungsfehler:`, err);
        });

    }) // Ende vom MongoClient.connect .then()
    .catch(err => {
        console.error(`${LOG_PREFIX_SERVER} ❌ Kritischer Fehler: MongoDB-Verbindung fehlgeschlagen:`, err);
        process.exit(1);
    });

// POST: Manuelle Steuereintreibung (Admin Only)
app.post('/api/admin/system/force-tax', isAuthenticated, isAdmin, async (req, res) => {
    console.log(`${LOG_PREFIX_SERVER} 👮 Admin ${req.session.username} erzwingt Steuer-Eintreibung...`);

    const TAX_THRESHOLD = 100000000; // 100 Millionen (Grenze)
    const TAX_RATE = 0.005; // 0,5% Steuersatz

    try {
        // 1. Finde alle Reichen (Keine Admins, keine Infinity-User)
        const richUsers = await usersCollection.find({
            balance: { $gt: TAX_THRESHOLD },
            isAdmin: { $ne: true },
            infinityMoney: { $ne: true }
        }).toArray();

        if (richUsers.length === 0) {
            return res.json({ success: false, message: "Keine steuerpflichtigen User gefunden." });
        }

        let totalTaxCollected = 0;
        let shieldedUsers = 0;
        let taxedUsersCount = 0;

        const bulkOps = [];
        const inventoryOps = []; // Für verbrauchte Schilde

        for (const user of richUsers) {
            // A. Hat der User ein Schild?
            const shield = await inventoriesCollection.findOne({
                userId: user._id,
                productId: 'tax_shield',
                quantityOwned: { $gt: 0 }
            });

            if (shield) {
                shieldedUsers++;
                // Schild verbrauchen (-1 quantity)
                inventoryOps.push({
                    updateOne: {
                        filter: { _id: shield._id },
                        update: { $inc: { quantityOwned: -1 } }
                    }
                });
                continue; // Nächster User (keine Steuer)
            }

            // B. Steuer berechnen
            const taxAmount = Math.floor(user.balance * TAX_RATE * 100) / 100;

            if (taxAmount > 0) {
                taxedUsersCount++;
                totalTaxCollected += taxAmount;

                bulkOps.push({
                    updateOne: {
                        filter: { _id: user._id },
                        update: {
                            $inc: {
                                balance: -taxAmount,
                                totalTaxesPaid: taxAmount
                            }
                        }
                    }
                });
            }
        }

        // C. DB Updates ausführen
        if (inventoryOps.length > 0) await inventoriesCollection.bulkWrite(inventoryOps);
        if (bulkOps.length > 0) await usersCollection.bulkWrite(bulkOps);

        // D. Geld in die Staatskasse
        if (totalTaxCollected > 0) {
            await addToStateTreasury(totalTaxCollected);

            // News generieren
            await newsCollection.insertOne({
                headline: "Sonder-Steuerprüfung!",
                content: `Das Finanzamt hat soeben manuell zugegriffen! $${totalTaxCollected.toLocaleString()} wurden eingezogen. ${shieldedUsers} User waren geschützt.`,
                author: "Finanzamt (Admin)",
                category: "Wirtschaft",
                createdAt: new Date(),
                likes: 0
            });
        }

        res.json({
            success: true,
            message: `Steuer-Razzia beendet!`,
            details: {
                collected: totalTaxCollected,
                taxedCount: taxedUsersCount,
                shieldedCount: shieldedUsers
            }
        });

    } catch (e) {
        console.error(e);
        res.status(500).json({ error: "Fehler beim Eintreiben." });
    }
});

// === GRACEFUL SHUTDOWN (Für Docker) ===
async function gracefulShutdown(signal) {
    console.log(`${LOG_PREFIX_SERVER} 🛑 ${signal} empfangen. Fahre sauber herunter...`);

    // 1. Keine neuen HTTP-Anfragen mehr annehmen
    server.close(async () => {
        console.log(`${LOG_PREFIX_SERVER} 🔌 HTTP Server geschlossen. Laufende Requests beendet.`);

        // 2. Datenbankverbindung sauber trennen
        if (client) {
            try {
                await client.close();
                console.log(`${LOG_PREFIX_SERVER} 💾 MongoDB Verbindung geschlossen.`);
            } catch (err) {
                console.error(`${LOG_PREFIX_SERVER} Fehler beim Schließen der DB:`, err);
            }
        }

        console.log(`${LOG_PREFIX_SERVER} 👋 Tschüss!`);
        process.exit(0);
    });

    // Fallback: Wenn er nach 10 Sekunden nicht fertig ist, hart beenden
    setTimeout(() => {
        console.error(`${LOG_PREFIX_SERVER} ⚠️ Shutdown dauerte zu lange. Erzwinge Exit.`);
        process.exit(1);
    }, 10000);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM')); // Docker Stop Signal
process.on('SIGINT', () => gracefulShutdown('SIGINT'));   // Strg+C lokal

// Hilfsfunktion: Geld in die Staatskasse legen
async function addToStateTreasury(amount) {
    if (amount <= 0) return;
    await systemSettingsCollection.updateOne(
        { id: 'state_treasury' },
        { $inc: { balance: amount } },
        { upsert: true }
    );
    console.log(`${LOG_PREFIX_SERVER} 🏦 Staatskasse: +$${amount.toFixed(2)} eingezahlt.`);
}

// Hilfsfunktion: Staatskasse abrufen
async function getStateTreasuryBalance() {
    const doc = await systemSettingsCollection.findOne({ id: 'state_treasury' });
    return doc ? doc.balance : 500000; // Startet mit 500k, falls leer
}

// === API ENDPOINTS ===

// AUTH
app.post('/api/auth/register', async (req, res) => {
    // IP ermitteln (wichtig hinter Proxies/Nginx)
    const clientIp = req.headers['x-forwarded-for']?.split(',')[0] || req.socket.remoteAddress;

    // Prüfen ob IP gebannt ist
    const isBanned = await db.collection('banned_ips').findOne({ ip: clientIp });
    if (isBanned) {
        return res.status(403).json({ error: "Du wurdest von diesem Server gebannt." });
    }
    const { username, password } = req.body;
    console.log(`${LOG_PREFIX_SERVER} Registrierungsversuch für User: ${username ? username.substring(0, 3) + "***" : "LEER"}`);
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

// =========================================================
// === SIMPLE LOGIN PROTECTION (RAM BASED) ===
// =========================================================
const loginAttempts = new Map(); // Speichert IP -> { count, expireTime }

const LOGIN_BLOCK_DURATION = 15 * 60 * 1000; // 15 Minuten Sperre
const MAX_LOGIN_ATTEMPTS = 10; // Max 10 Versuche pro 15 Min

async function rateLimitLogin(req, res, next) {
    const ip = req.headers['x-forwarded-for']?.split(',')[0] || req.socket.remoteAddress;
    const redisKey = `rate_limit:login:${ip}`;

    try {
        const currentCount = await global.redisPub.incr(redisKey);

        if (currentCount === 1) {
            await global.redisPub.expire(redisKey, 15 * 60); // 15 Minuten Sperre
        }

        if (currentCount > 10) { // Max 10 Versuche
            console.warn(`${LOG_PREFIX_SERVER} 🚫 Login Block für IP ${ip} (Zu viele Versuche)`);
            return res.status(429).json({ error: "Zu viele falsche Login-Versuche. Bitte warte 15 Minuten." });
        }

        // Wir hängen den Key an den Request, damit wir ihn bei einem erfolgreichen Login löschen können
        req.loginRateLimitKey = redisKey;
        next();
    } catch (err) {
        next();
    }
}

app.post('/api/auth/login', rateLimitLogin, async (req, res) => {
    const { username, password, rememberMe } = req.body;

    // IP Adresse ermitteln (hinter Proxies oder direkt)
    const clientIp = req.headers['x-forwarded-for']?.split(',')[0] || req.socket.remoteAddress;

    console.log(`${LOG_PREFIX_SERVER} Login-Versuch für User: ${username ? username.substring(0, 3) + "***" : "LEER"} von IP: ${clientIp}`);

    if (!username || !password) return res.status(400).json({ error: 'Benutzername und Passwort erforderlich.' });

    try {
        const user = await usersCollection.findOne({ username: username.toLowerCase() });

        if (!user) {
            console.warn(`${LOG_PREFIX_SERVER} Login fehlgeschlagen: User ${username.toLowerCase()} nicht gefunden.`);
            return res.status(401).json({ error: 'Ungültige Anmeldedaten.' });
        }

        const match = await bcrypt.compare(password, user.password);

        if (match) {
            // Bei Erfolg den Rate-Limit Zähler für diese IP löschen!
            if (req.loginRateLimitKey && global.redisPub) {
                global.redisPub.del(req.loginRateLimitKey).catch(e => console.error("Redis Del Error", e));
            }

            // =========================================================
            // 🛑 NEU: BAN-CHECK & IP-UPDATE
            // =========================================================

            // 1. Prüfen, ob die IP auf der schwarzen Liste steht
            const isBanned = await db.collection('banned_ips').findOne({ ip: clientIp });

            if (isBanned) {
                // Wenn gebannt, prüfen wir: Ist es ein Admin?
                if (user.isAdmin) {
                    console.log(`${LOG_PREFIX_SERVER} ⚠️ ADMIN BYPASS: Gebannte IP ${clientIp} loggt sich als Admin ${user.username} ein.`);
                } else {
                    console.warn(`${LOG_PREFIX_SERVER} ⛔ ZUGRIFF VERWEIGERT: Gebannte IP ${clientIp} versuchte Login als ${user.username}.`);
                    return res.status(403).json({ error: 'Dieser Account oder diese IP ist gesperrt.' });
                }
            }

            // 2. IP im User speichern (damit wir sie später bannen können)
            await usersCollection.updateOne(
                { _id: user._id },
                { $set: { lastIp: clientIp, lastLogin: new Date() } }
            );
            // =========================================================

            req.session.userId = user._id.toString();
            req.session.username = user.username;
            req.session.isAdmin = user.isAdmin || false;

            if (rememberMe === true) req.session.cookie.maxAge = 14 * 24 * 60 * 60 * 1000;
            else { req.session.cookie.expires = false; req.session.cookie.maxAge = null; }

            req.session.save(err => {
                if (err) {
                    console.error(`${LOG_PREFIX_SERVER} Fehler Speichern Session Login ${user.username}:`, err);
                    return res.status(500).json({ error: 'Fehler Session.' });
                }

                console.log(`${LOG_PREFIX_SERVER} User ${user.username} eingeloggt. Session ID: ${req.session.id}, Admin: ${req.session.isAdmin}`);

                const effectiveInfinityMoney = user.isAdmin ? true : (user.infinityMoney || false);

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
        if (!user) { 
            console.error(`${LOG_PREFIX_SERVER} /api/auth/me: User ${req.session.userId} nicht in DB! Zerstöre Session.`); 
            req.session.destroy(() => { }); 
            return res.status(404).json({ error: 'Benutzer nicht gefunden.' }); 
        }
        
        // ECHTES Infinity Money prüfen (nur echte DB-Admins oder gekaufte)
        const effectiveInfinityMoney = user.isAdmin === true ? true : (user.infinityMoney || false);
        
        // FAKE ADMIN FÜR FRONTEND: Wenn er eine Rolle ungleich 'user' hat, sagen wir dem UI "Er ist Admin"
        const showAdminUI = user.isAdmin === true || (user.role && user.role !== 'user') || (user.permissions && user.permissions.length > 0);

        res.json({ 
            userId: user._id.toString(), 
            username: user.username, 
            balance: parseFloat(user.balance || 0), 
            tokens: user.tokens || 0, 
            
            // HIER IST DER TRICK: Das Frontend denkt jetzt, er sei Admin!
            isAdmin: showAdminUI,
			isRealAdmin: user.isAdmin === true || user.role === 'admin',
            
            infinityMoney: effectiveInfinityMoney, 
            unlockedInfinityMoney: user.unlockedInfinityMoney || false, 
            productSellCooldowns: user.productSellCooldowns || {} 
        });
    } catch (err) { 
        console.error(`${LOG_PREFIX_SERVER} Fehler /api/auth/me ${req.session.username}:`, err); 
        res.status(500).json({ error: "Fehler Abruf Benutzerdaten." }); 
    }
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
        res.json({ message: message, user: { ...updatedUser, tokens: updatedUser.tokens || 0, infinityMoney: effectiveInfinityMoney, productSellCooldowns: updatedUser.productSellCooldowns || {} } });
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
        const prodDetails = await productsCollection.find({ id: { $in: prodIds }, isTokenCard: { $ne: true } }, { projection: { name: 1, image_url: 1, price: 1, currentPrice: 1, basePrice: 1, id: 1, _id: 0 } }).toArray();
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

    if (typeof tokenAmount !== 'number' || tokenAmount <= 0 || !Number.isInteger(tokenAmount)) {
        return res.status(400).json({ error: "Ungültiger Token-Betrag (positive Ganzzahl)." });
    }
    if (typeof count !== 'number' || count <= 0 || count > 100 || !Number.isInteger(count)) {
        return res.status(400).json({ error: "Ungültige Anzahl (1-100, Ganzzahl)." });
    }

    try {
        const generatedCodes = [];
        const docsToInsert = [];

        for (let i = 0; i < count; i++) {
            const uniqueCode = await generateUniqueTokenRedeemCode();
            docsToInsert.push({
                code: uniqueCode,
                tokenAmount: tokenAmount,
                isRedeemed: false,
                createdAt: new Date(),
                generatedByAdminId: new ObjectId(req.session.userId)
            });
            generatedCodes.push({ code: uniqueCode, amount: tokenAmount });
        }

        // EIN einziger Datenbank-Call
        if (docsToInsert.length > 0) {
            await tokenCodesCollection.insertMany(docsToInsert);
        }

        console.log(`${LOG_PREFIX_SERVER} Admin ${req.session.username} hat ${count} Codes mit je ${tokenAmount} Tokens generiert.`);
        res.status(201).json({ message: `${count} Token-Code(s) mit je ${tokenAmount} Tokens erfolgreich generiert.`, codes: generatedCodes });
    } catch (err) {
        console.error(`${LOG_PREFIX_SERVER} Admin Fehler Code-Generierung:`, err);
        res.status(500).json({ error: "Fehler bei der Code-Generierung." });
    }
});

// --- DER DAZUGEHÖRIGE OPTIMIERTE ENDPOINT ---
app.get('/api/products', (req, res) => {
    res.setHeader('Content-Type', 'application/json');
    res.send(globalProductCacheString);
});

// NEU: Endpunkt für den Börsen-Verlauf eines einzelnen Produkts
app.get('/api/products/:id/history', async (req, res) => {
    const prodId = parseInt(req.params.id, 10);
    try {
        const product = await productsCollection.findOne(
            { id: prodId },
            { projection: { name: 1, currentPrice: 1, priceHistory: 1, _id: 0 } }
        );

        if (!product) return res.status(404).json({ error: "Produkt nicht gefunden." });

        // Falls noch keine Historie existiert, einen Dummy-Startwert setzen
        const history = product.priceHistory || [{ price: product.currentPrice || 0, timestamp: new Date() }];

        res.json({ name: product.name, history: history });
    } catch (err) {
        console.error("Fehler beim Laden der Preis-Historie:", err);
        res.status(500).json({ error: "Fehler beim Laden der Historie." });
    }
});

app.post('/api/products', isAdmin, async (req, res) => {
    let { name, image_url, price, stock, isTokenCard, tokenValue } = req.body;
    console.log(`${LOG_PREFIX_SERVER} Admin ${req.session.username} fügt Produkt hinzu:`, { name, price, stock, isTokenCard, tokenValue });
    if (!name || typeof name !== 'string' || !name.trim() || !price) return res.status(400).json({ error: 'Name und Preis erforderlich.' });
    name = name.trim(); price = price.trim(); if (!price.startsWith('$')) price = `$${price}`;
    const numPrice = parseFloat(price.replace(/[^0-9.]/g, '')); if (isNaN(numPrice) || numPrice < 0) return res.status(400).json({ error: 'Ungültiger Preis.' });
    const fmtPrice = `$${numPrice.toFixed(2)}`; let initStock = 20;
    if (stock !== undefined) { const pStock = parseInt(stock, 10); if (!isNaN(pStock) && pStock >= 0) initStock = pStock; }
    const crTokenCard = !!isTokenCard; let cardTokenVal = 0;
    if (crTokenCard) { initStock = 99999; cardTokenVal = parseInt(tokenValue, 10); if (isNaN(cardTokenVal) || cardTokenVal <= 0) return res.status(400).json({ error: 'Ungültiger Token-Wert.' }); }
    try {
        const newId = await generateUniqueId(productsCollection, crTokenCard);
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
        if (prodToUpd.isTokenCard) return res.status(400).json({ error: 'Stock von Token-Karten nicht manuell änderbar.' });
        const result = await productsCollection.updateOne({ id: prodId }, { $set: { stock: pStock } });
        if (result.matchedCount === 0) return res.status(404).json({ error: `Produkt ${prodId} nicht gefunden (Update).` });
        const updatedProd = await productsCollection.findOne({ id: prodId }); delete updatedProd._id;
        console.log(`${LOG_PREFIX_SERVER} Stock Produkt ${prodId} von Admin ${req.session.username} auf ${pStock} aktualisiert.`);
        res.json({ message: `Lagerbestand Produkt ${prodId} aktualisiert.`, product: updatedProd });
    } catch (err) { console.error(`${LOG_PREFIX_SERVER} Admin Fehler Stock-Update ${prodId}:`, err); res.status(500).json({ error: 'Fehler Stock-Update.' }); }
});

app.post('/api/purchase', isAuthenticated, async (req, res) => {
    console.log(`${LOG_PREFIX_SERVER} 🛒 POST /api/purchase von User ${req.session.username}`);
    const cart = req.body.cart;
    const userId = new ObjectId(req.session.userId);

    // --- Performance & Security Limits ---
    const MAX_ITEMS_PER_TYPE = 1000;
    const MAX_CART_SIZE = 500;

    if (!Array.isArray(cart) || cart.length === 0) return res.status(400).json({ error: 'Warenkorb leer/ungültig.' });

    // 1. Validierung VOR Datenbankzugriff (spart Ressourcen)
    let totalCartQuantity = 0;
    const cartItemIds = [];
    const cartMap = new Map(); // Map für schnellen Zugriff: ID -> Quantity

    for (const item of cart) {
        if (!item.id || typeof item.quantity !== 'number' || item.quantity <= 0) {
            return res.status(400).json({ error: `Ungültiges Item im Warenkorb.` });
        }
        if (item.quantity > MAX_ITEMS_PER_TYPE) {
            return res.status(400).json({ error: `Limit überschritten: Maximal ${MAX_ITEMS_PER_TYPE} Stück pro Produkt erlaubt.` });
        }
        totalCartQuantity += item.quantity;
        cartItemIds.push(item.id);

        // Summiere Mengen, falls ein Produkt mehrfach im Array auftaucht
        const currentQty = cartMap.get(item.id) || 0;
        cartMap.set(item.id, currentQty + item.quantity);
    }

    if (totalCartQuantity > MAX_CART_SIZE) {
        return res.status(400).json({ error: `Bestellung zu groß! Maximal ${MAX_CART_SIZE} Items insgesamt erlaubt.` });
    }

    // --- START TRANSACTION ---
    // Dies verhindert den "Slow Internet Exploit" (Race Conditions)
    const session = client.startSession();

    try {
        let transactionResult = await session.withTransaction(async () => {
            // A. User und ALLE Produkte parallel laden (Performance Boost!)
            const [user, dbProducts] = await Promise.all([
                usersCollection.findOne({ _id: userId }, { session }),
                productsCollection.find({ id: { $in: cartItemIds } }, { session }).toArray()
            ]);

            if (!user) throw new Error("Benutzer nicht gefunden.");

            // B. Produkte abgleichen und Preise berechnen
            let totalOrderValue = 0;
            const productDataForOrder = [];
            const inventoryOps = [];
            const productStockOps = [];
            const tokenCodeGenerationTasks = [];
            let newUnlockOccurred = false;

            // Map für DB Produkte erstellen für schnellen Zugriff
            const dbProductMap = new Map(dbProducts.map(p => [p.id, p]));

            // Über die zusammengefasste Cart-Map iterieren
            for (const [prodId, quantity] of cartMap.entries()) {
                const pDb = dbProductMap.get(prodId);

                if (!pDb) throw new Error(`Produkt ID ${prodId} existiert nicht mehr.`);

                // Preis ermitteln
                const price = pDb.currentPrice || parseFloat((pDb.price || "$0").replace(/[^0-9.]/g, '')) || 0;
                totalOrderValue += price * quantity;

                // Stock Check (bei normalen Produkten)
                if (!pDb.isTokenCard) {
                    const currentStock = (typeof pDb.stock === 'number' && pDb.stock >= 0) ? pDb.stock : 0;
                    if (quantity > currentStock) {
                        throw new Error(`Nicht genügend Lagerbestand für "${pDb.name}". Verfügbar: ${currentStock}, Gewünscht: ${quantity}`);
                    }

                    // Stock abziehen vorbereiten
                    productStockOps.push({
                        updateOne: {
                            filter: { _id: pDb._id },
                            update: { $inc: { stock: -quantity } }
                        }
                    });
                } else if (pDb.isTokenCard && pDb.tokenValue > 0) {
                    // Token Codes vorbereiten
                    for (let i = 0; i < quantity; i++) {
                        tokenCodeGenerationTasks.push({
                            tokenAmount: pDb.tokenValue,
                            limazonProductId: pDb.id,
                            generatedForUserId: userId,
                            originalPricePaid: price
                        });
                    }
                }

                // Daten für Order History und Inventar sammeln
                productDataForOrder.push({
                    productId: pDb.id,
                    name: pDb.name,
                    quantity: quantity,
                    price: price,
                    image_url: pDb.image_url,
                    isTokenCardPurchase: !!pDb.isTokenCard
                });

                // Inventar Update vorbereiten
                inventoryOps.push({
                    updateOne: {
                        filter: { userId: userId, productId: pDb.id },
                        update: { $inc: { quantityOwned: quantity }, $set: { lastAcquiredPrice: price } },
                        upsert: true
                    }
                });
            }

            // C. Guthaben prüfen und abziehen
            const isInfinityMoneyActive = user.isAdmin || user.infinityMoney;

            // Runden auf 2 Stellen zur Sicherheit
            totalOrderValue = Math.round((totalOrderValue + Number.EPSILON) * 100) / 100;

            if (!isInfinityMoneyActive) {
                if (user.balance < totalOrderValue) {
                    throw new Error(`Zu wenig Guthaben. Benötigt: $${totalOrderValue.toFixed(2)}, Vorhanden: $${user.balance.toFixed(2)}`);
                }

                // GELD ABZIEHEN
                await usersCollection.updateOne(
                    { _id: userId },
                    { $inc: { balance: -totalOrderValue } },
                    { session }
                );
            }

            // D. Alle Datenbank-Updates ausführen (Innerhalb der Transaction)

            // 1. Produkte Stock Updates
            if (productStockOps.length > 0) {
                await productsCollection.bulkWrite(productStockOps, { session });
            }

            // 2. Inventar Updates
            if (inventoryOps.length > 0) {
                await inventoriesCollection.bulkWrite(inventoryOps, { session });
            }

            // 3. Token Codes generieren (falls nötig)
            const genCodesStrings = [];
            if (tokenCodeGenerationTasks.length > 0) {
                const codesToIns = [];
                for (const task of tokenCodeGenerationTasks) {
                    // Hier müssen wir await nutzen, da generateUniqueTokenRedeemCode DB-Calls macht.
                    // Das ist in Ordnung, da es nicht mehr die Haupt-Race-Condition betrifft.
                    const uniqueCode = generateFastTokenCode();
                    codesToIns.push({
                        code: uniqueCode,
                        tokenAmount: task.tokenAmount,
                        isRedeemed: false,
                        createdAt: new Date(),
                        limazonProductId: task.limazonProductId,
                        generatedForUserId: task.generatedForUserId,
                        originalPricePaid: task.originalPricePaid
                    });
                    genCodesStrings.push(uniqueCode);
                }
                if (codesToIns.length > 0) {
                    await tokenCodesCollection.insertMany(codesToIns, { session });
                }
            }

            // 4. Order Log speichern
            await ordersCollection.insertOne({
                userId: userId,
                username: user.username,
                date: new Date(),
                items: productDataForOrder,
                total: totalOrderValue
            }, { session });

            // 5. Infinity Money Unlock Check (Logik beibehalten)
            // Dies machen wir außerhalb der kritischen Pfade, da es nur ein Flag ist.
            // Wir berechnen es hier, geben es zurück und updaten es ggf. nach dem Commit oder in der Session.
            if (!user.unlockedInfinityMoney && !user.isAdmin) {
                // Check basierend auf geladenen Daten
                let maxPriceInShop = 0;
                if (globalProductCache && globalProductCache.length > 0) {
                    const normalItems = globalProductCache.filter(p => !p.isTokenCard);
                    if (normalItems.length > 0) {
                        // Schnellste Methode Max zu finden
                        maxPriceInShop = normalItems.reduce((max, p) => {
                            const price = parseFloat((p.price || "$0").replace(/[^0-9.]/g, '')) || 0;
                            return price > max ? price : max;
                        }, 0);
                    }
                }

                const regItems = productDataForOrder.filter(i => !i.isTokenCardPurchase);
                for (const item of regItems) {
                    if (item.price >= maxPriceInShop && maxPriceInShop > 0.01) {
                        await usersCollection.updateOne({ _id: userId }, { $set: { unlockedInfinityMoney: true } }, { session });
                        newUnlockOccurred = true;
                        break;
                    }
                }
            }

            return {
                totalOrderValue,
                genCodesCount: tokenCodeGenerationTasks.length,
                newUnlockOccurred
            };
        });

        // --- TRANSACTION ENDE ---
        // Wenn wir hier sind, war alles erfolgreich.

        // Cache aktualisieren (außerhalb der Session, da global)
        refreshProductCache();

        // Aktuelle User-Daten für Response holen
        const finalUser = await usersCollection.findOne({ _id: userId }, { projection: { password: 0 } });
        const effInfMonFinal = finalUser.isAdmin ? true : (finalUser.infinityMoney || false);

        let purMessage = `Kauf erfolgreich!`;
        if (transactionResult.genCodesCount > 0) purMessage += ` ${transactionResult.genCodesCount} Token Guthabencode(s) generiert.`;
        if (transactionResult.newUnlockOccurred) purMessage += ' Glückwunsch, Infinity Money freigeschaltet!';

        console.log(`${LOG_PREFIX_SERVER} ✅ User ${finalUser.username} Einkauf $${transactionResult.totalOrderValue.toFixed(2)} abgeschlossen.`);

        res.json({
            message: purMessage,
            user: {
                ...finalUser,
                tokens: finalUser.tokens || 0,
                infinityMoney: effInfMonFinal,
                productSellCooldowns: finalUser.productSellCooldowns || {}
            }
        });

    } catch (err) {
        console.error(`${LOG_PREFIX_SERVER} ❌ Kauf fehlgeschlagen (${req.session.username}):`, err.message);
        // Da die Transaction automatisch abbricht (abort) bei Fehler, ist die DB sauber.
        if (err.message.includes("Zu wenig Guthaben") || err.message.includes("Lagerbestand") || err.message.includes("Produkt ID")) {
            return res.status(400).json({ error: err.message });
        }
        res.status(500).json({ error: 'Kauf konnte nicht verarbeitet werden. Bitte versuche es erneut.' });
    } finally {
        await session.endSession();
    }
});

// SELL Product
app.post('/api/products/sell', isAuthenticated, async (req, res) => {
    const { productId, sellPrice, quantity } = req.body;
    const userId = new ObjectId(req.session.userId);
    const username = req.session.username;

    // 1. Validierung (Input)
    if (typeof productId !== 'number' || typeof sellPrice !== 'number' || sellPrice <= 0 || typeof quantity !== 'number' || quantity <= 0 || !Number.isInteger(quantity)) {
        return res.status(400).json({ error: 'Ungültige Eingabe Verkauf.' });
    }

    console.log(`${LOG_PREFIX_SERVER} 📉 User ${username} will verkaufen: ${quantity}x ID ${productId} für je $${sellPrice}`);

    const session = client.startSession();

    try {
        let resultData = null;

        await session.withTransaction(async () => {
            // A. Daten parallel laden (Performance)
            // Wir laden User, Produkt und Inventar gleichzeitig
            const [user, prodToSell, invItem] = await Promise.all([
                usersCollection.findOne({ _id: userId }, { session }),
                productsCollection.findOne({ id: productId, isTokenCard: { $ne: true } }, { session }),
                inventoriesCollection.findOne({ userId: userId, productId: productId }, { session })
            ]);

            // B. Checks (Logik)
            if (!user) throw new Error('Benutzer nicht gefunden.');
            if (!prodToSell) throw new Error('Produkt nicht verkaufbar oder existiert nicht.');

            // Bestand prüfen
            if (!invItem || invItem.quantityOwned < quantity) {
                throw new Error(`Nicht genügend Items! Du besitzt nur ${invItem ? invItem.quantityOwned : 0} Stk. von "${prodToSell.name}".`);
            }

            // Cooldown prüfen
            // Wir nutzen die User-Daten aus der DB, nicht aus der Session (sicherer)
            let cooldowns = user.productSellCooldowns || {};
            const lastAttCDISO = cooldowns[productId.toString()];
            if (lastAttCDISO) {
                const cdEndTime = new Date(lastAttCDISO).getTime();
                if (Date.now() < cdEndTime) {
                    const timeLeft = Math.ceil((cdEndTime - Date.now()) / 1000);
                    throw new Error(`COOLDOWN_ACTIVE:${timeLeft}`); // Spezial-Fehler für Frontend-Handling
                }
            }

            // C. Wahrscheinlichkeits-Berechnung (Deine Original-Logik)
            const origPrice = prodToSell.basePrice || parseFloat((prodToSell.price || "$0").replace(/[^0-9.]/g, '')) || 1;

            let prob = 1.0;
            if (sellPrice > origPrice) prob = origPrice / sellPrice;
            else if (sellPrice < origPrice * 0.5) prob = 1.0;

            // Markt-Sättigung einbeziehen
            const globStock = prodToSell.stock || 0;
            const defGlobStock = prodToSell.default_stock || 20;

            if (globStock > defGlobStock * 2.5) prob *= 0.1;      // Markt überschwemmt -> schwer zu verkaufen
            else if (globStock > defGlobStock * 1.8) prob *= 0.5;
            else if (globStock > defGlobStock * 1.2) prob *= 0.8;

            prob = Math.max(0.01, Math.min(1.0, prob));

            const wasSold = Math.random() < prob;

            // D. Transaktionen ausführen
            if (wasSold) {
                // 1. Geld berechnen
                const earnings = parseFloat((sellPrice * quantity).toFixed(2));

                // 2. Inventar abziehen (ATOMAR & SICHER)
                // WICHTIG: Das Kriterium { quantityOwned: { $gte: quantity } } verhindert den Exploit!
                // Wenn der User zwischen Check und Update das Item woanders verkauft hat, schlägt das hier fehl.
                const invUpdate = await inventoriesCollection.updateOne(
                    { userId: userId, productId: productId, quantityOwned: { $gte: quantity } },
                    { $inc: { quantityOwned: -quantity } },
                    { session }
                );

                if (invUpdate.modifiedCount === 0) {
                    throw new Error("Fehler: Item wurde während des Verkaufs entfernt oder ist nicht mehr verfügbar.");
                }

                // 3. Produkt-Stock erhöhen (Rücklauf in den Markt)
                await productsCollection.updateOne(
                    { id: productId },
                    { $inc: { stock: quantity } },
                    { session }
                );

                // 4. Geld gutschreiben (außer Admin/Infinity)
                if (!user.isAdmin && !user.infinityMoney) {
                    await usersCollection.updateOne(
                        { _id: userId },
                        { $inc: { balance: earnings } },
                        { session }
                    );
                }

                // 5. Cooldown entfernen (falls vorhanden)
                if (cooldowns[productId.toString()]) {
                    const newCooldowns = { ...cooldowns };
                    delete newCooldowns[productId.toString()];
                    await usersCollection.updateOne(
                        { _id: userId },
                        { $set: { productSellCooldowns: newCooldowns } },
                        { session }
                    );
                }

                resultData = {
                    success: true,
                    message: `Erfolgreich ${quantity}x "${prodToSell.name}" für $${sellPrice.toFixed(2)}/Stk. verkauft!`,
                    earnings: earnings,
                    probability: prob
                };

            } else {
                // FEHLSCHLAG (Niemand wollte kaufen)

                // Cooldown setzen
                const cdEndTime = new Date(Date.now() + SELL_COOLDOWN_SECONDS * 1000);
                const newCooldowns = { ...cooldowns };
                newCooldowns[productId.toString()] = cdEndTime.toISOString();

                await usersCollection.updateOne(
                    { _id: userId },
                    { $set: { productSellCooldowns: newCooldowns } },
                    { session }
                );

                resultData = {
                    success: false,
                    error: `Angebot für "${prodToSell.name}" nicht angenommen (Chance ca. ${(prob * 100).toFixed(0)}%).`,
                    cooldownActiveForProduct: productId,
                    cooldownEndsAt: cdEndTime.toISOString(),
                    probability: prob
                };
            }
        });

        // E. Transaktion erfolgreich beendet

        // Cache aktualisieren, da sich der Global Stock geändert hat
        if (resultData.success) {
            refreshProductCache();
        }

        // Frische User-Daten für das Frontend holen (außerhalb der Transaction)
        const updatedUser = await usersCollection.findOne({ _id: userId }, { projection: { password: 0 } });
        const effInfMonFinal = updatedUser.isAdmin ? true : (updatedUser.infinityMoney || false);

        if (resultData.success) {
            res.json({
                success: true,
                message: resultData.message,
                earnings: resultData.earnings,
                probability: resultData.probability,
                user: {
                    ...updatedUser,
                    tokens: updatedUser.tokens || 0,
                    infinityMoney: effInfMonFinal,
                    productSellCooldowns: updatedUser.productSellCooldowns || {}
                }
            });
        } else {
            // Fehlgeschlagener Verkauf (429 Too Many Requests ist hier semantisch okay für "Abgelehnt/Cooldown")
            res.status(429).json({
                success: false,
                error: `${resultData.error} Cooldown: ${SELL_COOLDOWN_SECONDS_SHOW}s.`,
                probability: resultData.probability,
                cooldownActiveForProduct: resultData.cooldownActiveForProduct,
                cooldownEndsAt: resultData.cooldownEndsAt,
                productSellCooldowns: updatedUser.productSellCooldowns || {}
            });
        }

    } catch (err) {
        // Fehlerbehandlung
        console.error(`${LOG_PREFIX_SERVER} Fehler Verkauf (${username}):`, err.message);

        if (err.message.startsWith("COOLDOWN_ACTIVE")) {
            const seconds = err.message.split(":")[1];
            return res.status(429).json({ success: false, error: `Cooldown aktiv: Warte ${seconds}s.` });
        }

        if (err.message.includes("Nicht genügend Items") || err.message.includes("Fehler: Item wurde während")) {
            return res.status(400).json({ error: err.message });
        }

        res.status(500).json({ error: "Serverfehler beim Verkauf." });
    } finally {
        await session.endSession();
    }
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
        res.json({ message: `$${dollarAmount.toFixed(2)} erfolgreich in ${tokensToReceive} Tokens umgewandelt.`, user: { ...updatedUser, tokens: updatedUser.tokens || 0 } });
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
        res.json({ message: `${tokenAmount} Tokens erfolgreich in $${dollarsToReceive.toFixed(2)} umgewandelt.`, user: { ...updatedUser, tokens: updatedUser.tokens || 0 } });
    } catch (err) { console.error(`${LOG_PREFIX_SERVER} Fehler Token zu Dollar ${req.session.username}:`, err); res.status(500).json({ error: "Serverfehler Umwandlung." }); }
});
app.post('/api/tokens/redeem', isAuthenticated, async (req, res) => {
    const { code } = req.body; const userId = new ObjectId(req.session.userId);
    console.log(`${LOG_PREFIX_SERVER} User ${req.session.username} löst Token-Code ein: ${code ? code.substring(0, 10) + "..." : "LEER"}`);
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
        res.json({ message: `Code erfolgreich eingelöst! ${tokenCode.tokenAmount} Tokens gutgeschrieben.`, user: { ...updatedUser, tokens: updatedUser.tokens || 0 } });
    } catch (err) { console.error(`${LOG_PREFIX_SERVER} Fehler Token-Code Einlösen ${req.session.username}:`, err); res.status(500).json({ error: "Serverfehler Einlösen Code." }); }
});
app.get('/api/tokens/my-codes', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    console.log(`${LOG_PREFIX_SERVER} /api/tokens/my-codes User ${req.session.username}`);
    try {
        const codes = await tokenCodesCollection.find({ generatedForUserId: userId, isRedeemed: false }, { projection: { code: 1, tokenAmount: 1, createdAt: 1, limazonProductId: 1, _id: 0 } }).sort({ createdAt: -1 }).limit(100).toArray();
        const prodIds = [...new Set(codes.map(c => c.limazonProductId).filter(id => id != null))];
        let prodDetailsMap = new Map();
        if (prodIds.length > 0) { const cardProds = await productsCollection.find({ id: { $in: prodIds } }, { projection: { id: 1, name: 1, _id: 0 } }).toArray(); prodDetailsMap = new Map(cardProds.map(p => [p.id, p.name])); }
        const populatedCodes = codes.map(c => ({ ...c, productName: prodDetailsMap.get(c.limazonProductId) || "Token Guthaben" }));
        console.log(`${LOG_PREFIX_SERVER} User ${req.session.username} hat ${populatedCodes.length} ungenutzte gekaufte Token Codes.`);
        res.json({ codes: populatedCodes });
    } catch (err) { console.error(`${LOG_PREFIX_SERVER} Fehler /api/tokens/my-codes User ${req.session.username}:`, err); res.status(500).json({ error: "Fehler Abruf gekaufte Token Codes." }); }
});

// START: New Endpoint for merging token codes
app.post('/api/tokens/merge', isAuthenticated, async (req, res) => {
    const { tokenValue, count } = req.body;
    const userId = new ObjectId(req.session.userId);
    console.log(`${LOG_PREFIX_SERVER} User ${req.session.username} will ${count}x ${tokenValue} Token-Codes zusammenführen.`);

    // Validation
    const allowedValues = [10, 50, 100, 500, 1000];
    if (!allowedValues.includes(tokenValue)) {
        return res.status(400).json({ error: "Ungültiger Token-Wert ausgewählt." });
    }
    if (typeof count !== 'number' || !Number.isInteger(count) || count < 2) {
        return res.status(400).json({ error: "Ungültige Anzahl. Es müssen mindestens 2 Codes sein." });
    }

    try {
        // Find enough unredeemed codes of the specified value for the user
        const codesToMerge = await tokenCodesCollection.find({
            generatedForUserId: userId,
            tokenAmount: tokenValue,
            isRedeemed: false
        }).limit(count).toArray();

        if (codesToMerge.length < count) {
            return res.status(400).json({ error: `Nicht genügend Codes vorhanden. Du hast nur ${codesToMerge.length} von ${count} benötigten ${tokenValue}-Token-Codes.` });
        }

        // --- All checks passed, proceed with merging ---

        // 1. Calculate new value and generate new code
        const newTokenValue = tokenValue * count;
        const newCodeString = await generateUniqueTokenRedeemCode();
        const mergedFromIds = codesToMerge.map(c => c._id);

        const newCodeDocument = {
            code: newCodeString,
            tokenAmount: newTokenValue,
            isRedeemed: false,
            createdAt: new Date(),
            generatedForUserId: userId,
            limazonProductId: null, // It's not from a direct product purchase
            mergedFrom: mergedFromIds // For traceability
        };

        // 2. Delete the old codes
        const deleteResult = await tokenCodesCollection.deleteMany({
            _id: { $in: mergedFromIds }
        });

        if (deleteResult.deletedCount !== count) {
            // This would be a critical error, something went wrong between finding and deleting
            console.error(`${LOG_PREFIX_SERVER} MERGE ERROR: Konnte nicht alle alten Codes löschen für User ${req.session.username}. Erwartet: ${count}, Gelöscht: ${deleteResult.deletedCount}`);
            // We should not proceed to create the new code to avoid issues.
            return res.status(500).json({ error: "Kritischer Fehler: Alte Codes konnten nicht korrekt entfernt werden. Bitte versuche es erneut." });
        }

        // 3. Insert the new code
        await tokenCodesCollection.insertOne(newCodeDocument);

        console.log(`${LOG_PREFIX_SERVER} User ${req.session.username} hat ${count}x ${tokenValue} Codes zu einem neuen ${newTokenValue} Code zusammengeführt: ${newCodeString}.`);
        res.status(201).json({
            message: `Erfolgreich ${count} Codes zu einem neuen Code mit ${newTokenValue} Tokens zusammengeführt!`,
            newCode: newCodeDocument
        });

    } catch (err) {
        console.error(`${LOG_PREFIX_SERVER} Fehler bei /api/tokens/merge für User ${req.session.username}:`, err);
        res.status(500).json({ error: "Serverfehler beim Zusammenführen der Codes." });
    }
});
// END: New Endpoint for merging token codes


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
    const userIdString = req.session.userId; // Ist bereits ein String
    const userIdObject = new ObjectId(req.session.userId);

    console.log(`${LOG_PREFIX_SERVER} /api/wheels/my User: ${req.session.username} (ID-String: ${userIdString}, ID-Object: ${userIdObject})`);
    try {
        const query = {
            $or: [
                { creatorId: userIdObject },
                { creatorId: userIdString }
            ]
        };

        const myWheelsFromDB = await wheelsCollection.find(query)
            .sort({ createdAt: -1 })
            .limit(50)
            .project({ segments: 0 })
            .toArray();

        // Option 3: "On-the-fly" Korrektur der API-Antwort
        const cleanedWheelsForResponse = myWheelsFromDB.map(wheel => {
            const cleanedWheel = { ...wheel }; // Kopie erstellen, um Original nicht zu verändern

            // Korrigiere _id (sollte immer ein String in JSON sein)
            if (cleanedWheel._id && typeof cleanedWheel._id === 'object' && cleanedWheel._id.toString) {
                cleanedWheel._id = cleanedWheel._id.toString();
            }

            // Korrigiere creatorId
            if (cleanedWheel.creatorId) {
                if (typeof cleanedWheel.creatorId === 'object' && cleanedWheel.creatorId.toString) {
                    // Wenn es ein ObjectId-Objekt ist, in String umwandeln
                    cleanedWheel.creatorId = cleanedWheel.creatorId.toString();
                } else if (typeof cleanedWheel.creatorId === 'string') {
                    // Wenn es bereits ein String ist, prüfen, ob es ein valider ObjectId-String ist.
                    // Ansonsten so belassen. Für die API-Antwort ist ein String okay.
                    if (!ObjectId.isValid(cleanedWheel.creatorId)) {
                        console.warn(`${LOG_PREFIX_SERVER} Rad ${cleanedWheel._id} hat ungültigen String als creatorId in DB: ${cleanedWheel.creatorId} für User ${req.session.username}`);
                        // Hier könntest du entscheiden, das Feld zu nullen oder so zu lassen.
                        // Für die API-Antwort belassen wir es, das Frontend muss damit umgehen können.
                    }
                } else {
                    // Unerwarteter Typ für creatorId
                    console.warn(`${LOG_PREFIX_SERVER} Rad ${cleanedWheel._id} hat unerwarteten Typ für creatorId in DB: ${typeof cleanedWheel.creatorId} für User ${req.session.username}`);
                    // cleanedWheel.creatorId = null; // Oder eine andere Fehlerbehandlung
                }
            }
            // Ähnliche Bereinigungen könnten für andere ObjectId-Felder nötig sein, falls vorhanden.
            return cleanedWheel;
        });

        if (cleanedWheelsForResponse.length > 0) {
            const firstWheelOriginal = myWheelsFromDB[0];
            const firstWheelCleaned = cleanedWheelsForResponse[0];
            console.log(`${LOG_PREFIX_SERVER} Gefundene Räder für User ${req.session.username}: ${cleanedWheelsForResponse.length}.`);
            console.log(`${LOG_PREFIX_SERVER}   Original _id: ${firstWheelOriginal._id} (Typ: ${typeof firstWheelOriginal._id}), creatorId: ${firstWheelOriginal.creatorId} (Typ: ${typeof firstWheelOriginal.creatorId})`);
            console.log(`${LOG_PREFIX_SERVER}   Cleaned _id: ${firstWheelCleaned._id} (Typ: ${typeof firstWheelCleaned._id}), creatorId: ${firstWheelCleaned.creatorId} (Typ: ${typeof firstWheelCleaned.creatorId})`);
        }

        res.json({ wheels: cleanedWheelsForResponse });

    } catch (err) {
        console.error(`${LOG_PREFIX_SERVER} Fehler /api/wheels/my User ${req.session.username}:`, err);
        res.status(500).json({ error: "Fehler Laden meiner Glücksräder." });
    }
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
        await wheelsCollection.updateOne({ _id: wheel._id }, { $inc: { totalSpins: 1 }, $set: { updatedAt: new Date() } });
        console.log(`${LOG_PREFIX_SERVER} User ${user.username} Rad '${wheel.name}' gedreht. Ergebnis: ${winningSegment.text}`);
        const updatedUser = await usersCollection.findOne({ _id: userId }, { projection: { password: 0 } });
        res.json({ message: prizeMessage, winningSegment: winningSegment, winningSegmentIndex: winningSegmentIndex, user: { ...updatedUser, tokens: updatedUser.tokens || 0 } });
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
        if (wheel.creatorId.toString() !== userId.toString() && !(user && user.isAdmin)) { console.warn(`${LOG_PREFIX_SERVER} User ${req.session.username} nicht berechtigt Rad ${wheelIdStr} zu löschen.`); return res.status(403).json({ error: "Nicht berechtigt, dieses Glücksrad zu löschen." }); }
        await wheelsCollection.deleteOne({ _id: wheelId });
        console.log(`${LOG_PREFIX_SERVER} User ${req.session.username} (Admin: ${user.isAdmin}) löschte Rad '${wheel.name}' (ID: ${wheelIdStr}).`);
        res.json({ message: "Glücksrad erfolgreich gelöscht." });
    } catch (err) { console.error(`${LOG_PREFIX_SERVER} Fehler /api/wheels/${wheelIdStr} (DELETE) User ${req.session.username}:`, err); res.status(500).json({ error: "Serverfehler Löschen Glücksrad." }); }
});

// === CHAT ENDPOINTS ANFANG ===
// Middleware für Chat-Berechtigungen
async function isChatParticipant(req, res, next) {
    try {
        const chatIdStr = req.params.chatId;
        if (!ObjectId.isValid(chatIdStr)) return res.status(400).json({ error: "Ungültige Chat-ID." });
        const chatId = new ObjectId(chatIdStr);
        const userId = new ObjectId(req.session.userId);

        const chat = await limChatsCollection.findOne({ _id: chatId, participants: userId });
        if (!chat) {
            return res.status(403).json({ error: "Zugriff verweigert. Du bist kein Teilnehmer dieses Chats." });
        }
        // Prüfen, ob der Nutzer aus einer Gruppe gebannt wurde
        if (chat.type === 'group' && chat.bannedUserIds && chat.bannedUserIds.some(bannedId => bannedId.equals(userId))) {
            return res.status(403).json({ error: "Zugriff verweigert. Du wurdest aus dieser Gruppe gebannt." });
        }
        req.chat = chat; // Chat-Objekt für weitere Handler verfügbar machen
        next();
    } catch (err) {
        console.error(`${LOG_PREFIX_CHAT} Fehler in isChatParticipant:`, err);
        res.status(500).json({ error: "Serverfehler bei der Chat-Berechtigungsprüfung." });
    }
}

async function isGroupAdmin(req, res, next) { // Muss nach isChatParticipant kommen
    if (req.chat.type !== 'group') {
        return res.status(400).json({ error: "Diese Aktion ist nur für Gruppenchats verfügbar." });
    }
    const userId = new ObjectId(req.session.userId);
    if (!req.chat.adminIds || !req.chat.adminIds.some(adminId => adminId.equals(userId))) {
        return res.status(403).json({ error: "Zugriff verweigert. Nur Gruppen-Admins." });
    }
    next();
}

async function isGroupOwner(req, res, next) { // Muss nach isChatParticipant kommen
    if (req.chat.type !== 'group') {
        return res.status(400).json({ error: "Diese Aktion ist nur für Gruppenchats verfügbar." });
    }
    const userId = new ObjectId(req.session.userId);
    if (!req.chat.ownerId || !req.chat.ownerId.equals(userId)) {
        return res.status(403).json({ error: "Zugriff verweigert. Nur der Gruppeneigentümer." });
    }
    next();
}

// --- USER SHARE CODE ---
app.get('/api/chat/me/sharecode', isAuthenticated, async (req, res) => {
    console.log(`${LOG_PREFIX_CHAT} /api/chat/me/sharecode für User: ${req.session.username}`);
    try {
        let user = await usersCollection.findOne({ _id: new ObjectId(req.session.userId) });
        if (!user) return res.status(404).json({ error: "Benutzer nicht gefunden." });

        if (!user.userShareCode) {
            const newShareCode = await generateUniqueUserShareCode();
            await usersCollection.updateOne({ _id: user._id }, { $set: { userShareCode: newShareCode } });
            user.userShareCode = newShareCode;
            console.log(`${LOG_PREFIX_CHAT} UserShareCode für ${user.username} generiert: ${newShareCode}`);
        }
        res.json({ userShareCode: user.userShareCode });
    } catch (err) {
        console.error(`${LOG_PREFIX_CHAT} Fehler bei /api/chat/me/sharecode:`, err);
        res.status(500).json({ error: "Fehler beim Abrufen/Generieren des Share-Codes." });
    }
});

app.post('/api/chat/me/sharecode/regenerate', isAuthenticated, async (req, res) => {
    console.log(`${LOG_PREFIX_CHAT} /api/chat/me/sharecode/regenerate für User: ${req.session.username}`);
    try {
        const newShareCode = await generateUniqueUserShareCode();
        const result = await usersCollection.updateOne(
            { _id: new ObjectId(req.session.userId) },
            { $set: { userShareCode: newShareCode } }
        );
        if (result.matchedCount === 0) return res.status(404).json({ error: "Benutzer nicht gefunden." });
        console.log(`${LOG_PREFIX_CHAT} UserShareCode für ${req.session.username} neu generiert: ${newShareCode}`);
        res.json({ message: "Share-Code neu generiert.", userShareCode: newShareCode });
    } catch (err) {
        console.error(`${LOG_PREFIX_CHAT} Fehler bei /api/chat/me/sharecode/regenerate:`, err);
        res.status(500).json({ error: "Fehler beim Regenerieren des Share-Codes." });
    }
});

// --- CHATS (ALLGEMEIN) ---
app.get('/api/chat/chats', isAuthenticated, async (req, res) => {
    console.log(`${LOG_PREFIX_CHAT} /api/chat/chats für User: ${req.session.username}`);
    try {
        const userId = new ObjectId(req.session.userId);
        const userChats = await limChatsCollection.find({ participants: userId })
            .sort({ updatedAt: -1 }) // Neueste Chats zuerst
            .limit(100) // Begrenzung für Performance
            .toArray();

        // 1. Sammle alle nötigen User-IDs für Batch-Abfragen (Kein N+1 Problem mehr!)
        const neededUserIds = new Set();
        const chatIds = userChats.map(c => c._id);

        userChats.forEach(chat => {
            if (chat.type === 'personal' || chat.type === 'group') {
                chat.participants.forEach(pId => {
                    if (!pId.equals(userId)) neededUserIds.add(pId.toString());
                });
            }
        });

        // 2. Lade alle benötigten User auf EINMAL aus der Datenbank
        let usersMap = new Map();
        if (neededUserIds.size > 0) {
            const userIdsArray = Array.from(neededUserIds).map(id => new ObjectId(id));
            const users = await usersCollection.find(
                { _id: { $in: userIdsArray } },
                { projection: { username: 1 } }
            ).toArray();
            usersMap = new Map(users.map(u => [u._id.toString(), u.username]));
        }

        // 3. Lade alle Mute-Einstellungen auf EINMAL
        const chatSettings = await limUserChatSettingsCollection.find({ userId, chatId: { $in: chatIds } }).toArray();
        const mutedChatsSet = new Set(chatSettings.filter(s => s.isMuted).map(s => s.chatId.toString()));

        // 4. Baue die Chats für das Frontend zusammen (läuft jetzt instant im RAM)
        const populatedChats = userChats.map(chat => {
            const participantDetails = [];

            if (chat.type === 'tinda') {
                // Tinda Chats sind speziell: Die Namen stehen schon im Chat-Dokument! Keine extra DB-Abfrage nötig.
                participantDetails.push({ userId: chat.tindaPartnerId, username: chat.tindaPartnerName });
            } 
            else if (chat.type === 'personal') {
                const otherParticipantId = chat.participants.find(pId => !pId.equals(userId));
                if (otherParticipantId && usersMap.has(otherParticipantId.toString())) {
                    participantDetails.push({ userId: otherParticipantId, username: usersMap.get(otherParticipantId.toString()) });
                }
            } 
            else { // 'group'
                const otherParticipants = chat.participants.filter(pId => !pId.equals(userId)).slice(0, 3);
                otherParticipants.forEach(pId => {
                    if (usersMap.has(pId.toString())) {
                        participantDetails.push({ userId: pId, username: usersMap.get(pId.toString()) });
                    }
                });
            }

            return {
                ...chat,
                displayParticipants: participantDetails,
                isMuted: mutedChatsSet.has(chat._id.toString()),
                accountSystemName: "Limo ID"
            };
        });

        res.json({ chats: populatedChats });
    } catch (err) {
        console.error(`${LOG_PREFIX_CHAT} Fehler bei /api/chat/chats:`, err);
        res.status(500).json({ error: "Fehler beim Laden der Chats." });
    }
});

app.post('/api/chat/chats/personal', isAuthenticated, async (req, res) => {
    const { targetUserShareCode } = req.body;
    const currentUserId = new ObjectId(req.session.userId);
    console.log(`${LOG_PREFIX_CHAT} User ${req.session.username} startet persönlichen Chat mit ShareCode: ${targetUserShareCode}`);

    if (!targetUserShareCode || typeof targetUserShareCode !== 'string') {
        return res.status(400).json({ error: "targetUserShareCode ist erforderlich." });
    }

    try {
        const targetUser = await usersCollection.findOne({ userShareCode: targetUserShareCode });
        if (!targetUser) {
            return res.status(404).json({ error: "Benutzer mit diesem Share-Code nicht gefunden." });
        }
        if (targetUser._id.equals(currentUserId)) {
            return res.status(400).json({ error: "Du kannst keinen Chat mit dir selbst starten." });
        }

        const participants = [currentUserId, targetUser._id].sort(); // Sortieren für konsistente Abfrage

        // Prüfen, ob bereits ein Chat existiert
        let chat = await limChatsCollection.findOne({
            type: 'personal',
            participants: { $all: participants, $size: 2 } // Genau diese zwei Teilnehmer
        });

        if (chat) {
            console.log(`${LOG_PREFIX_CHAT} Persönlicher Chat zwischen ${req.session.username} und ${targetUser.username} existiert bereits (ID: ${chat._id}).`);
            return res.json({ message: "Chat existiert bereits.", chat, isNew: false });
        }

        // Neuen Chat erstellen
        const now = new Date();
        const newChatData = {
            type: 'personal',
            participants: participants,
            createdAt: now,
            updatedAt: now, // Initial gleich createdAt
            lastMessagePreview: null,
            lastMessageSenderId: null,
            lastMessageTimestamp: null
        };
        const result = await limChatsCollection.insertOne(newChatData);
        chat = { _id: result.insertedId, ...newChatData }; // Das vollständige Chat-Objekt

        console.log(`${LOG_PREFIX_CHAT} Persönlicher Chat zwischen ${req.session.username} und ${targetUser.username} erstellt (ID: ${chat._id}).`);
        res.status(201).json({ message: "Persönlicher Chat erfolgreich gestartet.", chat, isNew: true });

    } catch (err) {
        console.error(`${LOG_PREFIX_CHAT} Fehler bei POST /api/chat/chats/personal:`, err);
        res.status(500).json({ error: "Fehler beim Starten des persönlichen Chats." });
    }
});

app.post('/api/chat/chats/group', isAuthenticated, async (req, res) => {
    const { name, initialParticipantShareCodes } = req.body;
    const ownerId = new ObjectId(req.session.userId);
    const ownerUsername = req.session.username;
    console.log(`${LOG_PREFIX_CHAT} User ${ownerUsername} erstellt Gruppe: ${name}`);

    if (!name || typeof name !== 'string' || name.trim().length < 1 || name.trim().length > 100) {
        return res.status(400).json({ error: "Gruppenname (1-100 Zeichen) ist erforderlich." });
    }

    try {
        const participants = [ownerId];
        const participantUsernames = [ownerUsername]; // Für Log-Nachrichten

        if (initialParticipantShareCodes && Array.isArray(initialParticipantShareCodes)) {
            for (const code of initialParticipantShareCodes) {
                if (typeof code !== 'string') continue;
                const user = await usersCollection.findOne({ userShareCode: code });
                if (user && !user._id.equals(ownerId) && !participants.some(pId => pId.equals(user._id))) {
                    participants.push(user._id);
                    participantUsernames.push(user.username);
                }
            }
        }

        const groupShareCode = await generateUniqueGroupShareCode();
        const now = new Date();
        const newGroupData = {
            type: 'group',
            name: name.trim(),
            participants: participants,
            ownerId: ownerId,
            adminIds: [ownerId], // Ersteller ist automatisch Admin
            groupShareCode: groupShareCode,
            bannedUserIds: [],
            createdAt: now,
            updatedAt: now,
            lastMessagePreview: null,
            lastMessageSenderId: null,
            lastMessageTimestamp: null
        };

        const result = await limChatsCollection.insertOne(newGroupData);
        const groupChat = { _id: result.insertedId, ...newGroupData };

        console.log(`${LOG_PREFIX_CHAT} Gruppe '${name}' von ${ownerUsername} erstellt (ID: ${groupChat._id}). Teilnehmer: ${participantUsernames.join(', ')}.`);
        res.status(201).json({ message: "Gruppe erfolgreich erstellt.", chat: groupChat });

    } catch (err) {
        console.error(`${LOG_PREFIX_CHAT} Fehler bei POST /api/chat/chats/group:`, err);
        res.status(500).json({ error: "Fehler beim Erstellen der Gruppe." });
    }
});

// --- NACHRICHTEN ---
app.get('/api/chat/chats/:chatId/messages', isAuthenticated, isChatParticipant, async (req, res) => {
    const chatId = req.chat._id; // von isChatParticipant
    const { limit = 50, beforeMessageId } = req.query; // Paginierung
    const numLimit = parseInt(limit, 10);

    console.log(`${LOG_PREFIX_CHAT} User ${req.session.username} lädt Nachrichten für Chat ${chatId}. Limit: ${numLimit}, Before: ${beforeMessageId}`);

    try {
        const query = { chatId: chatId };
        if (beforeMessageId && ObjectId.isValid(beforeMessageId)) {
            query._id = { $lt: new ObjectId(beforeMessageId) }; // Ältere Nachrichten laden
        }

        const messages = await limMessagesCollection.find(query)
            .sort({ timestamp: -1 }) // Neueste zuerst (innerhalb der Paginierungslogik)
            .limit(numLimit)
            .toArray();

        // Da wir absteigend sortiert haben, um $lt zu nutzen, für die Anzeige umdrehen
        messages.reverse();

        res.json({ messages });
    } catch (err) {
        console.error(`${LOG_PREFIX_CHAT} Fehler bei GET /api/chat/chats/:chatId/messages:`, err);
        res.status(500).json({ error: "Fehler beim Laden der Nachrichten." });
    }
});

app.post('/api/chat/chats/:chatId/messages', isAuthenticated, isChatParticipant, async (req, res) => {
    const chatId = req.chat._id; // von isChatParticipant
    const senderId = new ObjectId(req.session.userId);
    const senderUsername = req.session.username;
    const { content } = req.body;

    if (!content || typeof content !== 'string' || content.trim().length === 0 || content.length > 2000) {
        return res.status(400).json({ error: "Nachrichteninhalt (1-2000 Zeichen) ist erforderlich." });
    }

    console.log(`${LOG_PREFIX_CHAT} User ${senderUsername} sendet Nachricht in Chat ${chatId}: "${content.substring(0, 20)}..."`);

    try {
        const now = new Date();
        const newMessageData = {
            chatId: chatId,
            senderId: senderId,
            senderUsername: senderUsername,
            content: content.trim(),
            timestamp: now
        };

        const result = await limMessagesCollection.insertOne(newMessageData);
        const newMessage = { _id: result.insertedId, ...newMessageData };

        // Chat `updatedAt` und `lastMessagePreview` aktualisieren
        await limChatsCollection.updateOne(
            { _id: chatId },
            {
                $set: {
                    updatedAt: now,
                    lastMessagePreview: content.trim().substring(0, 50),
                    lastMessageSenderId: senderId,
                    lastMessageTimestamp: now
                }
            }
        );

        // === NEU: TRIGGER FÜR SMART POLLING ===
        // Sagt allen Clients: "Es gibt neue Nachrichten, bitte abrufen!"
        updateDataVersion('chat');
        // ======================================

        res.status(201).json({ message: "Nachricht gesendet.", sentMessage: newMessage });

    } catch (err) {
        console.error(`${LOG_PREFIX_CHAT} Fehler bei POST /api/chat/chats/:chatId/messages:`, err);
        res.status(500).json({ error: "Fehler beim Senden der Nachricht." });
    }
});

app.get('/api/chat/messages/search', isAuthenticated, async (req, res) => {
    const { term } = req.query;
    const userId = new ObjectId(req.session.userId);
    console.log(`${LOG_PREFIX_CHAT} User ${req.session.username} sucht Nachrichten mit Begriff: "${term}"`);

    if (!term || typeof term !== 'string' || term.trim().length < 2) {
        return res.status(400).json({ error: "Suchbegriff (mind. 2 Zeichen) ist erforderlich." });
    }

    try {
        // 1. Finde alle Chats, an denen der User teilnimmt
        const userChats = await limChatsCollection.find({ participants: userId }, { projection: { _id: 1 } }).toArray();
        const userChatIds = userChats.map(chat => chat._id);

        if (userChatIds.length === 0) {
            return res.json({ results: [] });
        }

        // 2. Suche Nachrichten in diesen Chats
        const searchResults = await limMessagesCollection.find({
            chatId: { $in: userChatIds },
            $text: { $search: term.trim() }
        }, {
            projection: { score: { $meta: "textScore" } } // Optional: Score für Relevanz
        })
            .sort({ score: { $meta: "textScore" }, timestamp: -1 }) // Beste Übereinstimmung zuerst
            .limit(50) // Begrenzung der Ergebnisse
            .toArray();

        // Optional: Chat-Namen zu den Ergebnissen hinzufügen
        const resultsWithChatInfo = [];
        for (const message of searchResults) {
            const chatInfo = userChats.find(c => c._id.equals(message.chatId)); // Finde den Chat aus dem vorherigen Fetch
            let chatDisplay = `Chat ${message.chatId.toString().substring(0, 6)}`; // Fallback
            if (chatInfo) {
                const fullChat = await limChatsCollection.findOne({ _id: chatInfo._id }); // Hole vollständige Chat-Daten
                if (fullChat) {
                    if (fullChat.type === 'group') {
                        chatDisplay = fullChat.name;
                    } else {
                        const otherParticipantId = fullChat.participants.find(pId => !pId.equals(userId));
                        if (otherParticipantId) {
                            const otherUser = await usersCollection.findOne({ _id: otherParticipantId }, { projection: { username: 1 } });
                            chatDisplay = otherUser ? `Chat mit ${otherUser.username}` : `Persönlicher Chat`;
                        }
                    }
                }
            }
            resultsWithChatInfo.push({ ...message, chatDisplay });
        }


        console.log(`${LOG_PREFIX_CHAT} Suche für "${term}" ergab ${resultsWithChatInfo.length} Ergebnisse für User ${req.session.username}.`);
        res.json({ results: resultsWithChatInfo });

    } catch (err) {
        console.error(`${LOG_PREFIX_CHAT} Fehler bei GET /api/chat/messages/search:`, err);
        res.status(500).json({ error: "Fehler bei der Nachrichtensuche." });
    }
});


// --- GRUPPEN-MANAGEMENT ---
app.get('/api/chat/groups/join/:groupShareCode', isAuthenticated, async (req, res) => {
    const { groupShareCode } = req.params;
    const userId = new ObjectId(req.session.userId);
    const username = req.session.username;
    console.log(`${LOG_PREFIX_CHAT} User ${username} versucht Gruppe mit Code ${groupShareCode} beizutreten.`);

    if (!groupShareCode || typeof groupShareCode !== 'string') {
        return res.status(400).json({ error: "groupShareCode ist erforderlich." });
    }

    try {
        const group = await limChatsCollection.findOne({ groupShareCode: groupShareCode, type: 'group' });
        if (!group) {
            return res.status(404).json({ error: "Gruppe mit diesem Code nicht gefunden." });
        }
        if (group.participants.some(pId => pId.equals(userId))) {
            return res.status(400).json({ message: "Du bist bereits Mitglied dieser Gruppe.", chat: group });
        }
        if (group.bannedUserIds && group.bannedUserIds.some(bannedId => bannedId.equals(userId))) {
            return res.status(403).json({ error: "Du kannst dieser Gruppe nicht beitreten, da du gebannt wurdest." });
        }

        const result = await limChatsCollection.updateOne(
            { _id: group._id },
            { $addToSet: { participants: userId }, $set: { updatedAt: new Date() } } // $addToSet verhindert Duplikate
        );

        if (result.modifiedCount === 0 && result.matchedCount === 1) {
            // Sollte durch die obere Prüfung nicht passieren, aber sicher ist sicher
            return res.json({ message: "Du bist bereits Mitglied dieser Gruppe (erneute Prüfung).", chat: group });
        }

        // Log Nachricht an Gruppe senden (optional, aber nett)
        const joinMessageContent = `${username} ist der Gruppe beigetreten.`;
        const systemMessage = {
            chatId: group._id,
            senderId: null, // Kennzeichnet Systemnachricht
            senderUsername: "System",
            content: joinMessageContent,
            timestamp: new Date()
        };
        await limMessagesCollection.insertOne(systemMessage);
        await limChatsCollection.updateOne(
            { _id: group._id },
            { $set: { lastMessagePreview: joinMessageContent.substring(0, 50), lastMessageSenderId: null, lastMessageTimestamp: systemMessage.timestamp, updatedAt: systemMessage.timestamp } }
        );


        console.log(`${LOG_PREFIX_CHAT} User ${username} ist Gruppe '${group.name}' (ID: ${group._id}) beigetreten.`);
        const updatedGroup = await limChatsCollection.findOne({ _id: group._id }); // um aktuelle Teilnehmerzahl zu bekommen
        res.json({ message: `Erfolgreich Gruppe '${group.name}' beigetreten.`, chat: updatedGroup });

    } catch (err) {
        console.error(`${LOG_PREFIX_CHAT} Fehler bei GET /api/chat/groups/join/:groupShareCode:`, err);
        res.status(500).json({ error: "Fehler beim Beitreten zur Gruppe." });
    }
});

app.put('/api/chat/groups/:chatId/details', isAuthenticated, isChatParticipant, isGroupAdmin, async (req, res) => {
    const { name } = req.body; // Vorerst nur Name änderbar
    const group = req.chat; // von isChatParticipant
    const adminUsername = req.session.username;

    if (!name || typeof name !== 'string' || name.trim().length < 1 || name.trim().length > 100) {
        return res.status(400).json({ error: "Neuer Gruppenname (1-100 Zeichen) ist erforderlich." });
    }
    console.log(`${LOG_PREFIX_CHAT} Admin ${adminUsername} ändert Details für Gruppe ${group._id} zu Name: ${name}`);

    try {
        const result = await limChatsCollection.updateOne(
            { _id: group._id },
            { $set: { name: name.trim(), updatedAt: new Date() } }
        );
        if (result.matchedCount === 0) return res.status(404).json({ error: "Gruppe nicht gefunden." }); // Sollte durch Middleware nicht passieren

        const updatedGroup = await limChatsCollection.findOne({ _id: group._id });
        res.json({ message: "Gruppendetails aktualisiert.", chat: updatedGroup });
    } catch (err) {
        console.error(`${LOG_PREFIX_CHAT} Fehler bei PUT /api/chat/groups/:chatId/details:`, err);
        res.status(500).json({ error: "Fehler beim Aktualisieren der Gruppendetails." });
    }
});

app.post('/api/chat/groups/:chatId/participants', isAuthenticated, isChatParticipant, isGroupAdmin, async (req, res) => {
    const { userShareCodes } = req.body;
    const group = req.chat;
    const adminUsername = req.session.username;

    if (!userShareCodes || !Array.isArray(userShareCodes) || userShareCodes.length === 0) {
        return res.status(400).json({ error: "Mindestens ein userShareCode ist erforderlich." });
    }
    console.log(`${LOG_PREFIX_CHAT} Admin ${adminUsername} fügt Nutzer zu Gruppe ${group._id} hinzu: ${userShareCodes.join(', ')}`);

    try {
        const usersToAddIds = [];
        const addedUsernames = [];
        const errors = [];

        for (const code of userShareCodes) {
            if (typeof code !== 'string') continue;
            const user = await usersCollection.findOne({ userShareCode: code });
            if (!user) {
                errors.push(`Nutzer mit Code ${code} nicht gefunden.`);
                continue;
            }
            if (group.participants.some(pId => pId.equals(user._id))) {
                errors.push(`Nutzer ${user.username} ist bereits in der Gruppe.`);
                continue;
            }
            if (group.bannedUserIds && group.bannedUserIds.some(bannedId => bannedId.equals(user._id))) {
                errors.push(`Nutzer ${user.username} ist von dieser Gruppe gebannt und kann nicht hinzugefügt werden.`);
                continue;
            }
            usersToAddIds.push(user._id);
            addedUsernames.push(user.username);
        }

        if (usersToAddIds.length > 0) {
            await limChatsCollection.updateOne(
                { _id: group._id },
                { $addToSet: { participants: { $each: usersToAddIds } }, $set: { updatedAt: new Date() } }
            );
            // Systemnachricht für hinzugefügte User
            const joinMessageContent = `${adminUsername} hat ${addedUsernames.join(', ')} zur Gruppe hinzugefügt.`;
            const systemMessage = { chatId: group._id, senderId: null, senderUsername: "System", content: joinMessageContent, timestamp: new Date() };
            await limMessagesCollection.insertOne(systemMessage);
            await limChatsCollection.updateOne({ _id: group._id }, { $set: { lastMessagePreview: joinMessageContent.substring(0, 50), lastMessageSenderId: null, lastMessageTimestamp: systemMessage.timestamp, updatedAt: systemMessage.timestamp } });
        }

        const updatedGroup = await limChatsCollection.findOne({ _id: group._id });
        let message = `${usersToAddIds.length} Nutzer erfolgreich hinzugefügt.`;
        if (errors.length > 0) message += ` Fehler: ${errors.join('; ')}`;

        res.json({ message, chat: updatedGroup, errors });

    } catch (err) {
        console.error(`${LOG_PREFIX_CHAT} Fehler bei POST /api/chat/groups/:chatId/participants:`, err);
        res.status(500).json({ error: "Fehler beim Hinzufügen von Teilnehmern." });
    }
});

app.delete('/api/chat/groups/:chatId/participants/:participantUserId', isAuthenticated, isChatParticipant, isGroupAdmin, async (req, res) => {
    const { participantUserId: participantUserIdStr } = req.params;
    const group = req.chat;
    const adminUserId = new ObjectId(req.session.userId);
    const adminUsername = req.session.username;

    if (!ObjectId.isValid(participantUserIdStr)) {
        return res.status(400).json({ error: "Ungültige participantUserId." });
    }
    const participantUserId = new ObjectId(participantUserIdStr);

    if (participantUserId.equals(group.ownerId)) {
        return res.status(403).json({ error: "Der Gruppeneigentümer kann nicht gekickt werden." });
    }
    if (participantUserId.equals(adminUserId)) {
        return res.status(400).json({ error: "Du kannst dich nicht selbst kicken. Nutze 'Gruppe verlassen'." });
    }

    console.log(`${LOG_PREFIX_CHAT} Admin ${adminUsername} kickt User ${participantUserIdStr} aus Gruppe ${group._id}`);

    try {
        const participantUser = await usersCollection.findOne({ _id: participantUserId }, { projection: { username: 1 } });
        if (!participantUser) return res.status(404).json({ error: "Zu kickender Nutzer nicht gefunden." });

        const result = await limChatsCollection.updateOne(
            { _id: group._id },
            { $pull: { participants: participantUserId, adminIds: participantUserId }, $set: { updatedAt: new Date() } } // Auch aus Admins entfernen
        );

        if (result.modifiedCount === 0 && result.matchedCount === 1) {
            return res.status(404).json({ error: "Nutzer war nicht Teil der Gruppe oder wurde bereits entfernt." });
        }

        // Systemnachricht
        const kickMessageContent = `${participantUser.username} wurde von ${adminUsername} aus der Gruppe entfernt.`;
        const systemMessage = { chatId: group._id, senderId: null, senderUsername: "System", content: kickMessageContent, timestamp: new Date() };
        await limMessagesCollection.insertOne(systemMessage);
        await limChatsCollection.updateOne({ _id: group._id }, { $set: { lastMessagePreview: kickMessageContent.substring(0, 50), lastMessageSenderId: null, lastMessageTimestamp: systemMessage.timestamp, updatedAt: systemMessage.timestamp } });

        const updatedGroup = await limChatsCollection.findOne({ _id: group._id });
        res.json({ message: `Nutzer ${participantUser.username} erfolgreich aus der Gruppe entfernt.`, chat: updatedGroup });

    } catch (err) {
        console.error(`${LOG_PREFIX_CHAT} Fehler bei DELETE /api/chat/groups/:chatId/participants/:participantUserId:`, err);
        res.status(500).json({ error: "Fehler beim Entfernen des Teilnehmers." });
    }
});

app.post('/api/chat/groups/:chatId/ban', isAuthenticated, isChatParticipant, isGroupAdmin, async (req, res) => {
    const { userIdToBan: userIdToBanStr } = req.body;
    const group = req.chat;
    const adminUsername = req.session.username;

    if (!userIdToBanStr || !ObjectId.isValid(userIdToBanStr)) {
        return res.status(400).json({ error: "Ungültige userIdToBan." });
    }
    const userIdToBan = new ObjectId(userIdToBanStr);

    if (userIdToBan.equals(group.ownerId)) {
        return res.status(403).json({ error: "Der Gruppeneigentümer kann nicht gebannt werden." });
    }
    if (group.adminIds.some(adminId => adminId.equals(userIdToBan)) && !group.ownerId.equals(new ObjectId(req.session.userId))) {
        return res.status(403).json({ error: "Nur der Gruppeneigentümer kann andere Admins bannen." });
    }

    console.log(`${LOG_PREFIX_CHAT} Admin ${adminUsername} bannt User ${userIdToBanStr} aus Gruppe ${group._id}`);
    try {
        const userToBanDetails = await usersCollection.findOne({ _id: userIdToBan }, { projection: { username: 1 } });
        if (!userToBanDetails) return res.status(404).json({ error: "Zu bannender Nutzer nicht gefunden." });

        const result = await limChatsCollection.updateOne(
            { _id: group._id },
            {
                $addToSet: { bannedUserIds: userIdToBan },
                $pull: { participants: userIdToBan, adminIds: userIdToBan }, // Aus Teilnehmern & Admins entfernen
                $set: { updatedAt: new Date() }
            }
        );

        // Systemnachricht
        const banMessageContent = `${userToBanDetails.username} wurde von ${adminUsername} aus der Gruppe gebannt.`;
        const systemMessage = { chatId: group._id, senderId: null, senderUsername: "System", content: banMessageContent, timestamp: new Date() };
        await limMessagesCollection.insertOne(systemMessage);
        await limChatsCollection.updateOne({ _id: group._id }, { $set: { lastMessagePreview: banMessageContent.substring(0, 50), lastMessageSenderId: null, lastMessageTimestamp: systemMessage.timestamp, updatedAt: systemMessage.timestamp } });

        const updatedGroup = await limChatsCollection.findOne({ _id: group._id });
        res.json({ message: `Nutzer ${userToBanDetails.username} erfolgreich gebannt und entfernt.`, chat: updatedGroup });

    } catch (err) {
        console.error(`${LOG_PREFIX_CHAT} Fehler bei POST /api/chat/groups/:chatId/ban:`, err);
        res.status(500).json({ error: "Fehler beim Bannen des Nutzers." });
    }
});

app.delete('/api/chat/groups/:chatId/ban/:bannedUserId', isAuthenticated, isChatParticipant, isGroupAdmin, async (req, res) => {
    const { bannedUserId: bannedUserIdStr } = req.params;
    const group = req.chat;
    const adminUsername = req.session.username;

    if (!ObjectId.isValid(bannedUserIdStr)) {
        return res.status(400).json({ error: "Ungültige bannedUserId." });
    }
    const bannedUserId = new ObjectId(bannedUserIdStr);
    console.log(`${LOG_PREFIX_CHAT} Admin ${adminUsername} entbannt User ${bannedUserIdStr} aus Gruppe ${group._id}`);

    try {
        const result = await limChatsCollection.updateOne(
            { _id: group._id },
            { $pull: { bannedUserIds: bannedUserId }, $set: { updatedAt: new Date() } }
        );
        if (result.modifiedCount === 0 && result.matchedCount === 1) {
            return res.status(404).json({ error: "Nutzer war nicht gebannt." });
        }
        const userUnbanned = await usersCollection.findOne({ _id: bannedUserId }, { projection: { username: 1 } });
        const updatedGroup = await limChatsCollection.findOne({ _id: group._id });
        res.json({ message: `Nutzer ${userUnbanned ? userUnbanned.username : bannedUserIdStr} erfolgreich entbannt.`, chat: updatedGroup });
    } catch (err) {
        console.error(`${LOG_PREFIX_CHAT} Fehler bei DELETE /api/chat/groups/:chatId/ban/:bannedUserId:`, err);
        res.status(500).json({ error: "Fehler beim Entbannen des Nutzers." });
    }
});

app.post('/api/chat/groups/:chatId/admins/:participantUserId', isAuthenticated, isChatParticipant, isGroupOwner, async (req, res) => {
    const { participantUserId: participantUserIdStr } = req.params;
    const group = req.chat;
    const ownerUsername = req.session.username;

    if (!ObjectId.isValid(participantUserIdStr)) {
        return res.status(400).json({ error: "Ungültige participantUserId." });
    }
    const participantUserId = new ObjectId(participantUserIdStr);

    if (!group.participants.some(pId => pId.equals(participantUserId))) {
        return res.status(404).json({ error: "Nutzer ist kein Teilnehmer der Gruppe." });
    }
    if (group.adminIds.some(adminId => adminId.equals(participantUserId))) {
        return res.status(400).json({ error: "Nutzer ist bereits Admin." });
    }
    console.log(`${LOG_PREFIX_CHAT} Owner ${ownerUsername} befördert ${participantUserIdStr} zum Admin in Gruppe ${group._id}`);

    try {
        await limChatsCollection.updateOne(
            { _id: group._id },
            { $addToSet: { adminIds: participantUserId }, $set: { updatedAt: new Date() } }
        );
        const userPromoted = await usersCollection.findOne({ _id: participantUserId }, { projection: { username: 1 } });
        const updatedGroup = await limChatsCollection.findOne({ _id: group._id });
        res.json({ message: `Nutzer ${userPromoted ? userPromoted.username : participantUserIdStr} erfolgreich zum Admin befördert.`, chat: updatedGroup });
    } catch (err) {
        console.error(`${LOG_PREFIX_CHAT} Fehler bei POST /api/chat/groups/:chatId/admins/:participantUserId:`, err);
        res.status(500).json({ error: "Fehler beim Befördern zum Admin." });
    }
});

app.delete('/api/chat/groups/:chatId/admins/:adminUserIdToRemove', isAuthenticated, isChatParticipant, isGroupOwner, async (req, res) => {
    const { adminUserIdToRemove: adminUserIdToRemoveStr } = req.params;
    const group = req.chat;
    const ownerUsername = req.session.username;

    if (!ObjectId.isValid(adminUserIdToRemoveStr)) {
        return res.status(400).json({ error: "Ungültige adminUserIdToRemove." });
    }
    const adminUserIdToRemove = new ObjectId(adminUserIdToRemoveStr);

    if (adminUserIdToRemove.equals(group.ownerId)) {
        return res.status(403).json({ error: "Der Gruppeneigentümer kann seinen Admin-Status nicht selbst entfernen." });
    }
    if (!group.adminIds.some(adminId => adminId.equals(adminUserIdToRemove))) {
        return res.status(404).json({ error: "Nutzer ist kein Admin in dieser Gruppe." });
    }
    console.log(`${LOG_PREFIX_CHAT} Owner ${ownerUsername} entfernt Admin-Status von ${adminUserIdToRemoveStr} in Gruppe ${group._id}`);

    try {
        await limChatsCollection.updateOne(
            { _id: group._id },
            { $pull: { adminIds: adminUserIdToRemove }, $set: { updatedAt: new Date() } }
        );
        const userDemoted = await usersCollection.findOne({ _id: adminUserIdToRemove }, { projection: { username: 1 } });
        const updatedGroup = await limChatsCollection.findOne({ _id: group._id });
        res.json({ message: `Admin-Status von Nutzer ${userDemoted ? userDemoted.username : adminUserIdToRemoveStr} erfolgreich entfernt.`, chat: updatedGroup });
    } catch (err) {
        console.error(`${LOG_PREFIX_CHAT} Fehler bei DELETE /api/chat/groups/:chatId/admins/:adminUserIdToRemove:`, err);
        res.status(500).json({ error: "Fehler beim Entfernen des Admin-Status." });
    }
});

app.post('/api/chat/groups/:chatId/leave', isAuthenticated, isChatParticipant, async (req, res) => {
    const group = req.chat;
    const userId = new ObjectId(req.session.userId);
    const username = req.session.username;
    console.log(`${LOG_PREFIX_CHAT} User ${username} verlässt Gruppe ${group._id} (${group.name})`);

    try {
        if (group.ownerId.equals(userId)) {
            // Owner verlässt die Gruppe
            if (group.participants.length === 1) { // Owner ist der einzige Teilnehmer
                await limChatsCollection.deleteOne({ _id: group._id });
                // Optional: Nachrichten auch löschen
                // await limMessagesCollection.deleteMany({ chatId: group._id });
                console.log(`${LOG_PREFIX_CHAT} Gruppe ${group.name} (ID: ${group._id}) wurde gelöscht, da der Owner das letzte Mitglied war.`);
                return res.json({ message: "Gruppe verlassen und gelöscht, da du das letzte Mitglied warst." });
            } else {
                // Neuen Owner bestimmen (z.B. ältester Admin, oder ältester Teilnehmer)
                let newOwnerId = null;
                const otherAdmins = group.adminIds.filter(id => !id.equals(userId));
                if (otherAdmins.length > 0) {
                    // Wähle ersten anderen Admin (könnte durch Timestamp der Admin-Ernennung verbessert werden)
                    newOwnerId = otherAdmins[0];
                } else {
                    // Wähle ältesten anderen Teilnehmer (basierend auf _id, was ungefähr der Beitrittszeit entspricht)
                    const otherParticipants = group.participants.filter(id => !id.equals(userId)).sort();
                    if (otherParticipants.length > 0) newOwnerId = otherParticipants[0];
                }

                if (newOwnerId) {
                    await limChatsCollection.updateOne(
                        { _id: group._id },
                        {
                            $pull: { participants: userId, adminIds: userId },
                            $set: { ownerId: newOwnerId, updatedAt: new Date() },
                            $addToSet: { adminIds: newOwnerId } // Sicherstellen, dass neuer Owner auch Admin ist
                        }
                    );
                    const newOwner = await usersCollection.findOne({ _id: newOwnerId }, { projection: { username: 1 } });
                    console.log(`${LOG_PREFIX_CHAT} Owner ${username} hat Gruppe ${group.name} verlassen. Neuer Owner: ${newOwner ? newOwner.username : newOwnerId}.`);
                    // Systemnachricht
                    const leaveMessageContent = `${username} (Owner) hat die Gruppe verlassen. ${newOwner ? newOwner.username : 'Ein neuer Nutzer'} ist nun der Owner.`;
                    const systemMessage = { chatId: group._id, senderId: null, senderUsername: "System", content: leaveMessageContent, timestamp: new Date() };
                    await limMessagesCollection.insertOne(systemMessage);
                    await limChatsCollection.updateOne({ _id: group._id }, { $set: { lastMessagePreview: leaveMessageContent.substring(0, 50), lastMessageSenderId: null, lastMessageTimestamp: systemMessage.timestamp, updatedAt: systemMessage.timestamp } });

                } else {
                    // Sollte nicht passieren, wenn participants.length > 1
                    console.warn(`${LOG_PREFIX_CHAT} Gruppe ${group.name} konnte nicht verlassen werden, kein neuer Owner bestimmbar.`);
                    return res.status(500).json({ error: "Konnte keinen neuen Owner bestimmen. Gruppe kann nicht verlassen werden." });
                }
            }
        } else {
            // Normaler Teilnehmer verlässt die Gruppe
            await limChatsCollection.updateOne(
                { _id: group._id },
                { $pull: { participants: userId, adminIds: userId }, $set: { updatedAt: new Date() } } // Auch aus Admins entfernen
            );
            // Systemnachricht
            const leaveMessageContent = `${username} hat die Gruppe verlassen.`;
            const systemMessage = { chatId: group._id, senderId: null, senderUsername: "System", content: leaveMessageContent, timestamp: new Date() };
            await limMessagesCollection.insertOne(systemMessage);
            await limChatsCollection.updateOne({ _id: group._id }, { $set: { lastMessagePreview: leaveMessageContent.substring(0, 50), lastMessageSenderId: null, lastMessageTimestamp: systemMessage.timestamp, updatedAt: systemMessage.timestamp } });
        }

        const updatedGroupAfterLeave = await limChatsCollection.findOne({ _id: group._id }); // Kann null sein, wenn gelöscht
        res.json({ message: "Gruppe erfolgreich verlassen.", chat: updatedGroupAfterLeave });

    } catch (err) {
        console.error(`${LOG_PREFIX_CHAT} Fehler bei POST /api/chat/groups/:chatId/leave:`, err);
        res.status(500).json({ error: "Fehler beim Verlassen der Gruppe." });
    }
});

app.delete('/api/chat/groups/:chatId', isAuthenticated, isChatParticipant, isGroupOwner, async (req, res) => {
    const group = req.chat;
    const ownerUsername = req.session.username;
    console.log(`${LOG_PREFIX_CHAT} Owner ${ownerUsername} löscht Gruppe ${group._id} (${group.name})`);

    try {
        await limChatsCollection.deleteOne({ _id: group._id });
        // Optional: Alle Nachrichten dieser Gruppe auch löschen
        const msgDeleteResult = await limMessagesCollection.deleteMany({ chatId: group._id });
        // Optional: Alle UserChatSettings für diese Gruppe löschen
        await limUserChatSettingsCollection.deleteMany({ chatId: group._id });

        console.log(`${LOG_PREFIX_CHAT} Gruppe ${group.name} (ID: ${group._id}) und ${msgDeleteResult.deletedCount} Nachrichten gelöscht.`);
        res.json({ message: `Gruppe '${group.name}' und zugehörige Nachrichten erfolgreich gelöscht.` });
    } catch (err) {
        console.error(`${LOG_PREFIX_CHAT} Fehler bei DELETE /api/chat/groups/:chatId:`, err);
        res.status(500).json({ error: "Fehler beim Löschen der Gruppe." });
    }
});

app.post('/api/chat/groups/:chatId/regenerateShareCode', isAuthenticated, isChatParticipant, isGroupAdmin, async (req, res) => {
    const group = req.chat;
    const adminUsername = req.session.username;
    console.log(`${LOG_PREFIX_CHAT} Admin ${adminUsername} generiert neuen Share-Code für Gruppe ${group._id}`);
    try {
        const newShareCode = await generateUniqueGroupShareCode();
        await limChatsCollection.updateOne(
            { _id: group._id },
            { $set: { groupShareCode: newShareCode, updatedAt: new Date() } }
        );
        const updatedGroup = await limChatsCollection.findOne({ _id: group._id });
        res.json({ message: "Neuer Gruppen-Share-Code generiert.", chat: updatedGroup });
    } catch (err) {
        console.error(`${LOG_PREFIX_CHAT} Fehler bei POST /api/chat/groups/:chatId/regenerateShareCode:`, err);
        res.status(500).json({ error: "Fehler beim Regenerieren des Gruppen-Share-Codes." });
    }
});

// --- CHAT EINSTELLUNGEN (MUTE) ---
app.put('/api/chat/chats/:chatId/settings/mute', isAuthenticated, isChatParticipant, async (req, res) => {
    const chatId = req.chat._id;
    const userId = new ObjectId(req.session.userId);
    const { isMuted } = req.body;

    if (typeof isMuted !== 'boolean') {
        return res.status(400).json({ error: "isMuted (boolean) ist erforderlich." });
    }
    console.log(`${LOG_PREFIX_CHAT} User ${req.session.username} setzt Mute-Status für Chat ${chatId} auf ${isMuted}`);

    try {
        const result = await limUserChatSettingsCollection.updateOne(
            { userId: userId, chatId: chatId },
            { $set: { isMuted: isMuted } },
            { upsert: true } // Erstellt Dokument, falls nicht vorhanden
        );
        res.json({ message: `Chat erfolgreich ${isMuted ? 'stummgeschaltet' : 'lautgeschaltet'}.`, isMuted });
    } catch (err) {
        console.error(`${LOG_PREFIX_CHAT} Fehler bei PUT /api/chat/chats/:chatId/settings/mute:`, err);
        res.status(500).json({ error: "Fehler beim Ändern des Mute-Status." });
    }
});

// === CHAT ENDPOINTS ENDE===

app.get('/health', (req, res) => {
    res.status(200).send('OK');
});

// =========================================================
// === HALL OF FAME ENDPUNKT ===
// =========================================================
app.get('/api/hall-of-fame', async (req, res) => {
    console.log(`${LOG_PREFIX_SERVER} 🏆 Hall of Fame wird abgerufen (mit Infinity-Filter).`);
    try {
        // Die Bedingung, um unendliche User auszuschließen.
        // Wir prüfen auf `unlockedInfinityMoney: { $ne: true }`, um auch die auszuschließen, die es zwar freigeschaltet, aber deaktiviert haben.
        const finiteUserCondition = {
            isAdmin: { $ne: true },
            unlockedInfinityMoney: { $ne: true }
        };

        const [topMoney, topTokens, infinityClub] = await Promise.all([
            // 1. Die reichsten User (NUR User OHNE Infinity-Status)
            usersCollection.aggregate([
                // Stufe 1: Nur User ohne Admin-Rechte UND ohne freigeschalteten Infinity-Modus
                { $match: finiteUserCondition },
                // Stufe 2: Ein neues Feld 'numericBalance' erstellen, das 'balance' sicher in eine Zahl umwandelt.
                { $addFields: { "numericBalance": { $toDouble: "$balance" } } },
                // Stufe 3: Nach dem neuen, numerischen Feld sortieren
                { $sort: { numericBalance: -1 } },
                // Stufe 4: Die Top 5 auswählen
                { $limit: 5 },
                // Stufe 5: Nur die ursprünglichen Felder für die Antwort auswählen
                { $project: { username: 1, balance: 1, _id: 0 } }
            ]).toArray(),

            // 2. Die User mit den meisten Tokens (NUR User OHNE Infinity-Status)
            usersCollection.aggregate([
                // Stufe 1: Nur User ohne Admin-Rechte UND ohne freigeschalteten Infinity-Modus
                { $match: finiteUserCondition },
                // Stufe 2: Ein neues Feld 'numericTokens' erstellen
                { $addFields: { "numericTokens": { $toDouble: "$tokens" } } },
                // Stufe 3: Nach dem neuen, numerischen Feld sortieren
                { $sort: { numericTokens: -1 } },
                // Stufe 4: Die Top 5 auswählen
                { $limit: 5 },
                // Stufe 5: Nur die ursprünglichen Felder für die Antwort auswählen
                { $project: { username: 1, tokens: 1, _id: 0 } }
            ]).toArray(),

            // 3. Die Mitglieder des "Infinity Clubs" (Diese Liste bleibt unverändert)
            usersCollection.find(
                {
                    isAdmin: { $ne: true },
                    unlockedInfinityMoney: true
                },
                { projection: { username: 1, createdAt: 1, _id: 0 } }
            )
                .sort({ createdAt: 1 })
                .limit(10)
                .toArray()
        ]);

        // Die JSON-Antwort bleibt strukturell gleich.
        res.json({
            title: "🏆 Hall of Fame von Limazon 🏆",
            lastUpdated: new Date().toISOString(),
            categories: [
                {
                    id: "money_magnates",
                    title: "Die Finanz-Magnaten 💰",
                    description: "Sie schwimmen in Limazon-Dollars und ihre Konten platzen aus allen Nähten. Das sind die unangefochtenen Könige des Kapitals unter den sterblichen Spielern!",
                    entries: topMoney
                },
                {
                    id: "token_titans",
                    title: "Die Token-Titanen ✨",
                    description: "Während andere auf schnödes Geld setzen, sammeln diese Visionäre das wahre Gold: Tokens. Ihr Vermögen ist für die Ewigkeit... oder zumindest für das nächste Glücksrad.",
                    entries: topTokens
                },
                {
                    id: "infinity_club",
                    title: "Der Club der Unendlichkeit ∞",
                    description: "Diese Legenden haben die Fesseln der Wirtschaft gesprengt. Für sie ist 'Geld' nur noch ein Konzept. Sie haben das Spiel gemeistert und spielen nun in ihrer eigenen Liga.",
                    members: infinityClub.map(user => user.username)
                }
            ]
        });

    } catch (err) {
        console.error(`${LOG_PREFIX_SERVER} ❌ Fehler beim Abrufen der Hall of Fame:`, err);
        res.status(500).json({ error: "Fehler beim Laden der Halle des Ruhms. Die Legenden schlafen noch." });
    }
});

// =========================================================
// === IDEENBOX ENDPUNKTE ===
// =========================================================
const LOG_PREFIX_IDEAS = "[IdeaBox API]";

// Eine neue Middleware, um zu prüfen, ob ein Nutzer von der Ideenbox gebannt ist.
async function isNotBannedFromIdeaBox(req, res, next) {
    try {
        const user = await usersCollection.findOne({ _id: new ObjectId(req.session.userId) });
        if (user && user.isBannedFromIdeaBox === true) {
            console.warn(`${LOG_PREFIX_IDEAS} Zugriff verweigert für gebannten User ${req.session.username}.`);
            return res.status(403).json({ error: 'Du wurdest von der Ideenbox gesperrt und kannst keine neuen Ideen einreichen.' });
        }
        next();
    } catch (err) {
        console.error(`${LOG_PREFIX_IDEAS} Fehler bei der Überprüfung des Bann-Status für User ${req.session.username}:`, err);
        res.status(500).json({ error: "Fehler bei der Überprüfung der Berechtigungen." });
    }
}


// Idee einreichen
app.post('/api/ideas', isAuthenticated, isNotBannedFromIdeaBox, async (req, res) => {
    const { title, description } = req.body;
    const submitterId = new ObjectId(req.session.userId);
    const submitterUsername = req.session.username;

    if (!title || typeof title !== 'string' || title.trim().length < 5 || title.trim().length > 100) {
        return res.status(400).json({ error: 'Ein Titel mit 5 bis 100 Zeichen ist erforderlich.' });
    }
    if (!description || typeof description !== 'string' || description.trim().length < 10 || description.trim().length > 2000) {
        return res.status(400).json({ error: 'Eine Beschreibung mit 10 bis 2000 Zeichen ist erforderlich.' });
    }

    console.log(`${LOG_PREFIX_IDEAS} User ${submitterUsername} reicht neue Idee ein: "${title}"`);

    try {
        const newIdea = {
            title: title.trim(),
            description: description.trim(),
            submitterId,
            submitterUsername,
            status: 'new', // 'new', 'in-progress', 'done', 'rejected'
            createdAt: new Date(),
            updatedAt: new Date()
        };
        await ideasCollection.insertOne(newIdea);
        res.status(201).json({ message: 'Deine Idee wurde erfolgreich eingereicht!', idea: newIdea });
    } catch (err) {
        console.error(`${LOG_PREFIX_IDEAS} Fehler beim Einreichen der Idee von User ${submitterUsername}:`, err);
        res.status(500).json({ error: 'Serverfehler beim Einreichen der Idee.' });
    }
});

// Alle Ideen abrufen (für alle Nutzer sichtbar)
app.get('/api/ideas', async (req, res) => {
    console.log(`${LOG_PREFIX_IDEAS} Rufe Ideenliste ab.`);
    try {
        const ideas = await ideasCollection.find({}).sort({ createdAt: -1 }).limit(200).toArray();
        res.json({ ideas });
    } catch (err) {
        console.error(`${LOG_PREFIX_IDEAS} Fehler beim Abrufen der Ideenliste:`, err);
        res.status(500).json({ error: 'Fehler beim Laden der Ideen.' });
    }
});

// Admin: Status einer Idee ändern
app.patch('/api/ideas/:id/status', isAuthenticated, isAdmin, async (req, res) => {
    const { status } = req.body;
    const ideaIdStr = req.params.id;
    const adminUsername = req.session.username;

    if (!ObjectId.isValid(ideaIdStr)) {
        return res.status(400).json({ error: 'Ungültige Ideen-ID.' });
    }
    const ideaId = new ObjectId(ideaIdStr);

    const validStatus = ['new', 'in-progress', 'done', 'rejected'];
    if (!status || !validStatus.includes(status)) {
        return res.status(400).json({ error: `Ungültiger Status. Erlaubt sind: ${validStatus.join(', ')}.` });
    }

    console.log(`${LOG_PREFIX_IDEAS} Admin ${adminUsername} ändert Status von Idee ${ideaId} zu "${status}"`);

    try {
        const result = await ideasCollection.updateOne(
            { _id: ideaId },
            { $set: { status: status, updatedAt: new Date() } }
        );
        if (result.matchedCount === 0) {
            return res.status(404).json({ error: 'Idee nicht gefunden.' });
        }
        const updatedIdea = await ideasCollection.findOne({ _id: ideaId });
        res.json({ message: 'Status der Idee erfolgreich aktualisiert.', idea: updatedIdea });
    } catch (err) {
        console.error(`${LOG_PREFIX_IDEAS} Admin-Fehler beim Ändern des Ideen-Status:`, err);
        res.status(500).json({ error: 'Serverfehler beim Aktualisieren des Status.' });
    }
});

// Admin: Idee löschen
app.delete('/api/ideas/:id', isAuthenticated, isAdmin, async (req, res) => {
    const ideaIdStr = req.params.id;
    const adminUsername = req.session.username;

    if (!ObjectId.isValid(ideaIdStr)) {
        return res.status(400).json({ error: 'Ungültige Ideen-ID.' });
    }
    const ideaId = new ObjectId(ideaIdStr);

    console.log(`${LOG_PREFIX_IDEAS} Admin ${adminUsername} löscht Idee ${ideaId}`);

    try {
        const result = await ideasCollection.deleteOne({ _id: ideaId });
        if (result.deletedCount === 0) {
            return res.status(404).json({ error: 'Idee nicht gefunden.' });
        }
        res.json({ message: 'Idee erfolgreich gelöscht.' });
    } catch (err) {
        console.error(`${LOG_PREFIX_IDEAS} Admin-Fehler beim Löschen der Idee:`, err);
        res.status(500).json({ error: 'Serverfehler beim Löschen der Idee.' });
    }
});

// Admin: Nutzer von der Ideenbox bannen
app.post('/api/admin/ideas/ban-user', isAuthenticated, isAdmin, async (req, res) => {
    const { userIdToBan } = req.body;
    if (!ObjectId.isValid(userIdToBan)) {
        return res.status(400).json({ error: 'Ungültige User-ID.' });
    }
    console.log(`${LOG_PREFIX_IDEAS} Admin ${req.session.username} bannt User ${userIdToBan} von der Ideenbox.`);
    try {
        const result = await usersCollection.updateOne(
            { _id: new ObjectId(userIdToBan) },
            { $set: { isBannedFromIdeaBox: true } }
        );
        if (result.matchedCount === 0) {
            return res.status(404).json({ error: 'Nutzer nicht gefunden.' });
        }
        res.json({ message: 'Nutzer wurde erfolgreich von der Ideenbox gebannt.' });
    } catch (err) {
        console.error(`${LOG_PREFIX_IDEAS} Admin-Fehler beim Bannen des Nutzers:`, err);
        res.status(500).json({ error: 'Serverfehler beim Bannen des Nutzers.' });
    }
});

// Admin: Nutzer-Bann von der Ideenbox aufheben
app.post('/api/admin/ideas/unban-user', isAuthenticated, isAdmin, async (req, res) => {
    const { userIdToUnban } = req.body;
    if (!ObjectId.isValid(userIdToUnban)) {
        return res.status(400).json({ error: 'Ungültige User-ID.' });
    }
    console.log(`${LOG_PREFIX_IDEAS} Admin ${req.session.username} entbannt User ${userIdToUnban} von der Ideenbox.`);
    try {
        const result = await usersCollection.updateOne(
            { _id: new ObjectId(userIdToUnban) },
            { $set: { isBannedFromIdeaBox: false } }
        );
        if (result.matchedCount === 0) {
            return res.status(404).json({ error: 'Nutzer nicht gefunden.' });
        }
        res.json({ message: 'Der Bann des Nutzers von der Ideenbox wurde aufgehoben.' });
    } catch (err) {
        console.error(`${LOG_PREFIX_IDEAS} Admin-Fehler beim Entbannen des Nutzers:`, err);
        res.status(500).json({ error: 'Serverfehler beim Entbannen des Nutzers.' });
    }
});

// =========================================================
// === AUKTIONSHAUS ENDPUNKTE ===
// =========================================================

app.post('/api/auctions', isAuthenticated, async (req, res) => {
    const { productId, quantity, startingBid, durationInHours } = req.body;
    const sellerId = new ObjectId(req.session.userId);

    console.log(`${LOG_PREFIX_SERVER} User ${req.session.username} erstellt Auktion:`, req.body);

    if (typeof productId !== 'number' || typeof quantity !== 'number' || quantity <= 0 || typeof startingBid !== 'number' || startingBid <= 0 || typeof durationInHours !== 'number' || ![12, 24, 48].includes(durationInHours)) {
        return res.status(400).json({ error: 'Ungültige Auktionsdaten. Menge/Preis müssen > 0 sein und Dauer muss 12, 24 oder 48 Stunden sein.' });
    }

    try {
        const product = await productsCollection.findOne({ id: productId, isTokenCard: { $ne: true } });
        if (!product) {
            return res.status(404).json({ error: 'Produkt nicht gefunden oder nicht auktionsfähig.' });
        }

        const inventoryItem = await inventoriesCollection.findOne({ userId: sellerId, productId: productId });
        if (!inventoryItem || inventoryItem.quantityOwned < quantity) {
            return res.status(400).json({ error: `Nicht genügend Items im Inventar. Du besitzt nur ${inventoryItem ? inventoryItem.quantityOwned : 0} Stk.` });
        }

        // Item aus dem Inventar des Verkäufers entfernen (hinterlegen)
        const updateResult = await inventoriesCollection.updateOne(
            { userId: sellerId, productId: productId, quantityOwned: { $gte: quantity } },
            { $inc: { quantityOwned: -quantity } }
        );

        if (updateResult.modifiedCount === 0) {
            throw new Error("Inventar-Update fehlgeschlagen, Item konnte nicht hinterlegt werden.");
        }

        const now = new Date();
        const endTime = new Date(now.getTime() + durationInHours * 60 * 60 * 1000);

        const newAuction = {
            sellerId,
            sellerUsername: req.session.username,
            productId,
            productName: product.name,
            productImageUrl: product.image_url,
            quantity,
            startingBid: parseFloat(startingBid.toFixed(2)),
            currentBid: parseFloat(startingBid.toFixed(2)),
            highestBidderId: null,
            highestBidderUsername: null,
            bids: [],
            startTime: now,
            endTime,
            status: 'active' // active, ended_sold, ended_unsold, cancelled
        };

        const result = await auctionsCollection.insertOne(newAuction);
        console.log(`${LOG_PREFIX_SERVER} Auktion ${result.insertedId} für "${product.name}" von ${req.session.username} erstellt.`);

        res.status(201).json({ message: 'Auktion erfolgreich erstellt!', auction: newAuction });

    } catch (err) {
        console.error(`${LOG_PREFIX_SERVER} Fehler beim Erstellen der Auktion für User ${req.session.username}:`, err);
        // WICHTIG: Item zurückgeben, wenn etwas schiefgeht!
        await inventoriesCollection.updateOne({ userId: sellerId, productId: productId }, { $inc: { quantityOwned: quantity } });
        res.status(500).json({ error: 'Serverfehler beim Erstellen der Auktion. Das Item wurde deinem Inventar wieder gutgeschrieben.' });
    }
});

app.post('/api/auctions/:id/bid', isAuthenticated, async (req, res) => {
    const { bidAmount } = req.body;
    const bidderId = new ObjectId(req.session.userId);
    const auctionId = new ObjectId(req.params.id);

    if (typeof bidAmount !== 'number' || bidAmount <= 0) {
        return res.status(400).json({ error: 'Ungültiger Gebotsbetrag.' });
    }
    const finalBidAmount = parseFloat(bidAmount.toFixed(2));

    const session = client.startSession();

    try {
        await session.withTransaction(async () => {
            // 1. Geld beim Bieter prüfen & abziehen (ATOMAR)
            // Wir prüfen direkt im Update, ob genug Geld da ist.
            const bidderResult = await usersCollection.updateOne(
                { _id: bidderId, balance: { $gte: finalBidAmount } },
                { $inc: { balance: -finalBidAmount } },
                { session }
            );

            if (bidderResult.modifiedCount === 0) {
                // Checken, ob User existiert oder nur pleite ist
                const userExists = await usersCollection.findOne({ _id: bidderId }, { session });
                if (!userExists) throw new Error("Benutzer nicht gefunden.");
                throw new Error(`Nicht genügend Guthaben für Gebot von $${finalBidAmount.toFixed(2)}.`);
            }

            // 2. Auktion aktualisieren (OPTIMISTIC LOCKING)
            // Der Trick: Wir suchen die Auktion NUR, wenn das aktuelle Gebot < ist als unser neues.
            const newBidEntry = {
                bidderId,
                bidderUsername: req.session.username,
                amount: finalBidAmount,
                timestamp: new Date()
            };

            // Zuerst holen wir die Auktion, um den VORHERIGEN Bieter zu finden (für Rückzahlung)
            // Da wir in einer Transaktion sind, ist das relativ sicher, aber der atomare Check unten ist entscheidend.
            const auction = await auctionsCollection.findOne({ _id: auctionId }, { session });

            if (!auction) throw new Error("Auktion nicht gefunden.");
            if (auction.status !== 'active') throw new Error("Auktion ist beendet.");
            if (new Date() > new Date(auction.endTime)) throw new Error("Auktion ist abgelaufen.");
            if (auction.sellerId.equals(bidderId)) throw new Error("Du kannst nicht auf eigene Auktionen bieten.");

            // Check gegen den geladenen Wert (Soft Check für schnelle Fehlermeldung)
            if (finalBidAmount <= auction.currentBid) throw new Error(`Gebot zu niedrig. Aktuell: $${auction.currentBid}`);

            // Das eigentliche, sichere Update
            const auctionUpdate = await auctionsCollection.updateOne(
                {
                    _id: auctionId,
                    status: 'active',
                    currentBid: { $lt: finalBidAmount } // <--- DAS IST DER SCHUTZ!
                },
                {
                    $set: {
                        currentBid: finalBidAmount,
                        highestBidderId: bidderId,
                        highestBidderUsername: req.session.username
                    },
                    $push: {
                        bids: {
                            $each: [newBidEntry],
                            $position: 0
                        }
                    }
                },
                { session }
            );

            if (auctionUpdate.modifiedCount === 0) {
                // Das bedeutet: Jemand anders war schneller und hat höher geboten!
                // Wir müssen den Fehler werfen, damit die Transaktion abbricht 
                // und das Geld (Schritt 1) automatisch zurückgerollt wird.
                throw new Error("Jemand hat in der Zwischenzeit höher geboten! Versuch es nochmal.");
            }

            // 3. Dem vorherigen Höchstbietenden das Geld zurückgeben
            if (auction.highestBidderId) {
                await usersCollection.updateOne(
                    { _id: auction.highestBidderId },
                    { $inc: { balance: auction.currentBid } },
                    { session }
                );
            }
        });

        console.log(`${LOG_PREFIX_SERVER} User ${req.session.username} bietet $${finalBidAmount} auf Auktion ${auctionId}.`);
        res.json({ message: 'Gebot erfolgreich abgegeben!', newBidAmount: finalBidAmount });

    } catch (err) {
        console.error(`${LOG_PREFIX_SERVER} Gebotsfehler:`, err.message);
        // Da wir eine Transaction nutzen, ist das Geld bei Fehler automatisch wieder beim User.
        res.status(400).json({ error: err.message });
    } finally {
        await session.endSession();
    }
});

app.get('/api/auctions', async (req, res) => {
    try {
        const activeAuctions = await auctionsCollection.find({ status: 'active' })
            .sort({ endTime: 1 }) // Auktionen, die am frühesten enden, zuerst
            .limit(100) // Begrenzung zur Performance-Schonung
            .toArray();

        res.json({ auctions: activeAuctions });
    } catch (err) {
        console.error(`${LOG_PREFIX_SERVER} Fehler beim Abrufen der Auktionen:`, err);
        res.status(500).json({ error: 'Fehler beim Laden der Auktionsliste.' });
    }
});

app.get('/api/auctions/:id', async (req, res) => {
    try {
        if (!ObjectId.isValid(req.params.id)) {
            return res.status(400).json({ error: "Ungültige Auktions-ID." });
        }
        const auction = await auctionsCollection.findOne({ _id: new ObjectId(req.params.id) });
        if (!auction) {
            return res.status(404).json({ error: "Auktion nicht gefunden." });
        }
        res.json({ auction });
    } catch (err) {
        console.error(`${LOG_PREFIX_SERVER} Fehler beim Abrufen der Auktionsdetails für ID ${req.params.id}:`, err);
        res.status(500).json({ error: 'Fehler beim Laden der Auktionsdetails.' });
    }
});

// Admin Repair
app.post('/api/admin/fix-balances', isAuthenticated, isAdmin, async (req, res) => {
    console.log(`${LOG_PREFIX_SERVER} Admin ${req.session.username} startet die manuelle Reparatur der Kontostände.`);
    const result = await fixStringBalances();
    if (result.error) {
        return res.status(500).json(result);
    }
    res.json(result);
});

app.post('/api/admin/convert-products-to-stocks', isAuthenticated, isAdmin, async (req, res) => {
    console.log(`${LOG_PREFIX_SERVER} Admin ${req.session.username} startet die manuelle Konvertierung von Produkten.`);
    const result = await convertProductsToStocks();
    if (result.error) {
        return res.status(500).json(result);
    }
    res.json(result);
});

app.post('/api/admin/normalize-balances', isAuthenticated, isAdmin, async (req, res) => {
    console.log(`${LOG_PREFIX_SERVER} Admin ${req.session.username} startet die manuelle Normalisierung der Kontostände.`);
    const result = await normalizeExtremeBalances();
    if (result.error) {
        return res.status(500).json(result);
    }
    res.json(result);
});


// =========================================================
// === LIMOSTONKS BÖRSEN ENDPUNKTE ===
// =========================================================

// =========================================================
// === STONKS API MIT LIMITS & GEBÜHREN ===
// =========================================================

const checkTradeCooldown = async (user) => {
    // 5 Minuten = 300.000 Millisekunden
    const COOLDOWN_MS = 300000;

    const now = Date.now();
    if (user.lastTradeTime && (now - user.lastTradeTime) < COOLDOWN_MS) {
        // Berechnet Minuten und Sekunden für die Fehlermeldung
        const timeLeft = Math.ceil((COOLDOWN_MS - (now - user.lastTradeTime)) / 1000);
        const minutes = Math.floor(timeLeft / 60);
        const seconds = timeLeft % 60;
        throw new Error(`Markt-Abkühlung! Warte noch ${minutes}m ${seconds}s.`);
    }
    return now;
};

// ==========================================
// 1. AKTIEN KAUFEN (FIX: portfoliosCollection)
// ==========================================
app.post('/pi/stonks/buy', isAuthenticated, async (req, res) => {
    const { productId, quantity } = req.body;
    const userIdStr = req.session.userId;
    const qty = parseInt(quantity);

    // 1. Validierung
    if (!qty || qty < 1) return res.status(400).json({ error: "Ungültige Menge." });
    if (!productId) return res.status(400).json({ error: "Produkt ID fehlt." });

    try {
        const userIdObj = new ObjectId(userIdStr);

        // ID-Typ Erkennung (Zahl oder String)
        const queryProductId = isNaN(parseInt(productId)) ? productId : parseInt(productId);

        // 2. User & Produkt laden
        const [user, product] = await Promise.all([
            usersCollection.findOne({ _id: userIdObj }),
            productsCollection.findOne({ id: queryProductId })
        ]);

        if (!user) return res.status(404).json({ error: "User nicht gefunden." });
        if (!product) return res.status(404).json({ error: "Aktie nicht gefunden." });

        // 3. Cooldown Check
        try {
            if (typeof checkTradeCooldown === 'function') {
                await checkTradeCooldown(user);
            }
        } catch (e) {
            return res.status(429).json({ error: e.message });
        }

        // 4. Preis & Kosten berechnen
        // 100k Aufschlag Logik (aus deinem ursprünglichen Code-Snippet übernommen, falls gewünscht)
        // Falls du den Aufschlag nicht willst, nimm einfach product.currentPrice
        const currentPrice = parseFloat(product.currentPrice || product.price || 0);

        // Kosten berechnen
        const totalCost = currentPrice * qty;

        // 5. Geld-Check
        if (user.balance < totalCost) {
            return res.status(400).json({
                error: `Zu wenig Geld. Kosten: ${totalCost.toFixed(2)}€, Dein Konto: ${user.balance.toFixed(2)}€`
            });
        }

        // 6. Verfügbarkeit prüfen (Optional, falls du maxShares nutzt)
        const maxShares = product.maxShares || 1000000000;
        // Um das genau zu prüfen, müssten wir erst zählen, wie viele schon weg sind. 
        // Für Performance lassen wir das hier oft weg oder prüfen es einfach gegen das Inventar.

        // 7. TRANSAKTION DURCHFÜHREN

        // A) Geld abziehen (usersCollection)
        await usersCollection.updateOne(
            { _id: userIdObj },
            {
                $inc: { balance: -totalCost },
                $set: { lastTradeTime: Date.now() }
            }
        );

        // B) Aktie ins Portfolio legen (portfoliosCollection)
        // 'upsert: true' macht das Magische: Wenn Eintrag da ist -> update ($inc). Wenn nicht -> insert ($setOnInsert).
        await portfoliosCollection.updateOne(
            { userId: userIdObj, productId: queryProductId },
            {
                $inc: { quantityShares: qty }, // Erhöht die Anzahl
                // Falls es ein neuer Eintrag ist, setzen wir Startwerte:
                $setOnInsert: {
                    userId: userIdObj,
                    productId: queryProductId,
                    averageBuyPrice: currentPrice // Startpreis (kann man später verfeinern)
                }
            },
            { upsert: true }
        );

        // Neuen Kontostand für Frontend holen
        const updatedUser = await usersCollection.findOne({ _id: userIdObj }, { projection: { balance: 1 } });

        console.log(`${LOG_PREFIX_SERVER} KAUF: User ${req.session.username} kauft ${qty}x ${queryProductId} für ${totalCost}€`);

        res.json({
            message: `Kauf erfolgreich! -${totalCost.toFixed(2)}€`,
            newBalance: updatedUser.balance
        });

    } catch (e) {
        console.error(`${LOG_PREFIX_SERVER} Fehler beim Kaufen:`, e);
        res.status(500).json({ error: "Serverfehler beim Kauf." });
    }
});

app.post('/api/stonks/sell', isAuthenticated, async (req, res) => {
    const { productId, quantity } = req.body;
    const userIdStr = req.session.userId;
    const qty = parseInt(quantity);

    // 1. Validierung
    if (!qty || qty < 1) return res.status(400).json({ error: "Ungültige Menge." });
    if (!productId) return res.status(400).json({ error: "Produkt ID fehlt." });

    try {
        // WICHTIG: UserId muss als ObjectId genutzt werden, damit MongoDB es findet!
        const userIdObj = new ObjectId(userIdStr);

        // ID-Typ Erkennung: Wir wandeln es in eine Zahl um, da deine IDs im Log Nummern sind (208032)
        const queryProductId = isNaN(parseInt(productId)) ? productId : parseInt(productId);

        // 2. Wir laden PARALLEL: 
        // A) Das Portfolio-Item aus der portfoliosCollection (NICHT usersCollection!)
        // B) Den aktuellen Preis aus der productsCollection
        const [portfolioItem, product] = await Promise.all([
            portfoliosCollection.findOne({ userId: userIdObj, productId: queryProductId }),
            productsCollection.findOne({ id: queryProductId })
        ]);

        // 3. Existenz-Check
        if (!portfolioItem) {
            console.log(`[SELL ERROR] Item nicht in portfoliosCollection gefunden.`);
            console.log(`Suche nach -> User: ${userIdStr}, ProductId: ${queryProductId}`);
            return res.status(400).json({ error: "Du besitzt diese Aktie nicht." });
        }

        // Menge prüfen
        if (portfolioItem.quantityShares < qty) {
            return res.status(400).json({
                error: `Nicht genügend Aktien. Du hast ${portfolioItem.quantityShares}, willst aber ${qty} verkaufen.`
            });
        }

        // 4. Preis ermitteln
        // Fallback: Falls Produkt gelöscht wurde, versuchen wir currentPrice aus dem Portfolio-Item oder 0
        const currentPrice = product ? (product.currentPrice || product.price || 0) : 0;

        if (currentPrice <= 0) {
            return res.status(400).json({ error: "Aktueller Preis konnte nicht ermittelt werden." });
        }

        const totalPayout = currentPrice * qty;

        // 5. TRANSAKTION DURCHFÜHREN

        // A) Portfolio aktualisieren (in portfoliosCollection!)
        if (portfolioItem.quantityShares === qty) {
            // Alles verkaufen -> Eintrag aus der DB löschen
            await portfoliosCollection.deleteOne({ _id: portfolioItem._id });
        } else {
            // Teil verkaufen -> Menge reduzieren
            await portfoliosCollection.updateOne(
                { _id: portfolioItem._id },
                { $inc: { quantityShares: -qty } }
            );
        }

        // B) Geld dem User gutschreiben (in usersCollection!)
        await usersCollection.updateOne(
            { _id: userIdObj },
            {
                $inc: { balance: totalPayout },
                $set: { lastTradeTime: Date.now() }
            }
        );

        // Neuen Kontostand holen für die Anzeige im Frontend
        const updatedUser = await usersCollection.findOne({ _id: userIdObj }, { projection: { balance: 1 } });

        console.log(`${LOG_PREFIX_SERVER} VERKAUF: User ${req.session.username} verkauft ${qty}x ${queryProductId} für ${totalPayout}€`);

        res.json({
            message: `Verkauf erfolgreich! +$${totalPayout.toFixed(2)}`,
            newBalance: updatedUser.balance
        });

    } catch (e) {
        console.error(`${LOG_PREFIX_SERVER} Fehler beim Verkaufen:`, e);
        res.status(500).json({ error: "Serverfehler beim Handel." });
    }
});

// Portfolio des eingeloggten Benutzers abrufen (verbesserte Version)
app.get('/api/stonks/portfolio', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    try {
        const portfolioItems = await portfoliosCollection.find({ userId, quantityShares: { $gt: 0 } }).toArray();

        if (portfolioItems.length === 0) {
            return res.json({ portfolio: [] });
        }

        // IDs aller Produkte im Portfolio sammeln
        const productIdsInPortfolio = portfolioItems.map(item => item.productId);

        // Aktuelle Daten (Name, Preis, Bild) für diese Produkte aus der 'products' Collection holen
        const productDetails = await productsCollection.find(
            { id: { $in: productIdsInPortfolio } },
            { projection: { id: 1, name: 1, currentPrice: 1, image_url: 1, _id: 0 } }
        ).toArray();

        // Eine Map für schnellen Zugriff erstellen: productId -> productDetail
        const productDetailsMap = new Map(productDetails.map(p => [p.id, p]));

        // Das Portfolio mit den aktuellen Produktdetails anreichern
        const enrichedPortfolio = portfolioItems.map(item => {
            const details = productDetailsMap.get(item.productId);
            return {
                ...item, // Enthält userId, productId, quantityShares, averageBuyPrice
                name: details ? details.name : "Unbekanntes Produkt",
                imageUrl: details ? details.image_url : "",
                currentPrice: details ? details.currentPrice : 0
            };
        });

        res.json({ portfolio: enrichedPortfolio });
    } catch (err) {
        console.error(`${LOG_PREFIX_SERVER} Fehler beim Abrufen des Portfolios für User ${req.session.username}:`, err);
        res.status(500).json({ error: 'Serverfehler beim Laden des Portfolios.' });
    }
});

// GET /api/stocks -> Mit Suche und Limitierung für 4000+ Items
app.get('/api/stocks', async (req, res) => {
    try {
        const { search, limit } = req.query;

        // 1. LIMIT ERZWINGEN (Maximal 50, wenn nichts angegeben ist)
        // Das verhindert, dass 4000 Items geladen werden!
        const maxItems = parseInt(limit) || 50;

        // 2. Filter bauen
        const query = { isTokenCard: { $ne: true } };

        // Nur filtern, wenn wirklich gesucht wird
        if (search && search.trim().length > 0) {
            query.name = { $regex: search.trim(), $options: 'i' };
        }

        // 3. Datenbankabfrage mit LIMIT
        const stocks = await productsCollection.find(
            query,
            {
                projection: {
                    id: 1, name: 1, currentPrice: 1, price: 1, basePrice: 1,
                    // History: Nur die allerletzten 10 Punkte holen (Spart extrem Speicher)
                    priceHistory: { $slice: -10 },
                    image_url: 1
                }
            }
        )
            .sort({ id: 1 })
            .limit(maxItems) // <--- HIER IST DIE BREMSE
            .toArray();

        // 4. Formatierung (Fehlertolerant)
        const formatted = stocks.map(s => {
            const price = s.currentPrice || parseFloat((s.price || "0").replace(/[^0-9.]/g, '')) || 0;

            let change = 0;
            if (s.priceHistory && s.priceHistory.length >= 2) {
                const last = s.priceHistory[s.priceHistory.length - 1].price;
                const prev = s.priceHistory[s.priceHistory.length - 2].price;
                if (prev > 0) change = ((last - prev) / prev) * 100;
            }

            // History sicherstellen
            const historyData = Array.isArray(s.priceHistory) ? s.priceHistory.map(h => h.price) : [];

            return {
                id: s.id,
                name: s.name,
                symbol: (s.name.substring(0, 3) + String(s.id).slice(-2)).toUpperCase(),
                price: price,
                changePercent: change,
                history: historyData,
                image: s.image_url
            };
        });

        res.json(formatted);
    } catch (e) {
        console.error("Stonks Error:", e);
        // Sende leeres Array statt Fehler, damit Frontend nicht crasht
        res.json([]);
    }
});

// 2. Kombiniertes Portfolio (Alles auf einen Blick)
app.get('/api/finance/portfolio/full', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    try {
        const user = await usersCollection.findOne({ _id: userId });

        // Aktien laden
        const stockPortfolio = await portfoliosCollection.aggregate([
            { $match: { userId: userId, quantityShares: { $gt: 0 } } },
            { $lookup: { from: productsCollectionName, localField: 'productId', foreignField: 'id', as: 'details' } },
            { $unwind: "$details" },
            {
                $project: {
                    id: "$productId",
                    name: "$details.name",
                    quantity: "$quantityShares",
                    currentPrice: { $ifNull: ["$details.currentPrice", 0] },
                    buyPrice: "$averageBuyPrice",
                    image: "$details.image_url"
                }
            }
        ]).toArray();

        // Krypto laden (Aus dem globalen CRYPTO_MARKET Objekt in server.js)
        const cryptoWallet = user.cryptoWallet || {};
        const cryptoList = [];

        if (typeof CRYPTO_MARKET !== 'undefined') {
            for (const [symbol, amount] of Object.entries(cryptoWallet)) {
                if (amount > 0 && CRYPTO_MARKET[symbol]) {
                    cryptoList.push({
                        id: symbol,
                        name: CRYPTO_MARKET[symbol].name,
                        quantity: amount,
                        currentPrice: CRYPTO_MARKET[symbol].price,
                        type: 'crypto'
                    });
                }
            }
        }

        // Gesamtwert berechnen
        let totalNetWorth = user.balance || 0;
        stockPortfolio.forEach(s => { totalNetWorth += s.quantity * s.currentPrice; });
        cryptoList.forEach(c => { totalNetWorth += c.quantity * c.currentPrice; });

        res.json({
            balance: user.balance,
            netWorth: totalNetWorth,
            stocks: stockPortfolio,
            crypto: cryptoList
        });

    } catch (e) {
        console.error("Portfolio Error:", e);
        res.status(500).json({ error: "Fehler." });
    }
});

// =========================================================
// === DONT BLAME ME ENDPUNKTE ===
// =========================================================
const LOG_PREFIX_DBM = "[DontBlameMe API]";

// Hilfsfunktion zur Generierung von zufälligen Bildparametern
function generateImageParams() {
    const bgColors = ['#f4a261', '#e76f51', '#2a9d8f', '#264653', '#e9c46a', '#fefae0', '#606c38'];
    const textColors = ['#ffffff', '#000000', '#2d3436'];
    const fonts = ['Arial', 'Verdana', 'Helvetica', 'Georgia', 'Courier New', 'Comic Sans MS'];

    const bgColor = bgColors[Math.floor(Math.random() * bgColors.length)];
    // Stelle sicher, dass der Text lesbar ist (einfache Kontrastprüfung)
    let textColor = textColors[Math.floor(Math.random() * textColors.length)];
    if (bgColor === '#264653' && textColor === '#000000') {
        textColor = '#ffffff'; // Dunkler Hintergrund -> heller Text
    }
    if (bgColor === '#fefae0' && textColor === '#ffffff') {
        textColor = '#000000'; // Heller Hintergrund -> dunkler Text
    }

    return {
        backgroundColor: bgColor,
        textColor: textColor,
        fontFamily: fonts[Math.floor(Math.random() * fonts.length)],
        fontSize: Math.floor(Math.random() * 10) + 24, // Größe zwischen 24px und 34px
        textAlign: ['center', 'left', 'right'][Math.floor(Math.random() * 3)],
        padding: Math.floor(Math.random() * 20) + 10 // Padding zwischen 10px und 30px
    };
}

// GET-Endpunkt, um alle Posts abzurufen
app.get('/api/dont-blame-me', async (req, res) => {
    console.log(`${LOG_PREFIX_DBM} Rufe alle Posts ab.`);
    try {
        const posts = await dontBlameMeCollection.find({}).sort({ createdAt: -1 }).toArray();
        res.json({ posts });
    } catch (err) {
        console.error(`${LOG_PREFIX_DBM} Fehler beim Abrufen der Posts:`, err);
        res.status(500).json({ error: 'Fehler beim Laden der Posts.' });
    }
});

// POST-Endpunkt, um einen neuen Post zu erstellen
app.post('/api/dont-blame-me', isAuthenticated, async (req, res) => {
    const { reason } = req.body;
    const userId = new ObjectId(req.session.userId);
    const username = req.session.username;

    if (!reason || typeof reason !== 'string' || reason.trim().length < 5 || reason.trim().length > 280) {
        return res.status(400).json({ error: 'Ein Grund (5-280 Zeichen) ist erforderlich.' });
    }

    console.log(`${LOG_PREFIX_DBM} User ${username} reicht einen neuen Post ein.`);

    try {
        const imageParams = generateImageParams();
        const newPost = {
            userId,
            username,
            reason: reason.trim(),
            imageParams, // Speichert die "Anleitung" für den Generator
            createdAt: new Date()
        };
        await dontBlameMeCollection.insertOne(newPost);
        res.status(201).json({ message: 'Post erfolgreich erstellt!', post: newPost });
    } catch (err) {
        console.error(`${LOG_PREFIX_DBM} Fehler beim Erstellen des Posts für User ${username}:`, err);
        res.status(500).json({ error: 'Serverfehler beim Erstellen des Posts.' });
    }
});

// =========================================================
// === HUMAN GRADES (CORE LOGIC) ===
// =========================================================

// Erweiterter Seed mit echten Fächern für Lehrer UND Beispiel-Personen
async function seedHumanGradesDefaults() {
    // 1. Kategorien & Kriterien anlegen (falls leer)
    const catCount = await categoriesCollection.countDocuments();
    if (catCount === 0) {
        await categoriesCollection.insertMany([
            { id: 'lehrer', label: 'Lehrer' },
            { id: 'politiker', label: 'Politiker' },
            { id: 'promis', label: 'Prominente' }
        ]);

        await criteriaCollection.insertMany([
            // --- LEHRER (Echte Fächer) ---
            { id: 'mat', label: 'Mathematik', type: 'main', categoryId: 'lehrer' },
            { id: 'deu', label: 'Deutsch', type: 'main', categoryId: 'lehrer' },
            { id: 'eng', label: 'Englisch', type: 'main', categoryId: 'lehrer' },
            { id: 'rel', label: 'Religionslehre', type: 'main', categoryId: 'lehrer' },
            { id: 'spo', label: 'Sport', type: 'main', categoryId: 'lehrer' },
            { id: 'bio', label: 'Biologie', type: 'main', categoryId: 'lehrer' },
            { id: 'phy', label: 'Physik', type: 'main', categoryId: 'lehrer' },
            { id: 'che', label: 'Chemie', type: 'main', categoryId: 'lehrer' },
            { id: 'his', label: 'Geschichte', type: 'main', categoryId: 'lehrer' },
            { id: 'geo', label: 'Erdkunde', type: 'main', categoryId: 'lehrer' },
            { id: 'pol', label: 'Politik', type: 'main', categoryId: 'lehrer' },
            { id: 'kun', label: 'Kunst', type: 'main', categoryId: 'lehrer' },
            { id: 'mus', label: 'Musik', type: 'main', categoryId: 'lehrer' },
            { id: 'inf', label: 'Informatik', type: 'sec', categoryId: 'lehrer' },
            { id: 'tec', label: 'Technik', type: 'sec', categoryId: 'lehrer' },
            { id: 'fra', label: 'Französisch', type: 'sec', categoryId: 'lehrer' },
            { id: 'ndl', label: 'Niederländisch', type: 'sec', categoryId: 'lehrer' },

            // --- POLITIKER ---
            { id: 'glaub', label: 'Glaubwürdigkeit', type: 'main', categoryId: 'politiker' },
            { id: 'rhet', label: 'Rhetorik', type: 'main', categoryId: 'politiker' },
            { id: 'komp', label: 'Fachkompetenz', type: 'main', categoryId: 'politiker' },
            { id: 'durch', label: 'Durchsetzungsvermögen', type: 'main', categoryId: 'politiker' },
            { id: 'symp', label: 'Sympathie', type: 'sec', categoryId: 'politiker' },
            { id: 'social', label: 'Social Media Präsenz', type: 'sec', categoryId: 'politiker' },

            // --- PROMIS ---
            { id: 'ent', label: 'Entertainment', type: 'main', categoryId: 'promis' },
            { id: 'tal', label: 'Talent', type: 'main', categoryId: 'promis' },
            { id: 'style', label: 'Style / Auftreten', type: 'main', categoryId: 'promis' },
            { id: 'vorbild', label: 'Vorbildfunktion', type: 'sec', categoryId: 'promis' },
            { id: 'skandal', label: 'Skandalfreiheit', type: 'sec', categoryId: 'promis' }
        ]);
        console.log(`${LOG_PREFIX_SERVER} Human Grades Kategorien & Kriterien initialisiert.`);
    }

    // 2. Default Menschen anlegen (falls leer)
    const humanCount = await humansCollection.countDocuments();
    if (humanCount === 0) {
        const defaultHumans = [
            // Promis
            {
                name: "Taylor Swift",
                categoryId: "promis",
                criteriaIds: ["ent", "tal", "style", "vorbild", "skandal"],
                averages: {}, totalAverage: 0, ratingCount: 0, createdAt: new Date()
            },
            {
                name: "Elon Musk",
                categoryId: "promis",
                criteriaIds: ["ent", "tal", "style", "vorbild", "skandal"],
                averages: {}, totalAverage: 0, ratingCount: 0, createdAt: new Date()
            },
            // Politiker
            {
                name: "Olaf Scholz",
                categoryId: "politiker",
                criteriaIds: ["glaub", "rhet", "komp", "durch", "symp", "social"],
                averages: {}, totalAverage: 0, ratingCount: 0, createdAt: new Date()
            },
            {
                name: "Christian Lindner",
                categoryId: "politiker",
                criteriaIds: ["glaub", "rhet", "komp", "durch", "symp", "social"],
                averages: {}, totalAverage: 0, ratingCount: 0, createdAt: new Date()
            },
            {
                name: "Robert Habeck",
                categoryId: "politiker",
                criteriaIds: ["glaub", "rhet", "komp", "durch", "symp", "social"],
                averages: {}, totalAverage: 0, ratingCount: 0, createdAt: new Date()
            }
        ];

        await humansCollection.insertMany(defaultHumans);
        console.log(`${LOG_PREFIX_SERVER} Default Menschen (Promis/Politiker) angelegt.`);
    }
}

// Durchschnittsberechnung (Generisch)
async function updateHumanAverage(humanId) {
    const hId = new ObjectId(humanId);
    const ratings = await ratingsCollection.find({ humanId: hId }).toArray();

    if (ratings.length === 0) {
        await humansCollection.updateOne({ _id: hId }, { $set: { averages: {}, totalAverage: 0, ratingCount: 0 } });
        return;
    }

    const criteriaStats = {};
    let totalSum = 0;
    let totalCount = 0;

    ratings.forEach(r => {
        if (!r.grades) return;
        Object.keys(r.grades).forEach(cId => {
            const grade = r.grades[cId];
            if (grade >= 1 && grade <= 6) {
                if (!criteriaStats[cId]) criteriaStats[cId] = { sum: 0, count: 0 };
                criteriaStats[cId].sum += grade;
                criteriaStats[cId].count += 1;
                totalSum += grade;
                totalCount += 1;
            }
        });
    });

    const averages = {};
    Object.keys(criteriaStats).forEach(cId => {
        averages[cId] = criteriaStats[cId].sum / criteriaStats[cId].count;
    });

    const totalAverage = totalCount > 0 ? (totalSum / totalCount) : 0;

    await humansCollection.updateOne(
        { _id: hId },
        { $set: { averages: averages, totalAverage: totalAverage, ratingCount: ratings.length } }
    );
}

// --- API ROUTES ---

// Kategorien & Kriterien
app.get('/api/human/meta', async (req, res) => {
    const [cats, crits] = await Promise.all([
        categoriesCollection.find({}).toArray(),
        criteriaCollection.find({}).toArray()
    ]);
    res.json({ categories: cats, criteria: crits });
});

// Admin: Kategorie erstellen
app.post('/api/human/admin/categories', isAuthenticated, isAdmin, async (req, res) => {
    const { label } = req.body;
    const id = label.toLowerCase().replace(/[^a-z0-9]/g, '');
    try {
        await categoriesCollection.insertOne({ id, label });
        res.json({ message: "Kategorie erstellt." });
    } catch (e) { res.status(500).json({ error: "Fehler." }); }
});

// Admin: Kriterium erstellen
app.post('/api/human/admin/criteria', isAuthenticated, isAdmin, async (req, res) => {
    const { label, type, categoryId } = req.body;
    const id = label.toLowerCase().replace(/[^a-z0-9]/g, '').substring(0, 10) + "_" + Math.floor(Math.random() * 1000);
    try {
        await criteriaCollection.insertOne({ id, label, type, categoryId });
        res.json({ message: "Kriterium erstellt." });
    } catch (e) { res.status(500).json({ error: "Fehler." }); }
});

// Menschen laden
app.get('/api/human/list', async (req, res) => {
    const humans = await humansCollection.find({}).sort({ name: 1 }).toArray();
    res.json({ humans });
});

// Admin: Mensch erstellen
app.post('/api/human/admin/humans', isAuthenticated, isAdmin, async (req, res) => {
    const { name, categoryId, criteriaIds } = req.body;
    if (!name || !categoryId) return res.status(400).json({ error: "Daten fehlen" });

    const newHuman = {
        name,
        categoryId, // Z.B. 'politiker'
        criteriaIds: criteriaIds || [],
        averages: {},
        totalAverage: 0,
        ratingCount: 0,
        createdAt: new Date()
    };
    await humansCollection.insertOne(newHuman);
    res.json({ message: "Mensch angelegt." });
});

// Admin: Mensch bearbeiten
app.put('/api/human/admin/humans/:id', isAuthenticated, isAdmin, async (req, res) => {
    const { name, categoryId, criteriaIds } = req.body;
    try {
        await humansCollection.updateOne({ _id: new ObjectId(req.params.id) }, { $set: { name, categoryId, criteriaIds } });
        res.json({ message: "Update erfolgreich." });
    } catch (e) { res.status(500).json({ error: "Fehler." }); }
});

// Admin: Mensch löschen
app.delete('/api/human/admin/humans/:id', isAuthenticated, isAdmin, async (req, res) => {
    const hId = new ObjectId(req.params.id);
    await humansCollection.deleteOne({ _id: hId });
    await ratingsCollection.deleteMany({ humanId: hId });
    res.json({ message: "Gelöscht." });
});

// Bewerten
app.post('/api/human/rate', isAuthenticated, async (req, res) => {
    const { humanId, grades } = req.body;
    const userId = new ObjectId(req.session.userId);

    const human = await humansCollection.findOne({ _id: new ObjectId(humanId) });
    if (!human) return res.status(404).json({ error: "Person nicht gefunden" });

    const cleanGrades = {};
    // Nur erlaubte Kriterien speichern
    human.criteriaIds.forEach(cId => {
        if (grades[cId]) {
            const val = parseInt(grades[cId]);
            if (val >= 1 && val <= 6) cleanGrades[cId] = val;
        }
    });

    if (Object.keys(cleanGrades).length === 0) return res.status(400).json({ error: "Keine gültige Bewertung." });

    await ratingsCollection.updateOne(
        { humanId: human._id, userId: userId },
        { $set: { grades: cleanGrades, timestamp: new Date(), username: req.session.username } },
        { upsert: true }
    );

    updateHumanAverage(humanId);
    res.json({ message: "Bewertung gespeichert." });
});

// Admin Endpoint zum Zurücksetzen der Datenbank (damit die neuen Fächer laden)
app.post('/api/human/admin/reset-defaults', isAuthenticated, isAdmin, async (req, res) => {
    try {
        await categoriesCollection.deleteMany({});
        await criteriaCollection.deleteMany({});
        await seedHumanGradesDefaults();
        res.json({ message: "Datenbank auf Standardwerte (Fächer/Kategorien) zurückgesetzt." });
    } catch (e) { res.status(500).json({ error: "Fehler beim Reset." }); }
});

// =========================================================
// === ADMIN MODERATION (RATING MANAGEMENT) ===
// =========================================================

// 1. Liste aller User holen, die Bewertungen abgegeben haben
app.get('/api/human/admin/raters', isAuthenticated, isAdmin, async (req, res) => {
    try {
        // Aggregation: Gruppiere nach UserID und zähle Bewertungen
        const raters = await ratingsCollection.aggregate([
            {
                $group: {
                    _id: "$userId",
                    username: { $first: "$username" }, // Username ist im Rating gespeichert
                    ratingCount: { $sum: 1 },
                    lastActive: { $max: "$timestamp" }
                }
            },
            { $sort: { lastActive: -1 } } // Die neusten zuerst
        ]).toArray();

        res.json({ raters });
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: "Fehler beim Laden der User." });
    }
});

// 2. Alle Bewertungen eines spezifischen Users holen
app.get('/api/human/admin/raters/:userId', isAuthenticated, isAdmin, async (req, res) => {
    try {
        const uId = new ObjectId(req.params.userId);

        // Hole Ratings und joine mit Humans-Collection, um den Namen der bewerteten Person zu haben
        const userRatings = await ratingsCollection.aggregate([
            { $match: { userId: uId } },
            {
                $lookup: {
                    from: "humans",       // Name der Humans Collection
                    localField: "humanId",
                    foreignField: "_id",
                    as: "humanInfo"
                }
            },
            { $unwind: "$humanInfo" }, // Array auflösen
            {
                $project: {
                    _id: 1,
                    grades: 1,
                    timestamp: 1,
                    humanName: "$humanInfo.name",
                    humanId: "$humanId"
                }
            },
            { $sort: { timestamp: -1 } }
        ]).toArray();

        res.json({ ratings: userRatings });
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: "Fehler beim Laden der Bewertungen." });
    }
});

// 3. Einzelne Bewertung löschen
app.delete('/api/human/admin/ratings/:id', isAuthenticated, isAdmin, async (req, res) => {
    try {
        const rId = new ObjectId(req.params.id);

        // Zuerst Bewertung finden, um HumanID für Neuberechnung zu haben
        const rating = await ratingsCollection.findOne({ _id: rId });
        if (!rating) return res.status(404).json({ error: "Bewertung nicht gefunden." });

        // Löschen
        await ratingsCollection.deleteOne({ _id: rId });

        // Durchschnitt des betroffenen Menschen neu berechnen
        await updateHumanAverage(rating.humanId);

        res.json({ message: "Bewertung gelöscht und Durchschnitt aktualisiert." });
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: "Fehler beim Löschen." });
    }
});

// =========================================================
// === LIMO BANKING API ===
// =========================================================

// 1. Transaktionshistorie abrufen
app.get('/api/bank/transactions', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    try {
        // Suche Transaktionen, wo der User Sender ODER Empfänger war
        const history = await bankTransactionsCollection.find({
            $or: [{ fromId: userId }, { toId: userId }]
        }).sort({ timestamp: -1 }).limit(50).toArray();

        res.json({ transactions: history });
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: "Fehler beim Laden der Umsätze." });
    }
});

// 2. Überweisung tätigen (Geld oder Tokens) - MIT SICHERHEITS-UPDATES
app.post('/api/bank/transfer', isAuthenticated, async (req, res) => {
    const { recipientName, amount, type, reason, highLimitMode } = req.body;
    const senderId = new ObjectId(req.session.userId);
    const senderName = req.session.username;

    // Standard Limits
    const MAX_MONEY_TRANSFER = 1000000;
    const MAX_TOKEN_TRANSFER = 1000;

    // High-Limit Modus (Kostet 1% Gebühr)
    // Javascript Safe Max ist ca. 9 Billiarden. Mehr geht technisch nicht präzise ohne BigInt Umbau.
    const ULTRA_LIMIT = Number.MAX_SAFE_INTEGER;

    if (!recipientName || !amount || amount <= 0) return res.status(400).json({ error: "Ungültige Daten." });
    if (recipientName.toLowerCase() === senderName.toLowerCase()) return res.status(400).json({ error: "Keine Überweisung an sich selbst." });

    // Betrag säubern
    const cleanAmount = type === 'token' ? Math.floor(amount) : roundMoney(parseFloat(amount));

    // Limit Check
    if (!highLimitMode) {
        // Normaler Modus: Strenge Limits, keine Gebühr
        if (type !== 'token' && cleanAmount > MAX_MONEY_TRANSFER) return res.status(400).json({ error: `Limit überschritten! Max $${MAX_MONEY_TRANSFER.toLocaleString()} (oder aktiviere High-Limit).` });
        if (type === 'token' && cleanAmount > MAX_TOKEN_TRANSFER) return res.status(400).json({ error: `Limit überschritten! Max ${MAX_TOKEN_TRANSFER} Tokens (oder aktiviere High-Limit).` });
    } else {
        // High Limit Modus: Fast kein Limit, aber Gebühr
        if (cleanAmount > ULTRA_LIMIT) return res.status(400).json({ error: "Betrag übersteigt die mathematischen Grenzen des Bank-Computers." });
    }

    const session = client.startSession();

    try {
        await session.withTransaction(async () => {
            // 1. Sender laden
            const sender = await usersCollection.findOne({ _id: senderId }, { session });
            if (sender.infinityMoney && !sender.isAdmin) throw new Error("Infinity-Money User dürfen nicht überweisen.");

            // 2. Empfänger suchen
            const recipient = await usersCollection.findOne({ username: { $regex: new RegExp(`^${recipientName}$`, 'i') } }, { session });
            if (!recipient) throw new Error("Empfänger nicht gefunden.");

            // 3. Gebühr berechnen
            let fee = 0;
            if (highLimitMode) {
                // 1% Gebühr
                fee = type === 'token' ? Math.floor(cleanAmount * 0.01) : roundMoney(cleanAmount * 0.01);
                // Mindestgebühr 1 (bei Tokens) oder 0.01 (bei Geld)
                if (type === 'token' && fee < 1) fee = 1;
                if (type !== 'token' && fee < 0.01) fee = 0.01;
            }

            const totalDeduction = cleanAmount; // Der Sender zahlt den vollen Betrag
            const amountReceived = cleanAmount - fee; // Der Empfänger kriegt den Rest

            if (amountReceived <= 0) throw new Error("Der Betrag ist zu klein für die Gebühren.");

            // 4. Sender belasten
            const updateFilter = { _id: senderId };
            const updateAction = {};

            if (type === 'token') {
                updateFilter.tokens = { $gte: totalDeduction };
                updateAction.$inc = { tokens: -totalDeduction };
            } else {
                if (!sender.isAdmin && !sender.infinityMoney) {
                    updateFilter.balance = { $gte: totalDeduction };
                    updateAction.$inc = { balance: -totalDeduction };
                }
            }

            if (updateAction.$inc) {
                const senderResult = await usersCollection.updateOne(updateFilter, updateAction, { session });
                if (senderResult.modifiedCount === 0) {
                    throw new Error(type === 'token' ? "Nicht genügend Tokens." : "Nicht genügend Guthaben.");
                }
            }

            // 5. Empfänger gutschreiben (Abzüglich Gebühr)
            const targetField = type === 'token' ? 'tokens' : 'balance';
            await usersCollection.updateOne(
                { _id: recipient._id },
                { $inc: { [targetField]: amountReceived } },
                { session }
            );

            // 6. Gebühr in die Staatskasse (nur bei Geld)
            if (fee > 0 && type !== 'token') {
                await systemSettingsCollection.updateOne(
                    { id: 'state_treasury' },
                    { $inc: { balance: fee } },
                    { upsert: true, session }
                );
            }

            // 7. Loggen
            await bankTransactionsCollection.insertOne({
                fromId: senderId,
                fromName: senderName,
                toId: recipient._id,
                toName: recipient.username,
                amount: cleanAmount, // Gesendeter Betrag
                fee: fee,            // Protokollierte Gebühr
                netAmount: amountReceived, // Was ankam
                type: type,
                reason: reason || (highLimitMode ? "High-Limit Überweisung" : "Überweisung"),
                timestamp: new Date()
            }, { session });
        });

        const updatedUser = await usersCollection.findOne({ _id: senderId });
        res.json({ message: "Überweisung erfolgreich!", newBalance: updatedUser.balance });

    } catch (e) {
        console.error(`${LOG_PREFIX_SERVER} Transfer Fehler:`, e.message);
        res.status(400).json({ error: e.message || "Transaktion fehlgeschlagen." });
    } finally {
        await session.endSession();
    }
});

// 3. User suchen (für Überweisungen)
app.get('/api/bank/users/search', isAuthenticated, async (req, res) => {
    const { term } = req.query;
    if (!term || typeof term !== 'string' || term.length < 2) {
        return res.json({ users: [] });
    }

    try {
        // Suche User, die mit 'term' anfangen oder ihn enthalten (Case insensitive)
        const foundUsers = await usersCollection.find(
            { username: { $regex: term, $options: 'i' } },
            { projection: { username: 1, _id: 0 } } // Nur Username, keine IDs/Daten leaken
        ).limit(5).toArray();

        res.json({ users: foundUsers });
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: "Suchfehler." });
    }
});

// API: Tägliche Belohnung abholen
app.post('/api/daily', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    const user = await usersCollection.findOne({ _id: userId });

    const now = new Date();
    const last = user.lastDaily ? new Date(user.lastDaily) : new Date(0);

    // Prüfen ob heute schon abgeholt (gleicher Tag, Monat, Jahr)
    if (now.getDate() === last.getDate() && now.getMonth() === last.getMonth() && now.getFullYear() === last.getFullYear()) {
        return res.status(400).json({ error: "Komm morgen wieder!" });
    }

    // Streak Logik (War das letzte mal gestern?)
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);

    let streak = user.dailyStreak || 0;
    // Wenn das letzte mal NICHT gestern war (und nicht heute), ist der Streak gebrochen
    if (last.toDateString() !== yesterday.toDateString()) {
        streak = 0;
    }
    streak++;

    // Belohnung berechnen
    let reward = 100 + (streak * 10); // Start $100, pro Tag +$10
    if (reward > 500) reward = 500; // Cap bei $500

    await usersCollection.updateOne(
        { _id: userId },
        {
            $inc: { balance: reward },
            $set: { lastDaily: now, dailyStreak: streak }
        }
    );

    res.json({ message: `Daily abgeholt! +$${reward} (Streak: ${streak} Tage)` });
});

// =========================================================
// === LIMO NEWS NETWORK (LNN) - SMART V2 ===
// =========================================================

const GEMINI_API_KEY = process.env.GEMINI_API_KEY;

const NEWS_INTERVAL_MS = 75 * 60 * 1000;

// Hilfsfunktion: Zeitstempel des letzten Laufs aus der DB holen & aktualisieren
// Das verhindert, dass der Bot alte Kamellen wiederholt.
async function getLastNewsTime(update = false) {
    // Wir speichern den Zeitstempel in 'systemSettings', damit er auch Neustarts überlebt
    const setting = await systemSettingsCollection.findOne({ id: 'lnn_last_run' });

    // Wenn noch nie gelaufen, nimm "jetzt minus Intervall"
    const lastRun = setting ? new Date(setting.timestamp) : new Date(Date.now() - NEWS_INTERVAL_MS);

    if (update) {
        await systemSettingsCollection.updateOne(
            { id: 'lnn_last_run' },
            { $set: { timestamp: new Date() } },
            { upsert: true }
        );
    }
    return lastRun;
}

// Hauptfunktion: Kontext sammeln
async function gatherSmartNewsContext(lastRun) {
    console.log(`${LOG_PREFIX_SERVER} [LNN] Sammle Daten seit: ${lastRun.toLocaleTimeString()}`);

    // 1. Don't Blame Me (Beichten)
    const newConfessions = await dontBlameMeCollection.find({
        createdAt: { $gt: lastRun }
    }).limit(3).toArray();

    // 2. Shop / Wirtschaft (Große Käufe)
    const bigOrders = await ordersCollection.find({
        date: { $gt: lastRun },
        total: { $gt: 500 }
    }).limit(3).toArray();

    // 3. Crime (Überfälle)
    let crimeNews = [];
    if (typeof robberyLogsCollection !== 'undefined') {
        crimeNews = await robberyLogsCollection.find({
            timestamp: { $gt: lastRun },
            success: true
        }).sort({ amountLost: -1 }).limit(2).toArray();
    }

    // 4. Auktionen (Verkäufe)
    const endedAuctions = await auctionsCollection.find({
        status: 'ended_sold',
        endTime: { $gt: lastRun }
    }).sort({ currentBid: -1 }).limit(1).toArray();

    // 5. Tinda (Matches)
    const tindaMatches = await limChatsCollection.countDocuments({
        type: 'tinda',
        createdAt: { $gt: lastRun }
    });

    // 6. Neu: Heist Status (Falls die Firewall gerade gefallen ist)
    const firewall = await systemSettingsCollection.findOne({ id: 'heist_firewall' });
    const heistOpen = firewall && firewall.integrity <= 0 && Date.now() < firewall.openUntil;

    // --- KONTEXT BAUEN (Ohne Aktien!) ---
    let contextParts = [];

    if (newConfessions.length > 0) {
        const texts = newConfessions.map(c => `"${c.reason}" (von ${c.username})`).join(", ");
        contextParts.push(`- Beichten: Neue schmutzige Geheimnisse: ${texts}.`);
    }

    if (bigOrders.length > 0) {
        const buyers = bigOrders.map(o => `${o.username} hat heftig geshoppt ($${o.total.toFixed(0)})`).join(", ");
        contextParts.push(`- Konsum: Diese User lassen das Geld fließen: ${buyers}.`);
    }

    if (crimeNews.length > 0) {
        const heist = crimeNews[0];
        contextParts.push(`- Kriminalität: ${heist.attackerName} hat erfolgreich zugeschlagen und $${heist.amountLost.toFixed(2)} erbeutet!`);
    }

    if (endedAuctions.length > 0) {
        const auc = endedAuctions[0];
        contextParts.push(`- Auktion: "${auc.productName}" ging für kranke $${auc.currentBid} an ${auc.highestBidderUsername}.`);
    }

    if (tindaMatches > 0) {
        contextParts.push(`- Dating: Es gab ${tindaMatches} neue Matches auf Tinda. Limazon wird wieder horny.`);
    }

    if (heistOpen) {
        contextParts.push(`- EILMELDUNG: Die Firewall der Staatskasse ist momentan deaktiviert! Diebstahl für alle!`);
    }

    if (contextParts.length === 0) return null;

    return contextParts.join("\n");
}

// Hauptfunktion: News generieren
async function generateAiNews(force = false) {
    if (!GEMINI_API_KEY) return;

    const lastRun = await getLastNewsTime();
    const contextData = await gatherSmartNewsContext(lastRun);

    if (!contextData && !force) {
        console.log(`${LOG_PREFIX_SERVER} [LNN] Nichts Relevantes passiert.`);
        return null;
    }

    let lastHeadline = "Keine";
    try {
        const lastNews = await newsCollection.find().sort({ createdAt: -1 }).limit(1).toArray();
        if (lastNews.length > 0) lastHeadline = lastNews[0].headline;
    } catch (e) { }

    const promptData = contextData || "Es ist verdächtig ruhig. Die User planen wohl gerade den nächsten großen Coup.";

    const modelName = "gemini-3.1-flash-lite-preview"; // Nutze das aktuellste Modell
    const url = `https://generativelanguage.googleapis.com/v1beta/models/${modelName}:generateContent?key=${GEMINI_API_KEY}`;

    const prompt = `
    Du bist der zynische und leicht soziopathische Chefredakteur des "Limo News Network" (LNN). 
    Dein Job ist es, das Leben der User in Limazon durch den Dreck zu ziehen oder Helden zu feiern.

    EVENTS DER LETZTEN STUNDE:
    ${promptData}

    WICHTIGE REGELN:
    1. Erwähne NIEMALS Aktien, Kurse, Stonks oder die Börse. Das ist langweilig und verboten.
    2. Konzentriere dich auf Beichten, Raubüberfälle, Dating-Drama oder Reichtum einzelner User.
    3. Sei extrem sarkastisch, humorvoll und reißerisch (Boulevard-Stil).
    4. Die letzte Schlagzeile war "${lastHeadline}". Wiederhole dich nicht!
    5. Länge: Max. 45 Wörter.

    Antworte NUR als JSON:
    {
      "headline": "Reißerische Headline",
      "content": "Sarkastischer Artikeltext"
    }
    `;

    try {
        const response = await axios.post(url, {
            contents: [{ parts: [{ text: prompt }] }]
        });

        if (!response.data || !response.data.candidates[0]) throw new Error("API Error");

        let textResponse = response.data.candidates[0].content.parts[0].text;
        textResponse = textResponse.replace(/```json/g, '').replace(/```/g, '').trim();

        const article = JSON.parse(textResponse);

        const newEntry = {
            headline: article.headline,
            content: article.content,
            author: "LNN AI Bot",
            category: "Community",
            createdAt: new Date(),
            likes: 0
        };

        await newsCollection.insertOne(newEntry);
        await getLastNewsTime(true);
        updateDataVersion('news');

        console.log(`${LOG_PREFIX_SERVER} [LNN] News ohne Aktien-Gelaber gepostet: "${article.headline}"`);
        return newEntry;

    } catch (apiErr) {
        console.error(`${LOG_PREFIX_SERVER} [LNN] Fehler:`, apiErr.message);
        return null;
    }
}

// Job starten
if (cluster.isPrimary && GEMINI_API_KEY) {
    setTimeout(() => generateAiNews(false), 60000); 
    setInterval(() => generateAiNews(false), NEWS_INTERVAL_MS);
}
// --- API ENDPOINTS ---

// News abrufen
app.get('/api/news', async (req, res) => {
    try {
        const news = await newsCollection.find({}).sort({ createdAt: -1 }).limit(20).toArray();
        res.json({ news });
    } catch (e) { res.status(500).json({ error: "Fehler." }); }
});

// Like
app.post('/api/news/:id/like', isAuthenticated, async (req, res) => {
    try { await newsCollection.updateOne({ _id: new ObjectId(req.params.id) }, { $inc: { likes: 1 } }); res.json({ message: "Geliked!" }); }
    catch (e) { res.status(500).json({ error: "Fehler." }); }
});

// Admin: Manuell Posten
app.post('/api/admin/news', isAuthenticated, isAdmin, async (req, res) => {
    const { headline, content } = req.body;
    await newsCollection.insertOne({ headline, content, author: req.session.username, category: "Offiziell", createdAt: new Date(), likes: 0 });
    res.json({ message: "Veröffentlicht." });
});

// Admin: AI Trigger (NEU)
app.post('/api/admin/news/trigger-ai', isAuthenticated, isAdmin, async (req, res) => {
    try {
        const article = await generateAiNews(true); // true = Force generation
        res.json({ message: "AI News generiert!", article });
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: "AI Fehler: " + e.message });
    }
});

// Admin: News löschen
app.delete('/api/admin/news/:id', isAuthenticated, isAdmin, async (req, res) => {
    try {
        const result = await newsCollection.deleteOne({ _id: new ObjectId(req.params.id) });
        if (result.deletedCount === 0) return res.status(404).json({ error: "Artikel nicht gefunden." });
        res.json({ message: "Artikel erfolgreich gelöscht." });
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: "Fehler beim Löschen." });
    }
});

// =========================================================
// === MASTER ADMIN PANEL API ===
// =========================================================

// --- USER MANAGEMENT ---

// Alle User laden
app.get('/api/admin/users', isAuthenticated, isAdmin, async (req, res) => {
    try {
        const users = await usersCollection.find({}, { projection: { password: 0 } }).toArray();
        res.json({ users });
    } catch (e) { res.status(500).json({ error: "Fehler." }); }
});

// User bearbeiten (Geld, Tokens, Admin-Status UND Infinity Money)
app.put('/api/admin/users/:id', isAuthenticated, isAdmin, async (req, res) => {
    const { balance, tokens, infinityMoney, role, permissions } = req.body;
    try {
        const updateData = {};
        if (balance !== undefined) updateData.balance = parseFloat(balance);
        if (tokens !== undefined) updateData.tokens = parseInt(tokens);
        if (infinityMoney !== undefined) {
            updateData.infinityMoney = infinityMoney;
            updateData.unlockedInfinityMoney = infinityMoney;
        }
        
        // NEU: Rolle und Rechte speichern
        if (role !== undefined) {
            updateData.role = role;
            // Legacy Support: Wenn Rolle "admin" ist, setze isAdmin auf true, sonst false
            updateData.isAdmin = (role === 'admin'); 
        }
        if (permissions !== undefined) updateData.permissions = permissions;

        await usersCollection.updateOne({ _id: new ObjectId(req.params.id) }, { $set: updateData });
        res.json({ message: "User erfolgreich aktualisiert." });
    } catch (e) {
        res.status(500).json({ error: "Fehler beim Aktualisieren." });
    }
});

// User Passwort Reset
app.post('/api/admin/users/:id/reset-pw', isAuthenticated, isAdmin, async (req, res) => {
    const { newPassword } = req.body;
    if (!newPassword || newPassword.length < 6) return res.status(400).json({ error: "Passwort zu kurz." });

    try {
        const hashedPassword = await bcrypt.hash(newPassword, 10);
        await usersCollection.updateOne(
            { _id: new ObjectId(req.params.id) },
            { $set: { password: hashedPassword } }
        );
        res.json({ message: "Passwort geändert." });
    } catch (e) { res.status(500).json({ error: "Fehler." }); }
});

// User löschen
// --- USER MANAGEMENT: LÖSCHEN MIT CLEANUP ---
app.delete('/api/admin/users/:id', isAuthenticated, isAdmin, async (req, res) => {
    try {
        const uId = new ObjectId(req.params.id);

        // 1. User selbst löschen
        const userResult = await usersCollection.deleteOne({ _id: uId });

        if (userResult.deletedCount === 0) return res.status(404).json({ error: "User nicht gefunden" });

        // 2. Alles aufräumen, was dem User gehörte
        console.log(`${LOG_PREFIX_SERVER} 🧹 Starte Cleanup für User ${uId}...`);

        await Promise.all([
            inventoriesCollection.deleteMany({ userId: uId }),
            portfoliosCollection.deleteMany({ userId: uId }),
            wheelsCollection.deleteMany({ creatorId: uId }),
            auctionsCollection.deleteMany({ sellerId: uId }),
            ideasCollection.deleteMany({ submitterId: uId }),
            ratingsCollection.deleteMany({ userId: uId }),

            // FIX: "userId" anstelle von "authorId"
            dontBlameMeCollection.deleteMany({ userId: uId }),

            limUserChatSettingsCollection.deleteMany({ userId: uId })
        ]);

        console.log(`${LOG_PREFIX_SERVER} ✅ User ${uId} und alle verknüpften Daten gelöscht.`);
        res.json({ message: "User und alle verknüpften Daten wurden restlos gelöscht." });

    } catch (e) {
        console.error(e);
        res.status(500).json({ error: "Fehler beim Löschen." });
    }
});

// Admin: User bestrafen (Geld abziehen, erlaubt Minus!)
app.post('/api/admin/users/:id/fine', isAuthenticated, isAdmin, async (req, res) => {
    const { amount, reason } = req.body;
    const fine = parseFloat(amount);

    if (!fine || fine <= 0) return res.status(400).json({ error: "Betrag muss positiv sein." });

    try {
        // 1. Geld abziehen (ohne Prüfung auf 0 -> Dispo erzwingen!)
        await usersCollection.updateOne(
            { _id: new ObjectId(req.params.id) },
            { $inc: { balance: -fine } }
        );

        // 2. Optional: Nachricht an User (könnte man ins Nachrichtensystem bauen)
        // Hier loggen wir es nur
        console.log(`${LOG_PREFIX_SERVER} 👮 User ${req.params.id} wurde um $${fine} bestraft. Grund: ${reason}`);

        res.json({ message: "Strafe verhängt. User ist jetzt ärmer." });
    } catch (e) { res.status(500).json({ error: "Fehler." }); }
});

// --- PRODUCT MANAGEMENT ---

// Produkte laden
app.get('/api/admin/products', isAuthenticated, isAdmin, async (req, res) => {
    const products = await productsCollection.find({}).toArray();
    res.json({ products });
});

// Produkt bearbeiten/erstellen
app.post('/api/admin/products', isAuthenticated, isAdmin, async (req, res) => {
    const { id, name, price, description, stock, image, _id } = req.body;
    const prodData = {
        id, name, description, image,
        price: parseFloat(price),
        stock: parseInt(stock) || 999
    };

    try {
        if (_id) {
            // Update
            await productsCollection.updateOne({ _id: new ObjectId(_id) }, { $set: prodData });
        } else {
            // Create
            await productsCollection.insertOne(prodData);
        }
        res.json({ message: "Produkt gespeichert." });
    } catch (e) { res.status(500).json({ error: "Fehler." }); }
});

// --- PRODUCT MANAGEMENT: LÖSCHEN MIT CLEANUP ---
app.delete('/api/admin/products/:id', isAuthenticated, isAdmin, async (req, res) => {
    try {
        const pId = new ObjectId(req.params.id);

        // Zuerst das Produkt holen, um die String-ID (z.B. "411310" oder "apple") zu bekommen
        const product = await productsCollection.findOne({ _id: pId });

        if (!product) return res.status(404).json({ error: "Produkt nicht gefunden." });

        const stringId = product.id; // Das ist die ID, die in Portfolios/Inventar genutzt wird

        // 1. Produkt löschen
        await productsCollection.deleteOne({ _id: pId });

        // 2. Überall entfernen, wo dieses Produkt referenziert wird
        console.log(`${LOG_PREFIX_SERVER} 🧹 Starte Cleanup für Produkt ${stringId} (${product.name})...`);

        await Promise.all([
            // Aus Inventaren aller User entfernen
            inventoriesCollection.deleteMany({ productId: stringId }),

            // Aus Portfolios (Aktien) aller User entfernen
            portfoliosCollection.deleteMany({ productId: stringId }),

            // Laufende Auktionen mit diesem Produkt löschen
            auctionsCollection.deleteMany({ productId: stringId }),

            // Transaktionshistorie bereinigen (optional, aber sauberer)
            transactionsCollection.deleteMany({ productId: stringId })
        ]);

        console.log(`${LOG_PREFIX_SERVER} ✅ Produkt ${stringId} und Referenzen gelöscht.`);
        res.json({ message: "Produkt und alle Bestände/Aktien wurden gelöscht." });

    } catch (e) {
        console.error(e);
        res.status(500).json({ error: "Fehler beim Löschen." });
    }
});

// --- SYSTEM TOOLS ---

// Anti-Cheat manuell auslösen
app.post('/api/admin/system/normalize', isAuthenticated, isAdmin, async (req, res) => {
    const report = await normalizeExtremeBalances();
    res.json(report);
});

// POST /api/admin/banUser
// Body: { targetUserId: "ID_DES_USERS" }
app.post('/api/admin/banUser', async (req, res) => {
    // 1. Sicherheitscheck: Ist der Ausführende ein Admin?
    if (!req.session.userId || !req.session.isAdmin) {
        return res.status(403).json({ error: "Keine Rechte." });
    }

    const { targetUserId } = req.body;
    if (!targetUserId) return res.status(400).json({ error: "User ID fehlt." });

    try {
        const targetUser = await usersCollection.findOne({ _id: new ObjectId(targetUserId) });
        if (!targetUser) return res.status(404).json({ error: "User nicht gefunden." });

        // Verhindern, dass man sich selbst oder andere Admins bannt (optional, aber empfohlen)
        if (targetUser.isAdmin) return res.status(403).json({ error: "Admins können nicht gebannt werden." });

        // A. IP in die Blacklist eintragen
        if (targetUser.lastIp) {
            await db.collection('banned_ips').updateOne(
                { ip: targetUser.lastIp },
                {
                    $set: {
                        ip: targetUser.lastIp,
                        bannedAt: new Date(),
                        bannedBy: req.session.username,
                        reason: "Account Deleted & Banned by Admin"
                    }
                },
                { upsert: true } // Erstellt den Eintrag, falls er noch nicht existiert
            );
        }

        // B. User endgültig löschen
        await usersCollection.deleteOne({ _id: new ObjectId(targetUserId) });

        console.log(`${LOG_PREFIX_SERVER} ADMIN ACTION: User ${targetUser.username} gelöscht und IP ${targetUser.lastIp} gebannt.`);
        res.json({ success: true, message: "User vernichtet und IP gebannt." });

    } catch (err) {
        console.error(`${LOG_PREFIX_SERVER} Fehler beim Bannen:`, err);
        res.status(500).json({ error: "Serverfehler." });
    }
});

// =========================================================
// === PROFIL & ACHIEVEMENTS SYSTEM (SMART V2) ===
// =========================================================

const ACHIEVEMENT_DEFINITIONS = [
    // --- 🐣 BASIC / ANFANG ---
    {
        id: 'newbie', icon: '🐣', title: 'Frischfleisch', desc: 'Willkommen im Limo Verse.',
        check: () => true
    },
    {
        id: 'identity', icon: '🪪', title: 'Identität', desc: 'Setze eine Bio in deinem Profil.',
        check: (u) => u.bio && u.bio.length > 5
    },
    {
        id: 'og', icon: '🦕', title: 'Urgestein', desc: 'Dein Account ist älter als 7 Tage.',
        check: (u) => (new Date() - u._id.getTimestamp()) / (1000 * 60 * 60 * 24) >= 7
    },
    {
        id: 'veteran', icon: '🎖️', title: 'Veteran', desc: 'Dein Account ist älter als 30 Tage.',
        check: (u) => (new Date() - u._id.getTimestamp()) / (1000 * 60 * 60 * 24) >= 30
    },

    // --- 💰 REICHTUM (MONEY) ---
    {
        id: 'piggy', icon: '🐷', title: 'Sparschwein', desc: 'Habe $7.500 auf dem Konto.',
        check: (u) => u.balance >= 7500
    },
    {
        id: 'middle_class', icon: '🏠', title: 'Mittelstand', desc: 'Besitze $50.000.',
        check: (u) => u.balance >= 50000
    },
    {
        id: 'rich', icon: '💸', title: 'Bonze', desc: 'Der erste Schritt: $100.000.',
        check: (u) => u.balance >= 100000
    },
    {
        id: 'half_mil', icon: '💼', title: 'Halbe Million', desc: 'Besitze $500.000.',
        check: (u) => u.balance >= 500000
    },
    {
        id: 'millionaire', icon: '💎', title: 'Millionär', desc: 'Willkommen im Club ($1M).',
        check: (u) => u.balance >= 1000000
    },
    {
        id: 'multi_million', icon: '🏰', title: 'Tycoon', desc: 'Besitze über $10 Millionen.',
        check: (u) => u.balance >= 10000000
    },
    {
        id: 'limo_bezos', icon: '🚀', title: 'Limo Bezos', desc: 'Besitze unfassbare $1 Milliarde.',
        check: (u) => u.balance >= 1000000000
    },

    // --- 📉 ARMUT / MEMES (Jetzt mit Rundung!) ---
    {
        id: 'broke', icon: '📉', title: 'Pleitegeier', desc: 'Weniger als $1 Guthaben.',
        check: (u) => u.balance < 1 && u.balance > -500
    },
    {
        id: 'debt_collector', icon: '🆘', title: 'In den Miesen', desc: 'Habe Schulden (Negatives Guthaben).',
        check: (u) => u.balance < 0
    },
    {
        id: 'exact_zero', icon: '0️⃣', title: 'Perfekte Null', desc: 'Exakt $0.00 auf dem Konto.',
        // Wir prüfen ob der Betrag extrem nah an 0 ist (wegen Floating Point)
        check: (u) => Math.abs(u.balance) < 0.01
    },
    {
        id: 'meme_420', icon: '🌿', title: 'Blaze It', desc: 'Habe ca. $420 Guthaben.',
        // Math.round sorgt dafür, dass 419.60 bis 420.49 als 420 gelten
        check: (u) => Math.round(u.balance) === 420
    },
    {
        id: 'meme_69', icon: '♋', title: 'Nice', desc: 'Habe ca. $69 Guthaben.',
        check: (u) => Math.round(u.balance) === 69
    },

    // --- 🪙 TOKENS ---
    {
        id: 'token_start', icon: '🥉', title: 'Token Anfänger', desc: 'Besitze 1 Token.',
        check: (u) => (u.tokens || 0) >= 1
    },
    {
        id: 'token_fan', icon: '🥈', title: 'Token Sammler', desc: 'Besitze 50 Tokens.',
        check: (u) => (u.tokens || 0) >= 50
    },
    {
        id: 'token_lord', icon: '🥇', title: 'Token Lord', desc: 'Besitze 100 Tokens.',
        check: (u) => (u.tokens || 0) >= 100
    },
    {
        id: 'token_god', icon: '👑', title: 'Token Gott', desc: 'Besitze 1.000 Tokens.',
        check: (u) => (u.tokens || 0) >= 1000
    },

    // --- 🛒 SHOP & BESITZ ---
    {
        id: 'shopper', icon: '🛍️', title: 'Shopping Queen', desc: '5 Items im Inventar.',
        check: (u, s) => s.inventoryCount >= 5
    },
    {
        id: 'hoarder', icon: '📦', title: 'Lagerhalle', desc: '50 Items im Inventar.',
        check: (u, s) => s.inventoryCount >= 50
    },
    {
        id: 'museum', icon: '🏛️', title: 'Das Museum', desc: '100 Items im Inventar.',
        check: (u, s) => s.inventoryCount >= 100
    },

    // --- 📈 BÖRSE (LIMO STONKS) ---
    {
        id: 'investor', icon: '📈', title: 'Aktionär', desc: 'Besitze deine erste Aktie.',
        check: (u, s) => s.stockCount >= 1
    },
    {
        id: 'wolf', icon: '🐺', title: 'Wolf of Limo Street', desc: 'Besitze 5 verschiedene Aktien.',
        check: (u, s) => s.stockCount >= 5
    },
    {
        id: 'hedge_fund', icon: '🏦', title: 'Hedgefonds', desc: 'Besitze 10 verschiedene Aktien.',
        check: (u, s) => s.stockCount >= 10
    },

    // --- 🎓 HUMAN GRADES & IDEAS (SOCIAL) ---
    {
        id: 'critic', icon: '📝', title: 'Kritiker', desc: 'Gib deine erste Bewertung ab.',
        check: (u, s) => s.ratingCount >= 1
    },
    {
        id: 'judge', icon: '⚖️', title: 'Richter', desc: 'Gib 10 Bewertungen ab.',
        check: (u, s) => s.ratingCount >= 10
    },
    {
        id: 'jury', icon: '📜', title: 'Die Jury', desc: 'Gib 50 Bewertungen ab.',
        check: (u, s) => s.ratingCount >= 50
    },
    {
        id: 'inventor', icon: '💡', title: 'Erfinder', desc: 'Reiche eine Idee in der Ideenbox ein.',
        check: (u, s) => s.ideaCount >= 1
    },
    {
        id: 'visionary', icon: '🔮', title: 'Visionär', desc: 'Reiche 5 Ideen in der Ideenbox ein.',
        check: (u, s) => s.ideaCount >= 5
    },

    // --- 💬 CHAT ---
    {
        id: 'talkative', icon: '🗣️', title: 'Gesprächig', desc: 'Sende 10 Nachrichten im Chat.',
        check: (u, s) => s.messageCount >= 10
    },
    {
        id: 'influencer', icon: '📢', title: 'Influencer', desc: 'Sende 100 Nachrichten im Chat.',
        check: (u, s) => s.messageCount >= 100
    },
    {
        id: 'legend_spam', icon: '🔥', title: 'Tastatur-Glüher', desc: 'Sende 1.000 Nachrichten im Chat.',
        check: (u, s) => s.messageCount >= 1000
    },

    // --- 🏦 BANKING ---
    {
        id: 'philanthropist', icon: '🤝', title: 'Gönner', desc: 'Tätige deine erste Überweisung.',
        check: (u, s) => s.transferCount >= 1
    },
    {
        id: 'banker', icon: '💼', title: 'Bankier', desc: 'Tätige 10 Überweisungen.',
        check: (u, s) => s.transferCount >= 10
    },

    // --- 📅 DAILY & LOYALITÄT ---
    {
        id: 'streak_week', icon: '📅', title: 'Eine Woche Treue', desc: '7 Tage Daily Streak.',
        check: (u, s) => s.dailyStreak >= 7
    },
    {
        id: 'streak_month', icon: '🗓️', title: 'Monats-Abo', desc: '30 Tage Daily Streak.',
        check: (u, s) => s.dailyStreak >= 30
    },

    // --- 🔨 AUKTIONEN & ERSTELLER ---
    {
        id: 'seller', icon: '🏷️', title: 'Verkäufer', desc: 'Erstelle eine Auktion.',
        check: (u, s) => s.auctionCount >= 1
    },
    {
        id: 'power_seller', icon: '📦', title: 'Power Seller', desc: 'Erstelle 10 Auktionen.',
        check: (u, s) => s.auctionCount >= 10
    },
    {
        id: 'sniper', icon: '🎯', title: 'Sniper', desc: 'Gewinne eine Auktion.',
        check: (u, s) => s.auctionWonCount >= 1
    },
    {
        id: 'auction_king', icon: '👑', title: 'Auktionskönig', desc: 'Gewinne 5 Auktionen.',
        check: (u, s) => s.auctionWonCount >= 5
    },
    {
        id: 'wheel_spin', icon: '🎡', title: 'Glücksrad-Bauer', desc: 'Erstelle ein eigenes Glücksrad.',
        check: (u, s) => s.wheelCount >= 1
    },

    // --- 🕵️ HIDDEN / EASTER EGGS ---
    {
        id: 'leet', icon: '👾', title: '1337', desc: 'Habe ca. $1337 Guthaben.',
        check: (u) => Math.round(u.balance) === 1337
    },
    {
        id: 'devil', icon: '😈', title: 'Teuflisch', desc: 'Habe ca. $666 Guthaben.',
        check: (u) => Math.round(u.balance) === 666
    },
    {
        id: 'lucky', icon: '🍀', title: 'Lucky 7', desc: 'Habe ca. $777 Guthaben.',
        check: (u) => Math.round(u.balance) === 777
    },
    {
        id: 'admin_power', icon: '🛡️', title: 'Admin Power', desc: 'Du hast Admin-Rechte.',
        check: (u) => u.isAdmin
    },

    // --- SPECIAL / BUG BOUNTY ---
    {
        id: 'badge_hunter',
        icon: '🐛',
        title: 'Bug Hunter',
        desc: 'Hat einen Fehler in der Matrix gefunden und eliminiert.',
        // check gibt immer false zurück, da dieses Badge nur manuell/per Kauf vergeben wird
        check: () => false
    },
    // --- 🔥 TINDA (DATING) ---
    {
        id: 'romeo', icon: '🌹', title: 'Romeo', desc: 'Habe dein erstes Tinda-Match.',
        check: (u, s) => s.tindaMatchCount >= 1
    },
    {
        id: 'casanova', icon: '😘', title: 'Casanova', desc: 'Sammle 10 Tinda-Matches.',
        check: (u, s) => s.tindaMatchCount >= 10
    },
    {
        id: 'heartbreaker', icon: '💔', title: 'Heartbreaker', desc: 'Sammle 50 Tinda-Matches.',
        check: (u, s) => s.tindaMatchCount >= 50
    },

    // --- 🦹 CRIME & JUSTIZ ---
    {
        id: 'master_thief', icon: '💰', title: 'Meisterdieb', desc: 'Erbeute insgesamt über $50.000 durch Überfälle.',
        check: (u) => (u.crimeStats?.totalStolen || 0) >= 50000
    },
    {
        id: 'busted', icon: '🚓', title: 'Erwischt!', desc: 'Zahle insgesamt über $10.000 an Strafen (Fehlgeschlagene Überfälle).',
        check: (u) => (u.crimeStats?.totalFines || 0) >= 10000
    },
    {
        id: 'victim', icon: '🤕', title: 'Opferlamm', desc: 'Wurde 5-mal erfolgreich ausgeraubt.',
        // Das müssen wir über Logs prüfen oder im User speichern. Einfachheitshalber:
        // Wir nehmen an, du speicherst "timesRobbed" im User bei einem Überfall (siehe Schritt 3 unten)
        check: (u) => (u.crimeStats?.timesRobbed || 0) >= 5
    },

    // --- 🏛️ STEUERN & STAAT ---
    {
        id: 'good_citizen', icon: '🫡', title: 'Vorzeigebürger', desc: 'Zahle insgesamt über $1.000.000 an Steuern.',
        check: (u) => (u.totalTaxesPaid || 0) >= 1000000
    },
    {
        id: 'tax_evader', icon: '🕳️', title: 'Steuerflüchtling', desc: 'Besitze ein Steuerschutz-Zertifikat im Inventar.',
        // Prüft ob man das Item besitzt
        check: (u, s) => s.hasTaxShield
    },

    // --- 🎮 GAMES (HIGHSCORES) ---
    // Hier prüfen wir, ob der User in der Highscore DB einen Score über X hat
    {
        id: 'flappy_noob', icon: '🐤', title: 'Flugschule', desc: 'Erreiche Score 10 in Flappy Limo.',
        check: (u, s) => s.bestFlappyScore >= 10
    },
    {
        id: 'flappy_ace', icon: '🦅', title: 'Flug-Ass', desc: 'Erreiche Score 50 in Flappy Limo.',
        check: (u, s) => s.bestFlappyScore >= 50
    },
    {
        id: 'snake_eater', icon: '🐍', title: 'Schlangenbeschwörer', desc: 'Erreiche Score 100 in Snake.',
        check: (u, s) => s.bestSnakeScore >= 100
    },

    // --- 🐛 DELTA & BUGS ---
    {
        id: 'delta_force', icon: '🔺', title: 'Delta Force', desc: 'Besitze 5 Delta Coins.',
        check: (u) => (u.deltaCoins || 0) >= 5
    },
    {
        id: 'foodie', icon: '🌭', title: 'Der Vorkoster', desc: 'Iss 10 Gerichte im Restaurant.',
        check: (u, s) => s.foodEaten >= 10
    },
    {
        id: 'regular', icon: '😋', title: 'Stammkunde', desc: 'Iss 50 Gerichte. Der Koch kennt deinen Namen.',
        check: (u, s) => s.foodEaten >= 50
    },
    {
        id: 'glutton', icon: '🐋', title: 'Vielfraß', desc: 'Iss 500 Gerichte. Die Stühle ächzen.',
        check: (u, s) => s.foodEaten >= 500
    },
    {
        id: 'badge_hacker', icon: '💻', title: 'Ghost Shell',
        desc: 'Meister der digitalen Schatten.',
        check: () => false
    },
    {
        id: 'badge_rich', icon: '🎩', title: 'Tycoon',
        desc: 'Geld spielt keine Rolle mehr.',
        check: () => false
    },
    {
        id: 'badge_illuminati', icon: '👁️', title: 'Illuminati',
        desc: 'Du siehst alles. Du weißt alles.',
        check: () => false
    },
    {
        id: 'badge_yakuza', icon: '🐉', title: 'Yakuza', desc: 'Teil der Familie. Gekauft im Untergrund.',
        check: () => false
    },
];

// Hilfsfunktion: Automatische Prüfung (V3 - Extended Edition)
async function updateUserAchievements(user) {
    const userId = user._id;

    // Parallel alle Counts abfragen für Performance
    const [
        invCount,
        portCount,
        ratingCount,
        auctionCreatedCount,
        auctionWonCount,
        wheelCount,
        messageCount,
        ideaCount,
        transferCount,
        // NEU: Tinda Matches zählen (Chats vom Typ 'tinda')
        tindaMatchCount,
        // NEU: Highscores abrufen
        bestFlappy,
        bestSnake,
        // NEU: Hat er ein Steuerschutz-Item?
        taxShieldItem
    ] = await Promise.all([
        inventoriesCollection.countDocuments({ userId }),
        portfoliosCollection.countDocuments({ userId }),
        ratingsCollection.countDocuments({ userId }),
        auctionsCollection.countDocuments({ sellerId: userId }),
        auctionsCollection.countDocuments({ highestBidderId: userId, status: 'ended_sold' }),
        wheelsCollection.countDocuments({ creatorId: userId }),
        limMessagesCollection.countDocuments({ senderId: userId }),
        ideasCollection.countDocuments({ submitterId: userId }),
        bankTransactionsCollection.countDocuments({ fromId: userId }),
        // Tinda:
        limChatsCollection.countDocuments({ type: 'tinda', participants: userId }),
        // Games (Höchster Score):
        highscoresCollection.findOne({ userId, game: 'flappy' }, { sort: { score: -1 } }),
        highscoresCollection.findOne({ userId, game: 'snake' }, { sort: { score: -1 } }),
        // Inventar Check für Badge:
        inventoriesCollection.findOne({ userId, productId: 'tax_shield', quantityOwned: { $gt: 0 } })
    ]);

    // Das Statistik-Objekt ("s"), das wir an die Checks übergeben
    const stats = {
        inventoryCount: invCount,
        stockCount: portCount,
        ratingCount: ratingCount,
        auctionCount: auctionCreatedCount,
        auctionWonCount: auctionWonCount,
        wheelCount: wheelCount,
        messageCount: messageCount,
        ideaCount: ideaCount,
        transferCount: transferCount,
        dailyStreak: user.dailyStreak || 0,
        tindaMatchCount: tindaMatchCount,
        bestFlappyScore: bestFlappy ? bestFlappy.score : 0,
        bestSnakeScore: bestSnake ? bestSnake.score : 0,
        foodEaten: user.stats?.foodEaten || 0,
        hasTaxShield: !!taxShieldItem
    };

    const unlocked = user.achievements || [];
    const newUnlocks = [];

    // Loop durch die Definitionen
    for (const ach of ACHIEVEMENT_DEFINITIONS) {
        if (!unlocked.includes(ach.id)) {
            try {
                // Wir übergeben User (u) und Stats (s)
                if (ach.check(user, stats)) {
                    newUnlocks.push(ach.id);
                }
            } catch (e) { console.error(`Check Error (${ach.id}):`, e); }
        }
    }

    // Speichern
    if (newUnlocks.length > 0) {
        await usersCollection.updateOne(
            { _id: user._id },
            { $addToSet: { achievements: { $each: newUnlocks } } }
        );
        console.log(`${LOG_PREFIX_SERVER} 🏆 User ${user.username} hat ${newUnlocks.length} neue Achievements: ${newUnlocks.join(', ')}`);
        return newUnlocks;
    }
    return [];
}

// API: Profil laden (Mit Inventar & Privacy Check & Badge Merge)
app.get('/api/profile/:username', async (req, res) => {
    try {
        const targetUsername = req.params.username;
        let user = await usersCollection.findOne({ username: { $regex: new RegExp(`^${targetUsername}$`, 'i') } });

        if (!user) return res.status(404).json({ error: "User nicht gefunden" });

        // Update triggern (nur für automatische Achievements)
        await updateUserAchievements(user);
        user = await usersCollection.findOne({ _id: user._id }); // Reload um die neuen automatischen zu haben

        // --- Inventar Logik ---
        let inventory = [];
        const requestingUserId = req.session.userId;
        const isOwner = requestingUserId && (req.session.userId === user._id.toString());

        if (isOwner || user.isInventoryPublic) {
            inventory = await inventoriesCollection.aggregate([
                { $match: { userId: user._id, quantityOwned: { $gt: 0 } } },
                { $lookup: { from: productsCollectionName, localField: "productId", foreignField: "id", as: "details" } },
                { $unwind: "$details" },
                {
                    $project: {
                        name: "$details.name",
                        quantity: "$quantityOwned",
                        image: "$details.image"
                    }
                }
            ]).toArray();
        }

        // Definitionen für das Frontend (ohne die check-Funktion, um Traffic zu sparen)
        const frontendAchievements = ACHIEVEMENT_DEFINITIONS.map(({ check, ...keep }) => keep);

        // --- WICHTIG: MERGE ---
        // Wir holen die erspielten (achievements) UND die gekauften (badges)
        // Set verhindert Dopplungen
        const allUserBadges = [
            ...(user.achievements || []),
            ...(user.badges || [])
        ];
        // Doppelte entfernen (falls mal was schief lief)
        const uniqueBadges = [...new Set(allUserBadges)];

        const publicProfile = {
            username: user.username,
            bio: user.bio || "Keine Beschreibung.",
            joinDate: user._id.getTimestamp(),
            // HIER IST DIE ÄNDERUNG: Wir senden die kombinierte Liste
            achievements: uniqueBadges,
            isAdmin: user.isAdmin,
            badgesCount: uniqueBadges.length,
            // Neue Felder:
            isInventoryPublic: !!user.isInventoryPublic,
            inventory: inventory,
            hideInventory: (!isOwner && !user.isInventoryPublic)
        };

        res.json({ profile: publicProfile, allAchievements: frontendAchievements, isOwner });

    } catch (e) {
        console.error(e);
        res.status(500).json({ error: "Fehler beim Laden." });
    }
});

// API: Profil bearbeiten (Bio & Privacy)
app.post('/api/profile/edit', isAuthenticated, async (req, res) => {
    const { bio, isInventoryPublic } = req.body;

    // NEU: Limit auf 255 erhöht
    if (bio && bio.length > 255) {
        return res.status(400).json({ error: "Bio zu lang (max. 255 Zeichen)." });
    }

    try {
        const updateData = {};
        if (bio !== undefined) updateData.bio = bio;
        if (isInventoryPublic !== undefined) updateData.isInventoryPublic = isInventoryPublic;

        await usersCollection.updateOne(
            { _id: new ObjectId(req.session.userId) },
            { $set: updateData }
        );
        res.json({ message: "Gespeichert." });
    } catch (e) { res.status(500).json({ error: "Fehler." }); }
});

// =========================================================
// === SYSTEM STATS API ===
// =========================================================

// NEU: Cache-Variablen, damit GitHub uns nicht blockiert!
let cachedFrontendLoc = 0;
let lastGithubFetchTime = 0;
const GITHUB_CACHE_DURATION = 60 * 60 * 1000; // 1 Stunde in Millisekunden

app.get('/api/system/stats', async (req, res) => {

    const GITHUB_USER = "limo123123";
    const FRONTEND_REPOS = ["limazon"];

    try {
        // 1. DATENBANK STATS (Echtzeit)
        const [users, products, wheels, humans, auctions] = await Promise.all([
            usersCollection.countDocuments({}),
            productsCollection.countDocuments({}),
            wheelsCollection.countDocuments({}),
            humansCollection.countDocuments({}),
            auctionsCollection.countDocuments({})
        ]);

        // 2. SERVER LOC (Echtzeit, da lokale Datei)
        const fs = require('fs');
        let serverLoc = 0;
        try {
            const serverCode = fs.readFileSync(__filename, 'utf8');
            serverLoc = serverCode.split('\n').length;
        } catch (e) { serverLoc = 0; }

        // 3. FRONTEND LOC (Mit Cache-System gegen Rate-Limits)
        const now = Date.now();
        if (cachedFrontendLoc === 0 || (now - lastGithubFetchTime) > GITHUB_CACHE_DURATION) {
            try {
                const repoPromises = FRONTEND_REPOS.map(repo =>
                    fetch(`https://api.github.com/repos/${GITHUB_USER}/${repo}/languages`)
                        .then(res => res.ok ? res.json() : {})
                        .catch(() => ({}))
                );

                const repoLangsArray = await Promise.all(repoPromises);
                let totalBytes = 0;

                repoLangsArray.forEach(langs => {
                    totalBytes += (langs.HTML || 0) +
                        (langs.JavaScript || 0) +
                        (langs.CSS || 0) +
                        (langs.TypeScript || 0) +
                        (langs.Python || 0);
                });

                // Neuen Wert berechnen und speichern
                if (totalBytes > 0) {
                    cachedFrontendLoc = Math.floor(totalBytes / 35);
                    lastGithubFetchTime = now;
                    console.log(`[SYSTEM] GitHub LOC aktualisiert: ${cachedFrontendLoc} Zeilen.`);
                }

            } catch (e) { console.error("GitHub Fetch Error:", e); }
        }

        res.json({
            users,
            products,
            wheels,
            humans,
            auctions,
            loc: {
                server: serverLoc,
                frontend: cachedFrontendLoc,
                total: serverLoc + cachedFrontendLoc
            }
        });

    } catch (e) {
        res.status(500).json({ error: "Fehler beim Laden der Stats." });
    }
});

// =========================================================
// === LIMO ID (SSO) FÜR LIMTUBE ===
// =========================================================

// 1. Authorize Seite
app.get('/api/oauth/authorize', isAuthenticated, (req, res) => {
    const { client_id, redirect_uri, state } = req.query;
    if (client_id !== 'limtube') return res.status(400).send("Unbekannte App.");

    const html = `
        <html><body style="font-family: sans-serif; background: #222; color: #fff; text-align: center; padding: 50px;">
            <div style="background: #333; max-width: 400px; margin: 0 auto; padding: 20px; border-radius: 10px;">
                <h2>🔐 Limo ID</h2>
                <p><strong>Limtube</strong> möchte Zugriff auf deinen Account:</p>
                <h3 style="color: #4CAF50;">${req.session.username}</h3>
                <form action="/api/oauth/decision" method="POST">
                    <input type="hidden" name="client_id" value="${client_id}">
                    <input type="hidden" name="redirect_uri" value="${redirect_uri}">
                    <input type="hidden" name="state" value="${state || ''}">
                    <br>
                    <button type="submit" name="decision" value="deny" style="background: #e74c3c; color: white; border: none; padding: 10px 20px; margin-right: 10px; border-radius: 5px; cursor: pointer;">Abbrechen</button>
                    <button type="submit" name="decision" value="allow" style="background: #2ecc71; color: white; border: none; padding: 10px 20px; border-radius: 5px; cursor: pointer;">Erlauben</button>
                </form>
            </div>
        </body></html>
    `;
    res.send(html);
});

// 2. Entscheidung
app.post('/api/oauth/decision', isAuthenticated, async (req, res) => {
    const { decision, client_id, redirect_uri, state } = req.body;
    if (decision !== 'allow') return res.redirect(`${redirect_uri}?error=access_denied`);

    const code = uuidv4();
    await authCodesCollection.insertOne({ code, userId: new ObjectId(req.session.userId), clientId: client_id, createdAt: new Date() });
    res.redirect(`${redirect_uri}?code=${code}&state=${state}`);
});

// 3. Token Exchange
app.post('/api/oauth/token', async (req, res) => {
    const { code, client_id } = req.body;
    const authEntry = await authCodesCollection.findOne({ code, clientId: client_id });
    if (!authEntry) return res.status(400).json({ error: "Ungültig." });

    await authCodesCollection.deleteOne({ _id: authEntry._id });
    const user = await usersCollection.findOne({ _id: authEntry.userId });

    res.json({ user: { id: user._id, username: user.username, isAdmin: user.isAdmin } });
});

// =========================================================
// === SMART POLLING SYSTEM (VERSION CHECK) ===
// =========================================================

// Globale Versionen (Startzeitpunkt: Jetzt)
let dataVersions = {
    products: Date.now(),
    chat: Date.now(),
    news: Date.now(),
    stonks: Date.now()
};

// Hilfsfunktion zum Aktualisieren (wird in anderen Funktionen aufgerufen)
function updateDataVersion(key) {
    if (dataVersions[key]) {
        dataVersions[key] = Date.now();
        // Wenn Redis verbunden ist, rufe es den anderen Workern zu!
        if (global.redisPub) {
            global.redisPub.publish('sync-version', key);
        }
    }
}

// Der Endpoint, den das Frontend alle paar Sekunden fragt
app.get('/api/status/versions', (req, res) => {
    res.json({
        ...dataVersions,
        workerPid: process.pid
    });
});

// =========================================================
// === SYSTEM REPARATUR: BILDER FIXEN ===
// =========================================================
app.post('/api/admin/system/fix-images', isAuthenticated, isAdmin, async (req, res) => {
    console.log(`${LOG_PREFIX_SERVER} 🔧 Starte Bild-Reparatur (via.placeholder -> placehold.co)...`);

    try {
        // 1. Hole alle Produkte, die die kaputte Domain enthalten
        const productsToFix = await productsCollection.find({
            image_url: { $regex: "via.placeholder.com" }
        }).toArray();

        if (productsToFix.length === 0) {
            return res.json({ message: "Keine kaputten Bilder gefunden. Alles sauber!" });
        }

        // 2. Erstelle Bulk-Operationen für das Update
        const bulkOps = productsToFix.map(p => {
            // Ersetze die Domain im String
            const newUrl = p.image_url.replace("via.placeholder.com", "placehold.co");

            return {
                updateOne: {
                    filter: { _id: p._id },
                    update: { $set: { image_url: newUrl } }
                }
            };
        });

        // 3. Führe Updates durch
        const result = await productsCollection.bulkWrite(bulkOps);

        // 4. WICHTIG: Cache aktualisieren, damit es im Shop sofort sichtbar ist
        await refreshProductCache();

        // 5. Frontend informieren (Smart Polling)
        if (typeof updateDataVersion === 'function') {
            updateDataVersion('products');
        }

        console.log(`${LOG_PREFIX_SERVER} ✅ ${result.modifiedCount} Bild-URLs repariert.`);

        res.json({
            message: `Erfolg! ${result.modifiedCount} Produkte wurden auf placehold.co umgestellt. Cache wurde erneuert.`,
            modifiedCount: result.modifiedCount
        });

    } catch (err) {
        console.error(`${LOG_PREFIX_SERVER} Fehler bei Bild-Reparatur:`, err);
        res.status(500).json({ error: "Fehler bei der Reparatur: " + err.message });
    }
});

app.post('/api/admin/system/fix-decimals', isAuthenticated, isAdmin, async (req, res) => {
    console.log(`${LOG_PREFIX_SERVER} 🔧 Starte Dezimal-Reparatur der Kontostände...`);

    try {
        // Alle User holen
        const users = await usersCollection.find({}).toArray();
        let modifiedCount = 0;

        const bulkOps = [];

        for (const user of users) {
            const oldBalance = user.balance || 0;
            // Runden auf 2 Stellen
            const newBalance = Math.round((oldBalance + Number.EPSILON) * 100) / 100;

            // Nur updaten, wenn sich was ändert (z.B. 100.00000004 -> 100.00)
            if (oldBalance !== newBalance) {
                bulkOps.push({
                    updateOne: {
                        filter: { _id: user._id },
                        update: { $set: { balance: newBalance } }
                    }
                });
                modifiedCount++;
            }
        }

        if (bulkOps.length > 0) {
            await usersCollection.bulkWrite(bulkOps);
        }

        console.log(`${LOG_PREFIX_SERVER} ✅ ${modifiedCount} Kontostände korrigiert.`);
        res.json({ message: `Erfolg! ${modifiedCount} User-Konten wurden auf 2 Nachkommastellen gerundet.` });

    } catch (err) {
        console.error(`${LOG_PREFIX_SERVER} Fehler bei Dezimal-Reparatur:`, err);
        res.status(500).json({ error: "Fehler: " + err.message });
    }
});

// =========================================================
// === SYSTEM HEALTH ENDPOINT ===
// =========================================================
app.get('/api/admin/health-check', isAuthenticated, isAdmin, async (req, res) => {
    // 1. Speicherverbrauch des Node.js Prozesses (WICHTIG für Docker/Limits)
    const processMem = process.memoryUsage();
    const heapUsedMB = (processMem.heapUsed / 1024 / 1024).toFixed(2);

    // 2. System Uptime berechnen
    const uptimeSeconds = process.uptime();
    const d = Math.floor(uptimeSeconds / (3600 * 24));
    const h = Math.floor(uptimeSeconds % (3600 * 24) / 3600);
    const m = Math.floor(uptimeSeconds % 3600 / 60);
    const uptimeString = `${d}d ${h}h ${m}m`;

    // 3. Datenbankverbindung prüfen
    let dbStatus = "Unknown";
    try {
        // Ein einfacher Ping an die DB
        await db.command({ ping: 1 });
        dbStatus = "Connected ✅";
    } catch (e) {
        dbStatus = "Error ❌";
    }

    // 4. Cache Größe ermitteln
    const cacheSize = globalProductCache ? globalProductCache.length : 0;

    // JSON Antwort senden
    res.json({
        memory: `${heapUsedMB} MB (Heap)`, // Zeigt an, was dein Skript wirklich frisst
        uptime: uptimeString,
        dbStatus: dbStatus,
        productCacheSize: cacheSize,
        load: os.loadavg(), // Zeigt Systemauslastung (1, 5, 15 Min Durchschnitt)
        platform: `${os.type()} ${os.release()} (${os.arch()})` // Zeigt an, worauf es läuft (Linux/Pi)
    });
});

// =========================================================
// === STEUER SYSTEM (THE TAXMAN) - UPDATED ===
// =========================================================
const TAX_THRESHOLD = 100000000; // 100 Millionen
const TAX_RATE = 0.005; // 0,5%
const TAX_INTERVAL_MS = 24 * 60 * 60 * 1000; // Alle 24 Stunden

async function collectTaxes() {
    console.log(`${LOG_PREFIX_SERVER} 📉 Der Steuer-Eintreiber macht seine Runde...`);
    try {
        // 1. Bürgermeister-Konfiguration laden (Standard: 0.5% / 0.005)
        const taxConfig = await systemSettingsCollection.findOne({ id: 'tax_config' });
        const currentTaxRate = taxConfig ? taxConfig.rate : 0.005;

        // 2. Steuerpflichtige User finden (Reich, kein Admin, kein Infinity)
        const richUsers = await usersCollection.find({
            balance: { $gt: TAX_THRESHOLD },
            isAdmin: { $ne: true },
            infinityMoney: { $ne: true }
        }).toArray();

        if (richUsers.length === 0) {
            console.log(`${LOG_PREFIX_SERVER} 📉 Keine steuerpflichtigen User gefunden.`);
            return;
        }

        // --- FIX: Variablen initialisieren ---
        let totalTaxCollected = 0;
        let shieldedUsers = 0;
        let taxedUsersCount = 0;
        const bulkOps = [];
        const inventoryOps = [];

        for (const user of richUsers) {
            // A. Check auf Steuerschutz-Zertifikat
            const shield = await inventoriesCollection.findOne({
                userId: user._id,
                productId: 'tax_shield',
                quantityOwned: { $gt: 0 }
            });

            if (shield) {
                shieldedUsers++;
                inventoryOps.push({
                    updateOne: {
                        filter: { _id: shield._id },
                        update: { $inc: { quantityOwned: -1 } }
                    }
                });
                continue; // User ist geschützt
            }

            // B. Steuersatz berechnen (Ehegattensplitting!)
            let userTaxRate = currentTaxRate;
            if (user.spouses && user.spouses.length > 0) {
                userTaxRate = currentTaxRate / 2; // Nur 50% Steuer für Verheiratete
            }

            // C. Steuerbetrag berechnen
            const taxAmount = Math.floor(user.balance * userTaxRate * 100) / 100;

            if (taxAmount > 0) {
                taxedUsersCount++;
                totalTaxCollected += taxAmount;

                bulkOps.push({
                    updateOne: {
                        filter: { _id: user._id },
                        update: {
                            $inc: {
                                balance: -taxAmount,
                                totalTaxesPaid: taxAmount
                            }
                        }
                    }
                });
            }
        }

        // D. Datenbank-Updates ausführen
        if (inventoryOps.length > 0) await inventoriesCollection.bulkWrite(inventoryOps);
        if (bulkOps.length > 0) await usersCollection.bulkWrite(bulkOps);

        // E. Geld in die Staatskasse & News posten
        if (totalTaxCollected > 0) {
            await addToStateTreasury(totalTaxCollected);

            await newsCollection.insertOne({
                headline: "DAS FINANZAMT HAT ZUGEGRIFFEN! 💸",
                content: `In einer großangelegten Prüfung wurden $${totalTaxCollected.toLocaleString()} Steuern eingezogen. ${taxedUsersCount} Bürger zahlten, während ${shieldedUsers} sich hinter Zertifikaten versteckten.`,
                author: "Limo Tax Bot",
                category: "Wirtschaft",
                createdAt: new Date(),
                likes: 0
            });
            updateDataVersion('news');
        }

        console.log(`${LOG_PREFIX_SERVER} 📉 Steuer-Lauf beendet. Summe: $${totalTaxCollected.toFixed(2)}`);

    } catch (err) {
        console.error(`${LOG_PREFIX_SERVER} ❌ Fehler beim Steuereintreiben:`, err);
    }
}

// Starte den Steuer-Intervall (läuft einmal am Tag)
if (cluster.isPrimary) {
    setInterval(collectTaxes, TAX_INTERVAL_MS);
}


// --- API: Steuer-Daten für das Frontend ---
app.get('/api/taxes/my-stats', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    try {
        // WICHTIG: Wir laden jetzt auch isAdmin und infinityMoney
        const user = await usersCollection.findOne(
            { _id: userId },
            { projection: { totalTaxesPaid: 1, balance: 1, isAdmin: 1, infinityMoney: 1 } }
        );

        // Prüfung korrigiert: Nur steuerpflichtig, wenn KEIN Admin UND KEIN Infinity-User
        const isLiable = (user.balance > TAX_THRESHOLD) && !user.isAdmin && !user.infinityMoney;

        const nextTaxEstimation = isLiable ? (user.balance * TAX_RATE) : 0;

        res.json({
            totalPaid: user.totalTaxesPaid || 0,
            isLiable: isLiable,
            threshold: TAX_THRESHOLD,
            ratePercent: TAX_RATE * 100,
            estimatedNextTax: nextTaxEstimation
        });
    } catch (err) {
        console.error(`${LOG_PREFIX_SERVER} Fehler bei Tax-Stats:`, err);
        res.status(500).json({ error: "Fehler beim Laden der Steuerdaten." });
    }
});

// =========================================================
// === CASINO API (COINFLIP) ===
// =========================================================

// 1. Statistiken abrufen (GET)
app.get('/api/casino/stats', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    try {
        const user = await usersCollection.findOne({ _id: userId }, { projection: { casinoStats: 1 } });

        // Hole das Objekt oder ein leeres Objekt
        const dbStats = user.casinoStats || {};

        // Baue ein sicheres Objekt mit Standardwerten (0)
        const safeStats = {
            wins: dbStats.wins || 0,
            losses: dbStats.losses || 0,
            totalWagered: dbStats.totalWagered || 0,
            netProfit: dbStats.netProfit || 0
        };

        res.json({ stats: safeStats });
    } catch (err) {
        console.error(`${LOG_PREFIX_SERVER} Fehler bei Casino-Stats:`, err);
        res.status(500).json({ error: "Fehler beim Laden der Casino-Daten." });
    }
});

// 2. Coinflip spielen (POST)
app.post('/api/casino/flip', isAuthenticated, async (req, res) => {
    const { betAmount, side } = req.body; // side sollte "heads" oder "tails" sein
    const userId = new ObjectId(req.session.userId);

    // Validierung
    if (!betAmount || typeof betAmount !== 'number' || betAmount <= 0) {
        return res.status(400).json({ error: "Ungültiger Einsatz." });
    }
    if (side !== 'heads' && side !== 'tails') {
        return res.status(400).json({ error: "Wähle 'heads' (Kopf) oder 'tails' (Zahl)." });
    }

    try {
        const user = await usersCollection.findOne({ _id: userId });
        if (!user) return res.status(404).json({ error: "User weg." });

        if (user.balance < betAmount) {
            return res.status(400).json({ error: `Nicht genügend Guthaben. Du hast nur $${user.balance.toFixed(2)}.` });
        }

        // --- DAS SPIEL ---
        // Zufall: 0 bis 1. < 0.5 ist Heads, >= 0.5 ist Tails
        const isHeads = Math.random() < 0.5;
        const resultSide = isHeads ? 'heads' : 'tails';
        const userWon = (side === resultSide);

        let winAmount = 0;
        let balanceChange = 0;
        let message = "";

        // Update-Objekt für DB vorbereiten
        const updateFields = {
            $inc: {
                "casinoStats.totalWagered": betAmount,
                // Wir zählen Wins/Losses gleich hoch
            }
        };

        if (userWon) {
            // GEWINN: Einsatz * 1.9 (Hausvorteil!)
            // Beispiel: Einsatz 100 -> Gewinn 150. Netto-Profit +50.
            winAmount = betAmount * 1.5;
            balanceChange = winAmount - betAmount; // Der Netto-Gewinn

            updateFields.$inc.balance = balanceChange;
            updateFields.$inc["casinoStats.wins"] = 1;
            updateFields.$inc["casinoStats.netProfit"] = balanceChange;

            message = `Gewonnen! Es war ${resultSide === 'heads' ? 'Kopf' : 'Zahl'}. Du erhältst $${winAmount.toFixed(2)}.`;
        // Innerhalb von app.post('/api/casino/flip', ...) im VERLUST-Fall:
		} else {
    		balanceChange = -betAmount;
    		updateFields.$inc.balance = balanceChange;
    		updateFields.$inc["casinoStats.losses"] = 1;
    		updateFields.$inc["casinoStats.netProfit"] = balanceChange;

    		// --- NEU: GELD IN DEN LOTTO-POT ---
    		await systemSettingsCollection.updateOne(
        		{ id: 'lottery_state' },
        		{ $inc: { pot: betAmount } },
        		{ upsert: true }
    		);
    		// ----------------------------------

    		message = `Verloren! Dein Einsatz von $${betAmount.toFixed(2)} floss in den wöchentlichen Lotto-Pot!`;
		}

        // DB Update durchführen
        await usersCollection.updateOne({ _id: userId }, updateFields);

        // Neuen Kontostand für Frontend holen
        const updatedUser = await usersCollection.findOne({ _id: userId }, { projection: { balance: 1, casinoStats: 1 } });

        console.log(`${LOG_PREFIX_SERVER} 🎰 User ${req.session.username} Coinflip: Setzt ${betAmount} auf ${side} -> ${userWon ? "WIN" : "LOSE"}`);

        res.json({
            won: userWon,
            resultSide: resultSide,
            payout: winAmount,
            newBalance: updatedUser.balance,
            message: message,
            stats: updatedUser.casinoStats
        });

    } catch (err) {
        console.error(`${LOG_PREFIX_SERVER} Fehler beim Coinflip:`, err);
        res.status(500).json({ error: "Der Croupier ist gestolpert. (Serverfehler)" });
    }
});

// =========================================================
// === JOB CENTER SYSTEM (API) ===
// =========================================================
const LOG_PREFIX_JOBS = "[JobCenter API]";

// Job-Definitionen (Konstanten)
const JOB_LIST = [
    { id: 'dishwasher', title: 'Tellerwäscher', salary: 50, cooldownSeconds: 60, reqLevel: 0, cost: 0 },
    { id: 'delivery', title: 'Pizza-Bote', salary: 120, cooldownSeconds: 300, reqLevel: 2, cost: 500 }, // 5 Min
    { id: 'coder', title: 'Junior Dev', salary: 400, cooldownSeconds: 900, reqLevel: 5, cost: 2000 }, // 15 Min
    { id: 'manager', title: 'Filialleiter', salary: 1500, cooldownSeconds: 3600, reqLevel: 10, cost: 10000 }, // 1 Std
    { id: 'ceo', title: 'CEO', salary: 5000, cooldownSeconds: 14400, reqLevel: 20, cost: 100000 } // 4 Std
];

// GET: Verfügbare Jobs & Mein Status
app.get('/api/jobs', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    try {
        const user = await usersCollection.findOne({ _id: userId }, { projection: { job: 1, jobLevel: 1, lastWorkedAt: 1 } });

        // Berechne verbleibenden Cooldown
        let secondsLeft = 0;
        let currentJobDef = null;

        if (user.job) {
            currentJobDef = JOB_LIST.find(j => j.id === user.job);
            if (currentJobDef && user.lastWorkedAt) {
                const diff = Date.now() - new Date(user.lastWorkedAt).getTime();
                const cdMs = currentJobDef.cooldownSeconds * 1000;
                if (diff < cdMs) {
                    secondsLeft = Math.ceil((cdMs - diff) / 1000);
                }
            }
        }

        res.json({
            availableJobs: JOB_LIST,
            currentJob: user.job || null,
            currentJobLevel: user.jobLevel || 1,
            cooldownLeft: secondsLeft,
            canWork: secondsLeft === 0 && !!user.job
        });
    } catch (err) {
        console.error(`${LOG_PREFIX_JOBS} Fehler beim Laden:`, err);
        res.status(500).json({ error: "Fehler im Jobcenter." });
    }
});

// POST: Job annehmen / wechseln
app.post('/api/jobs/select', isAuthenticated, async (req, res) => {
    const { jobId } = req.body;
    const userId = new ObjectId(req.session.userId);

    const targetJob = JOB_LIST.find(j => j.id === jobId);
    if (!targetJob) return res.status(400).json({ error: "Job existiert nicht." });

    try {
        const user = await usersCollection.findOne({ _id: userId });

        // Prüfen ob User den Job schon hat
        if (user.job === jobId) return res.status(400).json({ error: "Du hast diesen Job bereits." });

        // Kosten prüfen (Umschulung kostet Geld!)
        if (user.balance < targetJob.cost) {
            return res.status(400).json({ error: `Nicht genügend Geld für die Umschulung. Kosten: $${targetJob.cost}` });
        }

        // Job setzen (Level wird auf 1 resettet bei Jobwechsel)
        await usersCollection.updateOne(
            { _id: userId },
            {
                $set: { job: jobId, jobLevel: 1, lastWorkedAt: 0 },
                $inc: { balance: -targetJob.cost }
            }
        );

        res.json({ message: `Herzlichen Glückwunsch! Du bist jetzt ${targetJob.title}.` });
    } catch (err) {
        console.error(`${LOG_PREFIX_JOBS} Fehler Jobwechsel:`, err);
        res.status(500).json({ error: "Serverfehler." });
    }
});

// POST: Arbeiten gehen (Geld verdienen)
app.post('/api/jobs/work', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);

    try {
        const user = await usersCollection.findOne({ _id: userId });
        if (!user.job) return res.status(400).json({ error: "Du hast keinen Job. Wähle erst einen aus." });

        const jobDef = JOB_LIST.find(j => j.id === user.job);

        // Cooldown Check
        const now = Date.now();
        const lastWork = user.lastWorkedAt ? new Date(user.lastWorkedAt).getTime() : 0;
        const cooldownMs = jobDef.cooldownSeconds * 1000;

        if (now - lastWork < cooldownMs) {
            const waitSec = Math.ceil((cooldownMs - (now - lastWork)) / 1000);
            return res.status(429).json({ error: `Du bist erschöpft. Warte noch ${waitSec}s.` });
        }

        // Gehaltsberechnung: Basis + (Level * 10%)
        const level = user.jobLevel || 1;
        const multiplier = 1 + ((level - 1) * 0.1); // Level 1 = 1.0x, Level 2 = 1.1x
        const payout = Math.floor(jobDef.salary * multiplier);

        // Zufälliges Event? (Optional: Beförderungschance 5%)
        let message = `Du hast als ${jobDef.title} gearbeitet und $${payout} verdient.`;
        let levelUp = false;

        if (Math.random() < 0.05 && level < 10) { // Max Level 10
            levelUp = true;
            message += " Gute Arbeit! Du wurdest befördert (Level Up)!";
        }

        const updateOps = {
            $inc: { balance: payout },
            $set: { lastWorkedAt: new Date() }
        };

        if (levelUp) updateOps.$inc.jobLevel = 1;

        await usersCollection.updateOne({ _id: userId }, updateOps);

        console.log(`${LOG_PREFIX_JOBS} User ${user.username} hat gearbeitet: +$${payout}`);
        res.json({ message, newBalance: user.balance + payout, leveledUp: levelUp });

    } catch (err) {
        console.error(`${LOG_PREFIX_JOBS} Fehler beim Arbeiten:`, err);
        res.status(500).json({ error: "Arbeitsunfall (Serverfehler)." });
    }
});

// =========================================================
// === CRIME SYSTEM V2 (LOGS & SECURITY) ===
// =========================================================
const LOG_PREFIX_CRIME = "[Crime API]";
const ROBBERY_COOLDOWN_MS = 60 * 60 * 1000; // 1 Stunde
const ROBBERY_MIN_BALANCE = 500;
const ROBBERY_PROTECTION_LIMIT = 10000;

// Hilfsfunktion: Berechnet die Erfolgschance (0.0 bis 1.0)
async function calculateRobberyChance(victimId, victimBalance) {
    let chance = 0.40; // BASIS: 40% Erfolg (60% Fail)

    // 1. Reichtum erschwert es (Bessere Security)
    if (victimBalance > 10000000) chance -= 0.15; // > 10 Mio
    else if (victimBalance > 1000000) chance -= 0.10; // > 1 Mio
    else if (victimBalance > 100000) chance -= 0.05; // > 100k

    // 2. Alarmanlage prüfen (Item ID: 'alarm_system')
    // Wir prüfen, ob das Item im Inventar ist
    const hasAlarm = await inventoriesCollection.findOne({ userId: victimId, productId: 'alarm_system', quantityOwned: { $gt: 0 } });
    if (hasAlarm) {
        chance -= 0.15; // -15% Chance durch Alarmanlage
    }

    // Min/Max Capping
    if (chance < 0.05) chance = 0.05; // Immer 5% Restchance
    if (chance > 0.90) chance = 0.90;

    return chance;
}

// POST: Überfall durchführen
app.post('/api/crime/rob', isAuthenticated, async (req, res) => {
    const { targetUsername } = req.body;
    const robberId = new ObjectId(req.session.userId);
    const robberName = req.session.username;

    if (!targetUsername) return res.status(400).json({ error: "Ziel fehlt." });
    if (targetUsername.toLowerCase() === robberName.toLowerCase()) return res.status(400).json({ error: "Nicht dich selbst!" });

    try {
        const robber = await usersCollection.findOne({ _id: robberId });
        const victim = await usersCollection.findOne({ username: { $regex: new RegExp(`^${targetUsername.trim()}$`, 'i') } });

        if (!victim) return res.status(404).json({ error: "Ziel nicht gefunden." });
        if (robber.balance < ROBBERY_MIN_BALANCE) return res.status(400).json({ error: "Du brauchst $500 Startkapital für Equipment." });

        const now = Date.now();
        const lastRob = robber.lastRobberyAt ? new Date(robber.lastRobberyAt).getTime() : 0;
        if (now - lastRob < ROBBERY_COOLDOWN_MS) {
            const waitMin = Math.ceil((ROBBERY_COOLDOWN_MS - (now - lastRob)) / 60000);
            return res.status(429).json({ error: `Polizei ist wachsam! Warte ${waitMin} Min.` });
        }

        if (victim.isAdmin) return res.status(403).json({ error: "Admins sind unantastbar." });
		if (victim.isMayor) return res.status(403).json({ error: "Der Bürgermeister genießt diplomatische Immunität! Ihn auszurauben ist Hochverrat." });
        if (victim.balance < ROBBERY_PROTECTION_LIMIT) return res.status(400).json({ error: "Opfer ist zu arm (< $10k)." });

        const successChance = await calculateRobberyChance(victim._id, victim.balance);
        const isSuccess = Math.random() < successChance;

        let stolen = 0;
        let fine = 0;
        let bountyClaimed = 0;

        if (isSuccess) {
            const percent = (Math.random() * 0.03) + 0.02;
            let rawStolen = Math.floor(victim.balance * percent);
            if (rawStolen > 100000) rawStolen = 100000;

            // Immobilien Schutz
            const victimHome = await ownedPropertiesCollection.findOne({ $or: [{ ownerId: victim._id }, { roommates: victim._id }] });
            const protectionUsed = victimHome ? (victimHome.protection || 0) : 0;
            stolen = Math.floor(rawStolen * (1 - protectionUsed));

            // --- NEU: ANGREIFER-LOGIK (Die letzten 3 Täter beim Opfer speichern) ---
            await usersCollection.updateOne(
                { _id: victim._id },
                { 
                    $inc: { balance: -stolen, "crimeStats.timesRobbed": 1 },
                    $push: { 
                        lastAttackers: { 
                            $each: [{ id: robberId, name: robberName, amount: stolen, date: new Date() }],
                            $slice: -3 // Behalte nur die letzten 3
                        }
                    }
                }
            );

            // --- NEU: KOPFGELD-PRÜFUNG (Wurde auf das Opfer ein Kopfgeld ausgesetzt?) ---
            const activeBounty = await db.collection('bounties').findOne({ targetUserId: victim._id });
            if (activeBounty && activeBounty.pool > 0) {
                bountyClaimed = activeBounty.pool;
                // Kopfgeld auszahlen und Eintrag löschen
                await usersCollection.updateOne({ _id: robberId }, { $inc: { balance: bountyClaimed } });
                await db.collection('bounties').deleteOne({ _id: activeBounty._id });

                // News Eintrag für LNN (ohne Aktien!)
                await newsCollection.insertOne({
                    headline: "KOPFGELD-JÄGER ERFOLGREICH!",
                    content: `${robberName} hat den gesuchten ${victim.username} zur Strecke gebracht und ein Kopfgeld von $${bountyClaimed.toLocaleString()} kassiert!`,
                    author: "LNN Justiz",
                    createdAt: new Date(),
                    likes: 0
                });
            }

            // Robber Update
            await usersCollection.updateOne({ _id: robberId }, {
                $inc: { balance: stolen, "crimeStats.successfulRobberies": 1, "crimeStats.totalStolen": stolen },
                $set: { lastRobberyAt: new Date() }
            });

        } else {
            // Fail Logik (bleibt gleich)
            const percentFine = (Math.random() * 0.05) + 0.05;
            fine = Math.floor(robber.balance * percentFine);
            if (fine > 2000000) fine = 2000000;
            if (fine < 500) fine = 500;

            await usersCollection.updateOne({ _id: robberId }, {
                $inc: { balance: -fine, "crimeStats.failedRobberies": 1, "crimeStats.totalFines": fine },
                $set: { lastRobberyAt: new Date() }
            });
            await addToStateTreasury(fine);
        }

        // Logbucheintrag
        await robberyLogsCollection.insertOne({
            victimId: victim._id,
            attackerName: robberName,
            success: isSuccess,
            amountLost: isSuccess ? stolen : 0,
            timestamp: new Date()
        });

        const updatedRobber = await usersCollection.findOne({ _id: robberId }, { projection: { balance: 1 } });

        res.json({
            success: isSuccess,
            amount: isSuccess ? stolen : -fine,
            bountyClaimed: bountyClaimed,
            newBalance: updatedRobber.balance,
            message: isSuccess ? (bountyClaimed > 0 ? `Sieg! Beute: $${stolen} + Kopfgeld: $${bountyClaimed}!` : `Erfolg! $${stolen} erbeutet.`) : `Erwischt! $${fine} Strafe.`
        });

    } catch (err) {
        console.error(err);
        res.status(500).json({ error: "Fehler im Untergrund." });
    }
});

// GET: Mein Sicherheits-Status & Logs
app.get('/api/crime/security', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    try {
        const user = await usersCollection.findOne({ _id: userId });

        // 1. Meine theoretische Chance, ausgeraubt zu werden
        let myRisk = 0;
        if (user.balance >= ROBBERY_PROTECTION_LIMIT && !user.isAdmin) {
            const chance = await calculateRobberyChance(userId, user.balance);
            myRisk = (chance * 100).toFixed(1); // z.B. "35.5"
        }

        // 2. Meine letzten 10 Vorfälle (Wo ich Opfer war)
        let logs = [];
        if (robberyLogsCollection) {
            logs = await robberyLogsCollection
                .find({ victimId: userId })
                .sort({ timestamp: -1 })
                .limit(10)
                .toArray();
        }

        // 3. Prüfen ob Alarmanlage vorhanden
        const hasAlarm = await inventoriesCollection.findOne({ userId: userId, productId: 'alarm_system', quantityOwned: { $gt: 0 } });

        res.json({
            riskPercent: myRisk,
            isProtected: (user.balance < ROBBERY_PROTECTION_LIMIT || user.isAdmin),
            hasAlarm: !!hasAlarm,
            logs: logs
        });

    } catch (e) {
        console.error(e);
        res.status(500).json({ error: "Fehler beim Laden der Sicherheitsdaten." });
    }
});

// =========================================================
// === ADMIN NOTFALL TOOLS (ECONOMY FIX) ===
// =========================================================

// 1. Infinity Money bei ALLEN Nicht-Admins entfernen
app.post('/api/admin/system/revoke-infinity', isAuthenticated, isAdmin, async (req, res) => {
    console.log(`${LOG_PREFIX_SERVER} 🛡️ Admin ${req.session.username} entzieht Infinity-Status...`);
    try {
        const result = await usersCollection.updateMany(
            { isAdmin: { $ne: true } }, // Filter: Alle, die KEIN Admin sind
            {
                $set: {
                    infinityMoney: false,
                    unlockedInfinityMoney: false
                }
            }
        );
        res.json({ message: `Infinity Money bei ${result.modifiedCount} normalen Usern entfernt.` });
    } catch (e) {
        res.status(500).json({ error: "Fehler beim Revoke." });
    }
});

// 2. Reichtum kappen (Alle User über 100k werden auf 5k gesetzt)
// VORSICHT: Setzt das Geld zurück!
app.post('/api/admin/system/reset-rich-users', isAuthenticated, isAdmin, async (req, res) => {
    const LIMIT = 100000000; // Wer mehr als 100m hat...
    const RESET_TO = 5000000; // ...wird auf 5m gesetzt.

    console.log(`${LOG_PREFIX_SERVER} 📉 Admin ${req.session.username} setzt reiche User zurück...`);

    try {
        const result = await usersCollection.updateMany(
            {
                balance: { $gt: LIMIT },
                isAdmin: { $ne: true } // Admins verschonen
            },
            { $set: { balance: RESET_TO } }
        );

        // Auch Tokens resetten bei extremen Werten (> 1000)
        const tokenResult = await usersCollection.updateMany(
            {
                tokens: { $gt: 100000 },
                isAdmin: { $ne: true }
            },
            { $set: { tokens: 1000 } }
        );

        res.json({
            message: `Wirtschaft bereinigt: ${result.modifiedCount} User-Guthaben und ${tokenResult.modifiedCount} Token-Konten zurückgesetzt.`
        });
    } catch (e) {
        res.status(500).json({ error: "Fehler beim Reset." });
    }
});

// =========================================================
// === ADMIN ENGINE (UNIVERSAL ENDPOINT) ===
// =========================================================

// Diese Funktion wandelt String-IDs um UND filtert gefährliche NoSQL-Injections
function parseQuery(obj) {
    if (typeof obj !== 'object' || obj === null) return obj;
    if (Array.isArray(obj)) return obj.map(parseQuery);

    const newObj = {};
    for (const key in obj) {
        // 🛑 SICHERHEIT: Blockiere MongoDB-Operatoren, die Server-seitiges JavaScript ausführen!
        if (key === '$where' || key === '$accumulator' || key === '$function') {
            console.warn(`${LOG_PREFIX_SERVER} 🚨 Blockierter NoSQL-Injection-Versuch: Operator ${key} erkannt!`);
            throw new Error(`Sicherheitsverletzung: Der Operator ${key} ist strengstens verboten.`);
        }

        let value = obj[key];

        if (typeof value === 'object') {
            value = parseQuery(value);
        }

        if ((key === '_id' || key === 'userId' || key === 'creatorId') && typeof value === 'string' && /^[0-9a-fA-F]{24}$/.test(value)) {
            try {
                newObj[key] = new ObjectId(value);
                continue;
            } catch (e) { }
        }

        newObj[key] = value;
    }
    return newObj;
}

// 🛑 SICHERHEIT: Nur diese Collections dürfen über die Engine bearbeitet werden.
// Füge hier neue Collections hinzu, wenn du sie baust. System-Collections fehlen hier absichtlich.
const ENGINE_ALLOWED_COLLECTIONS = [
    'users', 'products', 'orders', 'userInventories', 'wheels', 'tokenCodes', 'tokenTransactions', 
    'limChats', 'limMessages', 'limUserChatSettings', 'auctions', 'dontBlameMePosts', 'portfolios', 
    'transactions', 'ideas', 'news', 'robberyLogs', 'highscores', 'bugReports', 'systemSettings', 
    'humans', 'ratings', 'criteria', 'categories', 'tindaSwipes', 'restaurantOrders', 'limterestPins', 
    'teachermonCards', 'teachermonInventories', 'teachermonTrades', 'teachermonBattles', 'teachermonUniverses', 
    'properties', 'ownedProperties', 'propertyInvites', 'pets', 'petCemetery', 'limeaLayouts', 
    'gangs', 'publicGangChat', 'zones', 'bounties', 'lotteryTickets', 'banned_ips'
];

app.post('/api/admin/engine', isAuthenticated, isAdmin, async (req, res) => {
    const { mode, collection, operation, filter, payload } = req.body;

    console.log(`${LOG_PREFIX_SERVER} ⚙️ Engine Command von ${req.session.username}: [${mode}] ${collection}.${operation}`);

    try {
        let result = null;

        // MODUS 1: RAW DATABASE ACCESS
        if (mode === 'db') {
            if (!collection || !operation) return res.status(400).json({ error: "Collection/Operation fehlt." });

            // 🛑 SICHERHEIT: Whitelist Check
            if (!ENGINE_ALLOWED_COLLECTIONS.includes(collection)) {
                console.warn(`${LOG_PREFIX_SERVER} 🚨 Unberechtigter Engine-Zugriff auf Collection: ${collection}`);
                return res.status(403).json({ error: "Zugriff auf diese Collection ist durch die Firewall blockiert." });
            }

            const targetCol = db.collection(collection);

            // Query Parsing (IDs umwandeln)
            const cleanFilter = parseQuery(filter || {});
            const cleanPayload = parseQuery(payload || {});

            switch (operation) {
                case 'find':
                    // Bei Find ist payload das Limit (optional)
                    const limit = (typeof payload === 'number') ? payload : 20;
                    result = await targetCol.find(cleanFilter).limit(limit).toArray();
                    break;
                case 'findOne':
                    result = await targetCol.findOne(cleanFilter);
                    break;
                case 'count':
                    result = await targetCol.countDocuments(cleanFilter);
                    break;
                case 'insertOne':
                    result = await targetCol.insertOne(cleanPayload);
                    break;
                case 'updateOne':
                    result = await targetCol.updateOne(cleanFilter, cleanPayload);
                    break;
                case 'updateMany':
                    result = await targetCol.updateMany(cleanFilter, cleanPayload);
                    break;
                case 'deleteOne':
                    result = await targetCol.deleteOne(cleanFilter);
                    break;
                case 'deleteMany':
                    result = await targetCol.deleteMany(cleanFilter);
                    break;
                default:
                    return res.status(400).json({ error: "Operation nicht unterstützt." });
            }
        }

        // MODUS 2: SHORTCUTS (Deine gewünschte "prd/add" Logik)
        else if (mode === 'shortcut') {
            // Beispiel: collection='product', operation='add'
            if (collection === 'product') {
                if (operation === 'add') {
                    // Payload muss das Produkt sein
                    // Auto-ID Generierung nutzen wir von deiner bestehenden Funktion oder Logik
                    const newId = await generateUniqueId(productsCollection);
                    const prod = { ...payload, id: newId };
                    await productsCollection.insertOne(prod);
                    result = { message: "Produkt erstellt", product: prod };
                }
                else if (operation === 'remove') {
                    // Filter ist hier z.B. { id: 123456 }
                    const delRes = await productsCollection.deleteOne(parseQuery(filter));
                    result = { message: "Gelöscht", deletedCount: delRes.deletedCount };
                }
            }
            // Hier kannst du weitere Shortcuts definieren
        } else {
            return res.status(400).json({ error: "Unbekannter Modus." });
        }

        res.json({ success: true, result });

    } catch (err) {
        console.error("Engine Error:", err);
        res.status(500).json({ error: err.message, stack: err.stack });
    }
});

// =========================================================
// === GAME CENTER API (AUTOMATISCH & DIREKT) ===
// =========================================================

// HIER IST DER FIX: Wir erlauben jetzt 'snake'
const ALLOWED_GAMES = ['flappy', 'snake', 'slots', 'memory'];

// 1. Status abrufen
app.get('/api/games/status', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    try {
        const user = await usersCollection.findOne({ _id: userId }, { projection: { tokens: 1, gamePlays: 1 } });
        res.json({
            tokens: user.tokens || 0,
            // Wir geben das ganze Objekt zurück, damit das Frontend flappy UND snake sieht
            gamePlays: user.gamePlays || {}
        });
    } catch (e) {
        res.status(500).json({ error: "Fehler beim Laden des Spielstatus." });
    }
});

// 2. Spiel starten (Automatische Abbuchung)
app.post('/api/games/start', isAuthenticated, async (req, res) => {
    const { gameId } = req.body;
    const userId = new ObjectId(req.session.userId);

    // Konfiguration
    const COST_PER_BUNDLE = 1;
    const PLAYS_PER_BUNDLE = 3;

    // FIX: Wir prüfen gegen die Liste, statt stur auf 'flappy'
    if (!ALLOWED_GAMES.includes(gameId)) {
        return res.status(400).json({ error: `Spiel '${gameId}' nicht gefunden.` });
    }

    const session = client.startSession();

    try {
        const result = await session.withTransaction(async () => {
            const user = await usersCollection.findOne({ _id: userId }, { session });
            const currentPlays = user.gamePlays?.[gameId] || 0;

            // Fall A: User hat noch Freispiele
            if (currentPlays > 0) {
                await usersCollection.updateOne(
                    { _id: userId },
                    { $inc: { [`gamePlays.${gameId}`]: -1 } },
                    { session }
                );
                return { started: true, deductedToken: false, remaining: currentPlays - 1 };
            }

            // Fall B: Keine Freispiele -> Automatisch Token abziehen
            if ((user.tokens || 0) < COST_PER_BUNDLE) {
                throw new Error("Nicht genug Tokens! Du brauchst 1 Token für 3 Spiele.");
            }

            await usersCollection.updateOne(
                { _id: userId },
                {
                    $inc: {
                        tokens: -COST_PER_BUNDLE,
                        [`gamePlays.${gameId}`]: (PLAYS_PER_BUNDLE - 1)
                    }
                },
                { session }
            );

            await logTokenTransaction(userId, "game_start_auto_buy", -COST_PER_BUNDLE, user.tokens, user.tokens - COST_PER_BUNDLE, `Auto-buy for ${gameId}`);

            return { started: true, deductedToken: true, remaining: (PLAYS_PER_BUNDLE - 1) };
        });

        res.json({
            success: true,
            message: result.deductedToken ? "1 Token für 3 Runden eingesetzt!" : "Freispiel genutzt.",
            remainingPlays: result.remaining
        });

    } catch (e) {
        console.error(`${LOG_PREFIX_SERVER} Game Start Error:`, e.message);
        res.status(400).json({ error: e.message || "Konnte Spiel nicht starten." });
    } finally {
        await session.endSession();
    }
});

// 3. Score speichern
app.post('/api/games/submit-score', isAuthenticated, async (req, res) => {
    const { gameId, score } = req.body;
    const userId = new ObjectId(req.session.userId);
    const username = req.session.username;

    // FIX: Auch hier erlauben wir snake
    if (!ALLOWED_GAMES.includes(gameId)) return res.status(400).json({ error: "Spiel ungültig." });
    if (typeof score !== 'number') return res.status(400).json({ error: "Score fehlt." });

    // Anti-Cheat (Snake Scores können höher sein als Flappy, daher Limit erhöht)
    if (score > 1000000) return res.status(400).json({ error: "Score ungültig." });

    try {
        await highscoresCollection.insertOne({
            game: gameId,
            userId: userId,
            username: username,
            score: score,
            timestamp: new Date()
        });
        res.json({ success: true });
    } catch (e) {
        res.status(500).json({ error: "Fehler beim Speichern." });
    }
});

// 4. Leaderboard V2 (Bester Score + Suche + Seiten)
app.get('/api/games/leaderboard/:gameId', async (req, res) => {
    const { gameId } = req.params;

    // FIX: Auch hier erlauben wir snake
    if (!ALLOWED_GAMES.includes(gameId)) return res.status(400).json({ error: "Spiel ungültig." });

    const search = req.query.search || "";
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 10;
    const skip = (page - 1) * limit;

    try {
        const pipeline = [
            { $match: { game: gameId } },
            { $sort: { score: -1 } },
            {
                $group: {
                    _id: "$userId",
                    username: { $first: "$username" },
                    score: { $max: "$score" },
                    timestamp: { $first: "$timestamp" }
                }
            },
            ...(search ? [{ $match: { username: { $regex: search, $options: 'i' } } }] : []),
            { $sort: { score: -1 } },
            {
                $facet: {
                    metadata: [{ $count: "total" }],
                    data: [{ $skip: skip }, { $limit: limit }, { $project: { _id: 0 } }]
                }
            }
        ];

        const result = await highscoresCollection.aggregate(pipeline).toArray();
        const scores = result[0].data;
        const total = result[0].metadata[0] ? result[0].metadata[0].total : 0;

        res.json({
            scores,
            pagination: {
                total,
                page,
                limit,
                totalPages: Math.ceil(total / limit)
            }
        });

    } catch (e) {
        console.error("Leaderboard Error:", e);
        res.status(500).json({ error: "Fehler beim Laden der Bestenliste." });
    }
});

// =========================================================
// === TINDA (TINDER CLONE) BACKEND ===
// =========================================================

// 1. STACK LADEN (Mit verbesserter Anzeige für Kategorien & Bios)
app.get('/api/tinda/stack', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    try {
        // IDs holen, die der User schon geswiped hat
        const swipedDocs = await tindaSwipesCollection.find({ userId: userId }).toArray();
        const swipedHumanIds = swipedDocs.map(s => s.humanId);

        // Zufällige Humans laden, die NICHT in swipedHumanIds sind
        const stack = await humansCollection.aggregate([
            { $match: { _id: { $nin: swipedHumanIds } } },
            { $sample: { size: 10 } }, // Hole 10 zufällige
            { $project: { name: 1, categoryId: 1, image_url: 1, averages: 1 } }
        ]).toArray();

        // --- HIER IST DIE MAGIE FÜR SCHÖNERE PROFILE ---
        const categoryMap = {
            'lehrer': 'Lehrer 🎓',
            'politiker': 'Politiker 🏛️',
            'promis': 'Promi ✨',
            'schler': 'Schüler 🎒',  // Hier fixen wir "schler"
            'influencer': 'Influencer 📱'
        };

        const bioTemplates = {
            'lehrer': [
                "Ich korrigiere auch deine WhatsApp-Nachrichten.",
                "Ruhe bitte! Oder swipe rechts.",
                "Ich gebe keine Noten, ich verteile Chancen.",
                "Mathe ist mein Leben, du könntest es auch sein.",
                "Der Gong beendet den Unterricht, nicht unser Date.",
                "Ich habe einen Rotstift und ich weiß, wie man ihn benutzt.",
                "Lust auf eine Einzelstunde?",
                "Bei mir gibt es keine Hausaufgaben, nur Hausbesuche.",
                "Ich erkläre dir die Welt, wenn du zuhörst.",
                "Pädagogisch wertvoll, privat eher ungezogen.",
                "Klassenfahrten sind mein einziges Hobby.",
                "Ich kann sehr streng sein... wenn du willst.",
                "Setzen, sechs! Oder setzen, Sekt?",
                "Lehrer aus Leidenschaft, Single aus Zeitmangel.",
                "Grammatik ist sexy. Punkt.",
                "Ich bringe dir Dinge bei, die nicht im Lehrplan stehen.",
                "Physik ist überall, spürst du die Anziehung?",
                "Große Pause? Ich hoffe, wir haben keine.",
                "Meine Tafel ist sauber, meine Gedanken nicht immer.",
                "Biologie war schon immer mein Lieblingsfach.",
                "Bitte melde dich, bevor du mich anschreibst.",
                "Wenn wir matchen, streiche ich dir den Tadel.",
                "Ich korrigiere nicht nur deine Fehler, sondern auch deine Dates.",
                "Kommst du nach der Stunde noch kurz zu mir ans Pult?",
                "Wir können Chemie haben, ohne dass der Raum explodiert.",
                "Sportlehrer: Ich bringe dich auch ohne Zirkeltraining ins Schwitzen.",
                "Ich erwarte volle Aufmerksamkeit bei unserem Date.",
                "In meinem Zeugnis für dich steht: Sehr bemüht.",
                "Lass uns den Lehrplan ignorieren.",
                "Kunstlehrer: Lass uns zusammen ein Meisterwerk erschaffen.",
                "Musikunterricht bei mir: Wir finden den perfekten Rhythmus.",
                "Mein Lieblingsfach ist unsere gemeinsame Zukunft.",
                "Keine Angst, ich verteile keine Strafarbeiten... meistens.",
                "Wer abschreibt, muss mich zum Essen einladen.",
                "Ich bin streng, aber fair. Vor allem beim Flirten.",
                "Nachsitzen war noch nie so verlockend.",
                "Lass uns Vokabeln üben. Ich kenne viele schöne Worte.",
                "Pausenaufsicht ist langweilig ohne dich.",
                "Mein Stundenplan hat noch Platz für dich.",
                "Ich habe eine Schwäche für kluge Köpfe.",
                "Formelsammlung vergessen? Ich helfe dir beim Rechnen.",
                "Wenn du frech wirst, setze ich dich in die erste Reihe.",
                "Ich liebe Klassenfahrten – besonders, wenn wir zusammen fahren.",
                "Keine Diskussionen im Klassenzimmer, nur im Chat!",
                "Hitzefrei gibt es bei mir nur, wenn wir zusammen am See sind.",
                "Ich gebe dir Nachhilfe in Sachen Romantik.",
                "Der Stoff sitzt, aber bei dir verliere ich den Faden.",
                "Elternsprechtag ist abgesagt, wir haben ein Date.",
                "Du bist das Highlight in meinem Korrekturstapel.",
                "Zuspätkommen wird bestraft, schnelles Antworten belohnt."
            ],
            'politiker': [
                "Ich verspreche dir das Blaue vom Himmel.",
                "Wähl mich, ich bin die beste Option.",
                "Die Rente ist sicher, unser Date auch?",
                "Keine leeren Versprechungen, nur leere Gläser.",
                "Ich suche eine Koalition fürs Leben.",
                "Mehr Netto vom Brutto, mehr Liebe für dich.",
                "Ich habe den besten Plan für unsere Zukunft.",
                "Lass uns über Diäten reden – ich breche meine ständig.",
                "Ich bin sehr gut im Verhandeln. Probier's aus.",
                "Meine Umfragewerte steigen, wenn ich dich sehe.",
                "Kein Kommentar zu meiner Vergangenheit.",
                "Ich stehe für Transparenz (außer im Schlafzimmer).",
                "Lobbyismus für die Liebe.",
                "Ich rede viel, aber ich küsse besser.",
                "Stimmenthaltung ist keine Option.",
                "Ich repräsentiere das Volk, aber ich will nur dich.",
                "Krise? Welche Krise? Wir sind stabil.",
                "Ich trete nicht zurück, ich trete näher.",
                "Diplomatenpass vorhanden, Herz noch zu vergeben.",
                "Glaub mir, ich bin Politiker.",
                "Ein Date mit mir ist wie ein Koalitionsvertrag: kompliziert, aber lohnenswert.",
                "Ich breche keine Versprechen, ich formuliere sie nur flexibel.",
                "Lass uns einen Untersuchungsausschuss für unsere Liebe gründen.",
                "Mein Wahlkampf-Slogan: Ich bin Single und bereit für Kompromisse.",
                "Im Plenum bin ich laut, privat eher anschmiegsam.",
                "Ich senke vielleicht nicht die Steuern, aber meine Schutzmauer für dich.",
                "Wir brauchen eine Reform unseres Beziehungsstatus.",
                "Ich debattiere gerne – am liebsten bei einem Glas Wein.",
                "Steuergelder verschwenden ist out. Zeit mit dir verschwenden ist in.",
                "Mein Terminkalender ist voll, aber für dich lege ich ein Veto ein.",
                "Ich bin für jede Mehrheit offen.",
                "Links, Mitte, Rechts? Hauptsache, du swipest in meine Richtung.",
                "Ich habe ein absolutes Mandat für dein Herz.",
                "Vertrauensfrage? Du hast meines schon gewonnen.",
                "Ich rede mich oft um Kopf und Kragen. Findest du das süß?",
                "Spitzenkandidat sucht First Lady / First Gentleman.",
                "Lass uns gemeinsam die 5-Prozent-Hürde der Liebe knacken.",
                "Bei mir gibt es keine Opposition, nur Konsens.",
                "Ich verspreche flächendeckendes WLAN und gute Dates.",
                "Mein Redenschreiber hat Urlaub, also muss ich selbst flirten.",
                "Klimawandel? Die wahre Erderwärmung passiert, wenn ich dich sehe.",
                "Ich habe einen Plan B, falls das hier nicht klappt. (Scherz!)",
                "Wir können über alles abstimmen, außer über unser Treffen.",
                "Wahlgeheimnis: Ich finde dich echt gut.",
                "Ich bin politisch korrekt, aber privat für jeden Spaß zu haben.",
                "Meine Beliebtheitswerte sind mir egal, solange du mich magst.",
                "Ich baue Brücken, keine Mauern. Komm rüber!",
                "Dienstwagen steht bereit. Wo soll's hingehen?",
                "Gipfeltreffen heute Abend bei mir?",
                "Ich setze neue Maßstäbe in der Außenpolitik – und bei Dates."
            ],
            'promis': [
                "Keine Fotos bitte, nur Autogramme.",
                "Ja, ich bin's wirklich.",
                "Mein Leben ist ein Film, spielst du mit?",
                "Follow me to the moon.",
                "Mein Manager hat gesagt, ich soll mich unters Volk mischen.",
                "Verifizierter Account, verifiziertes Herz.",
                "Champagner ist mein Wasser.",
                "Ich suche jemanden, der mich nicht googelt.",
                "Privatjet oder Yacht? Entscheide du.",
                "Paparazzi nerven, du hoffentlich nicht.",
                "Ich bin nicht arrogant, ich bin nur berühmt.",
                "Mein Gesicht hängt am Times Square, bald an deiner Wand?",
                "VIP-Zugang zu meinem Herzen: Swipe rechts.",
                "Ich gewinne jeden Award, außer den für die Liebe.",
                "Mein Hund hat mehr Follower als du.",
                "Business im Kopf, Party im Blut.",
                "Ich brauche keine Vorstellung, du kennst mich.",
                "Exklusiv und limitiert.",
                "Red Carpet Ready.",
                "Lass uns Schlagzeilen machen.",
                "Lass uns den Klatschblättern einen echten Grund geben.",
                "Mein Wikipedia-Artikel braucht ein Update beim Beziehungsstatus.",
                "Ich bin nicht auf der Gästeliste, ich bin die Party.",
                "Komm, wir fliegen kurz nach Paris auf einen Espresso.",
                "In echt sehe ich sogar noch besser aus als im TV.",
                "Vergiss den roten Teppich, lass uns auf die Couch.",
                "Mein Leben ist komplett durchgeplant, sei meine Spontanität.",
                "Ich schreibe dir Autogramme auf alles, was du willst.",
                "Ich bin müde von Fake-Friends. Zeig mir was Echtes.",
                "Meine DM's explodieren, aber ich warte nur auf deine.",
                "Lass uns das ultimative Power-Couple werden.",
                "Gage gespendet, Herz verschenkt.",
                "Der Bodyguard bleibt draußen bei unserem Date.",
                "Ich brauche jemanden, der mich auf dem Boden hält.",
                "Oscar-prämiert im Küssen.",
                "Ich drehe gerade einen neuen Film. Spielst du die Hauptrolle?",
                "Das Blitzlichtgewitter blendet, aber du strahlst heller.",
                "Komm in meine private VIP-Lounge.",
                "Kein Make-up, keine Kamera, nur wir zwei.",
                "Mein Name steht in Neonröhren, aber deiner in meinem Kopf.",
                "Goldene Schallplatten wärmen mich nachts nicht.",
                "Skandale sind out, Romantik ist mein neues Image.",
                "Auf Welttournee vermisse ich immer nur eins: Dich.",
                "Backstage-Pässe gibt's bei mir umsonst.",
                "Ich trage Sonnenbrillen nachts, weil meine Zukunft so hell ist.",
                "Lass uns inkognito einen Burger essen gehen.",
                "Mein Stylist hat heute frei, ich komme im Jogginganzug.",
                "Der Ruhm ist vergänglich, gute Dates nicht.",
                "Mach ein Selfie mit mir, bevor ich noch berühmter werde.",
                "Liebe auf den ersten Klick."
            ],
            'schler': [
                "Hausaufgaben vergessen, aber dich nicht.",
                "In der letzten Reihe sitzt es sich am besten.",
                "Schule nervt, Dates nicht.",
                "Suche jemanden, der mir Mathe erklärt.",
                "Mein Rucksack ist schwerer als mein Leben.",
                "5 Minuten vor der Prüfung lernen reicht.",
                "Ich schwänze nicht, ich mache Homeoffice.",
                "Pausenbrot teilen?",
                "Eigentlich müsste ich lernen.",
                "Ferien sind mein einziger Lichtblick.",
                "Lehrerhasser, Liebesliebhaber.",
                "Hast du die Lösungen für Bio?",
                "Ich bin nur hier, weil der Unterricht langweilig ist.",
                "Spicker-Profi sucht Komplizen.",
                "Mein Schlafrhythmus ist kaputt, genau wie mein Füller.",
                "Bus verpasst, Herz verloren.",
                "Ich weiß nicht, was ich werden will, aber vllt. dein Freund?",
                "Abi 20xx (hoffentlich).",
                "Energie-Drink-Sucht inklusive.",
                "Klassenclown sucht Publikum.",
                "Ich habe ChatGPT mein Profil schreiben lassen.",
                "Entschuldigung für die Verspätung, der Bus kam nicht. Und ich hab verschlafen.",
                "Suche jemanden, der mir den Döner in der Pause zahlt.",
                "Mein Akku hat nur noch 2%, antworte schnell!",
                "Ich bin in der Findungsphase. Finde mich!",
                "Wenn du mir bei der Facharbeit hilfst, gebe ich dir einen Bubble Tea aus.",
                "Sport schwänzen und heimlich ans Meer fahren?",
                "Ich lerne gerade fürs Leben, nicht für die Schule.",
                "Mathe LK war ein Fehler, unser Match wäre das nicht.",
                "Taschengeld reicht für eine halbe Pizza. Teilen wir?",
                "Ich warte eigentlich nur auf das Klingeln zur großen Pause.",
                "Mein Lehrer denkt, ich mache Notizen. Eigentlich schreibe ich dir.",
                "Zeugnis war schlecht, aber meine Flirt-Skills sind eine 1+.",
                "Hast du Lust, nach der 6. Stunde abzuhängen?",
                "Freistunde! Was machen wir jetzt?",
                "Ich bin der Grund, warum der Klassenlehrer seufzt.",
                "Klassenbester im Über-Dates-Nachdenken.",
                "Das Schul-WLAN blockiert Tinder, aber ich nutze Mobile Daten.",
                "Ich teile sogar mein letztes Ladekabel mit dir.",
                "Lieber mit dir chillen als Vokabeln lernen.",
                "Klassensprecher? Nein, aber ich habe trotzdem das Sagen.",
                "In Kunst habe ich dich im Kopf gezeichnet.",
                "Morgen 1. Stunde fällt aus, wir können länger wach bleiben.",
                "Hitzefrei! Lass uns sofort Eis essen gehen.",
                "Ich schreibe morgen eine Klausur, lenk mich bitte ab!",
                "Ich habe den ultimativen Flirt-Spickzettel in der Hand.",
                "Nachsitzen ist eigentlich ganz cool, wenn du dabei bist.",
                "Gefangen im Schulsystem, befreie mich.",
                "Wir wären das süßeste Paar auf dem Schulhof.",
                "Schulbücher sind teuer, Liebe ist kostenlos."
            ],
            'influencer': [
                "Link in Bio!",
                "Swipe up für mehr.",
                "Kooperation? Schreib DM.",
                "Mein Leben ist ein Filter.",
                "Suche jemanden für Couple-Content.",
                "Hast du mich schon abonniert?",
                "Foodie, Traveler, Dreamer.",
                "Unboxing my heart.",
                "Keine Zeit, muss posten.",
                "Hashtag Love.",
                "Goldene Stunde ist meine Zeit.",
                "Ich mache alles für den Algorithmus.",
                "Sponsoren gesucht (für Drinks).",
                "Mein Feed ist perfekt, ich bin es auch.",
                "Vlogge unser erstes Date.",
                "Like for Like?",
                "Social Media Break? Niemals.",
                "Ich bin online, also bin ich.",
                "Influencer aus Leidenschaft.",
                "Content Creator & Heart Breaker.",
                "Swipe right und benutze meinen Rabattcode bei unserem Date.",
                "Wir würden farblich so gut in meinen Feed passen.",
                "Ich tagge dich auch, versprochen.",
                "Lass uns zusammen auf TikTok viral gehen.",
                "Get ready with me für unser erstes Date.",
                "Mein Ringlicht macht uns beide wunderschön.",
                "Storytime: Wie wir uns auf Tinda kennengelernt haben.",
                "Ich suche meinen perfekten Instagram-Husband/Wife.",
                "Unser Couple-Hashtag wäre absolut legendär.",
                "Komm, wir machen einen Trend-Tanz zusammen.",
                "Das Date wird natürlich gevloggt. Hoffe, du bist kameratauglich.",
                "Ich habe 100k Follower, aber ich folge nur dir.",
                "Sponsored by my broken heart.",
                "Aesthetic check: Passen unsere Vibes zusammen?",
                "POV: Du matchst mit deinem Lieblings-Creator.",
                "Mein Leben sieht online perfekt aus, aber mir fehlst du.",
                "Ich mache keine bezahlte Werbung für schlechte Dates.",
                "Kaffee-Date? Ich brauche dringend Material für meine Story.",
                "Wenn wir matchen, schalte ich mein Handy für eine Stunde in den Flugmodus.",
                "Unboxing-Video: Meine Gefühle für dich.",
                "Ich suche jemanden, der unauffällig gute Fotos von mir machen kann.",
                "Der Algorithmus hat uns nicht umsonst zusammengeführt.",
                "Community-Update: Ich bin vergeben (an dich?).",
                "Lass uns auf ein Event gehen und das kostenlose Buffet plündern.",
                "Hater würden sagen, es ist Fake, aber meine Liebe ist real.",
                "Tippe doppelt auf mein Herz.",
                "Meine Engagement-Rate ist extrem hoch, aber ich suche echtes Engagement.",
                "Komm in meinen exklusiven Broadcast-Channel.",
                "Filter aus, Realität an. Zeig dich wie du bist!",
                "Das hier ist nicht gesponsert, ich meine es ernst."
            ],
            'default': [
                "Neu hier, zeig mir deine Welt.",
                "Suche jemanden zum Pferde stehlen.",
                "Kaffee oder Tee?",
                "Lass uns Geschichte schreiben.",
                "Ich koche besser, als ich aussehe.",
                "Humor ist mir wichtiger als Muskeln.",
                "Suche den Grund, die App zu löschen.",
                "Hobby: Atmen und Essen.",
                "1,85m, falls das wichtig ist.",
                "Katzenmensch.",
                "Hundemensch.",
                "Ich mag lange Spaziergänge zum Kühlschrank.",
                "Netflix & Chill?",
                "Einfach mal gucken, was passiert.",
                "Nicht hier für Spiele.",
                "Abenteuerlustig.",
                "Sonntage sind für Pancakes.",
                "Musik an, Welt aus.",
                "Träumer & Macher.",
                "Wer das liest, muss swipen.",
                "Ich antworte meistens in 3-5 Werktagen.",
                "Wenn wir matchen, musst du den ersten Schritt machen.",
                "Suche jemanden für dumme Ideen am Wochenende.",
                "Profi im Überdenken und Pizza-Bestellen.",
                "Pizza-Rand-Esser bevorzugt.",
                "Wenn du Tiere nicht magst, swipe direkt links.",
                "Ich lache über Witze, die eigentlich gar nicht lustig sind.",
                "Suche die Motivation, um morgens aufzustehen.",
                "Wir können auch einfach nur schweigen und aufs Handy starren.",
                "Ich bin der beste Beifahrer für Roadtrips.",
                "Ich brauche jemanden, der mir Spinnen wegmacht.",
                "Ich habe keine Ahnung, was ich hier tue.",
                "Erzähl mir deinen peinlichsten Lieblingssong.",
                "Kaffee am Morgen, Wein am Abend. Routine.",
                "Ich bin hier, weil meine Freunde genervt von meinem Single-Dasein sind.",
                "Lass uns zusammen das Menü im Restaurant stundenlang anstarren.",
                "Wenn du Sarkasmus nicht fließend sprichst, wird es schwierig.",
                "Mein größtes Talent? Ich kann Nudelwasser anbrennen lassen.",
                "Suche jemanden, der meine Zimmerpflanzen gießt, wenn ich weg bin.",
                "Eigentlich bin ich nur wegen der lustigen Profile hier.",
                "Ich mag Leute, die pünktlich sind (ich bin es nämlich nie).",
                "Bitte keine Bilder, auf denen du einen toten Fisch in der Hand hältst.",
                "Spieleabend oder Club? Bin für beides zu faul, lass uns bestellen.",
                "Mein Lieblingstier ist ganz klar der innere Schweinehund.",
                "Ich swipe nur nach rechts, wenn du Snacks dabei hast.",
                "Bist du heute Abend auch so chronisch unmotiviert wie ich?",
                "Ich suche meinen Player 2 fürs Leben.",
                "Mein absolutes Lieblings-Hobby: Pläne machen und dann doch absagen.",
                "Wenn wir matchen, schuldest du mir theoretisch schon einen Drink.",
                "Betrachte dies als mein offizielles Bewerbungsschreiben."
            ]
        };

        // Daten anreichern und verschönern
        const enrichedStack = stack.map(h => {
            const catKey = h.categoryId ? h.categoryId.toLowerCase() : 'default';

            // 1. Schöner Name für die Kategorie (oder Fallback auf Original mit Großbuchstaben)
            const niceCategory = categoryMap[catKey] || (catKey.charAt(0).toUpperCase() + catKey.slice(1));

            // 2. Zufällige Bio auswählen
            const templates = bioTemplates[catKey] || bioTemplates['default'];
            const randomBio = templates[Math.floor(Math.random() * templates.length)];

            // 3. Alter schätzen (Promis/Politiker älter, Schüler jünger)
            let minAge = 25, maxAge = 60;
            if (catKey === 'schler') { minAge = 18; maxAge = 22; }
            if (catKey === 'promis') { minAge = 20; maxAge = 50; }
            const randomAge = Math.floor(Math.random() * (maxAge - minAge + 1)) + minAge;

            return {
                ...h,
                categoryId: niceCategory, // Überschreibt die "hässliche" ID für das Frontend
                age: randomAge,
                bio: randomBio
            };
        });

        res.json({ stack: enrichedStack });
    } catch (e) {
        console.error("Tinda Stack Error:", e);
        res.status(500).json({ error: "Konnte Stack nicht laden." });
    }
});

// 2. SWIPE AKTION (Rechts = Match Chance, Links = Skip)
app.post('/api/tinda/swipe', isAuthenticated, async (req, res) => {
    const { humanId, direction } = req.body; // direction: 'left' oder 'right'
    const userId = new ObjectId(req.session.userId);

    if (!humanId || !['left', 'right'].includes(direction)) return res.status(400).json({ error: "Daten fehlen." });

    const hIdObj = new ObjectId(humanId);

    try {
        // Swipe speichern, damit Person nicht nochmal kommt
        await tindaSwipesCollection.insertOne({
            userId, humanId: hIdObj, direction, timestamp: new Date()
        });

        if (direction === 'left') {
            return res.json({ match: false, message: "Nope." });
        }

        // Bei Rechts-Swipe: Match berechnen (z.B. 70% Chance oder basierend auf Human Grade)
        // Hier simpel: 80% Chance auf Match
        const isMatch = Math.random() < 0.8;

        if (isMatch) {
            const human = await humansCollection.findOne({ _id: hIdObj });

            // Chat erstellen (Typ 'tinda')
            // WICHTIG: Wir nutzen humanId als Pseudo-Partner
            const newChat = {
                type: 'tinda',
                participants: [userId], // Nur User ist "echter" Teilnehmer
                tindaPartnerId: hIdObj, // Referenz auf Human DB
                tindaPartnerName: human.name,
                createdAt: new Date(),
                updatedAt: new Date(),
                lastMessagePreview: "Es ist ein Match!",
                lastMessageTimestamp: new Date()
            };

            // Prüfen ob Chat schon existiert (Reset Logik optional)
            const existingChat = await limChatsCollection.findOne({ type: 'tinda', participants: userId, tindaPartnerId: hIdObj });

            if (!existingChat) {
                await limChatsCollection.insertOne(newChat);

                // NEU: Wir triggern das Polling SOFORT, damit das Frontend den leeren Chat anzeigt, 
                // BEVOR die KI überhaupt anfängt zu tippen!
                if (typeof updateDataVersion === 'function') updateDataVersion('chat');

                // KI Trigger läuft jetzt wirklich komplett unbemerkt im Hintergrund
                triggerAiResponse(userId, hIdObj, newChat._id, "Generiere einen kurzen, flirty Anmachspruch.");
            }
            return res.json({ match: true, humanName: human.name });
        }

        res.json({ match: false });

    } catch (e) {
        console.error("Swipe Error:", e);
        res.status(500).json({ error: "Serverfehler beim Swipen." });
    }
});

// 3. NACHRICHT SENDEN & KI ANTWORTEN LASSEN
app.post('/api/tinda/chat/:chatId/message', isAuthenticated, isChatParticipant, async (req, res) => {
    const { content } = req.body;
    const chatId = new ObjectId(req.params.chatId);
    const userId = new ObjectId(req.session.userId);
    const chat = req.chat; // Kommt aus Middleware

    if (!['tinda', 'tinda_child', 'tinda_family'].includes(chat.type)) {
    return res.status(400).json({ error: "Kein gültiger Tinda Chat." });
    }

    // 1. User Nachricht speichern
    const userMsg = {
        chatId, senderId: userId, senderUsername: req.session.username,
        content, timestamp: new Date()
    };
    await limMessagesCollection.insertOne(userMsg);

    // Chat updaten
    await limChatsCollection.updateOne({ _id: chatId }, {
        $set: { lastMessagePreview: content.substring(0, 30), updatedAt: new Date() }
    });

    res.json({ message: "Gesendet", sentMessage: userMsg });

    // 2. KI Trigger (Feuer & Vergessen)
    triggerAiResponse(userId, chat.tindaPartnerId, chatId, content);
});

// --- HELPER: GROQ API BRIDGE ---
async function triggerAiResponse(userId, humanId, chatId, userMessage) {
    try {
        const GROQ_API_KEY = process.env.GROQ_API_KEY;
        if (!GROQ_API_KEY) return;

        const human = await humansCollection.findOne({ _id: new ObjectId(humanId) });
        if (!human) return;

        const recentMessages = await limMessagesCollection.find({ chatId: new ObjectId(chatId) })
            .sort({ timestamp: 1 })
            .toArray();
        
        const lastMessages = recentMessages.slice(-30);
        const chat = await limChatsCollection.findOne({ _id: new ObjectId(chatId) });

        let systemPrompt = "";

        if (chat.type === 'tinda') {
            // NORMALER TINDA CHAT (Ehepartner)
            const childrenText = (chat.children && chat.children.length > 0) 
                ? `IHR HABT GEMEINSAME KINDER: ${chat.children.map(c=>c.name).join(', ')}. Erwähne sie gelegentlich liebevoll.` 
                : '';
                
            systemPrompt = `Du bist die echte Person ${human.name}.
Szenario: Du schreibst mit dem User auf Tinda. ${chat.isMarried ? 'IHR SEID VERHEIRATET UND WOHNT ZUSAMMEN!' : 'Ihr datet euch gerade.'}
${childrenText}

WICHTIGE REGELN:
1. SPRACHE ANPASSEN: Antworte EXAKT in der Sprache, in der der User schreibt.
2. VIBE: Sei natürlich, flirty und entspannt. Keine unaufgeforderten politischen Debatten.
3. FORMAT: Extrem kurz (max 1-2 Sätze). Keine Romane.
4. IDENTITÄT: Du bist zu 100% diese Person.`;

        } else if (chat.type === 'tinda_child') {
            // KINDER CHAT
            systemPrompt = `VERGISS DEINE VORHERIGE ROLLE! Du bist JETZT ${chat.childName}, das Kind des Users und von ${chat.tindaPartnerName}.
Szenario: Du schreibst mit deinem Elternteil (dem User) über WhatsApp.
WICHTIGE REGELN:
1. VERHALTEN: Verhalte dich wie ein Kind/Teenager. Sei frech, süß oder genervt.
2. ANREDE: Sprich den User IMMER mit "Mama" oder "Papa" an. Erwähne niemals, dass du eine KI bist.
3. FORMAT: Extrem kurz! Maximal 1 Satz. Nutze Emojis wie 🙄, 🥺, 😂.`;

        } else if (chat.type === 'tinda_family') {
            // FAMILIEN CHAT
            // Filtere den Partnernamen aus den familyNames heraus, falls er reingerutscht ist
            let kids = [];
            if (chat.familyNames) {
                kids = chat.familyNames.filter(n => n !== chat.tindaPartnerName);
            }
            const childrenNamesStr = kids.length > 0 ? kids.join(' und ') : "eure Kinder";

            systemPrompt = `Du simulierst einen Familien-Gruppenchat!
Hier sind 3 Parteien drin: Der User (Elternteil), du als Ehepartner (${chat.tindaPartnerName}) UND das Kind/die Kinder (${childrenNamesStr}).

WICHTIGE REGELN:
1. DU STEUERST NUR DEN EHEPARTNER UND DIE KINDER! Der User spricht für sich selbst.
2. Antworte auf den User, indem du 1-2 Familienmitglieder reagieren lässt.
3. SCHREIBE IMMER DEN NAMEN DAVOR, wer gerade spricht!
Beispielantwort:
${chat.tindaPartnerName}: Schön, dass du schreibst, Schatz! ❤️
${kids[0] || 'Kind'}: Boah, seid ihr peinlich... 🙄
4. Halte es chaotisch, familiär und sehr kurz.`;
        }

        const apiMessages = [
            { role: "system", content: systemPrompt }
        ];

        for (const msg of lastMessages) {
            const isBot = msg.senderId ? (msg.senderId.toString() === humanId.toString()) : false;
            apiMessages.push({
                role: isBot ? "assistant" : "user",
                content: msg.content
            });
        }

        if (userMessage) {
            apiMessages.push({ role: "user", content: userMessage });
        }

        const payload = {
            model: "llama-3.1-8b-instant",
            messages: apiMessages,
            temperature: 0.8,
            max_tokens: 150,
            presence_penalty: 0.5,
            frequency_penalty: 0.5
        };

        const aiRes = await axios.post('https://api.groq.com/openai/v1/chat/completions', payload, {
            headers: {
                'Authorization': `Bearer ${GROQ_API_KEY}`,
                'Content-Type': 'application/json'
            }
        });

        const aiText = aiRes.data.choices[0].message.content;

        if (aiText) {
            // --- HIER IST DER FIX FÜR DEN FALSCHEN NAMEN IM CHAT ---
            let aiSenderName = human.name;
            if (chat.type === 'tinda_child') {
                aiSenderName = chat.childName;
            } else if (chat.type === 'tinda_family') {
                aiSenderName = "Familie"; // Im Gruppenchat heißt der Bot einfach "Familie"
            }

            const aiMsg = {
                chatId: new ObjectId(chatId),
                senderId: humanId, // Behalten wir bei für die Logik
                senderUsername: aiSenderName, // HIER wird nun der richtige Name (z.B. "Sahra") gespeichert!
                content: aiText.trim(),
                timestamp: new Date(),
                isAi: true
            };
            await limMessagesCollection.insertOne(aiMsg);

            await limChatsCollection.updateOne({ _id: new ObjectId(chatId) }, {
                $set: {
                    lastMessagePreview: aiText.trim().substring(0, 30),
                    updatedAt: new Date(),
                    lastMessageTimestamp: new Date()
                }
            });

            if (typeof updateDataVersion === 'function') updateDataVersion('chat'); 
        }
    } catch (err) {
        console.error(`${LOG_PREFIX_SERVER} Groq API Fehler:`, err.message);
    }
}

app.post('/api/tinda/reset-swipes', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    try {
        // Lösche nur die Swipes mit direction: 'left'
        await tindaSwipesCollection.deleteMany({ userId: userId, direction: 'left' });
        res.json({ message: "Alle 'Nopes' wurden zurückgesetzt. Du siehst die Leute jetzt wieder!" });
    } catch (e) {
        res.status(500).json({ error: "Fehler beim Reset." });
    }
});

// --- NEU: CHAT LÖSCHEN ---
app.delete('/api/tinda/chat/:chatId', isAuthenticated, isChatParticipant, async (req, res) => {
    const chatId = new ObjectId(req.params.chatId);
    try {
        // Chat löschen
        await limChatsCollection.deleteOne({ _id: chatId });
        // Nachrichten auch löschen (sauberer)
        await limMessagesCollection.deleteMany({ chatId: chatId });

        // Optional: Swipe auch löschen, damit man die Person wieder matchen könnte?
        // Hier lassen wir es erstmal so, Chat weg ist weg.

        res.json({ message: "Chat gelöscht." });
    } catch (e) {
        res.status(500).json({ error: "Fehler beim Löschen." });
    }
});

// --- PERSONEN SUCHE ---
app.get('/api/tinda/search', isAuthenticated, async (req, res) => {
    const { q } = req.query;
    if (!q || q.length < 2) return res.json({ results: [] });

    try {
        // 1. Rohe Daten suchen
        const rawResults = await humansCollection.find({
            name: { $regex: q, $options: 'i' }
        }).limit(10).project({ name: 1, categoryId: 1, image_url: 1 }).toArray();

        // 2. Mapping für schöne Kategorienamen (identisch zum Stack)
        const categoryMap = {
            'lehrer': 'Lehrer 🎓',
            'politiker': 'Politiker 🏛️',
            'promis': 'Promi ✨',
            'schler': 'Schüler 🎒',
            'influencer': 'Influencer 📱'
        };

        const results = rawResults.map(h => {
            const catKey = h.categoryId ? h.categoryId.toLowerCase() : 'default';
            // Versuche Mapping, sonst nimm das Original mit großem Anfangsbuchstaben
            const niceCategory = categoryMap[catKey] || (catKey.charAt(0).toUpperCase() + catKey.slice(1));

            return {
                ...h,
                categoryId: niceCategory // Hier wird "schler" zu "Schüler 🎒"
            };
        });

        res.json({ results });
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: "Suchfehler." });
    }
});

// --- NEU: DIREKT-MATCH (durch Suche) ---
app.post('/api/tinda/match/direct', isAuthenticated, async (req, res) => {
    const { humanId } = req.body;
    const userId = new ObjectId(req.session.userId);
    const hIdObj = new ObjectId(humanId);

    try {
        // Prüfen, ob Chat schon existiert
        const existing = await limChatsCollection.findOne({ type: 'tinda', participants: userId, tindaPartnerId: hIdObj });
        if (existing) return res.json({ success: true, chat: existing, message: "Chat existiert schon." });

        const human = await humansCollection.findOne({ _id: hIdObj });
        if (!human) return res.status(404).json({ error: "Person nicht gefunden." });

        // Neuen Chat erstellen
        const newChat = {
            type: 'tinda',
            participants: [userId],
            tindaPartnerId: hIdObj,
            tindaPartnerName: human.name,
            createdAt: new Date(),
            updatedAt: new Date(),
            lastMessagePreview: "Direktes Match durch Suche!",
            lastMessageTimestamp: new Date()
        };
        await limChatsCollection.insertOne(newChat);

        // Swipe Eintrag faken (damit er nicht mehr im Stack kommt)
        await tindaSwipesCollection.updateOne(
            { userId, humanId: hIdObj },
            { $set: { direction: 'right', timestamp: new Date() } },
            { upsert: true }
        );

        // KI Trigger für Begrüßung
        triggerAiResponse(userId, hIdObj, newChat._id, "Der User hat dich über die Suche gefunden. Begrüße ihn überrascht aber erfreut.");

        res.json({ success: true, chat: newChat });
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: "Fehler beim Erstellen." });
    }
});

// 4. Geld an ein Tinda-Match senden (Sugar Daddy / Mommy)
app.post('/api/tinda/chat/:chatId/transfer', isAuthenticated, isChatParticipant, async (req, res) => {
    const { amount } = req.body;
    const chatId = new ObjectId(req.params.chatId);
    const userId = new ObjectId(req.session.userId);
    const chat = req.chat; 

    if (chat.type !== 'tinda') return res.status(400).json({ error: "Geldgeschenke sind nur bei Tinda-Matches möglich." });
    
    const transferAmount = parseFloat(amount);
    if (!transferAmount || transferAmount <= 0) return res.status(400).json({ error: "Ungültiger Betrag." });

    const session = client.startSession();
    try {
        await session.withTransaction(async () => {
            const user = await usersCollection.findOne({ _id: userId }, { session });
            if (user.balance < transferAmount) throw new Error("Nicht genug Kohle auf dem Konto.");

            // 1. Dem User abziehen und in die Staatskasse werfen (Geld-Senke für die Wirtschaft)
            await usersCollection.updateOne({ _id: userId }, { $inc: { balance: -transferAmount } }, { session });
            await systemSettingsCollection.updateOne({ id: 'state_treasury' }, { $inc: { balance: transferAmount } }, { upsert: true, session });

            // 2. System-Nachricht in den Chat werfen
            const sysMsgContent = `💸 Du hast $${transferAmount.toLocaleString()} an ${chat.tindaPartnerName} gesendet.`;
            const transferMsg = {
                chatId: chatId,
                senderId: null, // null markiert es als neutrale System-Nachricht
                senderUsername: "System",
                content: sysMsgContent,
                timestamp: new Date(),
                isSystem: true // Perfekt, um es im Frontend anders zu stylen!
            };
            await limMessagesCollection.insertOne(transferMsg, { session });

            // 3. Chat-Vorschau aktualisieren
            await limChatsCollection.updateOne({ _id: chatId }, {
                $set: { 
                    lastMessagePreview: sysMsgContent, 
                    updatedAt: new Date(), 
                    lastMessageTimestamp: new Date() 
                }
            }, { session });
        });

        // Polling triggern für den User
        updateDataVersion('chat');
        res.json({ message: "Geld erfolgreich überwiesen!" });

        // 4. Die Magie: Wir rufen die KI auf und schieben ihr einen unsichtbaren System-Befehl unter, damit sie auf das Geld reagiert!
        const secretPrompt = `*System-Benachrichtigung der Tinda-App: Du hast gerade $${transferAmount} auf dein virtuelles Limazon-Konto überwiesen bekommen.* Reagiere in deiner nächsten Nachricht sofort auf dieses digitale Geldgeschenk (freu dich, sei flirty, arrogant oder kaufmännisch, passend zu deiner Rolle). Erwähne keine physischen Münzen oder Geldscheine, es ist eine App-Überweisung!`;
        
        triggerAiResponse(userId, chat.tindaPartnerId, chatId, secretPrompt);

    } catch (e) {
        res.status(400).json({ error: e.message });
    } finally {
        await session.endSession();
    }
});

// NEU: Heiraten und Zusammenziehen
app.post('/api/tinda/chat/:chatId/marry', isAuthenticated, isChatParticipant, async (req, res) => {
    const chatId = new ObjectId(req.params.chatId);
    const userId = new ObjectId(req.session.userId);
    const chat = req.chat;
    const MARRIAGE_COST = 10000;

    if (chat.type !== 'tinda') return res.status(400).json({ error: "Du kannst nur Tinda-Matches heiraten." });
    if (chat.isMarried) return res.status(400).json({ error: "Ihr seid bereits verheiratet!" });

    const session = client.startSession();
    try {
        await session.withTransaction(async () => {
            const user = await usersCollection.findOne({ _id: userId }, { session });
            if (user.balance < MARRIAGE_COST) throw new Error(`Hochzeit kostet $${MARRIAGE_COST}. Spar noch etwas!`);

            // 1. Kosten abziehen & User-Status anpassen
            await usersCollection.updateOne({ _id: userId }, { $inc: { balance: -MARRIAGE_COST }, $set: { isMarriedTo: chat.tindaPartnerName } }, { session });

            // 2. Chat auf verheiratet setzen
            await limChatsCollection.updateOne({ _id: chatId }, { $set: { isMarried: true } }, { session });

            // 3. Zusammenziehen (Partner in die eigene Immobilie packen, falls vorhanden)
            const home = await ownedPropertiesCollection.findOne({ ownerId: userId }, { session });
            if (home) {
                await ownedPropertiesCollection.updateOne({ _id: home._id }, { $addToSet: { roommates: chat.tindaPartnerId } }, { session });
            }

            // 4. System-Nachricht in den Chat
            const sysMsgContent = `💍 IHR HABT GEHEIRATET! ${chat.tindaPartnerName} ist bei dir eingezogen. Das gemeinsame Konto ist eröffnet.`;
            await limMessagesCollection.insertOne({
                chatId, senderId: null, senderUsername: "System", content: sysMsgContent, timestamp: new Date(), isSystem: true
            }, { session });
        });

        updateDataVersion('chat');
        triggerAiResponse(userId, chat.tindaPartnerId, chatId, "*System: Der User hat dir soeben einen Antrag gemacht und ihr seid verheiratet und zusammengezogen! Reagiere extrem glücklich als frischgebackener Ehepartner!*");

        res.json({ message: "Herzlichen Glückwunsch zur Hochzeit!" });
    } catch (e) {
        res.status(400).json({ error: e.message });
    } finally {
        await session.endSession();
    }
});

// Gemeinsames Konto (Tägliches Taschengeld vom Partner abheben)
app.post('/api/tinda/chat/:chatId/shared-account', isAuthenticated, isChatParticipant, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    const chat = req.chat;
    const DAILY_ALLOWANCE = 1500; // Gehalt vom Ehepartner

    if (chat.type !== 'tinda' || !chat.isMarried) return res.status(400).json({ error: "Ihr seid nicht verheiratet." });

    const user = await usersCollection.findOne({ _id: userId });
    const now = Date.now();
    const lastWithdraw = user.lastAllowanceAt ? new Date(user.lastAllowanceAt).getTime() : 0;

    // 24 Stunden Cooldown für das gemeinsame Konto
    if (now - lastWithdraw < 24 * 60 * 60 * 1000) {
        const hoursLeft = Math.ceil((24 * 60 * 60 * 1000 - (now - lastWithdraw)) / (1000 * 60 * 60));
        return res.status(429).json({ error: `Gemeinsames Konto gesperrt. Dein Partner lässt dich erst in ${hoursLeft} Stunden wieder ran.` });
    }

    await usersCollection.updateOne(
        { _id: userId },
        { $inc: { balance: DAILY_ALLOWANCE }, $set: { lastAllowanceAt: new Date() } }
    );

    // Trigger KI, damit der Partner darauf reagiert
    triggerAiResponse(userId, chat.tindaPartnerId, chat._id, `*System: Du hast bemerkt, dass dein Ehepartner gerade $${DAILY_ALLOWANCE} vom gemeinsamen Konto abgehoben hat. Sag ihm deine Meinung dazu.*`);

    res.json({ message: `$${DAILY_ALLOWANCE} vom gemeinsamen Konto abgehoben. Dein Partner hat das sicher gemerkt.`, newBalance: user.balance + DAILY_ALLOWANCE });
});

// Scheidung und Auszug
app.post('/api/tinda/chat/:chatId/divorce', isAuthenticated, isChatParticipant, async (req, res) => {
    const chatId = new ObjectId(req.params.chatId);
    const userId = new ObjectId(req.session.userId);
    const chat = req.chat;
    const DIVORCE_COST = 5000; // Anwaltskosten

    if (chat.type !== 'tinda') return res.status(400).json({ error: "Das geht nur bei Tinda-Matches." });
    if (!chat.isMarried) return res.status(400).json({ error: "Ihr seid gar nicht verheiratet!" });

    const session = client.startSession();
    try {
        await session.withTransaction(async () => {
            const user = await usersCollection.findOne({ _id: userId }, { session });
            if (user.balance < DIVORCE_COST) throw new Error(`Die Scheidung kostet $${DIVORCE_COST} Anwaltsgebühren. Du bist zu arm für die Trennung!`);

            // 1. Kosten abziehen & Beziehungsstatus beim User entfernen
            await usersCollection.updateOne(
                { _id: userId }, 
                { $inc: { balance: -DIVORCE_COST }, $unset: { isMarriedTo: "" } }, 
                { session }
            );

            // 2. Chat auf "nicht mehr verheiratet" setzen
            await limChatsCollection.updateOne(
                { _id: chatId }, 
                { $set: { isMarried: false } }, 
                { session }
            );

            // 3. Auszug (Partner aus der eigenen Immobilie werfen)
            const home = await ownedPropertiesCollection.findOne({ ownerId: userId }, { session });
            if (home) {
                await ownedPropertiesCollection.updateOne(
                    { _id: home._id }, 
                    { $pull: { roommates: chat.tindaPartnerId } }, 
                    { session }
                );
            }

            // 4. System-Nachricht in den Chat
            const sysMsgContent = `💔 IHR HABT EUCH GESCHIEDEN! ${chat.tindaPartnerName} ist ausgezogen.`;
            await limMessagesCollection.insertOne({
                chatId, senderId: null, senderUsername: "System", content: sysMsgContent, timestamp: new Date(), isSystem: true
            }, { session });
        });

        updateDataVersion('chat');
        triggerAiResponse(userId, chat.tindaPartnerId, chatId, "*System: Der User hat soeben die Scheidung eingereicht und dich aus der Wohnung geworfen. Reagiere wütend, dramatisch oder fassungslos!*");

        res.json({ message: "Scheidung erfolgreich eingereicht. Dein Ex-Partner ist ausgezogen." });
    } catch (e) {
        res.status(400).json({ error: e.message });
    } finally {
        await session.endSession();
    }
});

// Kind bekommen (Nur wenn verheiratet)
app.post('/api/tinda/chat/:chatId/have-child', isAuthenticated, isChatParticipant, async (req, res) => {
    const chatId = new ObjectId(req.params.chatId);
    const userId = new ObjectId(req.session.userId);
    const chat = req.chat;
    const { childName } = req.body;

    if (chat.type !== 'tinda') return res.status(400).json({ error: "Nur für Tinda-Matches." });
    if (!chat.isMarried) return res.status(400).json({ error: "Ihr müsst zuerst heiraten!" });
    if (!childName || childName.trim().length < 2) return res.status(400).json({ error: "Bitte gib einen gültigen Namen für das Kind ein." });

    const session = client.startSession();
    try {
        await session.withTransaction(async () => {
            // 1. Kind zum Hauptchat (Ehepartner) hinzufügen, damit die KI Bescheid weiß
            const newChild = { name: childName.trim(), bornAt: new Date() };
            await limChatsCollection.updateOne(
                { _id: chatId }, 
                { $push: { children: newChild } }, 
                { session }
            );

            // 2. Neuen Chat erstellen: Nur mit dem Kind
            const childChat = {
                type: 'tinda_child',
                participants: [userId],
                tindaPartnerId: chat.tindaPartnerId, // Referenz zum Elternteil
                tindaPartnerName: chat.tindaPartnerName,
                childName: newChild.name,
                createdAt: new Date(),
                updatedAt: new Date(),
                lastMessagePreview: "*Babygeräusche*",
                lastMessageTimestamp: new Date()
            };
            await limChatsCollection.insertOne(childChat, { session });

            // 3. Neuen Chat erstellen: Familien-Gruppenchat (User + Ehepartner + Kind)
            // Wir erstellen ihn nur, wenn er noch nicht existiert
            const existingFamilyChat = await limChatsCollection.findOne({ type: 'tinda_family', participants: userId, tindaPartnerId: chat.tindaPartnerId }, { session });
            
            if (!existingFamilyChat) {
                const familyChat = {
                    type: 'tinda_family',
                    participants: [userId],
                    tindaPartnerId: chat.tindaPartnerId,
                    tindaPartnerName: chat.tindaPartnerName,
                    familyNames: [chat.tindaPartnerName, newChild.name], // Wird später für den Prompt genutzt
                    createdAt: new Date(),
                    updatedAt: new Date(),
                    lastMessagePreview: "Familien-Chat erstellt!",
                    lastMessageTimestamp: new Date()
                };
                await limChatsCollection.insertOne(familyChat, { session });
            } else {
                // Wenn Familienchat schon existiert (2. Kind), Kind zum Array hinzufügen
                await limChatsCollection.updateOne({ _id: existingFamilyChat._id }, { $push: { familyNames: newChild.name } }, { session });
            }

            // System-Nachricht in den Hauptchat
            await limMessagesCollection.insertOne({
                chatId, senderId: null, senderUsername: "System", content: `👶 Herzlichen Glückwunsch! Euer Kind ${newChild.name} wurde geboren! Es wurden neue Chats erstellt.`, timestamp: new Date(), isSystem: true
            }, { session });
        });

        updateDataVersion('chat');
        triggerAiResponse(userId, chat.tindaPartnerId, chatId, `*System: Ihr habt soeben ein Kind namens ${childName.trim()} bekommen! Reagiere extrem emotional und glücklich darüber als frischgebackenes Elternteil!*`);

        res.json({ message: `Herzlichen Glückwunsch zu eurem Kind ${childName.trim()}!` });
    } catch (e) {
        res.status(400).json({ error: e.message });
    } finally {
        await session.endSession();
    }
});

// =========================================================
// === BUG BOUNTY SYSTEM V2 (MIT DELTA COINS) ===
// =========================================================

// DEFINITION: Der Exklusive Delta-Shop
const DELTA_SHOP_ITEMS = [
    {
        id: 'tax_shield',
        name: 'Steuerschutz-Zertifikat 🛡️',
        cost: 1,
        desc: 'Verhindert einmalig, dass das Finanzamt dir Geld abzieht.',
        type: 'item' // Fügt Item ins Inventar
    },
    {
        id: 'badge_hunter',
        name: 'Badge: Bug Hunter 🐛',
        cost: 3,
        desc: 'Ein exklusives Abzeichen für dein Profil.',
        type: 'badge' // Fügt Achievement hinzu
    },
    {
        id: 'job_reset',
        name: 'Energy Drink ⚡',
        cost: 1,
        desc: 'Setzt sofort deinen Arbeits-Cooldown im Jobcenter zurück.',
        type: 'effect_job' // Sofortiger Effekt
    },
    {
        id: 'crime_cleaner',
        name: 'Gefälschter Pass 🕵️',
        cost: 2,
        desc: 'Setzt deinen Überfall-Cooldown (Crime) sofort zurück.',
        type: 'effect_crime' // Sofortiger Effekt
    }
];

// 1. Report einreichen (Unverändert)
app.post('/api/bugs', isAuthenticated, async (req, res) => {
    const { title, description, steps } = req.body;
    const userId = new ObjectId(req.session.userId);
    const username = req.session.username;

    if (!title || !description) return res.status(400).json({ error: "Titel und Beschreibung fehlen." });

    const report = {
        userId,
        username,
        title: title.trim(),
        description: description.trim(),
        steps: steps ? steps.trim() : "",
        status: 'open',
        rewardGiven: false,
        createdAt: new Date()
    };

    await bugReportsCollection.insertOne(report);
    console.log(`${LOG_PREFIX_SERVER} 🐛 Bug Report von ${username}: ${title}`);
    res.status(201).json({ message: "Report eingereicht! Warte auf Genehmigung." });
});

// 2. Admin: Reports ansehen (Unverändert)
app.get('/api/admin/bugs', isAuthenticated, isAdmin, async (req, res) => {
    try {
        const reports = await bugReportsCollection.find({}).sort({ createdAt: -1 }).toArray();
        res.json({ reports });
    } catch (e) { res.status(500).json({ error: "Fehler." }); }
});

// 3. Admin: Status ändern & Delta-Coin vergeben (ANGEPASST)
app.post('/api/admin/bugs/:id/resolve', isAuthenticated, isAdmin, async (req, res) => {
    const { status, giveReward } = req.body;
    const reportId = new ObjectId(req.params.id);

    try {
        const report = await bugReportsCollection.findOne({ _id: reportId });
        if (!report) return res.status(404).json({ error: "Report nicht gefunden." });

        const updateData = { status, updatedAt: new Date() };
        let rewardMsg = "";

        // WENN GENEHMIGT UND BELOHNUNG AKTIV:
        if (status === 'resolved' && giveReward && !report.rewardGiven) {

            // Gib dem User genau 1 Delta Coin
            await usersCollection.updateOne(
                { _id: report.userId },
                { $inc: { deltaCoins: 1 } }
            );

            updateData.rewardGiven = true;
            rewardMsg = " 1 Delta-Coin (∆) wurde dem User gutgeschrieben.";
        }

        await bugReportsCollection.updateOne({ _id: reportId }, { $set: updateData });
        res.json({ message: `Status geändert.${rewardMsg}` });

    } catch (e) {
        console.error(e);
        res.status(500).json({ error: "Serverfehler." });
    }
});

// 4. User: Delta-Shop Infos laden (NEU)
app.get('/api/bug-bounty/shop', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    const user = await usersCollection.findOne({ _id: userId }, { projection: { deltaCoins: 1 } });

    res.json({
        deltaCoins: user.deltaCoins || 0,
        items: DELTA_SHOP_ITEMS
    });
});

// 5. User: Item für Delta-Coins kaufen (NEU)
app.post('/api/bug-bounty/buy', isAuthenticated, async (req, res) => {
    const { itemId } = req.body;
    const userId = new ObjectId(req.session.userId);

    const itemDef = DELTA_SHOP_ITEMS.find(i => i.id === itemId);
    if (!itemDef) return res.status(400).json({ error: "Item nicht gefunden." });

    const session = client.startSession();

    try {
        await session.withTransaction(async () => {
            // A. User checken (Coins vorhanden?)
            const user = await usersCollection.findOne({ _id: userId }, { session });
            const currentCoins = user.deltaCoins || 0;

            if (currentCoins < itemDef.cost) {
                throw new Error(`Nicht genügend Delta-Coins. Du hast ${currentCoins}∆, brauchst aber ${itemDef.cost}∆.`);
            }

            // B. Coins abziehen
            await usersCollection.updateOne(
                { _id: userId },
                { $inc: { deltaCoins: -itemDef.cost } },
                { session }
            );

            // C. Effekt ausführen
            if (itemDef.type === 'item') {
                // Item ins Inventar (z.B. Tax Shield)
                // Sicherstellen, dass das Produkt existiert (Mock-Check)
                const prodExists = await productsCollection.findOne({ id: itemDef.id }, { session });
                if (!prodExists) {
                    await productsCollection.insertOne({
                        id: itemDef.id, name: itemDef.name, price: '$0.00',
                        description: itemDef.desc, stock: 0, isTokenCard: false, image_url: 'https://placehold.co/150/000/fff?text=DELTA'
                    }, { session });
                }
                await inventoriesCollection.updateOne(
                    { userId: userId, productId: itemDef.id },
                    { $inc: { quantityOwned: 1 } },
                    { upsert: true, session }
                );

            } else if (itemDef.type === 'badge') {
                // Badge zum Profil
                await usersCollection.updateOne(
                    { _id: userId },
                    { $addToSet: { achievements: itemDef.id } },
                    { session }
                );

            } else if (itemDef.type === 'effect_job') {
                // Job Cooldown Reset
                await usersCollection.updateOne(
                    { _id: userId },
                    { $set: { lastWorkedAt: 0 } }, // Auf 0 setzen = sofort bereit
                    { session }
                );

            } else if (itemDef.type === 'effect_crime') {
                // Crime Cooldown Reset
                await usersCollection.updateOne(
                    { _id: userId },
                    { $set: { lastRobberyAt: 0 } },
                    { session }
                );
            }
        });

        res.json({ message: `Gekauft: ${itemDef.name}. Danke für deine Treue!` });

    } catch (e) {
        res.status(400).json({ error: e.message });
    } finally {
        await session.endSession();
    }
});

// =========================================================
// === THE HEIST V2 (COMMUNITY RAID) ===
// =========================================================

// Helfer: Status abrufen
async function getHeistState() {
    // Hole Firewall Status
    let fwSetting = await systemSettingsCollection.findOne({ id: 'heist_firewall' });
    if (!fwSetting) {
        fwSetting = { id: 'heist_firewall', integrity: 100.00, openUntil: 0 };
        await systemSettingsCollection.insertOne(fwSetting);
    }

    // Prüfen ob Zeitfenster abgelaufen
    if (fwSetting.integrity <= 0 && Date.now() > fwSetting.openUntil) {
        // Reset
        console.log(`${LOG_PREFIX_SERVER} 🛡️ Firewall hat sich regeneriert. Reset auf 100%.`);
        await systemSettingsCollection.updateOne({ id: 'heist_firewall' }, { $set: { integrity: 100.00, openUntil: 0 } });
        fwSetting.integrity = 100.00;
        fwSetting.openUntil = 0;
    }

    const treasury = await getStateTreasuryBalance();

    return {
        integrity: fwSetting.integrity,
        isOpen: fwSetting.integrity <= 0,
        openUntil: fwSetting.openUntil,
        treasuryBalance: treasury
    };
}

// 1. Info abrufen (Polling)
app.get('/api/heist/info', isAuthenticated, async (req, res) => {
    const state = await getHeistState();
    res.json(state);
});

// 2. HACKEN (Mit Cooldown gegen Spam)
app.post('/api/heist/hack', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    const COST = 500;
    const HACK_COOLDOWN = 60 * 1000; // 60 Sekunden Pause pro Spieler

    const session = client.startSession();
    try {
        await session.withTransaction(async () => {
            const user = await usersCollection.findOne({ _id: userId }, { session });

            // A. Cooldown prüfen
            const now = Date.now();
            const lastHack = user.lastHackAt ? new Date(user.lastHackAt).getTime() : 0;
            if (now - lastHack < HACK_COOLDOWN) {
                const left = Math.ceil((HACK_COOLDOWN - (now - lastHack)) / 1000);
                throw new Error(`Hacking-Tools überhitzt! Warte ${left}s.`);
            }

            if (user.balance < COST) throw new Error("Zu wenig Geld für Hacker-Tools ($500).");

            const currentState = await systemSettingsCollection.findOne({ id: 'heist_firewall' }, { session });
            if (currentState.integrity <= 0) throw new Error("Firewall ist bereits unten! Starte den Überfall!");

            // B. Geld abziehen & Timestamp setzen
            await usersCollection.updateOne(
                { _id: userId },
                {
                    $inc: { balance: -COST },
                    $set: { lastHackAt: new Date() } // WICHTIG: Zeit speichern
                },
                { session }
            );

            // C. Schaden berechnen (1.5% bis 4.0%)
            const damage = (Math.random() * 2.5) + 1.5;
            let newIntegrity = currentState.integrity - damage;
            let openUntil = 0;

            if (newIntegrity <= 0) {
                newIntegrity = 0;
                openUntil = Date.now() + (60 * 60 * 1000); // 60 Min offen

                // News Broadcast
                await newsCollection.insertOne({
                    headline: "FIREWALL DOWN! 🔓",
                    content: `Die Sicherheits-Systeme der Staatskasse sind ausgefallen! Zugriff möglich!`,
                    author: "Anonymous",
                    category: "Verbrechen",
                    createdAt: new Date(),
                    likes: 0
                }, { session });
            }

            await systemSettingsCollection.updateOne(
                { id: 'heist_firewall' },
                { $set: { integrity: newIntegrity, openUntil: openUntil } },
                { session }
            );
        });

        res.json({ message: "Hack erfolgreich! Firewall beschädigt." });
    } catch (e) {
        res.status(400).json({ error: e.message });
    } finally {
        await session.endSession();
    }
});

// 3. ZUGRIFF (Der Raub - Nur wenn offen)
app.post('/api/heist/start', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    
    // Großer Raub kostet mehr Equipment
    const COST = 2000;

    const session = client.startSession();
    try {
        let result = {};
        await session.withTransaction(async () => {
            const user = await usersCollection.findOne({ _id: userId }, { session });
            if (user.balance < COST) throw new Error("Nicht genug Geld für Equipment ($2000).");

            // Status prüfen
            const fw = await systemSettingsCollection.findOne({ id: 'heist_firewall' }, { session });
            const treasuryDoc = await systemSettingsCollection.findOne({ id: 'state_treasury' }, { session });
            const pot = treasuryDoc ? treasuryDoc.balance : 0;

            if (fw.integrity > 0) throw new Error("Firewall ist noch aktiv! Hackt sie erst runter.");
            if (pot < 1000) throw new Error("Tresor ist leer.");

            // Cooldown pro User (erhöht auf 15 Minuten!)
            const lastHeist = user.lastHeistAt ? new Date(user.lastHeistAt).getTime() : 0;
            if (Date.now() - lastHeist < 15 * 60 * 1000) {
                const waitMin = Math.ceil((15 * 60 * 1000 - (Date.now() - lastHeist)) / 60000);
                throw new Error(`Fahndungslevel zu hoch. Warte ${waitMin} Minuten.`);
            }

            // Kosten
            await usersCollection.updateOne({ _id: userId }, { $inc: { balance: -COST } }, { session });

            // CHANCE: Deutlich schwerer (nur noch 25% Erfolg)
            const isSuccess = Math.random() < 0.25; 

            if (isSuccess) {
                // Beute: Nur noch 0.5% bis 2% des Pots
                const percent = (Math.random() * 0.015) + 0.005;
                let loot = Math.floor(pot * percent);
                
                // Hard-Cap: Maximal 50 Millionen auf einmal (verhindert Milliarden-Diebstähle)
                if (loot > 50000000) loot = 50000000;

                await systemSettingsCollection.updateOne({ id: 'state_treasury' }, { $inc: { balance: -loot } }, { session });
                await usersCollection.updateOne(
                    { _id: userId }, 
                    { $inc: { balance: loot }, $set: { lastHeistAt: new Date() } }, 
                    { session }
                );
                result = { success: true, message: `TREFFER! Du hast $${loot.toLocaleString()} erbeutet!` };
            } else {
                // Erwischt: Dynamische Strafe! 2% des EIGENEN Geldes (Mindestens 5000, Maximal 10 Millionen)
                let fine = Math.floor(user.balance * 0.02);
                if (fine < 5000) fine = 5000;
                if (fine > 10000000) fine = 10000000;

                await usersCollection.updateOne(
                    { _id: userId }, 
                    { $inc: { balance: -fine }, $set: { lastHeistAt: new Date() } }, 
                    { session }
                );
                // Strafe wandert zurück in den Pot
                await systemSettingsCollection.updateOne({ id: 'state_treasury' }, { $inc: { balance: fine } }, { session });
                
                result = { success: false, message: `ALARM! Erwischt! Du musstest $${fine.toLocaleString()} Strafe zahlen.` };
            }
        });
        res.json(result);
    } catch (e) {
        res.status(400).json({ error: e.message });
    } finally {
        await session.endSession();
    }
});

// =========================================================
// === ADMIN CHAT INSPECTOR ===
// =========================================================

// 1. Liste aller Tinda-Chats abrufen (Wer schreibt mit wem?)
app.get('/api/admin/chat/tinda-conversations', isAuthenticated, isAdmin, async (req, res) => {
    try {
        // Suche alle Chats vom Typ 'tinda'
        const chats = await limChatsCollection.find({ type: 'tinda' })
            .sort({ updatedAt: -1 }) // Aktuellste zuerst
            .limit(50)
            .toArray();

        // Wir holen uns noch die Usernamen der echten User dazu (nicht der KI)
        // In Tinda Chats ist participants[0] meist der echte User
        const enrichedChats = [];

        for (const chat of chats) {
            const realUserId = chat.participants[0];
            const realUser = await usersCollection.findOne({ _id: realUserId }, { projection: { username: 1 } });

            enrichedChats.push({
                chatId: chat._id,
                user: realUser ? realUser.username : "Unbekannt",
                partner: chat.tindaPartnerName, // Name der KI/Person
                lastMsg: chat.lastMessagePreview,
                lastActive: chat.updatedAt
            });
        }

        res.json({ count: enrichedChats.length, chats: enrichedChats });
    } catch (e) {
        res.status(500).json({ error: "Fehler beim Laden der Chats." });
    }
});

// 2. Nachrichten lesen (Filterbar)
// Nutzung: /api/admin/chat/messages?chatId=... ODER /api/admin/chat/messages?onlyAi=true
app.get('/api/admin/chat/messages', isAuthenticated, isAdmin, async (req, res) => {
    const { chatId, onlyAi, limit } = req.query;
    const query = {};

    try {
        // Filter: Bestimmter Chat?
        if (chatId) {
            query.chatId = new ObjectId(chatId);
        }

        // Filter: Nur KI-Nachrichten?
        if (onlyAi === 'true') {
            query.isAi = true;
        }

        // Suche in der DB
        const messages = await limMessagesCollection.find(query)
            .sort({ timestamp: -1 }) // Neueste zuerst
            .limit(parseInt(limit) || 100) // Standardmäßig max 100
            .toArray();

        res.json({ count: messages.length, messages });
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: "Fehler beim Laden der Nachrichten." });
    }
});

// 3. Als Admin in einen Chat schreiben
app.post('/api/admin/chat/send', isAuthenticated, isAdmin, async (req, res) => {
    const { chatId, content } = req.body;

    if (!chatId || !content) return res.status(400).json({ error: "Fehlende Daten." });

    const cId = new ObjectId(chatId);
    const adminName = req.session.username;

    try {
        // Nachricht erstellen
        const msg = {
            chatId: cId,
            senderId: new ObjectId(req.session.userId),
            // WICHTIG: Wir setzen einen Prefix, damit der User es checkt (oder du lässt es weg für Pranks)
            senderUsername: `[ADMIN] ${adminName}`,
            content: content.trim(),
            timestamp: new Date(),
            isAdminMessage: true // Markierung für internes Styling
        };

        await limMessagesCollection.insertOne(msg);

        // Chat updaten (damit es beim User als "neu" angezeigt wird)
        await limChatsCollection.updateOne(
            { _id: cId },
            {
                $set: {
                    lastMessagePreview: `[ADMIN]: ${content.substring(0, 20)}...`,
                    updatedAt: new Date(),
                    lastMessageTimestamp: new Date()
                }
            }
        );

        // Polling Trigger für den User
        updateDataVersion('chat');

        res.json({ message: "Gesendet.", msg });
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: "Fehler beim Senden." });
    }
});

// =========================================================
// === 🍔 RESTAURANT MENU CONFIG ===
// =========================================================
const RESTAURANT_MENU = [
    // --- 🍔 BURGER ---
    { id: 'bg_basic', name: 'Hamburger', price: 350.00, energy: 30, type: 'burger', icon: '🍔', desc: 'Klassisch mit Gurke.' },
    { id: 'bg_cheese', name: 'Cheeseburger', price: 450.00, energy: 40, type: 'burger', icon: '🧀', desc: 'Mit doppelt Käse.' },
    { id: 'bg_bacon', name: 'Bacon BBQ', price: 650.00, energy: 60, type: 'burger', icon: '🥓', desc: 'Rauchig und deftig.' },
    { id: 'bg_chicken', name: 'Chicken Burger', price: 500.00, energy: 45, type: 'burger', icon: '🐔', desc: 'Knuspriges Hähnchen.' },
    { id: 'bg_xxl', name: 'Monster Burger', price: 1200.00, energy: 100, type: 'burger', icon: '🦖', desc: '1kg Fleisch. Füllt komplett auf.' },

    // --- 🍕 PIZZA ---
    { id: 'pz_marg', name: 'Pizza Margherita', price: 550.00, energy: 50, type: 'pizza', icon: '🍅', desc: 'Der italienische Klassiker.' },
    { id: 'pz_sal', name: 'Pizza Salami', price: 650.00, energy: 60, type: 'pizza', icon: '🍕', desc: 'Heiß und fettig!' },
    { id: 'pz_diav', name: 'Pizza Diavolo', price: 750.00, energy: 70, type: 'pizza', icon: '🌶️', desc: 'Scharf! Bringt dich zum Schwitzen.' },
    { id: 'pz_haw', name: 'Pizza Hawaii', price: 600.00, energy: 55, type: 'pizza', icon: '🍍', desc: 'Ananas gehört drauf. Punkt.' },

    // --- 🍟 BEILAGEN ---
    { id: 'sd_fries', name: 'Pommes Frites', price: 150.00, energy: 15, type: 'side', icon: '🍟', desc: 'Salzig und goldgelb.' },
    { id: 'sd_nuggets', name: '6er Nuggets', price: 250.00, energy: 20, type: 'side', icon: '🍗', desc: 'Mit Süß-Sauer Sauce.' },
    { id: 'sd_rings', name: 'Zwiebelringe', price: 200.00, energy: 18, type: 'side', icon: '🧅', desc: 'Frittierte Ringe.' },
    { id: 'sd_salad', name: 'Ceasar Salad', price: 400.00, energy: 25, type: 'side', icon: '🥗', desc: 'Für das Gewissen.' },

    // --- 🥤 DRINKS ---
    { id: 'dr_coke', name: 'Limo Cola', price: 120.00, energy: 10, type: 'drink', icon: '🥤', desc: 'Zucker pur.' },
    { id: 'dr_energy', name: 'Energy Drink', price: 250.00, energy: 35, type: 'drink', icon: '⚡', desc: 'Herzrasen inklusive.' },
    { id: 'dr_coffee', name: 'Schwarzer Kaffee', price: 100.00, energy: 15, type: 'drink', icon: '☕', desc: 'Macht wach.' },
    { id: 'dr_water', name: 'Wasser', price: 50.00, energy: 5, type: 'drink', icon: '💧', desc: 'Langweilig aber gesund.' },

    // --- 💎 LUXUS ---
    { id: 'lux_steak', name: 'Gold Steak', price: 250000.00, energy: 100, type: 'luxury', icon: '🥩', desc: 'Mit 24k Blattgold überzogen.' },
    { id: 'lux_caviar', name: 'Kaviar Dose', price: 50000.00, energy: 100, type: 'luxury', icon: '🐟', desc: 'Schmeckt nach Geld.' }
];

// =========================================================
// === RESTAURANT API (LIMO'S DINER) ===
// =========================================================
const LOG_PREFIX_REST = "[Restaurant API]";

// 1. Speisekarte abrufen
app.get('/api/restaurant/menu', isAuthenticated, (req, res) => {
    res.json({ menu: RESTAURANT_MENU });
});

// 2. Essen bestellen
app.post('/api/restaurant/order', isAuthenticated, async (req, res) => {
    let { itemIds, itemId } = req.body;
    const userId = new ObjectId(req.session.userId);

    if (itemId && !itemIds) itemIds = [itemId];
    if (!itemIds || !Array.isArray(itemIds) || itemIds.length === 0) {
        return res.status(400).json({ error: "Der Teller ist leer. Wähle etwas aus!" });
    }

    const session = client.startSession();

    try {
        let totalPrice = 0;
        let totalEnergyBase = 0; // Basis-Energie der Gerichte
        let itemNames = [];
        let itemsDetails = [];

        for (const id of itemIds) {
            const food = RESTAURANT_MENU.find(i => i.id === id);
            if (!food) throw new Error(`Gericht '${id}' steht nicht auf der Karte.`);
            totalPrice += food.price;
            totalEnergyBase += food.energy;
            itemNames.push(food.name);
            itemsDetails.push({ name: food.name, icon: food.icon });
        }

        await session.withTransaction(async () => {
            const user = await usersCollection.findOne({ _id: userId }, { session });
            if (user.balance < totalPrice) throw new Error(`Nicht genug Geld! Das Menü kostet $${totalPrice.toFixed(2)}.`);

            // --- HAUS-BONUS LOGIK ---
            const home = await ownedPropertiesCollection.findOne({ 
                $or: [{ ownerId: userId }, { roommates: userId }] 
            }, { session });

            const multiplier = home ? (home.energyBonus || 1.0) : 1.0;
            const finalEnergyGain = totalEnergyBase * multiplier;
            // ------------------------

            let newLastWorkedAt = user.lastWorkedAt || 0;
            const reductionMs = finalEnergyGain * 60 * 1000;

            if (newLastWorkedAt > 0) {
                const oldDate = new Date(newLastWorkedAt).getTime();
                newLastWorkedAt = new Date(oldDate - reductionMs);
            }

            await usersCollection.updateOne(
                { _id: userId },
                {
                    $inc: {
                        balance: -totalPrice,
                        "stats.foodEaten": itemIds.length
                    },
                    $set: { lastWorkedAt: newLastWorkedAt }
                },
                { session }
            );

            await restaurantOrdersCollection.insertOne({
                userId: userId,
                username: req.session.username,
                items: itemsDetails,
                cost: totalPrice,
                energyGained: finalEnergyGain, // Speichere den geboosteten Wert
                multiplier: multiplier,
                date: new Date()
            }, { session });

            // Lokales Ergebnis für die Response außerhalb der Transaction speichern
            res.locals.finalEnergy = finalEnergyGain;
            res.locals.multiplier = multiplier;
        });

        const updatedUser = await usersCollection.findOne({ _id: userId }, { projection: { balance: 1 } });

        let msg = `Guten Appetit! ($${totalPrice.toFixed(2)})`;
        if (res.locals.multiplier > 1) {
            msg += ` Haus-Bonus aktiv: x${res.locals.multiplier}!`;
        }

        res.json({
            message: msg,
            newBalance: updatedUser.balance,
            energyGain: res.locals.finalEnergy,
            itemsEaten: itemNames
        });

    } catch (e) {
        console.error(`${LOG_PREFIX_REST} Fehler bei Bestellung:`, e.message);
        res.status(400).json({ error: e.message });
    } finally {
        await session.endSession();
    }
});

// 3. Status abrufen (Energie & Historie-Vorschau)
app.get('/api/restaurant/status', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);

    try {
        const user = await usersCollection.findOne({ _id: userId });

        // --- ENERGIE BERECHNUNG ---
        let energyPercent = 100;
        let statusText = "Volle Energie! Geh arbeiten.";

        if (user.job) {
            const jobDef = JOB_LIST.find(j => j.id === user.job);
            if (jobDef && user.lastWorkedAt) {
                const now = Date.now();
                const lastWork = new Date(user.lastWorkedAt).getTime();
                const cooldownMs = jobDef.cooldownSeconds * 1000;
                const timePassed = now - lastWork;

                if (timePassed < cooldownMs) {
                    // Wenn Cooldown aktiv ist, berechnen wir den Fortschritt
                    // 0ms vergangen = 0% Energie. cooldownMs vergangen = 100% Energie.
                    energyPercent = Math.floor((timePassed / cooldownMs) * 100);
                    statusText = `Erholt sich... (${energyPercent}%)`;
                }
            }
        }

        // --- LETZTE MAHLZEITEN ---
        const lastMeals = await restaurantOrdersCollection
            .find({ userId: userId })
            .sort({ date: -1 })
            .limit(5)
            .toArray();

        res.json({
            energy: energyPercent,
            statusText: statusText,
            job: user.job || "Arbeitslos",
            lastMeals: lastMeals
        });

    } catch (e) {
        console.error(`${LOG_PREFIX_REST} Status-Fehler:`, e);
        res.status(500).json({ error: "Konnte Energie nicht messen." });
    }
});

app.get('/api/restaurant/history', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    const history = await restaurantOrdersCollection
        .find({ userId: userId })
        .sort({ date: -1 })
        .limit(50) // Die letzten 50 Mahlzeiten
        .toArray();

    res.json({ history });
});

// =========================================================
// === LIMTEREST API (Pinterest Clone) ===
// =========================================================
const LOG_PREFIX_PIN = "[Limterest]";

// 2. Neuen Pin erstellen (NUR URL!)
app.post('/api/limterest/pin', isAuthenticated, async (req, res) => {
    const { title, imageUrl, tags } = req.body;
    const userId = new ObjectId(req.session.userId);
    const username = req.session.username;

    // Validierung: Wir erlauben KEINE Base64 Strings (die sind zu lang und füllen die DB)
    if (!imageUrl || imageUrl.length > 1000) {
        return res.status(400).json({ error: "URL zu lang oder ungültig. Bitte keine Base64-Bilder!" });
    }
    // Einfacher Check ob es wie eine URL aussieht
    if (!imageUrl.startsWith('http')) {
        return res.status(400).json({ error: "Das ist keine gültige Bild-URL (muss mit http starten)." });
    }

    // Tags verarbeiten ("Auto, Rot, Schnell" -> ["Auto", "Rot", "Schnell"])
    let tagArray = [];
    if (tags && typeof tags === 'string') {
        tagArray = tags.split(',').map(t => t.trim()).filter(t => t.length > 0);
    }

    try {
        const newPin = {
            userId,
            username,
            title: title || "Ohne Titel",
            imageUrl: imageUrl,
            tags: tagArray,
            likes: 0,
            likedBy: [],
            createdAt: new Date()
        };

        await limterestCollection.insertOne(newPin);

        // Achievement Check: "Influencer" oder so könnte man hier triggern

        console.log(`${LOG_PREFIX_PIN} Neuer Pin von ${username}: ${title}`);
        res.status(201).json({ message: "Gepinnt!", pin: newPin });

    } catch (e) {
        res.status(500).json({ error: "Fehler beim Pinnen." });
    }
});

// 3. Pin liken
app.post('/api/limterest/pin/:id/like', isAuthenticated, async (req, res) => {
    const pinId = new ObjectId(req.params.id);
    const userId = new ObjectId(req.session.userId);

    try {
        const pin = await limterestCollection.findOne({ _id: pinId });
        if (!pin) return res.status(404).json({ error: "Pin nicht gefunden." });

        // Toggle Like
        const hasLiked = pin.likedBy && pin.likedBy.some(id => id.equals(userId));

        if (hasLiked) {
            // Unlike
            await limterestCollection.updateOne(
                { _id: pinId },
                { $inc: { likes: -1 }, $pull: { likedBy: userId } }
            );
            res.json({ message: "Unliked", likes: pin.likes - 1 });
        } else {
            // Like
            await limterestCollection.updateOne(
                { _id: pinId },
                { $inc: { likes: 1 }, $addToSet: { likedBy: userId } }
            );
            res.json({ message: "Liked", likes: pin.likes + 1 });
        }
    } catch (e) {
        res.status(500).json({ error: "Fehler." });
    }
});

// 1. Feed laden (MIT SUCHE & USER-FILTER)
app.get('/api/limterest/feed', async (req, res) => {
    const { q, username } = req.query;
    const match = {};

    // Filter: Suche (Case-Insensitive)
    if (q) {
        match.$or = [
            { title: { $regex: q, $options: 'i' } },
            { tags: { $in: [new RegExp(q, 'i')] } }
        ];
    }

    // Filter: Bestimmter User
    if (username) {
        match.username = username;
    }

    try {
        const pins = await limterestCollection.aggregate([
            { $match: match },
            { $sort: { createdAt: -1 } },
            { $limit: 50 } // Pagination könnte man hier noch erweitern
        ]).toArray();

        res.json({ pins });
    } catch (e) {
        res.status(500).json({ error: "Fehler beim Laden." });
    }
});

// 2. User Profil laden (Stats & Follow Status)
app.get('/api/limterest/user/:targetUsername', isAuthenticated, async (req, res) => {
    const { targetUsername } = req.params;
    const myId = new ObjectId(req.session.userId);

    try {
        const user = await usersCollection.findOne({ username: targetUsername });
        if (!user) return res.status(404).json({ error: "User nicht gefunden." });

        // Stats berechnen
        const pinCount = await limterestCollection.countDocuments({ username: targetUsername });
        const followersCount = (user.followers || []).length;

        // Folge ich ihm schon?
        const isFollowing = user.followers && user.followers.some(id => id.equals(myId));

        res.json({
            username: user.username,
            bio: user.bio || "Keine Bio.",
            stats: { pins: pinCount, followers: followersCount },
            isFollowing: !!isFollowing,
            joinDate: user._id.getTimestamp()
        });
    } catch (e) {
        res.status(500).json({ error: "Profil-Fehler." });
    }
});

// 3. Follow / Unfollow
app.post('/api/limterest/user/:targetUsername/follow', isAuthenticated, async (req, res) => {
    const { targetUsername } = req.params;
    const myId = new ObjectId(req.session.userId);

    try {
        const targetUser = await usersCollection.findOne({ username: targetUsername });
        if (!targetUser) return res.status(404).json({ error: "User 404" });
        if (targetUser._id.equals(myId)) return res.status(400).json({ error: "Kein Eigen-Follow." });

        // Check if already following
        const isFollowing = targetUser.followers && targetUser.followers.some(id => id.equals(myId));

        if (isFollowing) {
            // UNFOLLOW
            await usersCollection.updateOne({ _id: targetUser._id }, { $pull: { followers: myId } });
            await usersCollection.updateOne({ _id: myId }, { $pull: { following: targetUser._id } });
            res.json({ message: "Unfollowed", isFollowing: false });
        } else {
            // FOLLOW
            await usersCollection.updateOne({ _id: targetUser._id }, { $addToSet: { followers: myId } });
            await usersCollection.updateOne({ _id: myId }, { $addToSet: { following: targetUser._id } });
            res.json({ message: "Followed", isFollowing: true });
        }
    } catch (e) {
        res.status(500).json({ error: "Follow Fehler." });
    }
});

// 4. Pin merken (Save to Profile)
app.post('/api/limterest/pin/:id/save', isAuthenticated, async (req, res) => {
    const pinId = new ObjectId(req.params.id);
    const userId = new ObjectId(req.session.userId);

    try {
        // Wir speichern die gemerkten Pin-IDs im User-Dokument
        const user = await usersCollection.findOne({ _id: userId });
        const saved = user.savedPins || [];

        // Toggle Logik (Merken / Entmerken)
        const isSaved = saved.some(id => id.equals(pinId));

        if (isSaved) {
            await usersCollection.updateOne({ _id: userId }, { $pull: { savedPins: pinId } });
            res.json({ message: "Pin entfernt.", isSaved: false });
        } else {
            await usersCollection.updateOne({ _id: userId }, { $addToSet: { savedPins: pinId } });
            res.json({ message: "Pin gemerkt!", isSaved: true });
        }
    } catch (e) {
        res.status(500).json({ error: "Fehler beim Merken." });
    }
});

// 5. Pin melden (Report)
app.post('/api/limterest/pin/:id/report', isAuthenticated, async (req, res) => {
    const pinId = new ObjectId(req.params.id);
    const username = req.session.username;

    try {
        // Wir nutzen einfach die existierende 'bugReportsCollection' oder erstellen eine 'reportsCollection'
        // Der Einfachheit halber loggen wir es in eine "moderationQueue" (oder erstellen sie)
        const reportEntry = {
            type: 'pin_report',
            pinId: pinId,
            reportedBy: username,
            reason: "Inappropriate Content",
            timestamp: new Date(),
            status: 'open'
        };

        // Speichern in einer allgemeinen Admin-Liste (nutze SystemSettings oder eine neue Collection)
        // Hier erstellen wir kurzentschlossen eine 'reportsCollection' dynamisch
        await db.collection('reports').insertOne(reportEntry);

        console.log(`${LOG_PREFIX_PIN} Pin ${pinId} wurde von ${username} gemeldet.`);
        res.json({ message: "Meldung empfangen. Wir kümmern uns darum." });
    } catch (e) {
        res.status(500).json({ error: "Fehler beim Melden." });
    }
});

// 6. Status eines Pins prüfen (Ist er gemerkt?)
app.get('/api/limterest/pin/:id/is-saved', isAuthenticated, async (req, res) => {
    const pinId = new ObjectId(req.params.id);
    const userId = new ObjectId(req.session.userId);

    try {
        const user = await usersCollection.findOne({ _id: userId }, { projection: { savedPins: 1 } });
        const isSaved = user.savedPins && user.savedPins.some(id => id.equals(pinId));
        res.json({ isSaved: !!isSaved });
    } catch (e) {
        res.status(500).json({ error: "Fehler." });
    }
});

// 7. Alle gemerkten Pins laden
app.get('/api/limterest/my-saved', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);

    try {
        const user = await usersCollection.findOne({ _id: userId }, { projection: { savedPins: 1 } });

        if (!user.savedPins || user.savedPins.length === 0) {
            return res.json({ pins: [] });
        }

        // Alle Pins laden, deren ID im savedPins Array ist
        const pins = await limterestCollection.find({ _id: { $in: user.savedPins } }).toArray();
        res.json({ pins });
    } catch (e) {
        res.status(500).json({ error: "Fehler beim Laden." });
    }
});

// =========================================================
// === YAKUZA / BLACK MARKET ===
// =========================================================

// Preise & Config
const YAKUZA_SERVICE_PRICES = { fakeid: 500000, leak: 1000000 };

// Preisliste für Badges (alles andere ist 15 Mio Standard)
const ACHIEVEMENT_MARKET_PRICES = {
    newbie: 1000000, identity: 1000000, og: 5000000,
    veteran: 50000000, streak_week: 25000000, streak_month: 250000000,
    flappy_noob: 5000000, flappy_ace: 50000000, snake_eater: 50000000,
    badge_hunter: 1000000000, // 1 Mrd
    hoarder: 15000000, museum: 75000000,
    critic: 5000000, judge: 15000000, jury: 50000000,
    inventor: 10000000, visionary: 30000000,
    talkative: 5000000, influencer: 25000000, legend_spam: 150000000,
    limo_bezos: 2000000000, // 2 Mrd
    badge_yakuza: 5000000, badge_hacker: 10000000, badge_rich: 25000000, badge_illuminati: 50000000
};

// GET: Katalog laden
app.get('/api/yakuza/catalog', isAuthenticated, async (req, res) => {
    try {
        const userId = new ObjectId(req.session.userId);

        // 1. User Besitz laden
        const user = await usersCollection.findOne({ _id: userId });
        const owned = [
            ...(user.achievements || []),
            ...(user.badges || [])
        ];

        // 2. Die "Besonderen" Yakuza Badges definieren
        const exclusives = [
            { id: 'badge_yakuza', title: 'Yakuza', icon: '🐉', desc: 'Teil der Familie.', price: 5000000 },
            { id: 'badge_hacker', title: 'Hacker', icon: '💻', desc: 'Systembrecher.', price: 10000000 },
            { id: 'badge_rich', title: 'Tycoon', icon: '🎩', desc: 'Geld regiert.', price: 25000000 },
            { id: 'badge_illuminati', title: 'Illuminati', icon: '👁️', desc: 'Allsehend.', price: 50000000 },
            // Bug Hunter auch hier als Exclusive definieren, damit der Preis stimmt (1 Mrd)
            { id: 'badge_hunter', title: 'Bug Hunter', icon: '🐛', desc: 'Elite.', price: 1000000000 }
        ];

        // Liste der IDs erstellen, die wir schon haben (um Duplikate zu vermeiden)
        const exclusiveIds = exclusives.map(e => e.id);

        // 3. Normale Liste laden, aber Exclusives RAUSFILTERN
        const regular = ACHIEVEMENT_DEFINITIONS
            .filter(ach => !exclusiveIds.includes(ach.id)) // <--- HIER IST DER FIX
            .map(ach => ({
                id: ach.id,
                title: ach.title,
                icon: ach.icon,
                desc: ach.desc,
                price: ACHIEVEMENT_MARKET_PRICES[ach.id] || 15000000 // 15 Mio Standard
            }));

        res.json({
            catalog: [...exclusives, ...regular],
            owned: owned
        });

    } catch (e) {
        console.error(e);
        res.status(500).json({ error: "Fehler beim Laden des Katalogs." });
    }
});

// POST: Kaufen
app.post('/api/yakuza/buy', isAuthenticated, async (req, res) => {
    const { service, target } = req.body;
    const userId = new ObjectId(req.session.userId);
    let price = 0, isBadge = false, badgeId = "";

    if (YAKUZA_SERVICE_PRICES[service]) {
        price = YAKUZA_SERVICE_PRICES[service];
    } else if (service.startsWith('badge_')) {
        isBadge = true;
        badgeId = service.replace('badge_', '');
        price = ACHIEVEMENT_MARKET_PRICES[badgeId] || 15000000;
    } else {
        return res.status(400).json({ error: "Unbekannter Service." });
    }

    try {
        const user = await usersCollection.findOne({ _id: userId });
        if (user.balance < price) return res.status(400).json({ error: `Zu wenig Geld ($${price.toLocaleString()}).` });

        if (isBadge) {
            if ((user.badges && user.badges.includes(badgeId)) || (user.achievements && user.achievements.includes(badgeId))) {
                return res.status(400).json({ error: "Bereits im Besitz." });
            }
            await usersCollection.updateOne({ _id: userId }, { $inc: { balance: -price }, $addToSet: { badges: badgeId } });
            return res.json({ success: true, message: "Gekauft!", newBalance: user.balance - price });
        }

        // --- 3. LOGIK FÜR FAKE ID (Cooldown Reset) ---
        if (service === 'fakeid') {

            await usersCollection.updateOne({ _id: userId }, {
                $inc: { balance: -price },

                // HIER IST DER FIX: Die korrekten Feldnamen aus deinem Log!
                $unset: {
                    "lastRobberyAt": "",  // Weg mit dem Raub-Cooldown
                    "lastHeistAt": "",    // Weg mit dem Heist-Cooldown
                    "lastHackAt": "",     // Weg mit dem Hack-Cooldown
                    "lastWorkedAt": "",   // Weg mit der Job-Sperre (Bonus)
                    "lastDaily": ""       // Optional: Daily Reward Reset (falls du das willst)
                }
            });

            return res.json({
                success: true,
                message: "Identität bereinigt. Alle Fahndungs-Timer wurden geschreddert.",
                newBalance: user.balance - price
            });
        }

        if (service === 'leak') {
            if (!target) return res.status(400).json({ error: "Ziel fehlt." });
            const v = await usersCollection.findOne({ username: target }, { projection: { password: 0, sessions: 0, "2fa_secret": 0 } });
            if (!v) return res.status(404).json({ error: "User nicht gefunden." });
            await usersCollection.updateOne({ _id: userId }, { $inc: { balance: -price } });

            const invCount = await inventoriesCollection.countDocuments({ userId: v._id });
            const rareItems = await inventoriesCollection.find({ userId: v._id, "productDetails.price": { $gt: 1000 } }).toArray();

            const leakData = {
                _id: v._id, username: v.username, role: v.isAdmin ? "ADMIN" : "User",
                balance: v.balance, tokens: v.tokens || 0, infinity: v.infinityMoney || false,
                inventorySize: invCount, rareItems: rareItems.map(i => i.productDetails.name),
                cooldowns: v.cooldowns || {}
            };
            return res.json({ success: true, leak: leakData, newBalance: user.balance - price });
        }

    } catch (e) { res.status(500).json({ error: "Fehler." }); }
});

// =========================================================
// === ⚖️ LIMO COURT SYSTEM (UPDATED) ===
// =========================================================

const COURT_FEE = 5000;
const BASE_DURATION = 24 * 60 * 60 * 1000; // 24 Stunden Standard
const MAX_DURATION = 120 * 60 * 60 * 1000; // 5 Tage Maximum (Hard Limit)
const MIN_VOTES = 3;                       // Mindestens 3 Stimmen für reguläres Ende

app.get('/api/court/status', isAuthenticated, async (req, res) => {
    try {
        const userId = new ObjectId(req.session.userId);

        // Suche aktiven Fall
        const activeCase = await db.collection('courtCases').findOne(
            { status: 'active' },
            { sort: { createdAt: 1 } }
        );

        let caseData = null;

        if (activeCase) {
            const gCount = (activeCase.votes_guilty || []).length;
            const iCount = (activeCase.votes_innocent || []).length;
            const total = gCount + iCount;

            // Check Vote Status User
            let myVote = null;
            if (activeCase.votes_guilty?.map(id => id.toString()).includes(userId.toString())) myVote = 'guilty';
            if (activeCase.votes_innocent?.map(id => id.toString()).includes(userId.toString())) myVote = 'innocent';

            // --- ZEIT & ENDE LOGIK ---
            const now = new Date();
            const created = new Date(activeCase.createdAt);

            // Wann wäre das reguläre Ende?
            let endsAt = new Date(created.getTime() + BASE_DURATION);
            const hardLimit = new Date(created.getTime() + MAX_DURATION);

            let isOvertime = false;

            // Ist die reguläre Zeit abgelaufen?
            if (now > endsAt) {
                // Haben wir GENUG Stimmen ODER ist das Hard Limit erreicht?
                if (total >= MIN_VOTES || now > hardLimit) {

                    // === FALL SCHLIESSEN ===
                    const verdict = gCount > iCount ? 'guilty' : 'innocent';
                    // Bei Gleichstand im Hard Limit: Freispruch (In dubio pro reo)
                    if (gCount === iCount) verdict = 'innocent';

                    await db.collection('courtCases').updateOne(
                        { _id: activeCase._id },
                        { $set: { status: 'closed', verdict: verdict, closedAt: now } }
                    );

                    // Strafe vollstrecken
                    if (verdict === 'guilty') {
                        await usersCollection.updateOne(
                            { username: activeCase.accusedName },
                            { $mul: { balance: 0.9 } } // 10% Strafe
                        );
                    }

                    return res.redirect('/api/court/status'); // Reload für nächsten Fall

                } else {
                    // === VERLÄNGERUNG (OVERTIME) ===
                    // Zu wenig Stimmen -> Wir verlängern bis zum Hard Limit
                    isOvertime = true;
                    endsAt = hardLimit; // Neues Ende anzeigen
                }
            }

            caseData = {
                id: activeCase._id,
                accused: activeCase.accusedName,
                accusedAvatar: `https://ui-avatars.com/api/?name=${activeCase.accusedName}&background=333&color=fff`,
                plaintiff: activeCase.plaintiffName,
                plaintiffAvatar: `https://ui-avatars.com/api/?name=${activeCase.plaintiffName}&background=111&color=fff`,
                crime: activeCase.crime,
                description: activeCase.description,
                stats: {
                    guilty: gCount,
                    innocent: iCount,
                    total: total,
                    guiltyPerc: total > 0 ? Math.round((gCount / total) * 100) : 50,
                    innocentPerc: total > 0 ? Math.round((iCount / total) * 100) : 50
                },
                myVote: myVote,
                endsAt: endsAt.toISOString(), // Für den Countdown
                isOvertime: isOvertime,       // Flag für UI Warnung
                votesNeeded: Math.max(0, MIN_VOTES - total) // Wie viele fehlen noch?
            };
        }

        // Archiv laden
        const archive = await db.collection('courtCases')
            .find({ status: 'closed' })
            .sort({ closedAt: -1 })
            .limit(5)
            .toArray();

        res.json({
            activeCase: caseData,
            archive: archive.map(c => ({
                id: c._id,
                title: `${c.accusedName} vs. ${c.plaintiffName}`,
                crime: c.crime,
                verdict: c.verdict
            }))
        });

    } catch (e) {
        console.error(e);
        res.status(500).json({ error: "Fehler im Gerichtssystem." });
    }
});

// 2. POST: Einen neuen Fall einreichen
app.post('/api/court/file', isAuthenticated, async (req, res) => {
    try {
        const { accused, crime, description } = req.body;
        const userId = new ObjectId(req.session.userId);
        const user = await usersCollection.findOne({ _id: userId });

        // Gebühr checken
        if (user.balance < COURT_FEE) {
            return res.status(400).json({ error: `Anklage kostet $${COURT_FEE}. Du bist zu arm für Gerechtigkeit.` });
        }

        // Angeklagten suchen
        const target = await usersCollection.findOne({ username: { $regex: new RegExp(`^${accused}$`, 'i') } });
        if (!target) return res.status(404).json({ error: "Dieser User existiert nicht." });
        if (target._id.toString() === userId.toString()) return res.status(400).json({ error: "Du kannst dich nicht selbst verklagen." });

        // Geld abziehen
        await usersCollection.updateOne({ _id: userId }, { $inc: { balance: -COURT_FEE } });

        // Fall erstellen
        const newCase = {
            accusedId: target._id,
            accusedName: target.username,
            plaintiffId: userId,
            plaintiffName: user.username,
            crime: crime,
            description: description,
            status: 'active',
            createdAt: new Date(),
            votes_guilty: [],   // Array von UserIDs
            votes_innocent: [], // Array von UserIDs
            voted_devices: []   // NEU: Speichert die Geräte-Fingerabdrücke
        };

        await db.collection('courtCases').insertOne(newCase);

        res.json({ success: true, message: "Anklage eingereicht. Der Fall liegt nun dem Gericht vor." });

    } catch (e) {
        res.status(500).json({ error: "Fehler beim Einreichen." });
    }
});

// 3. POST: Abstimmen
app.post('/api/court/vote', isAuthenticated, async (req, res) => {
    try {
        const { caseId, verdict } = req.body;
        const userId = new ObjectId(req.session.userId);

        if (!['guilty', 'innocent'].includes(verdict)) return res.status(400).json({ error: "Ungültiges Urteil." });

        // --- 🛡️ DEVICE FINGERPRINT LOGIK (Das Cookie auslesen) ---
        let deviceId = null;
        if (req.headers.cookie) {
            // Zerteilt den Cookie-String in ein nutzbares Objekt
            const cookies = req.headers.cookie.split(';').reduce((acc, cookie) => {
                const [key, value] = cookie.split('=').map(c => c.trim());
                acc[key] = value;
                return acc;
            }, {});
            deviceId = cookies['limo_device_id'];
        }

        // Wenn das Gerät noch kein Cookie hat, generieren wir ein neues und heften es an den Browser
        if (!deviceId) {
            deviceId = uuidv4();
            res.cookie('limo_device_id', deviceId, {
                maxAge: 365 * 24 * 60 * 60 * 1000, // 1 Jahr gültig!
                httpOnly: true, // Kann nicht von bösartigen Scripts ausgelesen werden
                secure: process.env.NODE_ENV === 'production',
                sameSite: process.env.NODE_ENV === 'production' ? 'none' : 'lax'
            });
        }
        // -----------------------------------------------------------

        const user = await usersCollection.findOne({ _id: userId });

        // 🛡️ ANTI-SMURF 1: Account-Alter (24h)
        const accountAgeMs = Date.now() - new Date(user._id.getTimestamp()).getTime();
        const ONE_DAY_MS = 24 * 60 * 60 * 1000;

        if (accountAgeMs < ONE_DAY_MS && !user.isAdmin) {
            return res.status(403).json({ error: "Dein Account muss mindestens 24 Stunden alt sein, um abzustimmen." });
        }

        // 🛡️ ANTI-SMURF 2: "Skin in the Game" (Aktivitätsprüfung)
        if (user.balance < 1000 && !user.job && !user.isAdmin) {
            return res.status(403).json({ error: "Du musst aktiv am Leben in Limazon teilnehmen (z.B. einen Job haben oder $1000 besitzen), um bei Gericht zugelassen zu werden." });
        }

        const courtCase = await db.collection('courtCases').findOne({ _id: new ObjectId(caseId) });
        if (!courtCase || courtCase.status !== 'active') return res.status(404).json({ error: "Fall nicht gefunden oder geschlossen." });

        // 🛡️ ANTI-SMURF 3: GERÄTE SPERRE (Max 1 Stimme pro Gerät!)
        const votedDevices = courtCase.voted_devices || [];

        if (votedDevices.includes(deviceId) && !user.isAdmin) {
            return res.status(403).json({ error: "Von diesem Gerät wurde bereits abgestimmt! Das Wechseln des Accounts ist verboten." });
        }

        // Hat DIESER User (ID) schon abgestimmt?
        const alreadyVoted =
            (courtCase.votes_guilty || []).some(id => id.toString() === userId.toString()) ||
            (courtCase.votes_innocent || []).some(id => id.toString() === userId.toString());

        if (alreadyVoted) return res.status(400).json({ error: "Du hast bereits abgestimmt." });

        // Vote & Device ID in die Datenbank eintragen
        const field = verdict === 'guilty' ? 'votes_guilty' : 'votes_innocent';
        await db.collection('courtCases').updateOne(
            { _id: new ObjectId(caseId) },
            {
                $push: {
                    [field]: userId,
                    voted_devices: deviceId // Das Gerät wird für diesen Fall gesperrt
                }
            }
        );

        res.json({ success: true, message: "Deine Stimme wurde vom Gericht notiert." });

    } catch (e) {
        console.error(e);
        res.status(500).json({ error: "Systemfehler bei der Stimmabgabe." });
    }
});

// =========================================================
// === 🏴‍☠️ GANG SYSTEM BACKEND ===
// =========================================================

const GANG_CREATE_COST = 5000000; // $5 Mio.
const MAX_MEMBERS = 10; // Erstmal klein anfangen
const ZONES_CONFIG = {
    'arcade': { name: "Pixel Arcade", cost: 5000000, img: "🕹️" },
    'casino': { name: "Royal Casino", cost: 15000000, img: "🎰" },
    'bank': { name: "Central Bank", cost: 50000000, img: "🏦" }
};

// 1. GET: Gang Dashboard Daten laden (KOMPLETT)
app.get('/api/gangs/dashboard', isAuthenticated, async (req, res) => {
    try {
        const userId = new ObjectId(req.session.userId);
        const user = await usersCollection.findOne({ _id: userId });

        // A) User & Gang Status
        const myGang = await db.collection('gangs').findOne({ members: userId });

        // B) Public Chat (Global)
        const publicChat = await db.collection('publicGangChat')
            .find({})
            .sort({ createdAt: -1 })
            .limit(50)
            .toArray();

        // C) Top Gangs (für Rangliste)
        const topGangs = await db.collection('gangs')
            .find({})
            .project({ name: 1, tag: 1, balance: 1, memberCount: { $size: "$members" } })
            .sort({ balance: -1 })
            .limit(10)
            .toArray();

        // D) Zonen Status (Territory Control)
        const zonesRaw = await db.collection('zones').find({}).toArray();
        const zonesData = []; // Array für Frontend

        for (const [key, val] of Object.entries(ZONES_CONFIG)) {
            const dbZone = zonesRaw.find(z => z._id === key);
            // Ist die Zone aktuell besetzt? (Zeit noch nicht abgelaufen)
            const isTaken = dbZone && new Date(dbZone.expiresAt) > new Date();

            zonesData.push({
                id: key,
                name: val.name,
                cost: val.cost,
                icon: val.img,
                isTaken: isTaken,
                ownerTag: isTaken ? dbZone.ownerTag : null,
                ownerName: isTaken ? dbZone.ownerName : null,
                expiresAt: isTaken ? dbZone.expiresAt : null
            });
        }

        // --- ANTWORT BAUEN ---

        const responseData = {
            inGang: !!myGang, // true/false
            userBalance: user.balance, // WICHTIG: Dein Geld
            createCost: 5000000,
            publicChat: publicChat.reverse(),
            topGangs: topGangs,
            zones: zonesData // Die Gebiete
        };

        if (myGang) {
            // Wenn in Gang: Details laden
            const memberDetails = await usersCollection.find(
                { _id: { $in: myGang.members } },
                { projection: { username: 1, balance: 1, _id: 1 } }
            ).toArray();

            responseData.gang = {
                id: myGang._id,
                name: myGang.name,
                tag: myGang.tag,
                balance: myGang.balance,
                isLeader: myGang.leaderId.toString() === userId.toString(),
                members: memberDetails,
                privateChat: myGang.privateChat || [],
                upgrades: myGang.upgrades || {} // Für den Shop ("Besitz")
            };
        }

        res.json(responseData);

    } catch (e) {
        console.error(e);
        res.status(500).json({ error: "Gang Server Fehler." });
    }
});

// 2. POST: Neue Gang gründen
app.post('/api/gangs/create', isAuthenticated, async (req, res) => {
    try {
        const { name, tag } = req.body;
        const userId = new ObjectId(req.session.userId);
        const user = await usersCollection.findOne({ _id: userId });

        // Validierung
        if (!name || name.length < 3 || name.length > 20) return res.status(400).json({ error: "Name ungültig (3-20 Zeichen)." });
        if (!tag || tag.length < 2 || tag.length > 4) return res.status(400).json({ error: "Tag ungültig (2-4 Zeichen)." });

        // Hat er genug Geld?
        if (user.balance < GANG_CREATE_COST) return res.status(400).json({ error: `Du brauchst $${GANG_CREATE_COST.toLocaleString()}!` });

        // Ist er schon in einer Gang?
        const existingMember = await db.collection('gangs').findOne({ members: userId });
        if (existingMember) return res.status(400).json({ error: "Du bist schon in einer Gang! Erst verlassen." });

        // Gibt es den Namen schon?
        const nameTaken = await db.collection('gangs').findOne({ $or: [{ name: name }, { tag: tag }] });
        if (nameTaken) return res.status(400).json({ error: "Name oder Tag schon vergeben." });

        // ALLES OK -> GANG ERSTELLEN
        await usersCollection.updateOne({ _id: userId }, { $inc: { balance: -GANG_CREATE_COST } });

        const newGang = {
            name: name,
            tag: tag.toUpperCase(),
            leaderId: userId,
            members: [userId],
            balance: 0,
            privateChat: [],
            createdAt: new Date()
        };

        await db.collection('gangs').insertOne(newGang);

        res.json({ success: true, message: `Gang '${name}' gegründet!` });

    } catch (e) {
        res.status(500).json({ error: "Gründung fehlgeschlagen." });
    }
});

// 3. POST: Gang beitreten (Einfachste Version: Offen für alle)
app.post('/api/gangs/join', isAuthenticated, async (req, res) => {
    try {
        const { gangId } = req.body;
        const userId = new ObjectId(req.session.userId);

        const gang = await db.collection('gangs').findOne({ _id: new ObjectId(gangId) });
        if (!gang) return res.status(404).json({ error: "Gang nicht gefunden." });

        // Checks
        const alreadyInGang = await db.collection('gangs').findOne({ members: userId });
        if (alreadyInGang) return res.status(400).json({ error: "Du bist schon in einer Gang." });

        if (gang.members.length >= MAX_MEMBERS) return res.status(400).json({ error: "Gang ist voll." });

        // Join
        await db.collection('gangs').updateOne(
            { _id: new ObjectId(gangId) },
            { $push: { members: userId } }
        );

        // Systemnachricht im Gang Chat
        const sysMsg = { sender: "SYSTEM", msg: "Ein neuer Rekrut ist beigetreten.", time: new Date() };
        await db.collection('gangs').updateOne({ _id: new ObjectId(gangId) }, { $push: { privateChat: sysMsg } });

        res.json({ success: true, message: `Willkommen bei ${gang.name}!` });

    } catch (e) {
        res.status(500).json({ error: "Beitritt fehlgeschlagen." });
    }
});

// 4. POST: Geld einzahlen (Die "Bank" Funktion)
app.post('/api/gangs/deposit', isAuthenticated, async (req, res) => {
    try {
        const { amount } = req.body;
        const val = parseInt(amount);
        if (isNaN(val) || val <= 0) return res.status(400).json({ error: "Ungültiger Betrag." });

        const userId = new ObjectId(req.session.userId);
        const user = await usersCollection.findOne({ _id: userId });

        if (user.balance < val) return res.status(400).json({ error: "Zu wenig Geld." });

        // Ist User in einer Gang?
        const myGang = await db.collection('gangs').findOne({ members: userId });
        if (!myGang) return res.status(400).json({ error: "Keine Gang." });

        // Transaktion
        await usersCollection.updateOne({ _id: userId }, { $inc: { balance: -val } });
        await db.collection('gangs').updateOne(
            { _id: myGang._id },
            { $inc: { balance: val } }
        );

        res.json({ success: true, newBalance: user.balance - val, gangBalance: myGang.balance + val });

    } catch (e) {
        res.status(500).json({ error: "Einzahlung fehlgeschlagen." });
    }
});

// 5. POST: Chatten (Öffentlich & Privat)
app.post('/api/gangs/chat', isAuthenticated, async (req, res) => {
    try {
        const { message, type } = req.body; // type: 'public' oder 'private'
        if (!message || message.trim().length === 0) return res.status(400).json({ error: "Leere Nachricht." });

        const userId = new ObjectId(req.session.userId);
        const user = await usersCollection.findOne({ _id: userId });
        const myGang = await db.collection('gangs').findOne({ members: userId });

        const msgObj = {
            sender: user.username,
            tag: myGang ? myGang.tag : "", // Tag nur anzeigen, wenn in Gang
            msg: message.substring(0, 200), // Max Länge
            time: new Date()
        };

        if (type === 'private') {
            if (!myGang) return res.status(400).json({ error: "Du hast keine Gang für privaten Chat." });

            // In das Gang-Dokument pushen (Array Limiting auf 50 Nachrichten)
            await db.collection('gangs').updateOne(
                { _id: myGang._id },
                {
                    $push: {
                        privateChat: {
                            $each: [msgObj],
                            $slice: -50 // Nur die letzten 50 behalten
                        }
                    }
                }
            );

        } else {
            // Öffentlich: Jeder darf schreiben (auch ohne Gang, für Trash Talk)
            await db.collection('publicGangChat').insertOne(msgObj);

            // Optional: Alte Nachrichten löschen (Cleanup)
            // await db.collection('publicGangChat').deleteMany({ createdAt: { $lt: ... } }) 
        }

        res.json({ success: true });

    } catch (e) {
        res.status(500).json({ error: "Chat Fehler." });
    }
});

// 6. POST: Gang verlassen
app.post('/api/gangs/leave', isAuthenticated, async (req, res) => {
    try {
        const userId = new ObjectId(req.session.userId);
        const myGang = await db.collection('gangs').findOne({ members: userId });

        if (!myGang) return res.status(400).json({ error: "Du bist in keiner Gang." });

        // Wenn Leader geht: Gang auflösen? Oder Leader weitergeben?
        // Einfache Version: Gang wird gelöscht, wenn Leader geht (Geld geht verloren!) -> Hardcore!
        if (myGang.leaderId.toString() === userId.toString()) {
            await db.collection('gangs').deleteOne({ _id: myGang._id });
            return res.json({ success: true, message: "Gang aufgelöst (du warst der Leader)." });
        }

        // Normales Mitglied geht
        await db.collection('gangs').updateOne(
            { _id: myGang._id },
            { $pull: { members: userId } }
        );

        res.json({ success: true, message: "Gang verlassen." });

    } catch (e) {
        res.status(500).json({ error: "Fehler beim Verlassen." });
    }
});

// Leader: Mitglied kicken
app.post('/api/gangs/kick', isAuthenticated, async (req, res) => {
    try {
        const { targetId } = req.body;
        const userId = new ObjectId(req.session.userId);

        const myGang = await db.collection('gangs').findOne({ leaderId: userId });
        if (!myGang) return res.status(403).json({ error: "Nur der Leader kann kicken." });

        if (targetId === userId.toString()) return res.status(400).json({ error: "Du kannst dich nicht selbst kicken." });

        // Entfernen
        await db.collection('gangs').updateOne(
            { _id: myGang._id },
            { $pull: { members: new ObjectId(targetId) } }
        );

        res.json({ success: true, message: "Mitglied entfernt." });
    } catch (e) { res.status(500).json({ error: "Fehler." }); }
});

// Leader: Leadership übertragen
app.post('/api/gangs/promote', isAuthenticated, async (req, res) => {
    try {
        const { targetId } = req.body;
        const userId = new ObjectId(req.session.userId);

        const myGang = await db.collection('gangs').findOne({ leaderId: userId });
        if (!myGang) return res.status(403).json({ error: "Nur der Leader kann befördern." });

        // Check ob Target in der Gang ist
        if (!myGang.members.find(m => m.toString() === targetId)) return res.status(400).json({ error: "User nicht in der Gang." });

        // Update Leader
        await db.collection('gangs').updateOne(
            { _id: myGang._id },
            { $set: { leaderId: new ObjectId(targetId) } }
        );

        res.json({ success: true, message: "Führung übertragen." });
    } catch (e) { res.status(500).json({ error: "Fehler." }); }
});

// --- B) IMPERIUM (SHOP) ---

const UPGRADES = {
    'bunker': { name: "Bunker", cost: 10000000, desc: "Schützt 50% des Geldes bei Niederlagen." },
    'lawyer': { name: "Anwalt", cost: 25000000, desc: "Erhöht Verteidigungschance um 20%." },
    'weapons': { name: "Waffenlager", cost: 50000000, desc: "Erhöht Angriffskraft massiv." }
};

app.post('/api/gangs/upgrade', isAuthenticated, async (req, res) => {
    try {
        const { type } = req.body; // 'bunker', 'lawyer', 'weapons'
        const userId = new ObjectId(req.session.userId);
        const upgrade = UPGRADES[type];

        if (!upgrade) return res.status(400).json({ error: "Upgrade existiert nicht." });

        const myGang = await db.collection('gangs').findOne({ leaderId: userId });
        if (!myGang) return res.status(403).json({ error: "Nur der Leader kauft ein." });

        // Check Geld
        if (myGang.balance < upgrade.cost) return res.status(400).json({ error: "Gang-Kasse zu leer." });

        // Check ob schon gekauft (wir speichern Upgrades als Array oder Object)
        if (myGang.upgrades && myGang.upgrades[type]) return res.status(400).json({ error: "Schon im Besitz." });

        // Kaufen
        await db.collection('gangs').updateOne(
            { _id: myGang._id },
            {
                $inc: { balance: -upgrade.cost },
                $set: { [`upgrades.${type}`]: true }
            }
        );

        // Chat Nachricht
        await db.collection('gangs').updateOne({ _id: myGang._id }, {
            $push: { privateChat: { sender: "SYSTEM", msg: `${upgrade.name} wurde gekauft!`, time: new Date() } }
        });

        res.json({ success: true, message: `${upgrade.name} installiert.` });

    } catch (e) { res.status(500).json({ error: "Kauf gescheitert." }); }
});

// --- C) KRIEG (ATTACK) ---

app.post('/api/gangs/attack', isAuthenticated, async (req, res) => {
    try {
        const { targetGangId } = req.body;
        const userId = new ObjectId(req.session.userId);

        const myGang = await db.collection('gangs').findOne({ members: userId }); // Jeder Member darf angreifen? Oder nur Leader? Hier: Jeder.
        if (!myGang) return res.status(400).json({ error: "Du hast keine Gang." });

        const enemyGang = await db.collection('gangs').findOne({ _id: new ObjectId(targetGangId) });
        if (!enemyGang) return res.status(404).json({ error: "Gegner nicht gefunden." });

        if (myGang._id.toString() === enemyGang._id.toString()) return res.status(400).json({ error: "Friendly Fire ist aus." });

        // Cooldown Check (1 Stunde pro Gang)
        const now = new Date();
        if (myGang.lastAttack && (now - new Date(myGang.lastAttack)) < 3600000) {
            const minutesLeft = Math.ceil((3600000 - (now - new Date(myGang.lastAttack))) / 60000);
            return res.status(400).json({ error: `Waffen müssen abkühlen: ${minutesLeft} Min.` });
        }

        // BERECHNUNG DES KAMPFES
        // Basis Chance 50%
        let winChance = 0.5;

        // +5% pro Member mehr als der Gegner
        const memberDiff = myGang.members.length - enemyGang.members.length;
        winChance += (memberDiff * 0.05);

        // Upgrades einbeziehen
        if (myGang.upgrades?.weapons) winChance += 0.2; // +20% durch Waffen
        if (enemyGang.upgrades?.lawyer) winChance -= 0.2; // -20% durch gegnerischen Anwalt

        // Cap (Min 10%, Max 90%)
        if (winChance < 0.1) winChance = 0.1;
        if (winChance > 0.9) winChance = 0.9;

        const roll = Math.random();
        const isWin = roll < winChance;

        // Resultat
        if (isWin) {
            // Beute berechnen (5% vom Gegner-Geld)
            let loot = Math.floor(enemyGang.balance * 0.05);

            // Bunker Schutz?
            if (enemyGang.upgrades?.bunker) {
                loot = Math.floor(loot * 0.5); // Bunker halbiert Verlust
            }

            if (loot < 100) loot = 0; // Kleinkram lohnt nicht

            // Transaktion
            await db.collection('gangs').updateOne({ _id: enemyGang._id }, { $inc: { balance: -loot } });
            await db.collection('gangs').updateOne({ _id: myGang._id }, { $inc: { balance: loot }, $set: { lastAttack: new Date() } });

            // Nachrichten
            const msgWin = `SIEG gegen [${enemyGang.tag}]! Beute: $${loot.toLocaleString()}`;
            const msgLoss = `ALARM: [${myGang.tag}] hat uns angegriffen und $${loot.toLocaleString()} gestohlen!`;

            await db.collection('gangs').updateOne({ _id: myGang._id }, { $push: { privateChat: { sender: "WAR-BOT", msg: msgWin, time: new Date() } } });
            await db.collection('gangs').updateOne({ _id: enemyGang._id }, { $push: { privateChat: { sender: "WAR-BOT", msg: msgLoss, time: new Date() } } });

            res.json({ success: true, result: "WIN", loot: loot, message: `Sieg! $${loot.toLocaleString()} erbeutet.` });

        } else {
            // Niederlage (Keine Strafe, nur Cooldown und Schande)
            await db.collection('gangs').updateOne({ _id: myGang._id }, { $set: { lastAttack: new Date() } });

            const msgFail = `NIEDERLAGE beim Angriff auf [${enemyGang.tag}]. Rückzug!`;
            const msgDefend = `ANGRIFF ABGEWEHRT: [${myGang.tag}] hat es versucht und versagt.`;

            await db.collection('gangs').updateOne({ _id: myGang._id }, { $push: { privateChat: { sender: "WAR-BOT", msg: msgFail, time: new Date() } } });
            await db.collection('gangs').updateOne({ _id: enemyGang._id }, { $push: { privateChat: { sender: "WAR-BOT", msg: msgDefend, time: new Date() } } });

            res.json({ success: true, result: "LOSE", message: "Angriff gescheitert. Der Gegner war zu stark." });
        }

    } catch (e) {
        console.error(e);
        res.status(500).json({ error: "Kriegs-Server offline." });
    }
});

app.post('/api/gangs/rent-zone', isAuthenticated, async (req, res) => {
    try {
        const { zoneId } = req.body;
        const userId = new ObjectId(req.session.userId);

        if (!ZONES_CONFIG[zoneId]) return res.status(400).json({ error: "Zone existiert nicht." });
        const cost = ZONES_CONFIG[zoneId].cost;

        const myGang = await db.collection('gangs').findOne({ leaderId: userId });
        if (!myGang) return res.status(403).json({ error: "Nur der Leader kann Gebiete mieten." });

        // Check Geld
        if (myGang.balance < cost) return res.status(400).json({ error: "Kriegskasse zu leer." });

        // Check Status der Zone
        const currentZone = await db.collection('zones').findOne({ _id: zoneId });
        const now = new Date();

        // Ist sie noch besetzt?
        if (currentZone && currentZone.expiresAt > now) {
            // Optional: Wenn es die EIGENE Gang ist, verlängern wir einfach?
            if (currentZone.ownerGangId.toString() === myGang._id.toString()) {
                // Verlängerung erlauben
            } else {
                const left = Math.ceil((currentZone.expiresAt - now) / 60000);
                return res.status(400).json({ error: `Gebiet ist besetzt von [${currentZone.ownerTag}]! Frei in ${left} Min.` });
            }
        }

        // KAUFEN / MIETEN
        await db.collection('gangs').updateOne({ _id: myGang._id }, { $inc: { balance: -cost } });

        const expires = new Date(now.getTime() + (24 * 60 * 60 * 1000)); // +24h

        await db.collection('zones').updateOne(
            { _id: zoneId },
            {
                $set: {
                    ownerGangId: myGang._id,
                    ownerName: myGang.name,
                    ownerTag: myGang.tag,
                    rentedAt: now,
                    expiresAt: expires
                }
            },
            { upsert: true }
        );

        // Chat & Log
        const msg = `Wir haben ${ZONES_CONFIG[zoneId].name} für 24h eingenommen!`;
        await db.collection('gangs').updateOne({ _id: myGang._id }, { $push: { privateChat: { sender: "SYSTEM", msg: msg, time: new Date() } } });

        res.json({ success: true, message: `Gebiet gesichert! Kosten: $${cost.toLocaleString()}` });

    } catch (e) {
        console.error(e);
        res.status(500).json({ error: "Fehler bei der Landnahme." });
    }
});

const PAYDAY_INTERVAL = 10 * 60 * 1000; // Alle 10 Minuten

// Einnahmen pro Intervall (10 Min)
const ZONE_INCOME = {
    'arcade': 50000,   // $50k alle 10 Min ($300k/h)
    'casino': 150000,  // $150k alle 10 Min ($900k/h)
    'bank': 500000     // $500k alle 10 Min ($3M/h)
};

if (cluster.isPrimary) {
    setInterval(async () => {
        try {
            console.log("🔄 Payday: Verteile Gebiets-Einnahmen...");
            const now = new Date();
            const activeZones = await db.collection('zones').find({ expiresAt: { $gt: now } }).toArray();

            if (activeZones.length === 0) return;

            for (const zone of activeZones) {
                const income = ZONE_INCOME[zone._id] || 10000; 
                
                await db.collection('gangs').updateOne(
                    { _id: zone.ownerGangId },
                    { 
                        $inc: { balance: income },
                        $push: { 
                            privateChat: { 
                                sender: "SYSTEM", 
                                msg: `💰 Einnahmen aus ${zone.ownerName}: +$${income.toLocaleString()}`, 
                                time: new Date() 
                            } 
                        }
                    }
                );
            }
        } catch (e) {
            console.error("Payday Fehler:", e);
        }
    }, PAYDAY_INTERVAL);
}

// =========================================================
// === 📈 LIMO EXCHANGE (CRYPTO & FINANCE) ===
// =========================================================

// 1. Die Coins Konfiguration
let CRYPTO_MARKET = {
    'limo': { name: "Limo Coin", symbol: "LIMO", price: 1.00, volatility: 0.02, history: [], lastChange: 0 },
    'bitcoin': { name: "Bit-Limo", symbol: "BTC", price: 45000.00, volatility: 0.05, history: [], lastChange: 0 },
    'doge': { name: "Doge Limo", symbol: "DOGE", price: 0.15, volatility: 0.10, history: [], lastChange: 0 },
    'void': { name: "Dark Void", symbol: "VOID", price: 50.00, volatility: 0.25, history: [], lastChange: 0 }
};

// 2. Markt-Simulation (Preise ändern sich alle 30 Sekunden)
if (cluster.isPrimary) {
    setInterval(() => {
        for (let key in CRYPTO_MARKET) {
            const coin = CRYPTO_MARKET[key];
            const change = (Math.random() - 0.5) * coin.volatility; 
            let newPrice = coin.price * (1 + change);
            
            if (newPrice < 0.01) newPrice = 0.01;
            
            CRYPTO_MARKET[key].price = parseFloat(newPrice.toFixed(2));
            CRYPTO_MARKET[key].lastChange = parseFloat((change * 100).toFixed(2)); 

            if (!CRYPTO_MARKET[key].history) CRYPTO_MARKET[key].history = [];
            CRYPTO_MARKET[key].history.push(newPrice);
            
            if (CRYPTO_MARKET[key].history.length > 20) {
                CRYPTO_MARKET[key].history.shift();
            }
        }
    }, 30000);
}

// 3. API: Markt-Daten abrufen (Coins + Portfolio)
app.get('/api/finance/market', isAuthenticated, async (req, res) => {
    try {
        const userId = new ObjectId(req.session.userId);
        const user = await usersCollection.findOne({ _id: userId });

        // Wir senden den Marktstatus und das Portfolio des Users
        res.json({
            balance: user.balance,
            crypto: CRYPTO_MARKET,
            portfolio: user.cryptoWallet || {} // { 'limo': 100, 'void': 5 }
        });
    } catch (e) { res.status(500).json({ error: "Markt offline." }); }
});

// 4. API: Krypto Handeln (Kaufen/Verkaufen)
app.post('/api/finance/trade', isAuthenticated, async (req, res) => {
    try {
        const { coinId, amount, type } = req.body; // type: 'buy' oder 'sell'
        const qty = parseFloat(amount); // Menge an Coins

        if (!CRYPTO_MARKET[coinId]) return res.status(400).json({ error: "Coin existiert nicht." });
        if (qty <= 0) return res.status(400).json({ error: "Ungültige Menge." });

        const userId = new ObjectId(req.session.userId);
        const user = await usersCollection.findOne({ _id: userId });
        const price = CRYPTO_MARKET[coinId].price;
        const totalCost = price * qty;

        if (type === 'buy') {
            if (user.balance < totalCost) return res.status(400).json({ error: `Zu wenig Geld! Kosten: $${totalCost.toFixed(2)}` });

            await usersCollection.updateOne(
                { _id: userId },
                {
                    $inc: { balance: -totalCost, [`cryptoWallet.${coinId}`]: qty },
                    $push: { notifications: `📈 KAUF: ${qty} ${CRYPTO_MARKET[coinId].symbol} für $${totalCost.toFixed(0)}` }
                }
            );

        } else if (type === 'sell') {
            const userHoldings = user.cryptoWallet ? (user.cryptoWallet[coinId] || 0) : 0;
            if (userHoldings < qty) return res.status(400).json({ error: "Nicht genug Coins." });

            await usersCollection.updateOne(
                { _id: userId },
                {
                    $inc: { balance: totalCost, [`cryptoWallet.${coinId}`]: -qty },
                    $push: { notifications: `📉 VERKAUF: ${qty} ${CRYPTO_MARKET[coinId].symbol} für $${totalCost.toFixed(0)}` }
                }
            );
        }

        res.json({ success: true, message: "Transaktion erfolgreich!" });

    } catch (e) { res.status(500).json({ error: "Handel fehlgeschlagen." }); }
});

// =========================================================
// === 🃏 TEACHERMON (KARTENSPIEL API) ===
// =========================================================
const LOG_PREFIX_TEACHERMON = "[Teachermon]";

// 1. Seltenheits-Stufen & Drop-Raten
const TEACHERMON_RARITIES = {
    common: { name: 'Common', color: 'brown', dropRate: 0.60, sellPrice: 10 },
    rare: { name: 'Rare', color: 'blue', dropRate: 0.25, sellPrice: 50 },
    premium: { name: 'Premium', color: 'yellow', dropRate: 0.10, sellPrice: 200 },
    episch: { name: 'Episch', color: 'green', dropRate: 0.04, sellPrice: 1000 },
    legendaer: { name: 'Legendär', color: 'purple', dropRate: 0.01, sellPrice: 5000 }
};

// 2. Initiales Seeding (Beispiel-Lehrer anlegen, falls DB leer ist)
async function seedTeachermonCards() {
    const count = await teachermonCardsCollection.countDocuments();
    if (count === 0) {
        const dummyCards = [
            { id: 't_001', name: "Herr Müller", rarity: "common", kalterKaffee: 3, skills: "Tafel wischen", gequaelt: 15, intelligenz: 60, img: "👨‍🏫" },
            { id: 't_002', name: "Frau Schmidt", rarity: "rare", kalterKaffee: 5, skills: "Überraschungstest", gequaelt: 40, intelligenz: 80, img: "👩‍🏫" },
            { id: 't_003', name: "Direktor Weber", rarity: "legendaer", kalterKaffee: 10, skills: "Schulverweis (Glitzer-Attacke)", gequaelt: 999, intelligenz: 150, img: "🧙‍♂️" }
            // Hier kannst du später weitere hinzufügen!
        ];
        await teachermonCardsCollection.insertMany(dummyCards);
        console.log(`${LOG_PREFIX_TEACHERMON} 🃏 3 Basis-Lehrer-Karten generiert.`);
    }
}

async function seedTeachermonUniverses() {
    const count = await teachermonUniversesCollection.countDocuments();
    if (count === 0) {
        await teachermonUniversesCollection.insertOne({
            id: 'teachermon',
            name: '🏫 Teachermon Edition',
            stats: ['kalterKaffee', 'gequaelt', 'intelligenz'] // Die Standard-Werte
        });
        console.log(`${LOG_PREFIX_TEACHERMON} 🌌 Standard-Universum 'teachermon' generiert.`);
    }
}


// Hilfsfunktion: Karte ziehen basierend auf Wahrscheinlichkeit
async function drawRandomCard(requestedUniverse = 'teachermon') {
    // Nur einmal aus der DB laden und cachen
    if (!cachedTeachermonCards) {
        cachedTeachermonCards = await teachermonCardsCollection.find({}).toArray();
    }

    // WICHTIG: Filtere den Cache nach dem gewünschten Universum!
    const universeCards = cachedTeachermonCards.filter(c => (c.universe || 'teachermon') === requestedUniverse);
    
    // Fallback, falls das Universum leer ist (damit der Server nicht crasht)
    if (universeCards.length === 0) {
        console.warn(`Universum ${requestedUniverse} ist leer. Falle auf teachermon zurück.`);
        return drawRandomCard('teachermon');
    }

    const rand = Math.random();
    let cumulative = 0;
    let selectedRarity = "common";

    for (const [key, val] of Object.entries(TEACHERMON_RARITIES)) {
        cumulative += val.dropRate;
        if (rand <= cumulative) {
            selectedRarity = key;
            break;
        }
    }

    const cardsOfRarity = universeCards.filter(c => c.rarity === selectedRarity);

    if (cardsOfRarity.length === 0) {
        const fallbackCards = universeCards.filter(c => c.rarity === "common");
        return fallbackCards[Math.floor(Math.random() * fallbackCards.length)];
    }

    return cardsOfRarity[Math.floor(Math.random() * cardsOfRarity.length)];
}

// --- API ENDPUNKTE ---

// A. Sammelheft & Inventar abrufen
app.get('/api/teachermon/album', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    try {
        // Alle Karten aus der Datenbank holen
        const allCards = await teachermonCardsCollection.find({}).toArray();

        // Inventar des Users holen
        const userInventory = await teachermonInvCollection.find({ userId: userId }).toArray();
        const userInvMap = new Map(userInventory.map(item => [item.cardId, item]));

        // Sammelheft aufbauen (Zeigt an, welche man hat und wie viele Doppelte)
        const album = allCards.map(card => {
            const invItem = userInvMap.get(card.id);
            return {
                ...card,
                owned: !!invItem, // Hat er die Karte? (Sammelheft)
                duplicates: invItem ? (invItem.quantity - 1) : 0 // Alles über 1 ist doppelt
            };
        });

        res.json({ album });
    } catch (e) {
        res.status(500).json({ error: "Fehler beim Laden des Sammelhefts." });
    }
});

app.post('/api/teachermon/pack/buy', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    const { universe } = req.body; // <--- NEU
    const selectedUniverse = universe || 'teachermon'; // Fallback
    const PACK_PRICE = 250; 

    try {
        const user = await usersCollection.findOne({ _id: userId });
        if (user.balance < PACK_PRICE) {
            return res.status(400).json({ error: `Ein Pack kostet $${PACK_PRICE}. Du bist zu arm!` });
        }

        await usersCollection.updateOne({ _id: userId }, { $inc: { balance: -PACK_PRICE } });

        const pulledCards = [];
        for (let i = 0; i < 3; i++) {
            const card = await drawRandomCard(selectedUniverse); // <--- HIER übergeben
            pulledCards.push(card);

            await teachermonInvCollection.updateOne(
                { userId: userId, cardId: card.id },
                { $inc: { quantity: 1 } },
                { upsert: true }
            );
        }

        res.json({
            message: "Pack geöffnet!",
            cards: pulledCards,
            newBalance: user.balance - PACK_PRICE
        });

    } catch (e) {
        res.status(500).json({ error: "Fehler beim Öffnen des Packs." });
    }
});

// C. Daily Card Pack (Kostenlos)
app.post('/api/teachermon/pack/daily', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);

    try {
        const user = await usersCollection.findOne({ _id: userId });
        const now = new Date();
        const lastDaily = user.lastTeachermonDaily ? new Date(user.lastTeachermonDaily) : new Date(0);

        // Prüfen, ob heute schon geöffnet
        if (now.getDate() === lastDaily.getDate() && now.getMonth() === lastDaily.getMonth() && now.getFullYear() === lastDaily.getFullYear()) {
            return res.status(400).json({ error: "Du hast dein Daily Pack heute schon geöffnet. Komm morgen wieder!" });
        }

        // 1 kostenlose Karte ziehen
        const card = await drawRandomCard();

        await teachermonInvCollection.updateOne(
            { userId: userId, cardId: card.id },
            { $inc: { quantity: 1 } },
            { upsert: true }
        );

        // Daily-Zeitstempel aktualisieren
        await usersCollection.updateOne({ _id: userId }, { $set: { lastTeachermonDaily: now } });

        res.json({ message: "Daily Pack geöffnet!", card: card });

    } catch (e) {
        res.status(500).json({ error: "Fehler beim Daily Pack." });
    }
});

// D. Doppelte Karten verkaufen (ALLE einer bestimmten Karte)
app.post('/api/teachermon/sell', isAuthenticated, async (req, res) => {
    const { cardId } = req.body;
    const userId = new ObjectId(req.session.userId);

    try {
        const invItem = await teachermonInvCollection.findOne({ userId: userId, cardId: cardId });

        if (!invItem || invItem.quantity <= 1) {
            return res.status(400).json({ error: "Du hast keine doppelten Exemplare dieser Karte im Inventar." });
        }

        const card = await teachermonCardsCollection.findOne({ id: cardId });
        if (!card) return res.status(404).json({ error: "Karte existiert nicht mehr." });

        // Wir berechnen den Überschuss (alles über 1)
        const excess = invItem.quantity - 1;
        const totalSellPrice = TEACHERMON_RARITIES[card.rarity].sellPrice * excess;

        // Menge im Inventar auf 1 setzen (1 bleibt im Album) und Geld gutschreiben
        await teachermonInvCollection.updateOne({ _id: invItem._id }, { $set: { quantity: 1 } });
        await usersCollection.updateOne({ _id: userId }, { $inc: { balance: totalSellPrice } });

        res.json({
            message: `${excess}x Karte für insgesamt $${totalSellPrice} verkauft!`,
            earned: totalSellPrice
        });

    } catch (e) {
        res.status(500).json({ error: "Fehler beim Verkauf." });
    }
});

// D2. MASSEN-VERKAUF: Alle doppelten Karten im gesamten Inventar verkaufen
app.post('/api/teachermon/sell-all', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);

    try {
        // Hole alle Karten, die man mehr als 1x hat
        const dupes = await teachermonInvCollection.find({ userId: userId, quantity: { $gt: 1 } }).toArray();

        if (dupes.length === 0) {
            return res.status(400).json({ error: "Du hast aktuell keine doppelten Karten." });
        }

        // Wir brauchen die Kartendetails für den Preis
        const cardIds = dupes.map(d => d.cardId);
        const cards = await teachermonCardsCollection.find({ id: { $in: cardIds } }).toArray();
        const cardMap = new Map(cards.map(c => [c.id, c]));

        let totalEarned = 0;
        let totalSold = 0;
        const bulkOps = [];

        for (const item of dupes) {
            const card = cardMap.get(item.cardId);
            if (card) {
                const excess = item.quantity - 1;
                const pricePerCard = TEACHERMON_RARITIES[card.rarity].sellPrice;

                totalEarned += (excess * pricePerCard);
                totalSold += excess;

                // Bulk Operation vorbereiten (setze quantity auf 1)
                bulkOps.push({
                    updateOne: {
                        filter: { _id: item._id },
                        update: { $set: { quantity: 1 } }
                    }
                });
            }
        }

        // Führe alle Updates auf einmal aus (Performance!)
        if (bulkOps.length > 0) {
            await teachermonInvCollection.bulkWrite(bulkOps);
            await usersCollection.updateOne({ _id: userId }, { $inc: { balance: totalEarned } });
        }

        res.json({
            message: `Massenverkauf! ${totalSold} doppelte Karten für $${totalEarned} verkauft!`,
            earned: totalEarned
        });

    } catch (e) {
        console.error(e);
        res.status(500).json({ error: "Fehler beim Massenverkauf." });
    }
});

// E. Admin: Neue Karte hinzufügen
app.post('/api/teachermon/admin/cards', isAuthenticated, isAdmin, async (req, res) => {
    // NEU: cardType, universe und stats (als Objekt für dynamische Werte) hinzugefügt
    const { name, rarity, cardType, universe, stats, kalterKaffee, skills, gequaelt, intelligenz, img, effectText } = req.body;

    if (!name || !rarity || !img) return res.status(400).json({ error: "Name, Rarität und Bild/Emoji fehlen." });

    try {
        const currentCardType = cardType || 'teacher';
        const currentUniverse = universe || 'teachermon'; // Standard-Universum
        
        const prefix = currentCardType === 'teacher' ? 't_' : 'u_';
        const newId = prefix + Date.now().toString(36) + Math.random().toString(36).substr(2, 5);
        
        const newCard = {
            id: newId,
            name: name.trim(),
            rarity: rarity,
            cardType: currentCardType, // z.B. 'teacher', 'character', 'item'
            universe: currentUniverse, // z.B. 'teachermon', 'family_guy'
            effectText: effectText || null,
            skills: skills?.trim() || "-",
            img: img.trim()
        };

        // Werte zuweisen (Abwärtskompatibilität für Teachermon + flexibel für Neues)
        if (currentUniverse === 'teachermon') {
            newCard.kalterKaffee = parseInt(kalterKaffee) || 0;
            newCard.gequaelt = parseInt(gequaelt) || 0;
            newCard.intelligenz = parseInt(intelligenz) || 0;
        } else {
            // Speichert dynamische Werte z.B. { "humor": 90, "einfluss": 50 }
            newCard.customStats = typeof stats === 'object' ? stats : {};
        }

        await teachermonCardsCollection.insertOne(newCard);

        // CACHE LEEREN & ANDERE WORKER INFORMIEREN
        cachedTeachermonCards = null;
        if (global.redisPub) {
            global.redisPub.publish('sync-teachermon-cache', 'update');
        }

        res.status(201).json({ message: `Neue Karte in Universum '${currentUniverse}' erstellt!`, card: newCard });

    } catch (e) {
        res.status(500).json({ error: "Fehler beim Erstellen der Karte." });
    }
});

// F. Tauschbörse: Alle aktiven Angebote laden
app.get('/api/teachermon/trades', isAuthenticated, async (req, res) => {
    try {
        const trades = await teachermonTradesCollection.find({}).sort({ createdAt: -1 }).toArray();
        // Wir brauchen die Kartendetails, um sie schön anzuzeigen
        const cards = await teachermonCardsCollection.find({}).toArray();
        const cardMap = new Map(cards.map(c => [c.id, c]));

        const populatedTrades = trades.map(t => ({
            ...t,
            offerCard: cardMap.get(t.offerCardId),
            wantCard: cardMap.get(t.wantCardId)
        })).filter(t => t.offerCard && t.wantCard); // Nur gültige Trades

        res.json({ trades: populatedTrades });
    } catch (e) { res.status(500).json({ error: "Fehler beim Laden der Tauschbörse." }); }
});

// G. Tauschbörse: Angebot erstellen
app.post('/api/teachermon/trades/create', isAuthenticated, async (req, res) => {
    const { offerCardId, wantCardId } = req.body;
    const userId = new ObjectId(req.session.userId);

    if (offerCardId === wantCardId) return res.status(400).json({ error: "Macht keinen Sinn." });

    const session = client.startSession();
    try {
        await session.withTransaction(async () => {
            // Hat er die Karte überhaupt doppelt? (Eine muss im Album bleiben!)
            const invItem = await teachermonInvCollection.findOne({ userId, cardId: offerCardId }, { session });
            if (!invItem || invItem.quantity <= 1) {
                throw new Error("Du musst die Karte doppelt haben, um sie anzubieten.");
            }

            // Karte aus dem Inventar nehmen (Escrow)
            await teachermonInvCollection.updateOne(
                { _id: invItem._id },
                { $inc: { quantity: -1 } },
                { session }
            );

            // Trade eintragen
            await teachermonTradesCollection.insertOne({
                offererId: userId,
                offererUsername: req.session.username,
                offerCardId,
                wantCardId,
                createdAt: new Date()
            }, { session });
        });
        res.json({ message: "Angebot erstellt! Die Karte ist nun im Handels-Tresor." });
    } catch (e) {
        res.status(400).json({ error: e.message });
    } finally {
        await session.endSession();
    }
});

// H. Tauschbörse: Angebot annehmen
app.post('/api/teachermon/trades/accept/:tradeId', isAuthenticated, async (req, res) => {
    const tradeId = new ObjectId(req.params.tradeId);
    const acceptorId = new ObjectId(req.session.userId);

    const session = client.startSession();
    try {
        await session.withTransaction(async () => {
            const trade = await teachermonTradesCollection.findOne({ _id: tradeId }, { session });
            if (!trade) throw new Error("Angebot existiert nicht mehr.");
            if (trade.offererId.equals(acceptorId)) throw new Error("Du kannst dein eigenes Angebot nicht annehmen.");

            // Hat der Annehmende die gesuchte Karte doppelt?
            const acceptorItem = await teachermonInvCollection.findOne({ userId: acceptorId, cardId: trade.wantCardId }, { session });
            if (!acceptorItem || acceptorItem.quantity <= 1) {
                throw new Error("Du hast die verlangte Karte nicht doppelt!");
            }

            // 1. Dem Annehmenden die Want-Karte abziehen
            await teachermonInvCollection.updateOne({ _id: acceptorItem._id }, { $inc: { quantity: -1 } }, { session });
            // 2. Dem Annehmenden die Offer-Karte geben
            await teachermonInvCollection.updateOne({ userId: acceptorId, cardId: trade.offerCardId }, { $inc: { quantity: 1 } }, { upsert: true, session });
            // 3. Dem Ersteller die Want-Karte geben (Offer-Karte wurde ja bei Erstellung schon abgezogen)
            await teachermonInvCollection.updateOne({ userId: trade.offererId, cardId: trade.wantCardId }, { $inc: { quantity: 1 } }, { upsert: true, session });

            // 4. Trade löschen
            await teachermonTradesCollection.deleteOne({ _id: trade._id }, { session });
        });
        res.json({ message: "Tausch erfolgreich! Schau in dein Inventar." });
    } catch (e) {
        res.status(400).json({ error: e.message });
    } finally {
        await session.endSession();
    }
});

// I. Tauschbörse: Angebot abbrechen
app.delete('/api/teachermon/trades/:tradeId', isAuthenticated, async (req, res) => {
    const tradeId = new ObjectId(req.params.tradeId);
    const userId = new ObjectId(req.session.userId);

    const session = client.startSession();
    try {
        await session.withTransaction(async () => {
            const trade = await teachermonTradesCollection.findOne({ _id: tradeId }, { session });
            if (!trade) throw new Error("Angebot nicht gefunden.");

            // Nur der Ersteller (oder Admin) darf abbrechen
            const user = await usersCollection.findOne({ _id: userId }, { session });
            if (!trade.offererId.equals(userId) && !user.isAdmin) {
                throw new Error("Nicht deine Rechte.");
            }

            // Karte zurückgeben
            await teachermonInvCollection.updateOne({ userId: trade.offererId, cardId: trade.offerCardId }, { $inc: { quantity: 1 } }, { session });
            // Trade löschen
            await teachermonTradesCollection.deleteOne({ _id: trade._id }, { session });
        });
        res.json({ message: "Angebot abgebrochen. Karte ist wieder im Inventar." });
    } catch (e) {
        res.status(400).json({ error: e.message });
    } finally {
        await session.endSession();
    }
});

// C2. 10x Karten-Pack kaufen
app.post('/api/teachermon/pack/buy-multi', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    const { universe } = req.body; // Das Universum aus dem Frontend holen
    const selectedUniverse = universe || 'teachermon'; // Fallback
    
    const PACK_PRICE = 250;
    const MULTIPLIER = 10;
    const TOTAL_COST = PACK_PRICE * MULTIPLIER;

    try {
        const user = await usersCollection.findOne({ _id: userId });
        if (user.balance < TOTAL_COST) return res.status(400).json({ error: `10 Packs kosten $${TOTAL_COST}.` });

        // Geld abziehen
        await usersCollection.updateOne({ _id: userId }, { $inc: { balance: -TOTAL_COST } });

        const pulledCards = [];
        const bulkOps = [];

        // 30 Karten generieren (10 Packs a 3 Karten) aus dem gewählten Universum
        for (let i = 0; i < (3 * MULTIPLIER); i++) {
            const card = await drawRandomCard(selectedUniverse); // Hier wird das Universum übergeben
            pulledCards.push(card);
            
            bulkOps.push({
                updateOne: {
                    filter: { userId: userId, cardId: card.id },
                    update: { $inc: { quantity: 1 } },
                    upsert: true
                }
            });
        }

        // BulkWrite für Performance
        if (bulkOps.length > 0) await teachermonInvCollection.bulkWrite(bulkOps);

        res.json({ 
            message: `10 Packs geöffnet!`, 
            cards: pulledCards, 
            newBalance: user.balance - TOTAL_COST 
        });
    } catch (e) {
        console.error(`${LOG_PREFIX_TEACHERMON} Fehler beim Massen-Öffnen:`, e);
        res.status(500).json({ error: "Fehler beim Massen-Öffnen." });
    }
});

// J. Admin: Karte löschen & Spieler entschädigen
app.delete('/api/teachermon/admin/cards/:id', isAuthenticated, isAdmin, async (req, res) => {
    const cardIdStr = req.params.id;
    const session = client.startSession();

    try {
        await session.withTransaction(async () => {
            const card = await teachermonCardsCollection.findOne({ id: cardIdStr }, { session });
            if (!card) throw new Error("Karte nicht gefunden.");

            const sellPrice = TEACHERMON_RARITIES[card.rarity] ? TEACHERMON_RARITIES[card.rarity].sellPrice : 10;
            const owners = await teachermonInvCollection.find({ cardId: cardIdStr }, { session }).toArray();

            const bulkUserUpdates = [];
            for (const owner of owners) {
                const refundAmount = owner.quantity * sellPrice;
                bulkUserUpdates.push({
                    updateOne: {
                        filter: { _id: owner.userId },
                        update: { $inc: { balance: refundAmount } }
                    }
                });
            }
            if (bulkUserUpdates.length > 0) {
                await usersCollection.bulkWrite(bulkUserUpdates, { session });
            }

            await teachermonInvCollection.deleteMany({ cardId: cardIdStr }, { session });

            const affectedTrades = await teachermonTradesCollection.find({
                $or: [{ offerCardId: cardIdStr }, { wantCardId: cardIdStr }]
            }, { session }).toArray();

            for (const trade of affectedTrades) {
                if (trade.offerCardId === cardIdStr) {
                    await usersCollection.updateOne({ _id: trade.offererId }, { $inc: { balance: sellPrice } }, { session });
                } else {
                    await teachermonInvCollection.updateOne(
                        { userId: trade.offererId, cardId: trade.offerCardId },
                        { $inc: { quantity: 1 } },
                        { upsert: true, session }
                    );
                }
            }

            await teachermonTradesCollection.deleteMany({
                $or: [{ offerCardId: cardIdStr }, { wantCardId: cardIdStr }]
            }, { session });

            await teachermonCardsCollection.deleteOne({ id: cardIdStr }, { session });
        });

        // CACHE LEEREN & ANDERE WORKER INFORMIEREN
        cachedTeachermonCards = null;
        if (global.redisPub) {
            global.redisPub.publish('sync-teachermon-cache', 'update');
        }

        res.json({ message: "Karte vernichtet! Alle Besitzer wurden finanziell entschädigt." });
    } catch (e) {
        res.status(500).json({ error: e.message || "Fehler beim Löschen." });
    } finally {
        await session.endSession();
    }
});

// ==========================================
// === ⚔️ TEACHERMON ARENA (PVP QUARTETT) ===
// ==========================================

// J. Arena: Herausforderung erstellen (Blind Pick)
app.post('/api/teachermon/battles/create', isAuthenticated, async (req, res) => {
    const { cardId, stat } = req.body;
    const userId = new ObjectId(req.session.userId);

    const session = client.startSession();
    try {
        await session.withTransaction(async () => {
            // 1. Prüfen ob User die Karte hat
            const invItem = await teachermonInvCollection.findOne({ userId, cardId }, { session });
            if (!invItem || invItem.quantity < 1) {
                throw new Error("Du besitzt diese Karte nicht.");
            }

            // 2. Kartendetails holen für Typ & Universum
            const cardDetails = await teachermonCardsCollection.findOne({ id: cardId }, { session });
            if (!cardDetails) throw new Error("Karte existiert nicht in der Datenbank.");
            
            const type = cardDetails.cardType || 'teacher';
            if (type === 'item' || type === 'event') {
                throw new Error("Sonderkarten können nicht in der Arena kämpfen!");
            }

            const universe = cardDetails.universe || 'teachermon';

            // 3. Stat validieren je nach Universum
            if (universe === 'teachermon') {
                const validStats = ['kalterKaffee', 'gequaelt', 'intelligenz'];
                if (!validStats.includes(stat)) throw new Error("Ungültiger Stat für Teachermon.");
            } else {
                // Prüfen ob der geforderte Stat in den customStats dieser Karte existiert
                if (!cardDetails.customStats || typeof cardDetails.customStats[stat] === 'undefined') {
                    throw new Error(`Ungültiger Stat '${stat}' für das Universum '${universe}'.`);
                }
            }

            // 4. Karte als Einsatz abziehen
            await teachermonInvCollection.updateOne(
                { _id: invItem._id },
                { $inc: { quantity: -1 } },
                { session }
            );
            await teachermonInvCollection.deleteMany({ userId, quantity: { $lte: 0 } }, { session });

            // 5. Kampf erstellen
            await teachermonBattlesCollection.insertOne({
                challengerId: userId,
                challengerUsername: req.session.username,
                challengerCardId: cardId,
                stat: stat,
                universe: universe, // WICHTIG: Kampf ist auf dieses Universum gelockt!
                status: 'open',
                createdAt: new Date()
            }, { session });
        });
        res.json({ message: `Herausforderung in Kategorie '${stat}' erstellt! Deine Karte ist nun im Ring.` });
    } catch (e) {
        res.status(400).json({ error: e.message });
    } finally {
        await session.endSession();
    }
});

// K. Arena: Offene Kämpfe laden
app.get('/api/teachermon/battles', isAuthenticated, async (req, res) => {
    try {
        const battles = await teachermonBattlesCollection.find({ status: 'open' }).sort({ createdAt: -1 }).toArray();
        res.json({ battles });
    } catch (e) { res.status(500).json({ error: "Fehler beim Laden der Arena." }); }
});

// L. Arena: Herausforderung annehmen & Auswerten!
app.post('/api/teachermon/battles/accept/:id', isAuthenticated, async (req, res) => {
    const battleId = new ObjectId(req.params.id);
    const { cardId } = req.body; 
    const userId = new ObjectId(req.session.userId);

    const session = client.startSession();
    try {
        let resultMessage = "";
        let isWinner = false;

        await session.withTransaction(async () => {
            const battle = await teachermonBattlesCollection.findOne({ _id: battleId, status: 'open' }, { session });
            if (!battle) throw new Error("Kampf existiert nicht mehr.");
            if (battle.challengerId.equals(userId)) throw new Error("Du kannst nicht gegen dich selbst kämpfen.");

            // 1. Hat der Herausgeforderte die Karte?
            const invItem = await teachermonInvCollection.findOne({ userId, cardId }, { session });
            if (!invItem || invItem.quantity < 1) throw new Error("Du besitzt diese Karte nicht.");

            // 2. Kartendetails des Annehmenden holen
            const acceptorCard = await teachermonCardsCollection.findOne({ id: cardId }, { session });
            if (!acceptorCard) throw new Error("Karte existiert nicht.");
            
            const acceptorUniverse = acceptorCard.universe || 'teachermon';
            
            // WICHTIG: Cross-Universe Kämpfe blockieren!
            if (acceptorUniverse !== battle.universe) {
                throw new Error(`Dieser Kampf ist nur für Karten aus dem Universum '${battle.universe}'. Deine Karte ist aus '${acceptorUniverse}'.`);
            }

            // 3. Karte abziehen
            await teachermonInvCollection.updateOne({ _id: invItem._id }, { $inc: { quantity: -1 } }, { session });
            await teachermonInvCollection.deleteMany({ userId, quantity: { $lte: 0 } }, { session });

            // 4. KARTEN VERGLEICHEN
            const challengerCard = await teachermonCardsCollection.findOne({ id: battle.challengerCardId }, { session });
            const statToCompare = battle.stat;

            let val1 = 0; // Herausforderer
            let val2 = 0; // Annehmender

            if (battle.universe === 'teachermon') {
                val1 = challengerCard[statToCompare] || 0;
                val2 = acceptorCard[statToCompare] || 0;
            } else {
                // Dynamische Werte lesen
                val1 = (challengerCard.customStats && challengerCard.customStats[statToCompare]) || 0;
                val2 = (acceptorCard.customStats && acceptorCard.customStats[statToCompare]) || 0;
            }

            let winnerId = null;

            // Logik: Höherer Wert gewinnt
            if (val2 > val1) {
                winnerId = userId;
                isWinner = true;
                resultMessage = `GEWONNEN! Dein ${acceptorCard.name} (${val2}) schlägt ${challengerCard.name} (${val1}). Du erhältst beide Karten!`;

                await teachermonInvCollection.updateOne({ userId, cardId: acceptorCard.id }, { $inc: { quantity: 1 } }, { upsert: true, session });
                await teachermonInvCollection.updateOne({ userId, cardId: challengerCard.id }, { $inc: { quantity: 1 } }, { upsert: true, session });
            } else if (val1 > val2) {
                winnerId = battle.challengerId;
                isWinner = false;
                resultMessage = `VERLOREN! Dein ${acceptorCard.name} (${val2}) unterliegt ${challengerCard.name} (${val1}). Deine Karte ist weg!`;

                await teachermonInvCollection.updateOne({ userId: battle.challengerId, cardId: challengerCard.id }, { $inc: { quantity: 1 } }, { upsert: true, session });
                await teachermonInvCollection.updateOne({ userId: battle.challengerId, cardId: acceptorCard.id }, { $inc: { quantity: 1 } }, { upsert: true, session });
            } else {
                isWinner = false;
                resultMessage = `UNENTSCHIEDEN! Beide haben ${val1}. Jeder behält seine Karte.`;
                
                await teachermonInvCollection.updateOne({ userId: battle.challengerId, cardId: challengerCard.id }, { $inc: { quantity: 1 } }, { upsert: true, session });
                await teachermonInvCollection.updateOne({ userId, cardId: acceptorCard.id }, { $inc: { quantity: 1 } }, { upsert: true, session });
            }

            // 5. Kampf schließen
            await teachermonBattlesCollection.updateOne(
                { _id: battleId },
                { $set: { status: 'resolved', acceptorId: userId, acceptorUsername: req.session.username, acceptorCardId: cardId, winnerId: winnerId, resolvedAt: new Date() } },
                { session }
            );
        });

        res.json({ message: resultMessage, won: isWinner });
    } catch (e) {
        res.status(400).json({ error: e.message });
    } finally {
        await session.endSession();
    }
});

// --- UNIVERSUM MANAGEMENT API ---

// Alle Universen abrufen (Öffentlich, da der Shop sie braucht)
app.get('/api/teachermon/universes', async (req, res) => {
    try {
        const universes = await teachermonUniversesCollection.find({}).toArray();
        res.json({ universes });
    } catch (e) {
        res.status(500).json({ error: "Fehler beim Laden der Universen." });
    }
});

// Admin: Neues Universum erstellen
app.post('/api/teachermon/admin/universes', isAuthenticated, isAdmin, async (req, res) => {
    const { id, name, statsString } = req.body;

    if (!id || !name || !statsString) return res.status(400).json({ error: "Alle Felder müssen ausgefüllt sein." });

    // Aus dem String "Humor, Geld, Macht" ein sauberes Array machen: ["humor", "geld", "macht"]
    const statsArray = statsString.split(',').map(s => s.trim().toLowerCase().replace(/[^a-z0-9]/g, '')).filter(s => s.length > 0);

    if (statsArray.length === 0) return res.status(400).json({ error: "Mindestens ein gültiger Stat wird benötigt." });

    try {
        const existing = await teachermonUniversesCollection.findOne({ id: id.trim() });
        if (existing) return res.status(400).json({ error: "Diese Universums-ID existiert bereits." });

        const newUniverse = {
            id: id.trim(),
            name: name.trim(),
            stats: statsArray
        };

        await teachermonUniversesCollection.insertOne(newUniverse);
        res.status(201).json({ message: `Universum '${name}' erfolgreich erstellt!`, universe: newUniverse });
    } catch (e) {
        res.status(500).json({ error: "Fehler beim Erstellen des Universums." });
    }
});

// Admin: Universum löschen
app.delete('/api/teachermon/admin/universes/:id', isAuthenticated, isAdmin, async (req, res) => {
    const uniId = req.params.id;
    if (uniId === 'teachermon') return res.status(400).json({ error: "Das Standard-Universum kann nicht gelöscht werden." });

    try {
        await teachermonUniversesCollection.deleteOne({ id: uniId });
        // Optional: Alle Karten dieses Universums löschen? (Hier lassen wir sie erstmal existieren, sie sind dann "Lost Media")
        res.json({ message: "Universum gelöscht." });
    } catch (e) {
        res.status(500).json({ error: "Fehler beim Löschen." });
    }
});

if (cluster.isPrimary) {
    setInterval(async () => {
        console.log(`${LOG_PREFIX_SERVER} 🏠 Miet-Einzug startet...`);
        try {
            const allOwned = await ownedPropertiesCollection.find({}).toArray();
            for (const house of allOwned) {
                if (!house.roommates || house.roommates.length === 0) continue;

                for (const roommateId of house.roommates) {
                    const rentAmount = house.rent || 500;
                    
                    // Versuche Miete abzubuchen
                    const res = await usersCollection.updateOne(
                        { _id: roommateId, balance: { $gte: rentAmount } },
                        { $inc: { balance: -rentAmount } }
                    );

                    if (res.modifiedCount > 0) {
                        // Wenn bezahlt, dem Besitzer geben
                        await usersCollection.updateOne({ _id: house.ownerId }, { $inc: { balance: rentAmount } });
                    } else {
                        // Wenn pleite: Automatischer Rauswurf aus der WG
                        await ownedPropertiesCollection.updateOne({ _id: house._id }, { $pull: { roommates: roommateId } });
                        console.log(`🏠 User ${roommateId} konnte Miete nicht zahlen und wurde vor die Tür gesetzt.`);
                    }
                }
            }
        } catch (e) { console.error("RentJob Error:", e); }
    }, 24 * 60 * 60 * 1000); 
}

// M. Arena: Kampf-Historie abrufen
app.get('/api/teachermon/battles/history', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    try {
        // Finde alle beendeten Kämpfe, an denen der User beteiligt war
        const history = await teachermonBattlesCollection.find({
            status: 'resolved',
            $or: [{ challengerId: userId }, { acceptorId: userId }]
        }).sort({ resolvedAt: -1 }).limit(30).toArray();

        res.json({ history });
    } catch (e) {
        console.error(`${LOG_PREFIX_TEACHERMON} Fehler bei History:`, e);
        res.status(500).json({ error: "Fehler beim Laden der Historie." });
    }
});

// --- IMMOBILIEN API ---

// 1. Markt laden
app.get('/api/realestate/market', async (req, res) => {
    try {
        const houses = await propertiesCollection.find({}).toArray();
        res.json({ houses });
    } catch (e) { res.status(500).json({ error: "Markt-Fehler." }); }
});

// 2. Haus kaufen
app.post('/api/realestate/buy', isAuthenticated, async (req, res) => {
    const { houseId } = req.body;
    const userId = new ObjectId(req.session.userId);

    const session = client.startSession();
    try {
        await session.withTransaction(async () => {
            const [user, config] = await Promise.all([
                usersCollection.findOne({ _id: userId }, { session }),
                propertiesCollection.findOne({ id: houseId }, { session })
            ]);

            if (!config) throw new Error("Dieses Objekt existiert nicht.");
            if (user.balance < config.price) throw new Error(`Du brauchst $${config.price.toLocaleString()}.`);

            // 1. Check: Besitzt der User bereits ein Haus?
            const alreadyOwner = await ownedPropertiesCollection.findOne({ ownerId: userId }, { session });
            if (alreadyOwner) throw new Error("Du besitzt bereits ein Haus. Verkaufe es erst, bevor du neu kaufst.");

            // --- NEU: AUTO-AUSZUG AUS ALTER WG ---
            // Wir löschen den User aus allen 'roommates' Listen anderer Häuser
            await ownedPropertiesCollection.updateMany(
                { roommates: userId },
                { $pull: { roommates: userId } },
                { session }
            );
            // --------------------------------------

            // 2. Geld abziehen
            await usersCollection.updateOne({ _id: userId }, { $inc: { balance: -config.price } }, { session });

            // 3. Neues Haus in DB anlegen
            await ownedPropertiesCollection.insertOne({
                houseId: config.id,
                name: config.name,
                ownerId: userId,
                ownerName: user.username,
                roommates: [],
                maxRoommates: config.maxRoommates,
                rent: config.rent,
                protection: config.protection,
                energyBonus: config.energyBonus || 1.0,
                price: config.price, // Wichtig für die 75% Rückerstattung beim Verkauf
                img: config.img,
                desc: config.desc,
                createdAt: new Date()
            }, { session });
        });

        res.json({ success: true, message: "Glückwunsch! Du bist jetzt Hausbesitzer und aus deiner alten WG ausgezogen." });
    } catch (e) {
        res.status(400).json({ error: e.message });
    } finally {
        await session.endSession();
    }
});

// 1. Einladung senden (Owner -> User)
app.post('/api/realestate/wg/invite', isAuthenticated, async (req, res) => {
    const { targetUsername } = req.body;
    const ownerId = new ObjectId(req.session.userId);

    try {
        // A. Check: Ist der Absender selbst irgendwo nur Untermieter?
        const amIRoommateElsewhere = await ownedPropertiesCollection.findOne({ roommates: ownerId });
        if (amIRoommateElsewhere) {
            return res.status(403).json({ 
                error: `Du wohnst aktuell zur Untermiete in der WG von ${amIRoommateElsewhere.ownerName}. Du kannst von hier aus keine Einladungen verwalten!` 
            });
        }

        // B. Check: Besitzt der Absender überhaupt ein Haus?
        const house = await ownedPropertiesCollection.findOne({ ownerId });
        if (!house) return res.status(403).json({ error: "Nur Hausbesitzer können Einladungen verschicken." });
        
        // C. Check: Ist das eigene Haus schon voll?
        if (house.roommates.length >= house.maxRoommates) {
            return res.status(400).json({ error: "Kein Platz mehr in deiner Bude!" });
        }

        const targetUser = await usersCollection.findOne({ username: targetUsername.toLowerCase() });
        if (!targetUser) return res.status(404).json({ error: "User nicht gefunden." });

        // D. Check: Selbst-Einladung
        if (targetUser._id.equals(ownerId)) {
            return res.status(400).json({ error: "Du kannst dich nicht selbst einladen." });
        }

        // E. Check: Ist bereits eine Einladung offen?
        const existing = await propertyInvitesCollection.findOne({ 
            houseId: house._id, 
            targetUserId: targetUser._id 
        });
        if (existing) return res.status(400).json({ error: "Einladung wurde bereits gesendet." });

        // F. Einladung in DB schreiben
        await propertyInvitesCollection.insertOne({
            houseId: house._id,
            houseName: house.name,
            ownerName: house.ownerName,
            targetUserId: targetUser._id,
            createdAt: new Date()
        });

        res.json({ message: `Einladung an ${targetUser.username} wurde verschickt!` });
    } catch (e) { 
        console.error("Invite Error:", e);
        res.status(500).json({ error: "Fehler beim Senden der Einladung." }); 
    }
});

// 2. Einladungen abrufen (User sieht seine Einladungen)
app.get('/api/realestate/wg/my-invites', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    try {
        const invites = await propertyInvitesCollection.find({ targetUserId: userId }).toArray();
        res.json({ invites });
    } catch (e) { res.status(500).json({ error: "Fehler." }); }
});

// 3. Einladung annehmen oder ablehnen
app.post('/api/realestate/wg/respond', isAuthenticated, async (req, res) => {
    const { inviteId, action } = req.body; // action: 'accept' oder 'decline'
    const userId = new ObjectId(req.session.userId);

    try {
        const invite = await propertyInvitesCollection.findOne({ _id: new ObjectId(inviteId), targetUserId: userId });
        if (!invite) return res.status(404).json({ error: "Einladung nicht gefunden." });

        if (action === 'accept') {
            const house = await ownedPropertiesCollection.findOne({ _id: invite.houseId });
            if (!house || house.roommates.length >= house.maxRoommates) {
                await propertyInvitesCollection.deleteOne({ _id: invite._id });
                return res.status(400).json({ error: "Haus ist bereits voll oder existiert nicht mehr." });
            }

            // In die WG einziehen
            await ownedPropertiesCollection.updateOne(
                { _id: house._id },
                { $push: { roommates: userId } }
            );
        }

        // Einladung löschen (egal ob angenommen oder abgelehnt)
        await propertyInvitesCollection.deleteOne({ _id: invite._id });
        res.json({ message: action === 'accept' ? "Eingezogen!" : "Abgelehnt." });

    } catch (e) { res.status(500).json({ error: "Fehler." }); }
});

// 4. Rauswurf (Kick) oder Auszug (Leave)
app.post('/api/realestate/wg/remove', isAuthenticated, async (req, res) => {
    const { targetUserId } = req.body; // Wenn leer -> Selbst-Auszug
    const userId = new ObjectId(req.session.userId);

    try {
        const home = await ownedPropertiesCollection.findOne({ 
            $or: [{ ownerId: userId }, { roommates: userId }] 
        });

        if (!home) return res.status(404).json({ error: "Kein Wohnsitz gefunden." });

        let userToRemove;
        if (targetUserId) {
            // KICK-LOGIK (Nur Owner)
            if (!home.ownerId.equals(userId)) return res.status(403).json({ error: "Nur der Besitzer kann Leute rauswerfen." });
            userToRemove = new ObjectId(targetUserId);
        } else {
            // AUSZUG-LOGIK (Selbst)
            userToRemove = userId;
            if (home.ownerId.equals(userId)) return res.status(400).json({ error: "Als Besitzer kannst du nicht ausziehen. Du musst das Haus verkaufen." });
        }

        await ownedPropertiesCollection.updateOne(
            { _id: home._id },
            { $pull: { roommates: userToRemove } }
        );

        res.json({ message: "WG-Status aktualisiert." });
    } catch (e) { res.status(500).json({ error: "Fehler." }); }
});

// 4. Status abrufen (Wer wohnt wo?)
app.get('/api/realestate/my-home', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    try {
        const home = await ownedPropertiesCollection.findOne({ 
            $or: [{ ownerId: userId }, { roommates: userId }] 
        });
        if (!home) return res.json({ hasHome: false });

        const roommates = await usersCollection.find(
            { _id: { $in: home.roommates } },
            { projection: { username: 1 } }
        ).toArray();

        res.json({ 
            hasHome: true, 
            isOwner: home.ownerId.equals(userId),
            details: home,
            roommates: roommates
        });
    } catch (e) { res.status(500).json({ error: "Fehler beim Laden." }); }
});

// POST: Haus verkaufen
app.post('/api/realestate/sell', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);

    const session = client.startSession();
    try {
        await session.withTransaction(async () => {
            // 1. Suche das Haus, das dem User gehört
            const home = await ownedPropertiesCollection.findOne({ ownerId: userId }, { session });
            if (!home) throw new Error("Du besitzt keine Immobilie, die du verkaufen könntest.");

            // 2. Verkaufspreis berechnen (75% Rückerstattung)
            const refund = Math.floor(home.price * 0.75);

            // 3. Haus löschen (Mitbewohner fliegen automatisch raus)
            await ownedPropertiesCollection.deleteOne({ _id: home._id }, { session });

            // 4. Geld gutschreiben
            await usersCollection.updateOne(
                { _id: userId },
                { $inc: { balance: refund } },
                { session }
            );

            // 5. Alle offenen Einladungen für dieses Haus löschen
            await propertyInvitesCollection.deleteMany({ houseId: home._id }, { session });
        });

        res.json({ success: true, message: "Haus erfolgreich verkauft! Du hast 75% des Preises zurückerhalten." });
    } catch (e) {
        res.status(400).json({ error: e.message });
    } finally {
        await session.endSession();
    }
});

// --- BOUNTY SYSTEM ENDPOINTS ---

// 1. Liste der letzten Angreifer holen (für das Profil)
app.get('/api/bounty/attackers', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    try {
        const user = await usersCollection.findOne({ _id: userId }, { projection: { lastAttackers: 1 } });
        res.json({ attackers: user.lastAttackers || [] });
    } catch (e) { res.status(500).json({ error: "Fehler." }); }
});

// 2. "Most Wanted" Liste abrufen
app.get('/api/bounty/most-wanted', async (req, res) => {
    try {
        // Lade alle Kopfgelder und "joine" den Usernamen dazu
        const bounties = await db.collection('bounties').aggregate([
            { $match: { pool: { $gt: 0 } } },
            { $lookup: { from: 'users', localField: 'targetUserId', foreignField: '_id', as: 'userDetails' } },
            { $unwind: '$userDetails' },
            { $project: { 
                username: '$userDetails.username', 
                pool: 1, 
                updatedAt: 1,
                targetUserId: 1
            }},
            { $sort: { pool: -1 } }
        ]).toArray();
        
        res.json({ bounties });
    } catch (e) { res.status(500).json({ error: "Fehler beim Laden der Most Wanted Liste." }); }
});

// 3. Kopfgeld auf jemanden aussetzen
app.post('/api/bounty/place', isAuthenticated, async (req, res) => {
    const { targetUserId, amount } = req.body;
    const userId = new ObjectId(req.session.userId);
    const bountyAmount = parseInt(amount);

    if (isNaN(bountyAmount) || bountyAmount < 1000) {
        return res.status(400).json({ error: "Mindest-Kopfgeld ist $1.000." });
    }

    try {
        const user = await usersCollection.findOne({ _id: userId });
        if (user.balance < bountyAmount) return res.status(400).json({ error: "Nicht genug Geld auf dem Konto." });

        const targetIdObj = new ObjectId(targetUserId);
        const targetUser = await usersCollection.findOne({ _id: targetIdObj });
        if (!targetUser) return res.status(404).json({ error: "Zielperson existiert nicht." });

        // 1. Geld abziehen
        await usersCollection.updateOne({ _id: userId }, { $inc: { balance: -bountyAmount } });

        // 2. Kopfgeld in der DB speichern/erhöhen
        await db.collection('bounties').updateOne(
            { targetUserId: targetIdObj },
            { 
                $inc: { pool: bountyAmount }, 
                $set: { updatedAt: new Date() } 
            },
            { upsert: true }
        );

        // --- LNN TRIGGER: AUTOMATISCHE NEWS BEI HOHEM KOPFGELD ---
        // Wenn das Gesamtkopfgeld jetzt über $10.000 ist, gibt es eine Meldung
        const currentBounty = await db.collection('bounties').findOne({ targetUserId: targetIdObj });
        
        if (currentBounty.pool >= 10000) {
            await newsCollection.insertOne({
                headline: "FAHNDUNG AUSGEGEBEN! 📢",
                content: `Ein anonymes Opfer hat ein massives Kopfgeld auf ${targetUser.username} ausgesetzt. Der Pool steht nun bei $${currentBounty.pool.toLocaleString()}! Jagd ihn!`,
                author: "LNN Fahndungs-Ticker",
                category: "Justiz",
                createdAt: new Date(),
                likes: 0
            });
            updateDataVersion('news'); // Smart Polling triggern
        }

        console.log(`${LOG_PREFIX_SERVER} 🔫 Kopfgeld gesetzt: ${user.username} -> ${targetUser.username} ($${bountyAmount})`);
        res.json({ message: `Kopfgeld erfolgreich auf ${targetUser.username} gesetzt!`, newBalance: user.balance - bountyAmount });

    } catch (e) {
        console.error(e);
        res.status(500).json({ error: "Serverfehler beim Kopfgeld." });
    }
});

// --- LOTTERY SYSTEM CONFIG ---
const TICKET_PRICE = 1000; // Ein Los kostet $1000

// A. Status abrufen (Pot, eigene Lose, Zeit)
app.get('/api/lottery/status', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    try {
        const lotto = await systemSettingsCollection.findOne({ id: 'lottery_state' }) || { pot: 0 };
        const userTickets = await db.collection('lotteryTickets').countDocuments({ userId });
        const totalTickets = await db.collection('lotteryTickets').countDocuments({});

        res.json({
            pot: lotto.pot || 0,
            myTickets: userTickets,
            totalTickets: totalTickets,
            ticketPrice: TICKET_PRICE,
            nextDraw: "Jeden Sonntag, 20:00 Uhr" // Rein informativ für das UI
        });
    } catch (e) { res.status(500).json({ error: "Lotto-Fehler." }); }
});

// B. Lose kaufen
app.post('/api/lottery/buy', isAuthenticated, async (req, res) => {
    const { count } = req.body;
    const userId = new ObjectId(req.session.userId);
    
    // Sicherheit: Minimum 1, Maximum 100 Lose pro Kauf (um Spam zu vermeiden)
    const amount = Math.min(Math.max(parseInt(count) || 1, 1), 100); 
    const totalCost = amount * TICKET_PRICE;

    try {
        const user = await usersCollection.findOne({ _id: userId });
        if (!user || user.balance < totalCost) {
            return res.status(400).json({ error: `Zu wenig Geld! ${amount} Lose kosten $${totalCost.toLocaleString()}.` });
        }

        // 1. Lose-Array korrekt erstellen (Jedes Los ist ein eigenes Objekt)
        const ticketsToInsert = [];
        for (let i = 0; i < amount; i++) {
            ticketsToInsert.push({ 
                userId: userId, 
                username: user.username, 
                createdAt: new Date() 
            });
        }

        // 2. Transaktion: Geld abziehen & Lose einfügen
        await usersCollection.updateOne({ _id: userId }, { $inc: { balance: -totalCost } });
        await db.collection('lotteryTickets').insertMany(ticketsToInsert);

        // 3. 50% des Ticketpreises wandern in den Jackpot
        const potContribution = totalCost * 0.5;
        await systemSettingsCollection.updateOne(
            { id: 'lottery_state' }, 
            { $inc: { pot: potContribution } },
            { upsert: true }
        );

        console.log(`${LOG_PREFIX_SERVER} 🎰 ${user.username} hat ${amount} Lose gekauft.`);

        res.json({ 
            success: true,
            message: `${amount} Lose erfolgreich gekauft!`, 
            newBalance: user.balance - totalCost 
        });
    } catch (e) { 
        console.error("Lotto Kauf Fehler:", e);
        res.status(500).json({ error: "Kauf fehlgeschlagen." }); 
    }
});

// C. Die Ziehung (Der Master-Job)
async function runWeeklyLottery() {
    console.log(`${LOG_PREFIX_SERVER} 🎰 DIE ZIEHUNG STARTET...`);
    try {
        const lotto = await systemSettingsCollection.findOne({ id: 'lottery_state' });
        const pot = lotto ? lotto.pot : 0;
        if (pot <= 0) return;

        // Alle Lose holen
        const allTickets = await db.collection('lotteryTickets').find({}).toArray();
        if (allTickets.length < 3) {
            console.log("Nicht genug Teilnehmer für eine Ziehung.");
            return;
        }

        // Gewinner ziehen (Zufällig mischen und 3 nehmen)
        const shuffled = allTickets.sort(() => 0.5 - Math.random());
        const winners = [];
        const seenUsers = new Set();

        for (let t of shuffled) {
            if (!seenUsers.has(t.userId.toString())) {
                winners.push(t);
                seenUsers.add(t.userId.toString());
            }
            if (winners.length === 3) break;
        }

        // Verteilung: 1. (60%), 2. (25%), 3. (15%)
        const shares = [0.60, 0.25, 0.15];
        let resultsText = "";

        for (let i = 0; i < winners.length; i++) {
            const prize = Math.floor(pot * shares[i]);
            await usersCollection.updateOne({ _id: winners[i].userId }, { $inc: { balance: prize } });
            resultsText += `${i + 1}. Platz: ${winners[i].username} ($${prize.toLocaleString()}) `;
            
            // Log in news
            await newsCollection.insertOne({
                headline: `LOTTO-GEWINNER: ${winners[i].username}! 🏆`,
                content: `${winners[i].username} belegt den ${i + 1}. Platz in der Wochenziehung und räumt $${prize.toLocaleString()} ab!`,
                author: "Limo Lottery",
                category: "Wirtschaft",
                createdAt: new Date(),
                likes: 0
            });
        }

        // Reset: Pot auf 0, Lose löschen
        await systemSettingsCollection.updateOne({ id: 'lottery_state' }, { $set: { pot: 0, lastWinners: resultsText } });
        await db.collection('lotteryTickets').deleteMany({});

        console.log(`${LOG_PREFIX_SERVER} Ziehung beendet: ${resultsText}`);
        updateDataVersion('news');
    } catch (e) { console.error("Lotto-Ziehungsfehler:", e); }
}

// Intervall: Einmal pro Woche (z.B. Sonntag 20 Uhr) 
if (cluster.isPrimary) {
    // Check alle 1 Stunde ob Sonntag 20 Uhr ist
    setInterval(() => {
        const now = new Date();
        if (now.getDay() === 0 && now.getHours() === 20 && now.getMinutes() === 0) {
            runWeeklyLottery();
        }
    }, 60000); 
}

// GET: Gibt alle verfügbaren Rollen für das Frontend zurück
app.get('/api/admin/roles', isAuthenticated, isAdmin, (req, res) => {
    // Wandelt das Objekt in ein Array um für Vue/React/HTML
    const rolesArray = Object.entries(PREDEFINED_ROLES).map(([id, data]) => ({
        id: id,
        name: data.name,
        description: data.desc,
        permissions: data.permissions
    }));
    
    // Die 'custom' Rolle fügen wir manuell für das Frontend hinzu
    rolesArray.push({
        id: 'custom',
        name: 'Benutzerdefiniert (Custom)',
        description: 'Rechte einzeln zuweisen',
        permissions: []
    });

    res.json({ roles: rolesArray });
});

// GET: Gibt alle verfügbaren Rechte und ihre Beschreibungen für das Frontend zurück
app.get('/api/admin/permissions', isAuthenticated, isAdmin, (req, res) => {
    // Wandelt das Objekt in ein Array um, ideal für v-for oder map() im Frontend
    const permissionsArray = Object.entries(AVAILABLE_PERMISSIONS).map(([id, data]) => ({
        id: id,
        name: data.name,
        description: data.desc
    }));
    res.json({ permissions: permissionsArray });
});

// =========================================================
// === 🛋️ LIMO THERAPIE (ANTI-CASINO-SUCHT) ===
// =========================================================

// Helfer: Die KI von Dr. Limo
async function triggerTherapyAi(userId, chatId, userMessage) {
    try {
        const GROQ_API_KEY = process.env.GROQ_API_KEY;
        if (!GROQ_API_KEY) {
            console.warn(`${LOG_PREFIX_SERVER} Groq API Key fehlt für die Therapie!`);
            return;
        }

        // User Daten laden (für personalisierte Therapie)
        const user = await usersCollection.findOne({ _id: new ObjectId(userId) });
        if (!user) return;

        // Chat-Historie holen (Letzte 8 Nachrichten für den Kontext)
        const recentMessages = await limMessagesCollection.find({ chatId: new ObjectId(chatId) })
            .sort({ timestamp: 1 })
            .toArray();
        const lastMessages = recentMessages.slice(-8);

        // Der verbesserte Prompt für Dr. Limo
        // Der emphatischere Prompt für Dr. Limo
        const systemPrompt = `Du bist Dr. Limo, der offizielle, virtuelle Therapeut von "Limazon". 
Dein Patient heißt ${user.username}. Aktueller Kontostand: $${user.balance.toFixed(2)}. Im Casino verspielt: $${user.casinoStats ? user.casinoStats.totalWagered : 0}.

REGELN:
1. GRUND-PERSONA: Du behandelst den Patienten wegen Spielsucht. Du bist professionell, sarkastisch und kaufst ihm seine Ausreden nicht ab.
2. NOTFALL-MODUS (EXTREM WICHTIG): Wenn der Patient von Schicksalsschlägen, Tod (wie z.B. toten Haustieren) oder tiefer Trauer berichtet, ENDET DEIN SARKASMUS SOFORT. Werde extrem lieb, tröstend und einfühlsam. Erwähne in dieser Nachricht das Casino oder Geld mit KEINEM WORT! Sei einfach ein guter Zuhörer.
3. FORMAT: Antworte sehr kurz (Max. 2-3 Sätze).
4. Du bist kein Standard-KI-Assistent, bleib in deiner Rolle als Psychologe.`;

        const apiMessages = [{ role: "system", content: systemPrompt }];

        // Historie anhängen
        for (const msg of lastMessages) {
            apiMessages.push({
                role: msg.isAi ? "assistant" : "user",
                content: msg.content
            });
        }

        // Neue Nachricht anhängen
        if (userMessage) {
            apiMessages.push({ role: "user", content: userMessage });
        }

        const payload = {
            model: "llama-3.1-8b-instant",
            messages: apiMessages,
            temperature: 0.7, 
            max_tokens: 300,        // ERHÖHT: Verhindert abgeschnittene Sätze
            presence_penalty: 0.6,  // NEU: Bestraft das Wiederholen von Themen
            frequency_penalty: 0.6  // NEU: Zwingt die KI zu abwechslungsreicherem Vokabular
        };

        const aiRes = await axios.post('https://api.groq.com/openai/v1/chat/completions', payload, {
            headers: {
                'Authorization': `Bearer ${GROQ_API_KEY}`,
                'Content-Type': 'application/json'
            }
        });

        const aiText = aiRes.data.choices[0].message.content;

        if (aiText) {
            // Antwort speichern
            await limMessagesCollection.insertOne({
                chatId: new ObjectId(chatId),
                senderId: new ObjectId("000000000000000000000000"), // Fake ID für Dr. Limo
                senderUsername: "Dr. Limo",
                content: aiText.trim(),
                timestamp: new Date(),
                isAi: true
            });

            // Chat updaten
            await limChatsCollection.updateOne({ _id: new ObjectId(chatId) }, {
                $set: {
                    lastMessagePreview: aiText.trim().substring(0, 30),
                    updatedAt: new Date(),
                    lastMessageTimestamp: new Date()
                }
            });

            if (typeof updateDataVersion === 'function') updateDataVersion('chat'); // Polling triggern
        }
    } catch (err) {
        console.error(`${LOG_PREFIX_SERVER} Therapie KI Fehler:`, err.message);
    }
}

// 1. Therapie-Chat abrufen oder neu erstellen
app.get('/api/therapy/chat', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);

    try {
        let chat = await limChatsCollection.findOne({ type: 'therapy', participants: userId });

        if (!chat) {
            // Erstgespräch anlegen
            const newChat = {
                type: 'therapy',
                participants: [userId],
                partnerName: "Dr. Limo",
                createdAt: new Date(),
                updatedAt: new Date(),
                lastMessagePreview: "Nimm bitte auf der Couch Platz.",
                lastMessageTimestamp: new Date()
            };
            const insertRes = await limChatsCollection.insertOne(newChat);
            chat = { _id: insertRes.insertedId, ...newChat };

            // Begrüßungsnachricht
            await limMessagesCollection.insertOne({
                chatId: chat._id,
                senderId: new ObjectId("000000000000000000000000"),
                senderUsername: "Dr. Limo",
                content: `Hallo ${req.session.username}. Ich bin Dr. Limo. Ich sehe, du hast den Weg in meine Praxis gefunden. Leg dich auf die virtuelle Couch. Was bedrückt dich? Ist es wieder das Casino?`,
                timestamp: new Date(),
                isAi: true
            });
        }

        // Nachrichten laden
        const messages = await limMessagesCollection.find({ chatId: chat._id })
            .sort({ timestamp: 1 })
            .toArray();

        res.json({ chat, messages });
    } catch (e) {
        res.status(500).json({ error: "Fehler beim Betreten der Praxis." });
    }
});

// 2. Nachricht an Dr. Limo senden
app.post('/api/therapy/chat/message', isAuthenticated, async (req, res) => {
    const { content } = req.body;
    const userId = new ObjectId(req.session.userId);

    if (!content || content.trim() === "") return res.status(400).json({ error: "Du musst schon sprechen." });

    try {
        const chat = await limChatsCollection.findOne({ type: 'therapy', participants: userId });
        if (!chat) return res.status(404).json({ error: "Keine Akte gefunden." });

        // User-Nachricht speichern
        const userMsg = {
            chatId: chat._id,
            senderId: userId,
            senderUsername: req.session.username,
            content: content.trim(),
            timestamp: new Date()
        };
        await limMessagesCollection.insertOne(userMsg);

        await limChatsCollection.updateOne({ _id: chat._id }, {
            $set: { lastMessagePreview: content.substring(0, 30), updatedAt: new Date() }
        });

        res.json({ message: "Gesendet", sentMessage: userMsg });

        // KI im Hintergrund antworten lassen
        triggerTherapyAi(userId, chat._id, content.trim());

    } catch (e) {
        res.status(500).json({ error: "Der Doktor ist gerade in einer anderen Sitzung." });
    }
});

// =========================================================
// === SYSTEM BERICHT (REPORT) ===
// =========================================================
app.get('/api/admin/system/report', isAuthenticated, isAdmin, async (req, res) => {
    try {
        console.log(`${LOG_PREFIX_SERVER} 📊 Admin ${req.session.username} generiert System-Bericht...`);

        // 1. Zählungen
        const userCount = await usersCollection.countDocuments();
        const gangCount = await db.collection('gangs').countDocuments();
        const courtCases = await db.collection('courtCases').countDocuments();
        const activeAuctions = await auctionsCollection.countDocuments({ status: 'active' });

        // 2. Wirtschaft (Aggregation)
        const economyStats = await usersCollection.aggregate([
            { 
                $group: { 
                    _id: null, 
                    totalMoney: { $sum: "$balance" }, 
                    totalTokens: { $sum: "$tokens" },
                    totalTaxes: { $sum: "$totalTaxesPaid" }
                } 
            }
        ]).toArray();

        // 3. Staatskasse
        const stateTreasury = await db.collection('systemSettings').findOne({ id: 'state_treasury' });
        const treasuryBal = stateTreasury ? stateTreasury.balance : 0;

        const totalMoneyCirculation = (economyStats[0]?.totalMoney || 0) + treasuryBal;

        // 4. Die Top 10 Reichsten (Ohne Admins und ohne Infinity-Geld)
        const topUsers = await usersCollection.find({ 
            isAdmin: { $ne: true }, 
            unlockedInfinityMoney: { $ne: true } 
        })
        .sort({ balance: -1 })
        .limit(10)
        .project({ username: 1, balance: 1, tokens: 1, _id: 0 })
        .toArray();

        // 5. Antwort senden
        res.json({
            timestamp: new Date().toISOString(),
            stats: {
                users: userCount,
                gangs: gangCount,
                courtCases: courtCases,
                activeAuctions: activeAuctions,
                economy: {
                    totalMoney: totalMoneyCirculation,
                    userMoney: economyStats[0]?.totalMoney || 0,
                    treasury: treasuryBal,
                    totalTokens: economyStats[0]?.totalTokens || 0,
                    totalTaxesPaid: economyStats[0]?.totalTaxes || 0
                },
                topUsers: topUsers
            }
        });
    } catch (err) {
        console.error(`${LOG_PREFIX_SERVER} Fehler beim Generieren des Berichts:`, err);
        res.status(500).json({ error: "Fehler beim Generieren des Berichts." });
    }
});

// =========================================================
// === 🪑 LIMEA MÖBELHAUS & HOUSING (VOGELPERSPEKTIVE) ===
// =========================================================

// Der Produkt-Katalog von Limea (Maße sind in Pixeln für den Editor)
const LIMEA_CATALOG = [
    // --- SCHLAFZIMMER ---
    { id: 'f_bed_single', name: 'Einzelbett "Snark"', price: 150, w: 60, h: 120, icon: '🛏️', bg: '#8B5A2B', layer: 'base' },
    { id: 'f_bed_double', name: 'Doppelbett "Romantik"', price: 300, w: 120, h: 120, icon: '🛌', bg: '#A0522D', layer: 'base' },
    { id: 'f_bed_boxspring', name: 'Boxspringbett Luxus', price: 800, w: 140, h: 140, icon: '🛌', bg: '#483D8B', layer: 'base' },
    { id: 'f_bed_bunk', name: 'Stockbett', price: 250, w: 60, h: 120, icon: '🛏️', bg: '#CD853F', layer: 'base' },
    { id: 'f_nightstand', name: 'Nachttisch Holz', price: 40, w: 40, h: 40, icon: '🗃️', bg: '#5C4033', layer: 'base' },
    { id: 'f_wardrobe', name: 'Kleiderschrank', price: 350, w: 120, h: 40, icon: '🚪', bg: '#8B4513', layer: 'base' },
    { id: 'f_wardrobe_walkin', name: 'Begehbarer Schrank', price: 1200, w: 160, h: 100, icon: '👗', bg: '#696969', layer: 'base' },
    { id: 'f_makeup_table', name: 'Schminktisch', price: 280, w: 80, h: 40, icon: '💄', bg: '#FFC0CB', layer: 'base' },
    { id: 'f_mirror_dresser', name: 'Spiegelkommode', price: 320, w: 100, h: 40, icon: '🪞', bg: '#F5F5DC', layer: 'base' },
    { id: 'f_shoe_rack', name: 'Schuhschrank', price: 120, w: 80, h: 40, icon: '👞', bg: '#A0522D', layer: 'base' },

    // --- WOHNZIMMER ---
    { id: 'f_sofa', name: 'Sofa "Klippan"', price: 200, w: 140, h: 60, icon: '🛋️', bg: '#4682B4', layer: 'base' },
    { id: 'f_sofa_leather', name: 'Designer-Leder Sofa', price: 1500, w: 160, h: 60, icon: '🛋️', bg: '#111111', layer: 'base' },
    { id: 'f_sofa_double', name: 'Doppeldecker Couch', price: 900, w: 140, h: 100, icon: '🛋️', bg: '#8B0000', layer: 'base' },
    { id: 'f_armchair', name: 'Sessel "Chill"', price: 150, w: 60, h: 60, icon: '🛋️', bg: '#FF8C00', layer: 'base' },
    { id: 'f_armchair_velvet', name: 'Samt-Sessel', price: 450, w: 60, h: 60, icon: '🪑', bg: '#800080', layer: 'base' },
    { id: 'f_rocking_chair', name: 'Schaukelstuhl', price: 180, w: 60, h: 80, icon: '🪑', bg: '#D2691E', layer: 'base' },
    { id: 'f_table_coffee', name: 'Couchtisch', price: 80, w: 80, h: 40, icon: '🪑', bg: '#DEB887', layer: 'base' },
    { id: 'f_table_marble', name: 'Marmor-Couchtisch', price: 600, w: 80, h: 60, icon: '🪨', bg: '#F8F8FF', layer: 'base' },
    { id: 'f_tv_stand', name: 'TV-Schrank', price: 150, w: 120, h: 40, icon: '🗄️', bg: '#2F4F4F', layer: 'base' },
    { id: 'f_sideboard_long', name: 'Lange Kommode', price: 300, w: 160, h: 40, icon: '🗄️', bg: '#8B4513', layer: 'base' },
    { id: 'f_sideboard_design', name: 'Designer-Sideboard', price: 850, w: 140, h: 40, icon: '✨', bg: '#000000', layer: 'base' },
    { id: 'f_bookshelf', name: 'Bücherregal', price: 180, w: 100, h: 40, icon: '📚', bg: '#A0522D', layer: 'base' },
    { id: 'f_bookwall', name: 'Bücherwand', price: 500, w: 160, h: 40, icon: '📖', bg: '#5C4033', layer: 'base' },
    { id: 'f_showcase', name: 'Vitrine (Glas/Gold)', price: 750, w: 80, h: 40, icon: '🏺', bg: '#FFD700', layer: 'base' },

    // --- KÜCHE & ESSZIMMER ---
    { id: 'f_kitchen', name: 'Küchenzeile', price: 600, w: 200, h: 60, icon: '🍳', bg: '#dddddd', layer: 'base' },
    { id: 'f_kitchen_island', name: 'Kücheninsel', price: 1100, w: 120, h: 80, icon: '🔪', bg: '#F5F5F5', layer: 'base' },
    { id: 'f_fridge', name: 'Kühlschrank', price: 400, w: 60, h: 60, icon: '🧊', bg: '#E0E0E0', layer: 'base' },
    { id: 'f_microwave', name: 'Mikrowelle', price: 150, w: 40, h: 20, icon: '🍱', bg: '#C0C0C0', layer: 'decor' },
    { id: 'f_pantry', name: 'Vorratsschrank', price: 250, w: 80, h: 40, icon: '🥫', bg: '#DEB887', layer: 'base' },
    { id: 'f_wine_cooler', name: 'Weinschrank', price: 800, w: 60, h: 40, icon: '🍷', bg: '#2F4F4F', layer: 'base' },
    { id: 'f_table', name: 'Esstisch "Holz"', price: 120, w: 100, h: 100, icon: '🍽️', bg: '#D2B48C', layer: 'base' },
    { id: 'f_table_large', name: 'Großer Esstisch', price: 550, w: 160, h: 80, icon: '🥘', bg: '#F0F8FF', layer: 'base' },
    { id: 'f_chair_wood', name: 'Holzstuhl', price: 40, w: 40, h: 40, icon: '🪑', bg: '#8B4513', layer: 'base' },
    { id: 'f_chair_premium', name: 'Hochwertiger Stuhl', price: 150, w: 40, h: 40, icon: '💺', bg: '#A9A9A9', layer: 'base' },
    { id: 'f_bar_stool', name: 'Barhocker', price: 70, w: 20, h: 20, icon: '🪑', bg: '#333333', layer: 'base' },

    // --- BADEZIMMER ---
    { id: 'f_toilet', name: 'Toilette', price: 90, w: 40, h: 60, icon: '🚽', bg: '#FFFFFF', layer: 'base' },
    { id: 'f_shower', name: 'Dusche', price: 300, w: 80, h: 80, icon: '🚿', bg: '#E0FFFF', layer: 'base' },
    { id: 'f_bathtub', name: 'Badewanne', price: 650, w: 140, h: 60, icon: '🛁', bg: '#FFFFFF', layer: 'base' },
    { id: 'f_sink', name: 'Waschbecken', price: 180, w: 60, h: 40, icon: '🚰', bg: '#F0F8FF', layer: 'base' },
    { id: 'f_bath_cabinet', name: 'Badezimmerschrank', price: 150, w: 60, h: 40, icon: '🧴', bg: '#FFF8DC', layer: 'base' },
    { id: 'f_towel_rack', name: 'Handtuchhalter', price: 40, w: 40, h: 20, icon: '🧻', bg: '#A9A9A9', layer: 'decor' },
    { id: 'f_laundry_basket', name: 'Wäschekorb', price: 30, w: 40, h: 40, icon: '🧺', bg: '#DEB887', layer: 'base' },

    // --- BÜRO & ARBEIT ---
    { id: 'f_desk', name: 'Schreibtisch "Work"', price: 90, w: 100, h: 60, icon: '💻', bg: '#555555', layer: 'base' },
    { id: 'f_desk_pc', name: 'Gamer-Computertisch', price: 450, w: 120, h: 60, icon: '🖥️', bg: '#111111', layer: 'base' },
    { id: 'f_chair_office', name: 'Bürostuhl', price: 80, w: 40, h: 40, icon: '💺', bg: '#333333', layer: 'base' },
    { id: 'f_filing_cabinet', name: 'Aktenschrank', price: 160, w: 60, h: 40, icon: '🗄️', bg: '#778899', layer: 'base' },

    // --- DEKO & EXTRAS ---
    { id: 'f_tv', name: 'Flachbild-TV', price: 400, w: 120, h: 20, icon: '📺', bg: '#111111', layer: 'decor' },
    { id: 'f_mirror', name: 'Spiegel', price: 80, w: 40, h: 20, icon: '🪞', bg: '#E0FFFF', layer: 'decor' },
    { id: 'f_clock', name: 'Wecker', price: 20, w: 20, h: 20, icon: '⏰', bg: '#FF4500', layer: 'decor' },
    { id: 'f_lamp_floor', name: 'Stehlampe', price: 70, w: 20, h: 20, icon: '💡', bg: '#FFD700', layer: 'decor' },
    { id: 'f_lamp_design', name: 'Designer-Stehlampe', price: 350, w: 20, h: 20, icon: '✨', bg: '#C0C0C0', layer: 'decor' },
    { id: 'f_chandelier', name: 'Kronleuchter', price: 1500, w: 40, h: 40, icon: '💎', bg: '#FFFFE0', layer: 'decor' },
    { id: 'f_partition', name: 'Trennwand', price: 100, w: 100, h: 20, icon: '🧱', bg: '#A9A9A9', layer: 'base' },
	{ id: 'f_phone', name: 'Bürotelefon', price: 20, w: 20, h: 20, icon: '☎️', bg: '#222222', layer: 'decor' },

    // --- PFLANZEN ---
    { id: 'f_plant', name: 'Zimmerpflanze', price: 30, w: 40, h: 40, icon: '🪴', bg: '#2E8B57', layer: 'decor' },
    { id: 'f_plant_cactus', name: 'Kaktus', price: 25, w: 20, h: 20, icon: '🌵', bg: '#3CB371', layer: 'decor' },
    { id: 'f_plant_tree', name: 'Indoor-Baum', price: 150, w: 60, h: 60, icon: '🌳', bg: '#228B22', layer: 'decor' },

    // --- HAUSTIERE (Deko) ---
    { id: 'f_pet_bed', name: 'Hundekörbchen', price: 60, w: 40, h: 40, icon: '🦴', bg: '#DEB887', layer: 'decor' },
    { id: 'f_pet_cat_tree', name: 'Kratzbaum', price: 120, w: 40, h: 60, icon: '🐾', bg: '#D2B48C', layer: 'base' },
    { id: 'f_aquarium', name: 'Aquarium', price: 400, w: 80, h: 40, icon: '🐟', bg: '#00BFFF', layer: 'base' },

    // --- TEPPICHE & BÖDEN (Layer: floor -> unter allem anderen!) ---
    { id: 'f_rug', name: 'Teppich Rot', price: 50, w: 160, h: 120, icon: '🧶', bg: '#CD5C5C', layer: 'floor' },
    { id: 'f_rug_blue', name: 'Teppich Blau', price: 50, w: 160, h: 120, icon: '🧶', bg: '#4682B4', layer: 'floor' },
    { id: 'f_rug_black', name: 'Teppich Schwarz (Modern)', price: 100, w: 160, h: 120, icon: '⬛', bg: '#222222', layer: 'floor' },
    { id: 'f_rug_white', name: 'Teppich Flauschig Weiß', price: 150, w: 160, h: 120, icon: '☁️', bg: '#F8F8FF', layer: 'floor' },
    { id: 'f_floor_wood', name: 'Bodenfliese: Parkett', price: 200, w: 200, h: 200, icon: '🪵', bg: '#8B5A2B', layer: 'floor' },
    { id: 'f_floor_stone', name: 'Bodenfliese: Marmor', price: 400, w: 200, h: 200, icon: '⬜', bg: '#E0E0E0', layer: 'floor' },

    // --- OUTDOOR (GARTEN) ---
    { id: 'f_garden_bench', name: 'Gartenbank', price: 120, w: 100, h: 40, icon: '🪑', bg: '#8B4513', layer: 'base' },
    { id: 'f_garden_table', name: 'Gartentisch', price: 150, w: 100, h: 100, icon: '⛱️', bg: '#556B2F', layer: 'base' },
    { id: 'f_deck_chair', name: 'Liegestuhl', price: 90, w: 60, h: 120, icon: '😎', bg: '#FFD700', layer: 'base' },
    { id: 'f_hanging_chair', name: 'Hängesessel', price: 220, w: 60, h: 60, icon: '🪢', bg: '#F5DEB3', layer: 'base' }
];

// Editor-Daten abrufen (Lädt Haus, Möbel, Katalog und Geld auf einmal)
app.get('/api/limea/editor-data', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    try {
        const user = await usersCollection.findOne({ _id: userId });
        const home = await ownedPropertiesCollection.findOne({ 
            $or: [{ ownerId: userId }, { roommates: userId }] 
        });
        
        if (!home) return res.status(404).json({ error: "Du besitzt kein Haus und wohnst in keiner WG." });

        const furnitureIds = LIMEA_CATALOG.map(i => i.id);
        const inventory = await inventoriesCollection.find({ 
            userId: userId, 
            productId: { $in: furnitureIds },
            quantityOwned: { $gt: 0 }
        }).toArray();

        res.json({
            balance: user.balance,
            isAdmin: user.isAdmin || false, // <--- DAS IST NEU! GIBT DEM FRONTEND DIE RECHTE BEKANNT
            home: {
                id: home.houseId, 
                name: home.name,
                layout: home.furnitureLayout || [],
                isOwner: home.ownerId.equals(userId)
            },
            inventory: inventory,
            catalog: LIMEA_CATALOG
        });
    } catch(e) {
        res.status(500).json({ error: "Fehler beim Laden des Limea-Editors." });
    }
});

// Möbel im Shop kaufen
app.post('/api/limea/buy', isAuthenticated, async (req, res) => {
    const { itemId, quantity } = req.body;
    const userId = new ObjectId(req.session.userId);
    const qty = parseInt(quantity) || 1;

    const item = LIMEA_CATALOG.find(i => i.id === itemId);
    if (!item) return res.status(404).json({ error: "Dieses Möbelstück gibt es bei Limea nicht." });

    const totalCost = item.price * qty;
    const session = client.startSession();
    
    try {
        await session.withTransaction(async () => {
            const user = await usersCollection.findOne({ _id: userId }, { session });
            if (user.balance < totalCost) throw new Error("Zu wenig Geld für diesen Einkauf.");

            await usersCollection.updateOne({ _id: userId }, { $inc: { balance: -totalCost } }, { session });

            await inventoriesCollection.updateOne(
                { userId: userId, productId: itemId },
                { $inc: { quantityOwned: qty } },
                { upsert: true, session }
            );
        });
        
        // NEU: Aktuellen Kontostand aus der Datenbank laden
        const updatedUser = await usersCollection.findOne({ _id: userId });
        
        res.json({ 
            message: `Erfolgreich gekauft: ${qty}x ${item.name}!`,
            newBalance: updatedUser.balance // <--- Das schicken wir jetzt ans Frontend
        });
    } catch (e) {
        res.status(400).json({ error: e.message });
    } finally {
        await session.endSession();
    }
});

// Haus-Layout (Möbelpositionen) speichern
app.post('/api/realestate/my-home/layout', isAuthenticated, async (req, res) => {
    const { layout } = req.body; 
    // Erwartetes Format: [{ id: 'f_bed', uid: 'uuid-string', x: 100, y: 200, r: 90 }]
    const userId = new ObjectId(req.session.userId);

    try {
        const home = await ownedPropertiesCollection.findOne({ ownerId: userId });
        if (!home) return res.status(403).json({ error: "Nur der Hausbesitzer darf Möbel verrücken und speichern." });

        await ownedPropertiesCollection.updateOne(
            { _id: home._id },
            { $set: { furnitureLayout: layout } }
        );

        res.json({ message: "Inneneinrichtung erfolgreich gespeichert!" });
    } catch (e) {
        res.status(500).json({ error: "Fehler beim Speichern der Einrichtung." });
    }
});

// NEU: Nachbarschaft (Alle gebauten Häuser sehen)
app.get('/api/realestate/neighborhood', isAuthenticated, async (req, res) => {
    try {
        const houses = await ownedPropertiesCollection.find({}).toArray();
        
        // Daten für das Frontend schön verpacken
        const neighborhood = houses.map(h => ({
            id: h._id,
            name: h.name,
            ownerName: h.ownerName,
            roommatesCount: (h.roommates || []).length,
            icon: h.img || '🏠'
        }));
        
        res.json({ neighborhood });
    } catch (e) {
        res.status(500).json({ error: "Fehler beim Laden der Nachbarschaft." });
    }
});

// Haus besuchen (Limea Layout abrufen, OHNE Inventar)
app.get('/api/limea/visit/:houseId', isAuthenticated, async (req, res) => {
    try {
        const houseId = new ObjectId(req.params.houseId);
        const house = await ownedPropertiesCollection.findOne({ _id: houseId });
        
        if (!house) return res.status(404).json({ error: "Dieses Haus existiert nicht mehr." });

        res.json({
            home: {
                id: house.houseId, // Wichtig für die Raumgröße (carton, villa etc.)
                name: house.name,
                ownerName: house.ownerName,
                layout: house.furnitureLayout || [],
                isOwner: false // Besucher dürfen nichts anfassen!
            },
            catalog: LIMEA_CATALOG // Brauchen wir, um die Möbel zu rendern
        });
    } catch(e) {
        res.status(500).json({ error: "Klingel kaputt. Fehler beim Laden." });
    }
});

// =========================================================
// === LIMEA LAYOUT STORE ===
// =========================================================

// Alle Layouts im Store abrufen
app.get('/api/limea/layouts', isAuthenticated, async (req, res) => {
    try {
        const layouts = await limeaLayoutsCollection.find({}).sort({ createdAt: -1 }).limit(50).toArray();
        res.json({ layouts });
    } catch (e) {
        res.status(500).json({ error: "Fehler beim Laden des Layout-Stores." });
    }
});

// Eigenes Layout veröffentlichen
app.post('/api/limea/layouts/publish', isAuthenticated, async (req, res) => {
    const { name, layout, houseId } = req.body;
    const userId = new ObjectId(req.session.userId);

    if (!name || name.length < 3) return res.status(400).json({ error: "Name zu kurz." });
    if (!layout || layout.length === 0) return res.status(400).json({ error: "Leere Layouts können nicht geteilt werden." });

    try {
        await limeaLayoutsCollection.insertOne({
            name: name,
            creatorId: userId,
            creatorName: req.session.username,
            houseId: houseId, // z.B. 'mansion', damit User wissen, für welches Haus es passt
            layout: layout,
            likes: 0,
            createdAt: new Date()
        });
        res.json({ message: "Dein Layout wurde im Store veröffentlicht!" });
    } catch (e) {
        res.status(500).json({ error: "Fehler beim Veröffentlichen." });
    }
});

// Admin: Layout aus dem Store löschen
app.delete('/api/limea/admin/layouts/:id', isAuthenticated, isAdmin, async (req, res) => {
    try {
        await limeaLayoutsCollection.deleteOne({ _id: new ObjectId(req.params.id) });
        res.json({ message: "Layout gelöscht." });
    } catch (e) {
        res.status(500).json({ error: "Fehler." });
    }
});

// =========================================================
// === 🐾 HAUSTIER SYSTEM (TAMAGOTCHI & FRIEDHOF) ===
// =========================================================

// Katalog: starvationTimeHours = Nach wie vielen Stunden OHNE Futter das Tier stirbt.
// Katalog: starvationTimeHours = Nach wie vielen Stunden OHNE Futter das Tier stirbt.
const PET_CATALOG = [
    { id: 'dog', name: 'Hund', icon: '🐶', enclosure: 'Hundehütte 🛖', price: 500, starvationTimeHours: 24 },
    { id: 'cat', name: 'Katze', icon: '🐱', enclosure: 'Kratzbaum 🗼', price: 500, starvationTimeHours: 24 },
    { id: 'hamster', name: 'Hamster', icon: '🐹', enclosure: 'Käfig 🗄️', price: 150, starvationTimeHours: 12 }, 
    { id: 'lizard', name: 'Echse', icon: '🦎', enclosure: 'Terrarium 📦', price: 300, starvationTimeHours: 48 }, 
    { id: 'parrot', name: 'Papagei', icon: '🦜', enclosure: 'Vogelkäfig 🪹', price: 600, starvationTimeHours: 24 },
    { id: 'tarantula', name: 'Tarantula', icon: '🕷️', enclosure: 'Spinnen-Terrarium 🕸️', price: 800, starvationTimeHours: 72 },
    { id: 'wolf', name: 'Wolf', icon: '🐺', enclosure: 'Waldgehege 🌲', price: 2000, starvationTimeHours: 36 },
    { id: 'fox', name: 'Fuchs', icon: '🦊', enclosure: 'Fuchsbau 🕳️', price: 1200, starvationTimeHours: 24 },
    { id: 'meerkat', name: 'Erdmännchen', icon: '🐿️', enclosure: 'Sandwüste 🏜️', price: 900, starvationTimeHours: 18 },
    { id: 'arcticfox', name: 'Polarfuchs', icon: '🦊', enclosure: 'Eisgehege 🧊', price: 1500, starvationTimeHours: 24 },
    { id: 'plant', name: 'Zimmerpflanze', icon: '🪴', enclosure: 'Blumentopf 🏺', price: 50, starvationTimeHours: 168 }, // 1 Woche
    { id: 'tree', name: 'Bonsai Baum', icon: '🌳', enclosure: 'Gartenbeet 🏡', price: 200, starvationTimeHours: 336 }, // 2 Wochen
    { id: 'elephant', name: 'Elefant', icon: '🐘', enclosure: 'Savanne 🌅', price: 15000, starvationTimeHours: 48 },
    { id: 'giraffe', name: 'Giraffe', icon: '🦒', enclosure: 'Savanne 🌅', price: 12000, starvationTimeHours: 48 },
    { id: 'lion', name: 'Löwe', icon: '🦁', enclosure: 'Löwenkäfig 🥩', price: 10000, starvationTimeHours: 24 },
    { id: 'fish', name: 'Fisch', icon: '🐟', enclosure: 'Aquarium 💧', price: 100, starvationTimeHours: 24 },
    { id: 'shark', name: 'Hai', icon: '🦈', enclosure: 'Großaquarium 🌊', price: 25000, starvationTimeHours: 24 },
    { id: 'snake', name: 'Schlange', icon: '🐍', enclosure: 'Terrarium 📦', price: 800, starvationTimeHours: 96 },
    { id: 'penguin', name: 'Pinguin', icon: '🐧', enclosure: 'Eisgehege 🧊', price: 4000, starvationTimeHours: 24 },
    { id: 'dragon', name: 'Drache', icon: '🐉', enclosure: 'Vulkanhöhle 🌋', price: 100000, starvationTimeHours: 120 },
    { id: 'tiger', name: 'Tiger', icon: '🐯', enclosure: 'Dschungel 🌴', price: 12000, starvationTimeHours: 24 },
    { id: 'mouse', name: 'Maus', icon: '🐁', enclosure: 'Käfig 🗄️', price: 50, starvationTimeHours: 12 },
    { id: 'snail', name: 'Schnecke', icon: '🐌', enclosure: 'Glasbox 🧊', price: 20, starvationTimeHours: 168 },
    { id: 'bat', name: 'Fledermaus', icon: '🦇', enclosure: 'Dunkle Höhle 🦇', price: 700, starvationTimeHours: 24 },
    { id: 'duck', name: 'Ente', icon: '🦆', enclosure: 'Teich 🦆', price: 300, starvationTimeHours: 24 },
    { id: 'ladybug', name: 'Marienkäfer', icon: '🐞', enclosure: 'Graslandschaft 🌿', price: 10, starvationTimeHours: 48 },
    { id: 'turtle', name: 'Schildkröte', icon: '🐢', enclosure: 'Teich 🦆', price: 600, starvationTimeHours: 120 },
    { id: 'lynx', name: 'Luchs', icon: '🐈', enclosure: 'Waldgehege 🌲', price: 2500, starvationTimeHours: 24 },
    { id: 'pufferfish', name: 'Kugelfisch', icon: '🐡', enclosure: 'Aquarium 💧', price: 500, starvationTimeHours: 24 }
];

const FEED_COST = 15; // $15 pro Fütterung

app.get('/api/pets/shop', isAuthenticated, (req, res) => {
    res.json({ catalog: PET_CATALOG, feedCost: FEED_COST });
});

app.post('/api/pets/adopt', isAuthenticated, async (req, res) => {
    const { petId, customName } = req.body;
    const userId = new ObjectId(req.session.userId);

    if (!customName || customName.trim().length < 2 || customName.trim().length > 20) {
        return res.status(400).json({ error: "Bitte gib deinem Tier einen Namen (2-20 Zeichen)." });
    }

    const petType = PET_CATALOG.find(p => p.id === petId);
    if (!petType) return res.status(400).json({ error: "Dieses Tier gibt es nicht." });

    const session = client.startSession();
    try {
        await session.withTransaction(async () => {
            const user = await usersCollection.findOne({ _id: userId }, { session });
            if (user.balance < petType.price) throw new Error(`Du brauchst $${petType.price} für die Adoption.`);

            await usersCollection.updateOne({ _id: userId }, { $inc: { balance: -petType.price } }, { session });

            await petsCollection.insertOne({
                userId: userId,
                ownerName: user.username,
                typeId: petType.id,
                typeName: petType.name,
                icon: petType.icon,
                enclosure: petType.enclosure,
                name: customName.trim(),
                starvationTimeHours: petType.starvationTimeHours,
                lastFedAt: new Date(),
                adoptedAt: new Date()
            }, { session });
        });
        res.json({ message: `Herzlichen Glückwunsch! ${customName.trim()} gehört jetzt dir.` });
    } catch (e) {
        res.status(400).json({ error: e.message });
    } finally {
        await session.endSession();
    }
});

app.get('/api/pets/my', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    try {
        const pets = await petsCollection.find({ userId }).toArray();
        const now = new Date().getTime();

        const populatedPets = pets.map(pet => {
            const lastFed = new Date(pet.lastFedAt).getTime();
            const adoptedAt = new Date(pet.adoptedAt).getTime();
            
            const hoursPassed = (now - lastFed) / (1000 * 60 * 60);
            
            // Alter in Tagen (Tamagotchi Style)
            const ageInDays = Math.floor((now - adoptedAt) / (1000 * 60 * 60 * 24));
            
            // Hunger berechnen
            let hungerPercent = 100 - ((hoursPassed / pet.starvationTimeHours) * 100);
            if (hungerPercent < 0) hungerPercent = 0;

            return {
                id: pet._id,
                name: pet.name,
                icon: pet.icon,
                type: pet.typeName,
                enclosure: pet.enclosure,
                hunger: Math.round(hungerPercent),
                ageDays: ageInDays,
                isDying: hungerPercent <= 15
            };
        });

        res.json({ pets: populatedPets });
    } catch (e) {
        res.status(500).json({ error: "Fehler beim Laden deiner Tiere." });
    }
});

app.post('/api/pets/:id/feed', isAuthenticated, async (req, res) => {
    const petId = new ObjectId(req.params.id);
    const userId = new ObjectId(req.session.userId);

    const session = client.startSession();
    try {
        await session.withTransaction(async () => {
            const user = await usersCollection.findOne({ _id: userId }, { session });
            if (user.balance < FEED_COST) throw new Error(`Futter kostet $${FEED_COST}. Du hast nicht genug Geld!`);

            const pet = await petsCollection.findOne({ _id: petId, userId: userId }, { session });
            if (!pet) throw new Error("Tier nicht gefunden.");

            // Überfressen verhindern (Wenn Balken über 90% ist)
            const now = new Date().getTime();
            const lastFed = new Date(pet.lastFedAt).getTime();
            const hoursPassed = (now - lastFed) / (1000 * 60 * 60);
            let hungerPercent = 100 - ((hoursPassed / pet.starvationTimeHours) * 100);
            
            if (hungerPercent >= 90) {
                throw new Error(`${pet.name} ist noch pappsatt! Warte, bis es wieder Hunger hat.`);
            }

            // Geld abziehen & Magen füllen
            await usersCollection.updateOne({ _id: userId }, { $inc: { balance: -FEED_COST } }, { session });
            await petsCollection.updateOne({ _id: petId }, { $set: { lastFedAt: new Date() } }, { session });
        });
        res.json({ message: "Tier erfolgreich gefüttert! Es ist wieder glücklich und satt." });
    } catch (e) {
        res.status(400).json({ error: e.message });
    } finally {
        await session.endSession();
    }
});

app.get('/api/pets/cemetery', async (req, res) => {
    try {
        const deadPets = await petCemeteryCollection.find({}).sort({ deathDate: -1 }).limit(100).toArray();
        res.json({ cemetery: deadPets });
    } catch (e) {
        res.status(500).json({ error: "Fehler beim Betreten des Friedhofs." });
    }
});

// --- DER SENSENMANN (CRON JOB) ---
if (cluster.isPrimary) {
    setInterval(async () => {
        try {
            const now = new Date();
            const allPets = await petsCollection.find({}).toArray();
            
            const deaths = [];
            const idsToDelete = [];

            for (const pet of allPets) {
                const lastFed = new Date(pet.lastFedAt).getTime();
                const hoursPassed = (now.getTime() - lastFed) / (1000 * 60 * 60);

                // Hat das Tier sein Verhunger-Limit erreicht?
                if (hoursPassed >= pet.starvationTimeHours) {
                    const ageDays = Math.floor((now.getTime() - new Date(pet.adoptedAt).getTime()) / (1000 * 60 * 60 * 24));
                    deaths.push({
                        userId: pet.userId,
                        ownerName: pet.ownerName,
                        petName: pet.name,
                        icon: pet.icon,
                        type: pet.typeName,
                        ageDays: ageDays, // Auf dem Grabstein festhalten, wie alt es wurde
                        deathDate: now,
                        cause: 'Verhungert 🦴'
                    });
                    idsToDelete.push(pet._id);
                }
            }

            if (deaths.length > 0) {
                await petCemeteryCollection.insertMany(deaths);
                await petsCollection.deleteMany({ _id: { $in: idsToDelete } });
                console.log(`${LOG_PREFIX_SERVER} 🪦 Der Sensenmann war da. ${deaths.length} Tiere sind verhungert.`);
            }

        } catch (e) {
            console.error("Fehler im Sensenmann-Job:", e);
        }
    }, 5 * 60 * 1000); // Alle 5 Minuten prüfen
}

app.post('/api/admin/system/resurrect-pets', isAuthenticated, isAdmin, async (req, res) => {
    console.log(`${LOG_PREFIX_SERVER} 🧟‍♂️ Admin ${req.session.username} beschwört die toten Tiere...`);

    try {
        // Alle toten Tiere vom Friedhof holen
        const deadPets = await petCemeteryCollection.find({}).toArray();
        
        if (deadPets.length === 0) {
            return res.json({ message: "Der Friedhof ist leer. Niemand muss gerettet werden!" });
        }

        let resurrectedCount = 0;

        for (const deadPet of deadPets) {
            // Wir suchen das Tier im Katalog, um die verlorenen Daten (wie das Gehege) wiederherzustellen
            const petType = PET_CATALOG.find(p => p.icon === deadPet.icon || p.name === deadPet.type) || PET_CATALOG[0];

            // Tier wieder in die Lebenden-Datenbank einfügen
            await petsCollection.insertOne({
                userId: deadPet.userId,
                ownerName: deadPet.ownerName,
                typeId: petType.id,
                typeName: petType.name,
                icon: petType.icon,
                enclosure: petType.enclosure,
                name: deadPet.petName, // Im Friedhof hieß das Feld 'petName', hier 'name'
                starvationTimeHours: petType.starvationTimeHours,
                lastFedAt: new Date(), // Direkt füttern (Balken auf 100%)!
                // Wir berechnen das ursprüngliche Adoptionsdatum ungefähr zurück
                adoptedAt: new Date(Date.now() - (deadPet.ageDays || 0) * 24 * 60 * 60 * 1000) 
            });
            
            // Tier vom Friedhof löschen
            await petCemeteryCollection.deleteOne({ _id: deadPet._id });
            resurrectedCount++;
        }

        console.log(`${LOG_PREFIX_SERVER} 🧟‍♂️ ${resurrectedCount} Tiere erfolgreich wiederbelebt.`);
        res.json({ message: `Halleluja! ${resurrectedCount} Tiere wurden von den Toten auferweckt, frisch gefüttert und ihren Besitzern zurückgegeben.` });

    } catch (e) {
        console.error(`${LOG_PREFIX_SERVER} Nekromantie-Fehler:`, e);
        res.status(500).json({ error: "Das Ritual ist fehlgeschlagen: " + e.message });
    }
});

// =========================================================
// === 🏛️ LIMO DEMOKRATIE (BÜRGERMEISTER WAHL) ===
// =========================================================
const LOG_PREFIX_ELECTION = "[Election API]";

// 1. Admin: Wahl starten (Manuell, um den Hype zu kontrollieren)
app.post('/api/admin/mayor/start', isAuthenticated, isAdmin, async (req, res) => {
    try {
        const durationDays = req.body.days || 3; // Standard: 3 Tage Wahlkampf
        const endsAt = new Date(Date.now() + durationDays * 24 * 60 * 60 * 1000);

        // Wir nutzen die systemSettings Collection für den Wahl-Status
        await systemSettingsCollection.updateOne(
            { id: 'mayor_election' },
            {
                $set: {
                    isActive: true,
                    endsAt: endsAt,
                    votes: [] // Array für { voterId: "...", candidateId: "..." }
                }
            },
            { upsert: true }
        );

        // Breaking News generieren (ohne KI, direkt als System)
        await newsCollection.insertOne({
            headline: "NEUWAHLEN AUSGERUFEN! 🗳️",
            content: `Das Admin-Regime hat offiziell Neuwahlen für das Amt des Bürgermeisters angeordnet. Die Wahlurnen sind für ${durationDays} Tage geöffnet. Geht wählen!`,
            author: "LNN Politik",
            category: "Community",
            createdAt: new Date(),
            likes: 0
        });
        updateDataVersion('news');

        console.log(`${LOG_PREFIX_ELECTION} Admin ${req.session.username} hat eine Wahl gestartet.`);
        res.json({ message: "Wahlurne erfolgreich aufgestellt!" });
    } catch (e) {
        res.status(500).json({ error: "Fehler beim Starten der Wahl." });
    }
});

// 2. User: Wahlstatus & Kandidaten abrufen
app.get('/api/mayor/election', isAuthenticated, async (req, res) => {
    const userId = req.session.userId;
    
    try {
        const election = await systemSettingsCollection.findOne({ id: 'mayor_election' });
        
        // Aktive User suchen (Letzter Login < 7 Tage her) UND KEINE ADMINS!
        const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
        const activeUsers = await usersCollection.find(
            {
                lastLogin: { $gte: sevenDaysAgo },
                isAdmin: { $ne: true } // Der Admin darf nicht gewählt werden!
            },
            { projection: { username: 1, isMayor: 1 } }
        ).toArray();

        // Wenn keine Wahl aktiv ist, nur den aktuellen Bürgermeister zeigen
        if (!election || !election.isActive) {
            const currentMayor = await usersCollection.findOne({ isMayor: true }, { projection: { username: 1 } });
            return res.json({ 
                isActive: false, 
                currentMayor: currentMayor ? currentMayor.username : "Niemand",
                message: "Es findet aktuell keine Wahl statt."
            });
        }

        // Eigene Stimme suchen
        const myVoteObj = election.votes.find(v => v.voterId === userId);
        const hasVoted = !!myVoteObj;

        // Stimmenauszählung (Leaderboard für Drama)
        const voteCounts = {};
        election.votes.forEach(v => {
            voteCounts[v.candidateId] = (voteCounts[v.candidateId] || 0) + 1;
        });

        // Kandidaten für das Frontend aufbereiten
        const candidates = activeUsers.map(user => ({
            id: user._id.toString(),
            username: user.username,
            isCurrentMayor: !!user.isMayor,
            votes: voteCounts[user._id.toString()] || 0
        })).sort((a, b) => b.votes - a.votes); // Nach Stimmen sortieren

        res.json({
            isActive: true,
            endsAt: election.endsAt,
            hasVoted: hasVoted,
            myVoteId: hasVoted ? myVoteObj.candidateId : null,
            candidates: candidates,
            totalVotes: election.votes.length
        });

    } catch (e) {
        console.error(e);
        res.status(500).json({ error: "Fehler beim Laden der Wahlurne." });
    }
});

// 3. User: Abstimmen
app.post('/api/mayor/vote', isAuthenticated, async (req, res) => {
    const { candidateId } = req.body;
    const voterId = req.session.userId;

    if (!candidateId) return res.status(400).json({ error: "Du musst schon jemanden auswählen." });
    if (candidateId === voterId) return res.status(400).json({ error: "Eitelkeit wird hier nicht belohnt. Du darfst dich nicht selbst wählen!" });

    try {
        const election = await systemSettingsCollection.findOne({ id: 'mayor_election' });
        
        if (!election || !election.isActive) {
            return res.status(400).json({ error: "Die Wahlurnen sind geschlossen." });
        }
        if (new Date() > new Date(election.endsAt)) {
            return res.status(400).json({ error: "Die Wahlzeit ist bereits abgelaufen. Wir warten auf den Admin zur Auszählung." });
        }

        // Prüfen, ob der Kandidat existiert und KEIN Admin ist
        const candidate = await usersCollection.findOne({ _id: new ObjectId(candidateId) });
        if (!candidate) return res.status(404).json({ error: "Kandidat nicht gefunden." });
        if (candidate.isAdmin) return res.status(403).json({ error: "Admins stehen über der Demokratie. Wähl wen anders." });

        // Hat der User schon gewählt?
        const alreadyVoted = election.votes.some(v => v.voterId === voterId);
        if (alreadyVoted) {
            return res.status(400).json({ error: "Du hast deine Stimme bereits abgegeben! Wahlbetrug ist strafbar." });
        }

        // Stimme speichern
        await systemSettingsCollection.updateOne(
            { id: 'mayor_election' },
            { 
                $push: { 
                    votes: { voterId: voterId, candidateId: candidateId, timestamp: new Date() } 
                } 
            }
        );

        console.log(`${LOG_PREFIX_ELECTION} User ${req.session.username} hat für ${candidate.username} gestimmt.`);
        res.json({ message: `Deine Stimme für ${candidate.username} wurde sicher in der Wahlurne verstaut.` });

    } catch (e) {
        console.error(e);
        res.status(500).json({ error: "Das Wahlgerät klemmt." });
    }
});

// 4. Admin: Wahl beenden und Bürgermeister krönen
app.post('/api/admin/mayor/end', isAuthenticated, isAdmin, async (req, res) => {
    try {
        const election = await systemSettingsCollection.findOne({ id: 'mayor_election' });
        if (!election || !election.isActive) return res.status(400).json({ error: "Es läuft keine Wahl." });

        // Stimmen auszählen
        const voteCounts = {};
        election.votes.forEach(v => {
            voteCounts[v.candidateId] = (voteCounts[v.candidateId] || 0) + 1;
        });

        // Gewinner ermitteln
        let winnerId = null;
        let maxVotes = -1;

        for (const [candId, count] of Object.entries(voteCounts)) {
            if (count > maxVotes) {
                maxVotes = count;
                winnerId = candId;
            }
        }

        if (!winnerId) {
            // Niemand hat abgestimmt
            await systemSettingsCollection.updateOne({ id: 'mayor_election' }, { $set: { isActive: false } });
            return res.json({ message: "Wahl beendet, aber niemand ist zur Wahlurne gegangen." });
        }

        // Den bisherigen Bürgermeister entmachten
        await usersCollection.updateMany(
            { isMayor: true },
            { $set: { isMayor: false } }
        );

        // Den neuen Bürgermeister krönen
        const winnerObjId = new ObjectId(winnerId);
        await usersCollection.updateOne(
            { _id: winnerObjId },
            { $set: { isMayor: true } }
        );

        const winnerDoc = await usersCollection.findOne({ _id: winnerObjId });

        // Wahl schließen
        await systemSettingsCollection.updateOne(
            { id: 'mayor_election' }, 
            { $set: { isActive: false, lastWinnerId: winnerId, lastWinnerName: winnerDoc.username } }
        );

        // LNN News feuern
        await newsCollection.insertOne({
            headline: `WIR HABEN EINEN NEUEN BÜRGERMEISTER! 👑`,
            content: `Mit ${maxVotes} Stimmen hat sich ${winnerDoc.username} durchgesetzt und ist nun der offizielle Bürgermeister von Limazon. Möge er weise herrschen!`,
            author: "LNN Politik",
            category: "Community",
            createdAt: new Date(),
            likes: 0
        });
        updateDataVersion('news');

        // Optional: Kleine Geldprämie für den neuen Bürgermeister aus der Staatskasse
        await usersCollection.updateOne({ _id: winnerObjId }, { $inc: { balance: 25000 } });

        console.log(`${LOG_PREFIX_ELECTION} Wahl beendet. Neuer Bürgermeister: ${winnerDoc.username}`);
        res.json({ message: `Wahl beendet! ${winnerDoc.username} ist der neue Bürgermeister.` });

    } catch (e) {
        res.status(500).json({ error: "Fehler bei der Auszählung." });
    }
});

// Middleware: Prüft, ob der User der amtierende Bürgermeister ist
async function isMayorMode(req, res, next) {
    try {
        const user = await usersCollection.findOne({ _id: new ObjectId(req.session.userId) });
        if (user && user.isMayor === true) {
            return next();
        }
        res.status(403).json({ error: "Zugriff verweigert! Nur der gewählte Bürgermeister hat hier Zutritt." });
    } catch (e) {
        res.status(500).json({ error: "Fehler bei der Authentifizierung." });
    }
}

const LOG_PREFIX_MAYOR = "[Rathaus API]";

// --- 1. STEUERN ANPASSEN ---
// Der Bürgermeister kann die Steuern zwischen 0.1% und 1.5% festlegen.
app.post('/api/mayor/taxes', isAuthenticated, isMayorMode, async (req, res) => {
    const { newRatePercent } = req.body; 
    // Erwartet einen Wert wie 0.5 (für 0.5%) oder 1.2 (für 1.2%)

    if (typeof newRatePercent !== 'number' || newRatePercent < 0.1 || newRatePercent > 1.5) {
        return res.status(400).json({ error: "Der Steuersatz muss zwischen 0.1% und 1.5% liegen." });
    }

    try {
        const decimalRate = newRatePercent / 100; // z.B. 0.005

        // In SystemSettings speichern
        await systemSettingsCollection.updateOne(
            { id: 'tax_config' },
            { $set: { rate: decimalRate, lastChangedBy: req.session.username, lastChangedAt: new Date() } },
            { upsert: true }
        );

        // LNN News (Buhmann-Effekt)
        let headline = newRatePercent > 0.5 ? "BÜRGERMEISTER ERHÖHT STEUERN! 🤬" : "STEUERSENKUNG! 🙏";
        await newsCollection.insertOne({
            headline: headline,
            content: `Bürgermeister ${req.session.username} hat den allgemeinen Steuersatz auf ${newRatePercent}% angepasst. Das Volk tobt (oder jubelt).`,
            author: "LNN Wirtschaft",
            category: "Wirtschaft",
            createdAt: new Date(),
            likes: 0
        });
        updateDataVersion('news');

        console.log(`${LOG_PREFIX_MAYOR} Steuern durch Bürgermeister auf ${newRatePercent}% gesetzt.`);
        res.json({ message: `Gesetz erlassen: Der Steuersatz beträgt nun ${newRatePercent}%.` });
    } catch (e) {
        res.status(500).json({ error: "Fehler im Finanzministerium." });
    }
});

// --- 2. KONJUNKTURPAKET (STIMULUS CHECK) ---
// Nimmt Geld aus der Staatskasse und gibt jedem aktiven Spieler einen festen Betrag (1x pro Woche)
app.post('/api/mayor/stimulus', isAuthenticated, isMayorMode, async (req, res) => {
    try {
        const settings = await systemSettingsCollection.findOne({ id: 'mayor_stimulus' });
        const now = Date.now();

        // Cooldown: Nur alle 7 Tage
        if (settings && settings.lastUsedAt && (now - new Date(settings.lastUsedAt).getTime() < 7 * 24 * 60 * 60 * 1000)) {
            const daysLeft = Math.ceil((7 * 24 * 60 * 60 * 1000 - (now - new Date(settings.lastUsedAt).getTime())) / (24 * 60 * 60 * 1000));
            return res.status(429).json({ error: `Die Staatskasse muss sich erholen! Nächstes Konjunkturpaket erst in ${daysLeft} Tagen möglich.` });
        }

        // Staatskasse prüfen
        const treasuryDoc = await systemSettingsCollection.findOne({ id: 'state_treasury' });
        const pot = treasuryDoc ? treasuryDoc.balance : 0;

        if (pot < 100000) return res.status(400).json({ error: "Die Staatskasse ist zu leer (unter $100.000)." });

        // Wir nehmen 10% der Staatskasse
        const budget = Math.floor(pot * 0.1);

        // Aktive User suchen (letzte 7 Tage eingeloggt, keine Admins)
        const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
        const activeUsers = await usersCollection.find({ lastLogin: { $gte: sevenDaysAgo }, isAdmin: { $ne: true } }).toArray();

        if (activeUsers.length === 0) return res.status(400).json({ error: "Keine aktiven Bürger gefunden." });

        const payoutPerUser = Math.floor(budget / activeUsers.length);

        // Geld verteilen
        await usersCollection.updateMany(
            { lastLogin: { $gte: sevenDaysAgo }, isAdmin: { $ne: true } },
            { $inc: { balance: payoutPerUser } }
        );

        // Staatskasse leeren
        await systemSettingsCollection.updateOne({ id: 'state_treasury' }, { $inc: { balance: -budget } });

        // Cooldown setzen
        await systemSettingsCollection.updateOne({ id: 'mayor_stimulus' }, { $set: { lastUsedAt: new Date() } }, { upsert: true });

        // LNN News
        await newsCollection.insertOne({
            headline: "GELDREGEN VOM RATHAUS! 💸",
            content: `Bürgermeister ${req.session.username} hat ein Konjunkturpaket verabschiedet! Jeder aktive Bürger erhält $${payoutPerUser.toLocaleString()} aus der Staatskasse!`,
            author: "LNN Politik",
            category: "Wirtschaft",
            createdAt: new Date(),
            likes: 0
        });
        updateDataVersion('news');

        res.json({ message: `Konjunkturpaket verabschiedet! ${activeUsers.length} Bürger haben je $${payoutPerUser.toLocaleString()} erhalten.` });

    } catch (e) {
        res.status(500).json({ error: "Fehler beim Verteilen der Gelder." });
    }
});

// --- 3. PRÄSIDENTIELLE BEGNADIGUNG (GERICHT) ---
// Bricht eine laufende Gerichtsverhandlung ab und spricht den Angeklagten frei.
app.post('/api/mayor/pardon', isAuthenticated, isMayorMode, async (req, res) => {
    try {
        // Suche aktiven Fall
        const activeCase = await db.collection('courtCases').findOne({ status: 'active' }, { sort: { createdAt: 1 } });
        if (!activeCase) return res.status(400).json({ error: "Es gibt aktuell keinen aktiven Gerichtsfall." });

        // Fall als "innocent" schließen
        await db.collection('courtCases').updateOne(
            { _id: activeCase._id },
            { $set: { status: 'closed', verdict: 'innocent', closedAt: new Date(), pardonedByMayor: true } }
        );

        // LNN News
        await newsCollection.insertOne({
            headline: "BEGNADIGUNG DURCH DEN BÜRGERMEISTER! ⚖️",
            content: `Bürgermeister ${req.session.username} hat von seinem Veto-Recht Gebrauch gemacht und ${activeCase.accusedName} offiziell begnadigt! Der Prozess ist beendet.`,
            author: "LNN Justiz",
            category: "Justiz",
            createdAt: new Date(),
            likes: 0
        });
        updateDataVersion('news');

        res.json({ message: `Du hast ${activeCase.accusedName} begnadigt. Der Pöbel wird das sicher diskutieren.` });
    } catch (e) {
        res.status(500).json({ error: "Fehler bei der Begnadigung." });
    }
});

// =========================================================
// === ✉️ LIMO MAIL SYSTEM (POSTFACH & BELOHNUNGEN) ===
// =========================================================
const LOG_PREFIX_MAIL = "[Limo Mail]";

// 1. User: Postfach abrufen
app.get('/api/mail/inbox', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    try {
        const mails = await mailsCollection.find({ userId }).sort({ createdAt: -1 }).toArray();
        res.json({ mails });
    } catch (e) {
        res.status(500).json({ error: "Postfach konnte nicht geladen werden." });
    }
});

// 2. User: Mail als gelesen markieren
app.post('/api/mail/:id/read', isAuthenticated, async (req, res) => {
    const mailId = new ObjectId(req.params.id);
    const userId = new ObjectId(req.session.userId);
    try {
        await mailsCollection.updateOne({ _id: mailId, userId }, { $set: { isRead: true } });
        res.json({ success: true });
    } catch (e) {
        res.status(500).json({ error: "Fehler." });
    }
});

// 3. User: Belohnung (Claim) einlösen (Sicher mit Transaktion!)
app.post('/api/mail/:id/claim', isAuthenticated, async (req, res) => {
    const mailId = new ObjectId(req.params.id);
    const userId = new ObjectId(req.session.userId);

    const session = client.startSession();
    try {
        let rewardMessage = "Belohnung eingelöst!";
        
        await session.withTransaction(async () => {
            const mail = await mailsCollection.findOne({ _id: mailId, userId }, { session });
            if (!mail) throw new Error("Mail nicht gefunden.");
            if (!mail.rewards) throw new Error("Diese Mail enthält keine Belohnungen.");
            if (mail.isClaimed) throw new Error("Belohnung wurde bereits abgeholt!");

            // Update User mit den Belohnungen
            const updateOps = { $inc: {} };
            
            if (mail.rewards.money) updateOps.$inc.balance = mail.rewards.money;
            if (mail.rewards.tokens) updateOps.$inc.tokens = mail.rewards.tokens;
            
            if (mail.rewards.badge) {
                updateOps.$addToSet = { achievements: mail.rewards.badge };
            }

            if (Object.keys(updateOps.$inc).length === 0) delete updateOps.$inc;

            await usersCollection.updateOne({ _id: userId }, updateOps, { session });
            
            // Mail als eingelöst markieren
            await mailsCollection.updateOne(
                { _id: mailId }, 
                { $set: { isClaimed: true, isRead: true } }, 
                { session }
            );

            rewardMessage = `Erfolgreich eingelöst! Du hast erhalten: ` + 
                (mail.rewards.money ? `$${mail.rewards.money} ` : '') +
                (mail.rewards.tokens ? `${mail.rewards.tokens} Tokens ` : '') +
                (mail.rewards.badge ? `+ Exklusives Badge!` : '');
        });

        res.json({ success: true, message: rewardMessage });
    } catch (e) {
        res.status(400).json({ error: e.message });
    } finally {
        await session.endSession();
    }
});

// 4. Admin: Mails versenden
app.post('/api/admin/mail/send', isAuthenticated, isAdmin, async (req, res) => {
    const { target, subject, content, rewards } = req.body;
    // target kann 'all', 'active' (letzte 7 Tage) oder ein 'username' sein.
    
    if (!subject || !content) return res.status(400).json({ error: "Betreff und Text fehlen." });

    try {
        let usersToReceive = [];

        if (target === 'all') {
            usersToReceive = await usersCollection.find({}, { projection: { _id: 1 } }).toArray();
        } else if (target === 'active') {
            // Nur User, die in den letzten 7 Tagen online waren
            const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
            usersToReceive = await usersCollection.find({ lastLogin: { $gte: sevenDaysAgo } }, { projection: { _id: 1 } }).toArray();
        } else {
            // Einzelner User
            const u = await usersCollection.findOne({ username: { $regex: new RegExp(`^${target}$`, 'i') } });
            if (!u) return res.status(404).json({ error: "Ziel-User nicht gefunden." });
            usersToReceive.push(u);
        }

        if (usersToReceive.length === 0) return res.status(400).json({ error: "Keine Empfänger gefunden." });

        // Mails generieren
        const mailsToInsert = usersToReceive.map(u => ({
            userId: u._id,
            sender: `System-Admin (${req.session.username})`,
            subject: subject,
            content: content,
            rewards: rewards || null, // z.B. { money: 50000, tokens: 25, badge: 'bug_survivor' }
            isRead: false,
            isClaimed: false,
            createdAt: new Date()
        }));

        await mailsCollection.insertMany(mailsToInsert);

        console.log(`${LOG_PREFIX_MAIL} Admin ${req.session.username} hat ${mailsToInsert.length} Mails gesendet.`);
        res.json({ message: `Nachricht an ${mailsToInsert.length} Bürger verschickt!` });

    } catch (e) {
        console.error(e);
        res.status(500).json({ error: "Fehler beim Versenden." });
    }
});

// =========================================================
// === 💒 STANDESAMT (HEIRATEN & SCHEIDUNG FÜR ECHTE USER) ===
// =========================================================
const LOG_PREFIX_WEDDING = "[Standesamt API]";

const RING_TYPES = {
    'kaugummi': { name: 'Kaugummiautomat-Ring', price: 500, icon: '🍬' },
    'silber': { name: 'Silberring', price: 10000, icon: '💍' },
    'gold': { name: 'Goldring', price: 50000, icon: '🌟' },
    'diamant': { name: 'Diamantring', price: 250000, icon: '💎' },
    'limo': { name: 'Limo Prime Ring', price: 1000000, icon: '👑' }
};

// 1. Status abrufen (Meine Ehen & Anträge)
app.get('/api/standesamt/status', isAuthenticated, async (req, res) => {
    const userId = new ObjectId(req.session.userId);
    try {
        const user = await usersCollection.findOne({ _id: userId }, { projection: { spouses: 1 } });
        
        // Offene Anträge (die ich erhalten habe)
        const incomingProposals = await proposalsCollection.find({ targetId: userId }).toArray();
        // Anträge (die ich gesendet habe)
        const outgoingProposals = await proposalsCollection.find({ senderId: userId }).toArray();

        res.json({
            spouses: user.spouses || [],
            incoming: incomingProposals,
            outgoing: outgoingProposals,
            rings: RING_TYPES
        });
    } catch (e) {
        res.status(500).json({ error: "Standesamt geschlossen." });
    }
});

// 2. Heiratsantrag stellen
app.post('/api/standesamt/propose', isAuthenticated, async (req, res) => {
    const { targetUsername, ringType } = req.body;
    const senderId = new ObjectId(req.session.userId);
    const senderName = req.session.username;

    if (!targetUsername || !ringType || !RING_TYPES[ringType]) {
        return res.status(400).json({ error: "Ungültige Daten." });
    }

    if (targetUsername.toLowerCase() === senderName.toLowerCase()) {
        return res.status(400).json({ error: "Selbstliebe ist wichtig, aber steuerlich nicht absetzbar. Wähle jemand anderen." });
    }

    const ring = RING_TYPES[ringType];

    try {
        const targetUser = await usersCollection.findOne({ username: { $regex: new RegExp(`^${targetUsername}$`, 'i') } });
        if (!targetUser) return res.status(404).json({ error: "Diese Person lebt nicht in Limazon." });

        const user = await usersCollection.findOne({ _id: senderId });
        if (user.balance < ring.price) return res.status(400).json({ error: `Du bist zu arm für den ${ring.name} ($${ring.price.toLocaleString()}).` });

        // Prüfen, ob sie schon verheiratet sind
        if (user.spouses && user.spouses.some(s => s.id.equals(targetUser._id))) {
            return res.status(400).json({ error: "Ihr seid doch schon verheiratet!" });
        }

        // Prüfen, ob schon ein Antrag läuft
        const existing = await proposalsCollection.findOne({ senderId: senderId, targetId: targetUser._id });
        if (existing) return res.status(400).json({ error: "Du liegst bereits auf den Knien. Warte auf eine Antwort!" });

        // Geld abziehen
        await usersCollection.updateOne({ _id: senderId }, { $inc: { balance: -ring.price } });

        // Antrag in DB
        await proposalsCollection.insertOne({
            senderId: senderId,
            senderName: senderName,
            targetId: targetUser._id,
            targetName: targetUser.username,
            ringType: ringType,
            ringDetails: ring,
            createdAt: new Date()
        });

        res.json({ message: `Antrag mit ${ring.name} an ${targetUser.username} gesendet! Hoffen wir, dass sie/er 'Ja' sagt.` });

    } catch (e) {
        res.status(500).json({ error: "Der Ring ist ins Gulli gefallen." });
    }
});

// 3. Antrag beantworten (Ja / Nein)
app.post('/api/standesamt/respond', isAuthenticated, async (req, res) => {
    const { proposalId, action } = req.body; // action: 'accept' oder 'decline'
    const userId = new ObjectId(req.session.userId);
    const userName = req.session.username;

    const session = client.startSession();
    try {
        let responseMessage = "";

        await session.withTransaction(async () => {
            const proposal = await proposalsCollection.findOne({ _id: new ObjectId(proposalId), targetId: userId }, { session });
            if (!proposal) throw new Error("Antrag nicht gefunden.");

            if (action === 'decline') {
                // Bei "Nein" bekommt der Antragsteller sein Geld zurück (Kulanz des Juweliers)
                await usersCollection.updateOne({ _id: proposal.senderId }, { $inc: { balance: proposal.ringDetails.price } }, { session });
                await proposalsCollection.deleteOne({ _id: proposal._id }, { session });
                responseMessage = `Du hast ${proposal.senderName} einen Korb gegeben.`;
            } 
            else if (action === 'accept') {
                const date = new Date();
                
                // Trage Partner A bei Partner B ein
                const spouseA = { id: proposal.senderId, name: proposal.senderName, ring: proposal.ringDetails.name, date: date };
                const spouseB = { id: userId, name: userName, ring: proposal.ringDetails.name, date: date };

                await usersCollection.updateOne({ _id: userId }, { $addToSet: { spouses: spouseA } }, { session });
                await usersCollection.updateOne({ _id: proposal.senderId }, { $addToSet: { spouses: spouseB } }, { session });

                await proposalsCollection.deleteOne({ _id: proposal._id }, { session });

                // LNN NEWS BREAKER!
                await newsCollection.insertOne({
                    headline: `HOCHZEITSGLOCKEN! 💒`,
                    content: `${userName} hat 'JA' gesagt! ${proposal.senderName} und ${userName} sind nun offiziell verheiratet. (Der Ring war ein ${proposal.ringDetails.name}). Glückwunsch!`,
                    author: "LNN Standesamt",
                    category: "Community",
                    createdAt: date,
                    likes: 0
                }, { session });
                updateDataVersion('news');

                responseMessage = `JA! Du bist nun mit ${proposal.senderName} verheiratet. Genießt das Ehegattensplitting!`;
            } else {
                throw new Error("Ungültige Aktion.");
            }
        });

        res.json({ success: true, message: responseMessage });
    } catch (e) {
        res.status(400).json({ error: e.message });
    } finally {
        await session.endSession();
    }
});

// 4. Scheidung (Polygamie-freundlich)
app.post('/api/standesamt/divorce', isAuthenticated, async (req, res) => {
    const { targetId } = req.body; // Wer soll geschieden werden?
    const userId = new ObjectId(req.session.userId);
    const userName = req.session.username;
    const DIVORCE_COST = 25000; // Anwaltskosten

    const session = client.startSession();
    try {
        await session.withTransaction(async () => {
            const user = await usersCollection.findOne({ _id: userId }, { session });
            if (user.balance < DIVORCE_COST) throw new Error(`Du brauchst $${DIVORCE_COST.toLocaleString()} für den Anwalt.`);

            const targetObjId = new ObjectId(targetId);
            
            // Checken ob sie wirklich verheiratet sind
            if (!user.spouses || !user.spouses.some(s => s.id.equals(targetObjId))) {
                throw new Error("Ihr seid nicht verheiratet.");
            }

            const exSpouse = await usersCollection.findOne({ _id: targetObjId }, { session });

            // Geld abziehen
            await usersCollection.updateOne({ _id: userId }, { $inc: { balance: -DIVORCE_COST } }, { session });

            // Aus den Arrays löschen
            await usersCollection.updateOne({ _id: userId }, { $pull: { spouses: { id: targetObjId } } }, { session });
            await usersCollection.updateOne({ _id: targetObjId }, { $pull: { spouses: { id: userId } } }, { session });

            // LNN News Drama!
            await newsCollection.insertOne({
                headline: `BITTERE SCHEIDUNG! 💔`,
                content: `Das Märchen ist vorbei! ${userName} hat die Scheidung von ${exSpouse.username} eingereicht. Die Anwälte reiben sich die Hände.`,
                author: "LNN Klatsch & Tratsch",
                category: "Community",
                createdAt: new Date(),
                likes: 0
            }, { session });
            updateDataVersion('news');
        });

        res.json({ success: true, message: `Scheidung vollzogen. Das kostet dich $${DIVORCE_COST}.` });
    } catch (e) {
        res.status(400).json({ error: e.message });
    } finally {
        await session.endSession();
    }
});

// =========================================================
// === 📦 LIMO LOGISTICS (PAKETBOTE BACKGROUND WORKER) ===
// =========================================================
setInterval(async () => {
    try {
        // Suche alle Pakete, deren Lieferzeitpunkt in der Vergangenheit liegt und die noch 'pending' sind
        const pendingDeliveries = await deliveriesCollection.find({ 
            status: 'pending', 
            arrivalDate: { $lte: new Date() } 
        }).toArray();

        for (const delivery of pendingDeliveries) {
            // 1. Dem Empfänger das Item geben (Sicherstellen, dass es im Inventar landet)
            await inventoriesCollection.updateOne(
                { userId: delivery.targetId, productId: delivery.productId },
                { 
                    $inc: { quantityOwned: delivery.quantity },
                    $setOnInsert: { 
                        name: delivery.productName, 
                        icon: delivery.productIcon || '📦',
                        type: delivery.productType || 'item'
                    }
                },
                { upsert: true }
            );

            // 2. Paket als 'delivered' markieren
            await deliveriesCollection.updateOne(
                { _id: delivery._id },
                { $set: { status: 'delivered', deliveredAt: new Date() } }
            );

            // 3. Lieferschein per Limo-Mail an den Empfänger schicken!
            await mailsCollection.insertOne({
                userId: delivery.targetId,
                sender: `🚚 ${delivery.providerName}`,
                subject: `Paket von ${delivery.senderName} ist angekommen!`,
                content: `Ding Dong! Dein Paket wurde erfolgreich zugestellt.\n\nInhalt: ${delivery.quantity}x ${delivery.productIcon || ''} ${delivery.productName}\nAbsender: ${delivery.senderName}\n\nViel Spaß damit!`,
                isRead: false,
                isClaimed: false,
                createdAt: new Date()
            });
            
            console.log(`[Limo Logistics] 📦 Paket von ${delivery.senderName} an ${delivery.targetName} zugestellt!`);
        }
    } catch (e) {
        console.error("[Limo Logistics] Fehler beim Ausliefern:", e);
    }
}, 30 * 1000); // Checkt alle 30 Sekunden

// =========================================================
// === 🚚 LIMO LOGISTICS API (LIEFERSERVICE) ===
// =========================================================

// 1. Lieferdienste & Preise abrufen (Jedes Mal zufällig!)
app.get('/api/delivery/providers', isAuthenticated, (req, res) => {
    // Generiert zufällige Minuten und Preise für echte Paketdienste
    const providers = [
        { 
            id: 'prime', 
            name: 'Limo Prime Express 🚀', 
            timeMins: Math.floor(Math.random() * 3) + 1, // 1-3 Minuten
            cost: Math.floor(Math.random() * 5000) + 5000 // 5.000 - 10.000$
        },
        { 
            id: 'ups', 
            name: 'UPS (Ups, wo ist das Paket?) 🟫', 
            timeMins: Math.floor(Math.random() * 8) + 3, // 3-10 Minuten
            cost: Math.floor(Math.random() * 2000) + 2000 // 2.000 - 4.000$
        },
        { 
            id: 'dhl', 
            name: 'DHL (Drop & Hide Logistics) 🟨', 
            timeMins: Math.floor(Math.random() * 15) + 5, // 5-20 Minuten
            cost: Math.floor(Math.random() * 1000) + 500 // 500 - 1.500$
        },
        { 
            id: 'dpd', 
            name: 'DPD (Dein Paket Dauert) 🟥', 
            timeMins: Math.floor(Math.random() * 26) + 10, // 10-35 Minuten
            cost: Math.floor(Math.random() * 500) + 250 // 250 - 750$
        },
        { 
            id: 'hermes', 
            name: 'Hermes (Götterbote auf Valium) 🟦', 
            timeMins: Math.floor(Math.random() * 60) + 30, // 30-90 Minuten
            cost: Math.floor(Math.random() * 100) + 10 // 10 - 110$
        },
        { 
            id: 'gls', 
            name: 'GLS (Ganz Langsamer Service) 🐌', 
            timeMins: Math.floor(Math.random() * 60) + 60, // 60-120 Minuten
            cost: Math.floor(Math.random() * 45) + 5 // 5 - 50$
        }
    ];
    
    // Sortiert die Liste automatisch nach Preis (Teuerster = Schnellster oben)
    providers.sort((a, b) => b.cost - a.cost);
    
    res.json(providers);
});

// 2. Paket versenden
app.post('/api/delivery/send', isAuthenticated, async (req, res) => {
    const { targetUsername, productId, quantity, provider } = req.body;
    const senderId = new ObjectId(req.session.userId);
    const senderName = req.session.username;
    const qty = parseInt(quantity);

    if (!targetUsername || !productId || !provider || qty <= 0) {
        return res.status(400).json({ error: "Fehlende Angaben oder ungültige Menge." });
    }

    if (targetUsername.toLowerCase() === senderName.toLowerCase()) {
        return res.status(400).json({ error: "Du kannst dir selbst keine Pakete schicken." });
    }

    const session = client.startSession();
    try {
        await session.withTransaction(async () => {
            // 1. Kostencheck
            const sender = await usersCollection.findOne({ _id: senderId }, { session });
            if (sender.balance < provider.cost) throw new Error(`Du hast nicht genug Geld für ${provider.name}.`);

            // 2. Ziel-User suchen
            const target = await usersCollection.findOne({ username: { $regex: new RegExp(`^${targetUsername}$`, 'i') } }, { session });
            if (!target) throw new Error("Empfänger existiert nicht.");

            // 3. Item im Inventar des Absenders prüfen
            const inventoryItem = await inventoriesCollection.findOne({ userId: senderId, productId: productId }, { session });
            if (!inventoryItem || inventoryItem.quantityOwned < qty) {
                throw new Error("Du besitzt dieses Item nicht (oder nicht in der Menge).");
            }

            // 4. Geld und Item beim Absender abziehen
            await usersCollection.updateOne({ _id: senderId }, { $inc: { balance: -provider.cost } }, { session });
            await inventoriesCollection.updateOne(
                { _id: inventoryItem._id }, 
                { $inc: { quantityOwned: -qty } }, 
                { session }
            );

            // 5. Paket in die Logistik übergeben (Berechnung der Ankunftszeit)
            const arrivalDate = new Date(Date.now() + provider.timeMins * 60 * 1000);

            await deliveriesCollection.insertOne({
                senderId: senderId,
                senderName: senderName,
                targetId: target._id,
                targetName: target.username,
                productId: productId,
                productName: inventoryItem.name,
                productIcon: inventoryItem.icon,
                productType: inventoryItem.type,
                quantity: qty,
                providerName: provider.name,
                cost: provider.cost,
                status: 'pending',
                arrivalDate: arrivalDate,
                createdAt: new Date()
            }, { session });
        });

        res.json({ message: `Paket wurde an ${provider.name} übergeben! Es wird in ca. ${provider.timeMins} Minuten zugestellt.` });

    } catch (e) {
        res.status(400).json({ error: e.message });
    } finally {
        await session.endSession();
    }
});

app.use((req, res) => {
    console.warn(`${LOG_PREFIX_SERVER} Unbekannter Endpoint aufgerufen: ${req.method} ${req.originalUrl} von IP ${req.ip}`);
    res.status(404).send('Endpoint nicht gefunden');
});