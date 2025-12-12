// server.js - Full Backend for Limo Open Source Project and all of the components of it

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
const { ok } = require('assert');
const LOG_PREFIX_CHAT = "[WhatsLim API]";
const CHAT_COLLECTION_NAME = 'limChats';
const MESSAGE_COLLECTION_NAME = 'limMessages';
const USER_CHAT_SETTINGS_COLLECTION_NAME = 'limUserChatSettings';

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
    'https://tcg.limazon.v6.rocks',
];
if (frontendProdUrl) { allowedOrigins.push(frontendProdUrl); }
console.log(`${LOG_PREFIX_SERVER} Erlaubte CORS Origins:`, allowedOrigins);

app.use(cors({
    origin: function (origin, callback) {
        // Prüfen, ob die Origin in der statischen Liste ist ODER dem dynamischen Muster entspricht
        const isAllowed = !origin || 
                          allowedOrigins.includes(origin) || 
                          (origin && origin.endsWith('.scf.usercontent.goog'));

        if (isAllowed) {
            callback(null, true); // Anfrage erlauben
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
let limChatsCollection, limMessagesCollection, limUserChatSettingsCollection;
let ideasCollection;
let auctionsCollection;
let portfoliosCollection, transactionsCollection;
let dontBlameMeCollection;
let humansCollection, ratingsCollection, criteriaCollection, categoriesCollection;
let bankTransactionsCollection;
let newsCollection;

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
        
        // NEU: Human Grades Collections
        humansCollection = db.collection('humans');      // Früher teachers
        ratingsCollection = db.collection('ratings');
        criteriaCollection = db.collection('criteria');  // Früher subjects
        categoriesCollection = db.collection('categories');
	
		bankTransactionsCollection = db.collection('bankTransactions');
        console.log(`${LOG_PREFIX_SERVER} ✅ MongoDB verbunden & alle Collections initialisiert.`);

        // --- 2. Indizes & Reparaturen ---
        try {
            // WICHTIG: Alten, fehlerhaften Index löschen (falls vorhanden), um Crash zu verhindern
            try {
                await ratingsCollection.dropIndex("teacherId_1_userId_1");
                console.log(`${LOG_PREFIX_SERVER} ♻️ Alter Index 'teacherId_1_userId_1' erfolgreich entfernt.`);
            } catch (e) { /* Index existiert nicht mehr, alles gut */ }

            // Neuen korrekten Index für Human Grades erstellen
            await ratingsCollection.createIndex({ humanId: 1, userId: 1 }, { unique: true });

            // Standard Indizes
            await humansCollection.createIndex({ id: 1 }, { unique: true, sparse: true });
            await criteriaCollection.createIndex({ id: 1 }, { unique: true });
            await categoriesCollection.createIndex({ id: 1 }, { unique: true });

            // Bestehende Indizes
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
            
            if (tokenTransactionsCollection) {
                await tokenTransactionsCollection.createIndex({ userId: 1 });
                await tokenTransactionsCollection.createIndex({ type: 1 });
                await tokenTransactionsCollection.createIndex({ timestamp: -1 });
            }
            await dontBlameMeCollection.createIndex(
                { "createdAt": 1 },
                { expireAfterSeconds: 72 * 60 * 60 }
            );

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
        setInterval(runAutomatedSecurityChecks, SECURITY_CHECK_INTERVAL_MS);
        console.log(`${LOG_PREFIX_SERVER} ⏰ Automatische Sicherheits-Prüfung aktiv.`);

        // AUKTION-ENDE-JOB
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

        // BÖRSEN-JOB
        setInterval(async () => {
            const now = new Date();
            try {
                const stocksToUpdate = await productsCollection.find({ isTokenCard: { $ne: true }, currentPrice: { $exists: true } }).toArray();
                if (stocksToUpdate.length === 0) return;
                
                const bulkOps = stocksToUpdate.map(stock => {
                    const buys = stock.buysLastInterval || 0;
                    const sells = stock.sellsLastInterval || 0;
                    const oldPrice = stock.currentPrice;
                    let newPrice = oldPrice;

                    if (buys > 0 || sells > 0) {
                        const netDemand = buys - sells;
                        const priceChangeFactor = 1 + (netDemand * PRICE_VOLATILITY_FACTOR);
                        newPrice = Math.max(MINIMUM_PRODUCT_PRICE, oldPrice * priceChangeFactor);
                    }
                    return {
                        updateOne: {
                            filter: { _id: stock._id },
                            update: {
                                $set: { currentPrice: parseFloat(newPrice.toFixed(2)), buysLastInterval: 0, sellsLastInterval: 0 },
                                $push: { priceHistory: { $each: [{ price: parseFloat(newPrice.toFixed(2)), timestamp: now }], $slice: -30 } }
                            }
                        }
                    };
                });
                if (bulkOps.length > 0) await productsCollection.bulkWrite(bulkOps);
            } catch (err) { console.error(`${LOG_PREFIX_SERVER} [StockMarketJob] Fehler:`, err); }
        }, PRICE_UPDATE_INTERVAL_MS);

        // --- 5. HTTP Server Starten ---
        http.createServer(app).listen(HTTP_PORT, () => {
            console.log(`${LOG_PREFIX_SERVER} 🌐 HTTP-Server läuft auf Port ${HTTP_PORT}`);
        });
    })
    .catch(err => { 
        console.error(`${LOG_PREFIX_SERVER} ❌ Kritischer Fehler: MongoDB-Verbindung fehlgeschlagen:`, err); 
        process.exit(1); 
    });

// === API ENDPOINTS ===

// AUTH
app.post('/api/auth/register', async (req, res) => {
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

app.post('/api/auth/login', async (req, res) => {
    const { username, password, rememberMe } = req.body;
    console.log(`${LOG_PREFIX_SERVER} Login-Versuch für User: ${username ? username.substring(0, 3) + "***" : "LEER"}`);
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
                res.json({ message: 'Login erfolgreich!', user: { userId: user._id.toString(), username: user.username, balance: user.balance, tokens: user.tokens || 0, isAdmin: user.isAdmin || false, infinityMoney: effectiveInfinityMoney, unlockedInfinityMoney: user.unlockedInfinityMoney || false, productSellCooldowns: user.productSellCooldowns || {} } });
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
        if (!user) { console.error(`${LOG_PREFIX_SERVER} /api/auth/me: User ${req.session.userId} nicht in DB! Zerstöre Session.`); req.session.destroy(() => { }); return res.status(404).json({ error: 'Benutzer nicht gefunden.' }); }
        const effectiveInfinityMoney = user.isAdmin ? true : (user.infinityMoney || false);
        res.json({ userId: user._id.toString(), username: user.username, balance: parseFloat(user.balance || 0), tokens: user.tokens || 0, isAdmin: user.isAdmin || false, infinityMoney: effectiveInfinityMoney, unlockedInfinityMoney: user.unlockedInfinityMoney || false, productSellCooldowns: user.productSellCooldowns || {} });
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
    if (typeof tokenAmount !== 'number' || tokenAmount <= 0 || !Number.isInteger(tokenAmount)) return res.status(400).json({ error: "Ungültiger Token-Betrag (positive Ganzzahl)." });
    if (typeof count !== 'number' || count <= 0 || count > 100 || !Number.isInteger(count)) return res.status(400).json({ error: "Ungültige Anzahl (1-100, Ganzzahl)." });
    try {
        const generatedCodes = [];
        for (let i = 0; i < count; i++) {
            const uniqueCode = await generateUniqueTokenRedeemCode();
            await tokenCodesCollection.insertOne({ code: uniqueCode, tokenAmount: tokenAmount, isRedeemed: false, createdAt: new Date(), generatedByAdminId: new ObjectId(req.session.userId) });
            generatedCodes.push({ code: uniqueCode, amount: tokenAmount });
        }
        console.log(`${LOG_PREFIX_SERVER} Admin ${req.session.username} hat ${count} Codes mit je ${tokenAmount} Tokens generiert.`);
        res.status(201).json({ message: `${count} Token-Code(s) mit je ${tokenAmount} Tokens erfolgreich generiert.`, codes: generatedCodes });
    } catch (err) { console.error(`${LOG_PREFIX_SERVER} Admin Fehler Code-Generierung:`, err); res.status(500).json({ error: "Fehler bei der Code-Generierung." }); }
});

// PRODUCTS
app.get('/api/products', async (req, res) => {
    try {
        // === KORREKTUR 2: PERFORMANCE-OPTIMIERUNG DURCH PROJEKTION ===
        const prods = await productsCollection.find({}, {
            projection: {
                priceHistory: 0
            }
        }).sort({ id: 1 }).toArray();

        // Sanitize products for both classic shop and stonk market
        const sanitized = prods.map(p => {
            const s = { ...p };
            if (p.hasOwnProperty('currentPrice') && !p.isTokenCard) {
                s.price = `$${parseFloat(p.currentPrice || 0).toFixed(2)}`;
            }
            s.stock = (typeof p.stock === 'number' && p.stock >= 0) ? p.stock : 0;
            s.default_stock = (typeof p.default_stock === 'number' && p.default_stock >= 0) ? p.default_stock : (p.isTokenCard ? 99999 : 20);
            delete s._id;
            return s;
        });

        res.json({ products: sanitized });
    } catch (err) {
        console.error(`${LOG_PREFIX_SERVER} Fehler Abruf Produkte:`, err);
        res.status(500).json({ error: 'Fehler Abruf Produktliste.' });
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
            productChecks.push(productsCollection.findOne({ id: item.id }).then(async pDb => {
                if (!pDb) { errors.push(`Produkt "${item.name || item.id}" nicht gefunden.`); return null; }
                const price = pDb.currentPrice || parseFloat((pDb.price || "$0").replace(/[^0-9.]/g, '')) || 0;
                totalOrderValue += price * item.quantity;
                if (pDb.isTokenCard && pDb.tokenValue > 0) {
                    for (let i = 0; i < item.quantity; i++) { tokenCodeGenerationTasks.push({ tokenAmount: pDb.tokenValue, limazonProductId: pDb.id, generatedForUserId: userId, originalPricePaid: price }); }
                    productDataForOrder.push({ productId: pDb.id, name: pDb.name, quantity: item.quantity, price: price, image_url: pDb.image_url, isTokenCardPurchase: true });
                    return { id: item.id, quantityToDecrement: 0, priceAtPurchase: price, isTokenCard: true, name: pDb.name };
                } else {
                    const stockDb = (typeof pDb.stock === 'number' && pDb.stock >= 0) ? pDb.stock : 0;
                    if (item.quantity > stockDb) { errors.push(`"${pDb.name}": Nur ${stockDb} Stk. verfügbar.`); return null; }
                    productDataForOrder.push({ productId: pDb.id, name: pDb.name, quantity: item.quantity, price: price, image_url: pDb.image_url });
                    inventoryOps.push({ updateOne: { filter: { userId: userId, productId: pDb.id }, update: { $inc: { quantityOwned: item.quantity }, $set: { lastAcquiredPrice: price } }, upsert: true } });
                    return { id: item.id, quantityToDecrement: item.quantity, priceAtPurchase: price, name: pDb.name };
                }
            }).catch(e => { errors.push(`DB-Fehler Produktprüfung: ${item.id}`); console.error(`${LOG_PREFIX_SERVER} Product check error:`, e); return null; }));
        }
        if (errors.length > 0 && productChecks.length === 0) throw new Error(errors.join('; '));
        const results = await Promise.all(productChecks);
        const validationErrorsFromPromises = results.filter(r => r === null).map((r, idx) => errors[idx] || `Produktprüfung Item ${idx + 1} fehlgeschlagen.`);
        const allErrors = errors.filter(e => e && !validationErrorsFromPromises.includes(e)).concat(validationErrorsFromPromises);
        if (allErrors.length > 0) { console.warn(`${LOG_PREFIX_SERVER} Validierungsfehler Kauf ${user.username}:`, allErrors); throw new Error(allErrors.join('; ')); }
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
            if (balUpdRes.modifiedCount !== 1) { console.error(`${LOG_PREFIX_SERVER} Kritischer Fehler Guthabenabzug ${userId}! Soll ${totalOrderValue}.`); throw new Error('Kritischer Fehler Guthabenabzug.'); }
        }
        if (!user.unlockedInfinityMoney && !user.isAdmin) { const regItemsForUnlock = productDataForOrder.filter(item => !item.isTokenCardPurchase); if (regItemsForUnlock.length > 0) { const allShopProds = await productsCollection.find({ id: { $gte: 100000 }, isTokenCard: { $ne: true } }, { projection: { price: 1, _id: 0 } }).sort({ price: -1 }).limit(1).toArray(); let maxPriceInShop = 0; if (allShopProds.length > 0) maxPriceInShop = parseFloat((allShopProds[0].price || "$0").replace(/[^0-9.]/g, '')) || 0; for (const boughtItemData of regItemsForUnlock) { if (boughtItemData.price >= maxPriceInShop && maxPriceInShop > 0.01) { await usersCollection.updateOne({ _id: userId }, { $set: { unlockedInfinityMoney: true } }); newUnlockOccurred = true; console.log(`${LOG_PREFIX_SERVER} Infinity Money User ${userId} freigeschaltet.`); break; } } } }
        try { const order = { userId: userId, username: user.username, date: new Date(), items: productDataForOrder, total: totalOrderValue }; await ordersCollection.insertOne(order); } catch (orderError) { console.error(`${LOG_PREFIX_SERVER} Fehler Speicher Bestellung ${userId}:`, orderError); }
        const finalUser = await usersCollection.findOne({ _id: userId }, { projection: { password: 0 } });
        const effInfMonFinal = finalUser.isAdmin ? true : (finalUser.infinityMoney || false);
        let purMessage = `Kauf erfolgreich!`;
        if (genCodesUserMsg.length > 0) purMessage += ` ${genCodesUserMsg.length} Token Guthabencode(s) generiert. Siehe "Meine Token Codes".`;
        if (newUnlockOccurred) purMessage += ' Glückwunsch, Infinity Money freigeschaltet!';
        console.log(`${LOG_PREFIX_SERVER} User ${user.username} Einkauf $${totalOrderValue.toFixed(2)}. ${genCodesUserMsg.length} Token Codes.`);
        res.json({ message: purMessage, user: { ...finalUser, tokens: finalUser.tokens || 0, infinityMoney: effInfMonFinal, productSellCooldowns: finalUser.productSellCooldowns || {} } });
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
        if (lastAttCDISO) { const cdEndTime = new Date(lastAttCDISO).getTime(); if (Date.now() < cdEndTime) { const timeLeft = Math.ceil((cdEndTime - Date.now()) / 1000); return res.status(429).json({ success: false, error: `Cooldown aktiv: Warte ${timeLeft}s.`, cooldownActiveForProduct: productId, cooldownEndsAt: lastAttCDISO, productSellCooldowns: cooldowns }); } else { delete cooldowns[productId.toString()]; await usersCollection.updateOne({ _id: userId }, { $set: { productSellCooldowns: cooldowns } }); } }

        // === KORREKTUR 1: VERKAUFS-BUG BEHOBEN ===
        const origPrice = prodToSell.basePrice || parseFloat((prodToSell.price || "$0").replace(/[^0-9.]/g, '')) || 1;

        let prob = 1.0;
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
        if (oneDevBaseUrl === "http://reason-nurse.gl.at.ply.gg:21182" || (oneDevBaseUrl.startsWith('http://') && oneDevBaseUrl.includes('ply.gg'))) { // Genauer für ply.gg oder allgemeiner für HTTP
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

    const currentDbCollection = db.collection(collectionName);
    let dbResult;

    const sanitizeQueryIds = (q) => {
        if (!q || typeof q !== 'object') return {};
        const sanitized = { ...q };
        if (sanitized._id && typeof sanitized._id === 'string' && ObjectId.isValid(sanitized._id)) {
            sanitized._id = new ObjectId(sanitized._id);
        }
        if (sanitized.userId && typeof sanitized.userId === 'string' && ObjectId.isValid(sanitized.userId)) {
            if ([ordersCollectionName, inventoriesCollectionName, wheelsCollectionName, tokenCodesCollectionName, tokenTransactionsCollectionName].includes(collectionName)) {
                sanitized.userId = new ObjectId(sanitized.userId);
            }
        }
        return sanitized;
    };

    // 1. Initialisiere die Basis-Query vom User
    let finalQuery = sanitizeQueryIds(query);

    // 2. Initialisiere die Basis-Optionen vom User und bereinige sie
    const userProvidedOptions = options && typeof options === 'object' ? { ...options } : {};
    const sanitizedOptionsForDB = { ...userProvidedOptions }; // Kopie zum Bearbeiten

    // Write Concern ('w') bereinigen/validieren
    if (sanitizedOptionsForDB.hasOwnProperty('w')) {
        const writeConcernValue = sanitizedOptionsForDB.w;
        console.warn(`${logPrefixAdminData} User hat Write Concern Option 'w: ${writeConcernValue}' gesendet.`);
        let isValidWriteConcern = false;
        if (typeof writeConcernValue === 'number' && writeConcernValue >= 0 && writeConcernValue <= 50) {
            isValidWriteConcern = true;
        } else if (typeof writeConcernValue === 'string' && writeConcernValue.toLowerCase() === 'majority') {
            isValidWriteConcern = true;
        }
        if (!isValidWriteConcern) {
            console.warn(`${logPrefixAdminData} Ungültiger oder nicht unterstützter 'w' Wert '${writeConcernValue}' wird entfernt.`);
            delete sanitizedOptionsForDB.w;
        } else {
            console.log(`${logPrefixAdminData} Gültiger 'w' Wert '${writeConcernValue}' wird beibehalten.`);
        }
    }

    // 3. Wende serverseitige `searchTerm` Logik an, die `finalQuery` modifizieren kann
    if (operation === 'find' && searchTerm && typeof searchTerm === 'string' && searchTerm.trim() !== '') {
        const searchTermCleaned = searchTerm.trim();
        const collectionsTextSearchFields = {
            [productsCollectionName]: ['name', 'description'],
            [usersCollectionName]: ['username', 'fullName', 'email'],
            [ordersCollectionName]: ['username', 'items.name'],
            [wheelsCollectionName]: ['name', 'description', 'creatorUsername'],
        };
        const fieldsToSearch = collectionsTextSearchFields[collectionName];
        // Wende searchTerm nur an, wenn die ursprüngliche User-Query (finalQuery) leer war
        if (fieldsToSearch && fieldsToSearch.length > 0 && Object.keys(finalQuery).length === 0) {
            const orConditions = fieldsToSearch.map(field => {
                if (field === 'id' && collectionName === productsCollectionName && !isNaN(parseInt(searchTermCleaned))) {
                    return { [field]: parseInt(searchTermCleaned) };
                }
                return { [field]: { $regex: searchTermCleaned, $options: 'i' } };
            });
            finalQuery = { $or: orConditions }; // finalQuery wird hier überschrieben/modifiziert
            console.log(`${logPrefixAdminData} Nutze serverseitige Textsuche für: "${searchTermCleaned}". Modifizierte finalQuery: ${JSON.stringify(finalQuery)}`);
        } else if (Object.keys(finalQuery).length > 0) {
            console.log(`${logPrefixAdminData} Spezifische Query vom User vorhanden (${JSON.stringify(finalQuery)}), serverseitiger searchTerm wird ignoriert.`);
        } else {
            console.log(`${logPrefixAdminData} Keine Suchfelder für Collection ${collectionName} definiert oder searchTerm-Logik nicht anwendbar.`);
        }
    }

    // 4. Standardoptionen wie limit und projection auf `sanitizedOptionsForDB` anwenden
    if (operation === 'find' && !sanitizedOptionsForDB.hasOwnProperty('limit')) { // Prüfe, ob limit schon existiert
        sanitizedOptionsForDB.limit = 100;
    }
    if (collectionName === usersCollectionName && (operation === 'find' || operation === 'findOne')) {
        if (!sanitizedOptionsForDB.projection) {
            sanitizedOptionsForDB.projection = { password: 0 };
        } else if (typeof sanitizedOptionsForDB.projection === 'object' && sanitizedOptionsForDB.projection.password === undefined) {
            sanitizedOptionsForDB.projection.password = 0;
        }
    }

    // Logge die endgültige Query und die bereinigten Optionen
    console.log(`${logPrefixAdminData} Führe Datenbankoperation aus: ${operation} auf Collection: ${collectionName}. Query: ${JSON.stringify(finalQuery)} Opts: ${JSON.stringify(sanitizedOptionsForDB)}`);

    try {
        switch (operation) {
            case 'findOne':
                if (Object.keys(finalQuery).length === 0) return res.status(400).json({ error: '`query` (nicht leer) ist für `findOne` erforderlich.' });
                dbResult = await currentDbCollection.findOne(finalQuery, sanitizedOptionsForDB);
                break;
            case 'find':
                dbResult = await currentDbCollection.find(finalQuery, sanitizedOptionsForDB).toArray();
                break;
            case 'insertOne':
                if (!document || typeof document !== 'object' || Object.keys(document).length === 0) return res.status(400).json({ error: '`document` (nicht leeres Objekt) ist für `insertOne` erforderlich.' });
                dbResult = await currentDbCollection.insertOne(document, sanitizedOptionsForDB);
                break;
            case 'insertMany':
                if (!documents || !Array.isArray(documents) || documents.length === 0) return res.status(400).json({ error: '`documents` (nicht leeres Array) ist für `insertMany` erforderlich.' });
                dbResult = await currentDbCollection.insertMany(documents, sanitizedOptionsForDB);
                break;
            case 'updateOne':
            case 'updateMany':
                if (Object.keys(finalQuery).length === 0) return res.status(400).json({ error: `\`query\` (nicht leer) für \`${operation}\` erforderlich.` });
                if (!update || typeof update !== 'object' || Object.keys(update).length === 0) return res.status(400).json({ error: `\`update\` (nicht leeres Objekt) für \`${operation}\` erforderlich.` });
                dbResult = await currentDbCollection[operation](finalQuery, update, sanitizedOptionsForDB);
                break;
            case 'deleteOne':
            case 'deleteMany':
                if (Object.keys(finalQuery).length === 0) return res.status(400).json({ error: `\`query\` (nicht leer) für \`${operation}\` erforderlich.` });
                dbResult = await currentDbCollection[operation](finalQuery, sanitizedOptionsForDB);
                break;
            case 'countDocuments':
                dbResult = await currentDbCollection.countDocuments(finalQuery, sanitizedOptionsForDB);
                break;
            case 'aggregate':
                if (!pipeline || !Array.isArray(pipeline) || pipeline.length === 0) return res.status(400).json({ error: '`pipeline` (nicht leeres Array) ist für `aggregate` erforderlich.' });
                dbResult = await currentDbCollection.aggregate(pipeline, sanitizedOptionsForDB).toArray();
                break;
            default:
                return res.status(400).json({ error: `Unbekannte Datenbankoperation: ${operation}` });
        }
        console.log(`${logPrefixAdminData} DB-Op ${operation} erfolgreich.`);
        res.json({ success: true, operation, collectionName, result: dbResult });
    } catch (dbError) {
        // ... (deine bestehende Fehlerbehandlung) ...
        console.error(`${logPrefixAdminData} DB-Op Fehler '${operation}' auf '${collectionName}':`, dbError);
        if (dbError.message.toLowerCase().includes("not found") && ['findOne', 'updateOne', 'deleteOne'].includes(operation)) {
            return res.status(404).json({ error: `Dokument nicht gefunden für Operation '${operation}'.`, details: dbError.message });
        }
        res.status(500).json({ error: `DB-Fehler bei Operation '${operation}'.`, details: dbError.message });
    }
}
app.post('/api/admin/data-manipulation', isAuthenticated, isAdmin, adminDataManipulationEndpoint);
// Fallback für unbekannte Routen

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

        // Optional: Teilnehmernamen hinzufügen (ohne den aktuellen Nutzer selbst)
        // und Mute-Status hinzufügen
        const populatedChats = [];
        for (const chat of userChats) {
            const participantDetails = [];
            if (chat.type === 'personal') {
                const otherParticipantId = chat.participants.find(pId => !pId.equals(userId));
                if (otherParticipantId) {
                    const otherUser = await usersCollection.findOne({ _id: otherParticipantId }, { projection: { username: 1 } });
                    if (otherUser) participantDetails.push({ userId: otherUser._id, username: otherUser.username });
                }
            } else { // 'group'
                // Für Gruppen könnten wir die Anzahl der Teilnehmer oder die ersten paar Namen holen
                const otherParticipants = await usersCollection.find(
                    { _id: { $in: chat.participants.filter(pId => !pId.equals(userId)) } },
                    { projection: { username: 1 } }
                ).limit(3).toArray(); // Zeige bis zu 3 andere Teilnehmer
                participantDetails.push(...otherParticipants.map(u => ({ userId: u._id, username: u.username })));
            }

            const userChatSetting = await limUserChatSettingsCollection.findOne({ userId, chatId: chat._id });

            populatedChats.push({
                ...chat,
                displayParticipants: participantDetails,
                isMuted: userChatSetting ? userChatSetting.isMuted : false,
                // Limo ID anstelle von Limazon Konto (nur ein String für die Antwort)
                accountSystemName: "Limo ID"
            });
        }

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
            senderUsername: senderUsername, // Denormalisiert für einfache Anzeige
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
                    lastMessagePreview: content.trim().substring(0, 50), // Kurze Vorschau
                    lastMessageSenderId: senderId,
                    lastMessageTimestamp: now
                }
            }
        );

        // Hier würde man normalerweise via WebSockets die Nachricht an andere Teilnehmer pushen.
        // Da keine Benachrichtigungen gewünscht sind, entfällt das für den Server. Clients pollen.

        console.log(`${LOG_PREFIX_CHAT} Nachricht von ${senderUsername} in Chat ${chatId} gespeichert (ID: ${newMessage._id}).`);
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

    try {
        // --- 1. Daten abrufen und validieren ---
        const auction = await auctionsCollection.findOne({ _id: auctionId });
        if (!auction) {
            return res.status(404).json({ error: 'Auktion nicht gefunden.' });
        }
        if (auction.status !== 'active') {
            return res.status(400).json({ error: 'Diese Auktion ist bereits beendet.' });
        }
        if (new Date() > new Date(auction.endTime)) {
            return res.status(400).json({ error: 'Diese Auktion ist bereits abgelaufen.' });
        }
        if (auction.sellerId.equals(bidderId)) {
            return res.status(400).json({ error: 'Du kannst nicht auf deine eigene Auktion bieten.' });
        }
        if (finalBidAmount <= auction.currentBid) {
            return res.status(400).json({ error: `Dein Gebot muss höher als das aktuelle Gebot von $${auction.currentBid.toFixed(2)} sein.` });
        }

        const bidder = await usersCollection.findOne({ _id: bidderId });
        if (!bidder || bidder.balance < finalBidAmount) {
            return res.status(400).json({ error: `Nicht genügend Guthaben. Du benötigst $${finalBidAmount.toFixed(2)}.` });
        }

        // --- 2. Transaktionen durchführen ---

        // Geld vom neuen Bieter abziehen (reservieren)
        const bidderDebitResult = await usersCollection.updateOne(
            { _id: bidderId, balance: { $gte: finalBidAmount } },
            { $inc: { balance: -finalBidAmount } }
        );
        if (bidderDebitResult.modifiedCount === 0) {
            throw new Error("Guthabenabzug beim neuen Bieter fehlgeschlagen. Möglicherweise Race Condition.");
        }

        // Wenn es einen vorherigen Höchstbietenden gab, Geld zurückgeben
        if (auction.highestBidderId) {
            await usersCollection.updateOne(
                { _id: auction.highestBidderId },
                { $inc: { balance: auction.currentBid } }
            );
        }

        // --- 3. Auktionsdokument aktualisieren ---
        const newBid = {
            bidderId,
            bidderUsername: req.session.username,
            amount: finalBidAmount,
            timestamp: new Date()
        };

        const auctionUpdateResult = await auctionsCollection.updateOne(
            { _id: auctionId, status: 'active' }, // Erneute Sicherheitsprüfung
            {
                $set: {
                    currentBid: finalBidAmount,
                    highestBidderId: bidderId,
                    highestBidderUsername: req.session.username
                },
                $push: {
                    bids: {
                        $each: [newBid],
                        $position: 0 // Neues Gebot an den Anfang des Arrays setzen
                    }
                }
            }
        );

        if (auctionUpdateResult.modifiedCount === 0) {
            throw new Error("Auktions-Update fehlgeschlagen. Auktion wurde möglicherweise in der Zwischenzeit beendet.");
        }

        console.log(`${LOG_PREFIX_SERVER} User ${req.session.username} bietet $${finalBidAmount} auf Auktion ${auctionId}.`);
        res.json({ message: 'Gebot erfolgreich abgegeben!', newBid });

    } catch (err) {
        console.error(`${LOG_PREFIX_SERVER} Fehler beim Bieten für User ${req.session.username} auf Auktion ${auctionId}:`, err);
        // WICHTIG: Im Fehlerfall sicherstellen, dass das Geld nicht verloren geht.
        // Da das Geld bereits abgezogen wurde, müssen wir es hier zurückgeben.
        await usersCollection.updateOne({ _id: bidderId }, { $inc: { balance: finalBidAmount } });
        res.status(500).json({ error: 'Serverfehler beim Bieten. Dein Geld wurde dir zurückerstattet.' });
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

// Aktien KAUFEN
app.post('/api/stonks/buy', isAuthenticated, async (req, res) => {
    const { productId, quantity } = req.body;
    const userId = new ObjectId(req.session.userId);

    if (typeof productId !== 'number' || typeof quantity !== 'number' || !Number.isInteger(quantity) || quantity <= 0) {
        return res.status(400).json({ error: 'Ungültige Produkt-ID oder Menge.' });
    }

    try {
        const stock = await productsCollection.findOne({ id: productId, isTokenCard: { $ne: true } });
        if (!stock) {
            return res.status(404).json({ error: 'Aktie nicht gefunden.' });
        }

        const pricePerShare = stock.currentPrice;
        const totalCost = pricePerShare * quantity;

        const user = await usersCollection.findOne({ _id: userId });
        if (!user || user.balance < totalCost) {
            return res.status(400).json({ error: `Nicht genügend Guthaben. Benötigt: $${totalCost.toFixed(2)}.` });
        }

        // --- Transaktion durchführen ---
        // 1. Geld vom User abziehen
        const debitResult = await usersCollection.updateOne({ _id: userId, balance: { $gte: totalCost } }, { $inc: { balance: -totalCost } });
        if (debitResult.modifiedCount === 0) throw new Error("Guthabenabzug fehlgeschlagen.");

        // 2. Kauf-Zähler für das Produkt erhöhen
        await productsCollection.updateOne({ _id: stock._id }, { $inc: { buysLastInterval: quantity } });

        // 3. Portfolio des Users aktualisieren
        const portfolioItem = await portfoliosCollection.findOne({ userId, productId });
        let newAveragePrice;
        if (portfolioItem) {
            const oldTotalValue = portfolioItem.averageBuyPrice * portfolioItem.quantityShares;
            const newTotalValue = oldTotalValue + totalCost;
            const newTotalQuantity = portfolioItem.quantityShares + quantity;
            newAveragePrice = newTotalValue / newTotalQuantity;
        } else {
            newAveragePrice = pricePerShare;
        }

        await portfoliosCollection.updateOne(
            { userId, productId },
            {
                $inc: { quantityShares: quantity },
                $set: { averageBuyPrice: newAveragePrice }
            },
            { upsert: true }
        );

        // 4. Transaktion loggen
        await transactionsCollection.insertOne({ userId, productId, type: 'buy', quantity, pricePerShare, totalValue: totalCost, timestamp: new Date() });

        res.json({ message: `Erfolgreich ${quantity} Anteile von "${stock.name}" für $${totalCost.toFixed(2)} gekauft.` });

    } catch (err) {
        console.error(`${LOG_PREFIX_SERVER} Fehler bei Aktienkauf für User ${req.session.username}:`, err);
        // Rollback versuchen (Geld zurückgeben), falls etwas schiefging
        const stock = await productsCollection.findOne({ id: productId });
        if (stock) {
            const totalCost = stock.currentPrice * quantity;
            await usersCollection.updateOne({ _id: userId }, { $inc: { balance: totalCost } });
        }
        res.status(500).json({ error: 'Serverfehler beim Aktienkauf. Transaktion wurde rückgängig gemacht.' });
    }
});

// Aktien VERKAUFEN
app.post('/api/stonks/sell', isAuthenticated, async (req, res) => {
    const { productId, quantity } = req.body;
    const userId = new ObjectId(req.session.userId);

    if (typeof productId !== 'number' || typeof quantity !== 'number' || !Number.isInteger(quantity) || quantity <= 0) {
        return res.status(400).json({ error: 'Ungültige Produkt-ID oder Menge.' });
    }

    try {
        const stock = await productsCollection.findOne({ id: productId, isTokenCard: { $ne: true } });
        if (!stock) return res.status(404).json({ error: 'Aktie nicht gefunden.' });

        const portfolioItem = await portfoliosCollection.findOne({ userId, productId });
        if (!portfolioItem || portfolioItem.quantityShares < quantity) {
            return res.status(400).json({ error: `Nicht genügend Anteile im Portfolio. Du besitzt nur ${portfolioItem ? portfolioItem.quantityShares : 0}.` });
        }

        const pricePerShare = stock.currentPrice;
        const totalCredit = pricePerShare * quantity;

        // --- Transaktion durchführen ---
        // 1. Anteile aus Portfolio entfernen
        const portfolioUpdateResult = await portfoliosCollection.updateOne({ userId, productId, quantityShares: { $gte: quantity } }, { $inc: { quantityShares: -quantity } });
        if (portfolioUpdateResult.modifiedCount === 0) throw new Error("Portfolio-Update fehlgeschlagen.");

        // 2. Verkauf-Zähler für das Produkt erhöhen
        await productsCollection.updateOne({ _id: stock._id }, { $inc: { sellsLastInterval: quantity } });

        // 3. User das Geld gutschreiben
        await usersCollection.updateOne({ _id: userId }, { $inc: { balance: totalCredit } });

        // 4. Transaktion loggen
        await transactionsCollection.insertOne({ userId, productId, type: 'sell', quantity, pricePerShare, totalValue: totalCredit, timestamp: new Date() });

        // Portfolio-Eintrag löschen, wenn keine Anteile mehr vorhanden
        await portfoliosCollection.deleteOne({ userId, productId, quantityShares: 0 });

        res.json({ message: `Erfolgreich ${quantity} Anteile von "${stock.name}" für $${totalCredit.toFixed(2)} verkauft.` });

    } catch (err) {
        console.error(`${LOG_PREFIX_SERVER} Fehler bei Aktienverkauf für User ${req.session.username}:`, err);
        // Rollback versuchen (Anteile zurückgeben), falls etwas schiefging
        await portfoliosCollection.updateOne({ userId, productId }, { $inc: { quantityShares: quantity } });
        res.status(500).json({ error: 'Serverfehler beim Aktienverkauf. Transaktion wurde rückgängig gemacht.' });
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
    } catch(e) { res.status(500).json({ error: "Fehler." }); }
});

// Admin: Kriterium erstellen
app.post('/api/human/admin/criteria', isAuthenticated, isAdmin, async (req, res) => {
    const { label, type, categoryId } = req.body;
    const id = label.toLowerCase().replace(/[^a-z0-9]/g, '').substring(0,10) + "_" + Math.floor(Math.random()*1000);
    try {
        await criteriaCollection.insertOne({ id, label, type, categoryId });
        res.json({ message: "Kriterium erstellt." });
    } catch(e) { res.status(500).json({ error: "Fehler." }); }
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
    } catch(e) { res.status(500).json({ error: "Fehler." }); }
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
    } catch(e) { res.status(500).json({ error: "Fehler beim Reset." }); }
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
    } catch(e) { 
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
    } catch(e) { 
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
    } catch(e) { 
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
            $or: [ { fromId: userId }, { toId: userId } ]
        }).sort({ timestamp: -1 }).limit(50).toArray();
        
        res.json({ transactions: history });
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: "Fehler beim Laden der Umsätze." });
    }
});

// 2. Überweisung tätigen (Geld oder Tokens)
app.post('/api/bank/transfer', isAuthenticated, async (req, res) => {
    const { recipientName, amount, type, reason } = req.body; // type: 'money' oder 'token'
    const senderId = new ObjectId(req.session.userId);
    const senderName = req.session.username;

    if (!recipientName || !amount || amount <= 0) return res.status(400).json({ error: "Ungültige Daten." });
    if (recipientName.toLowerCase() === senderName.toLowerCase()) return res.status(400).json({ error: "Keine Überweisung an sich selbst." });

    const client = db.client;
    const session = client.startSession();

    try {
        await session.withTransaction(async () => {
            // 1. Sender prüfen
            const sender = await usersCollection.findOne({ _id: senderId }, { session });
            const recipient = await usersCollection.findOne({ username: recipientName.toLowerCase() }, { session });

            if (!recipient) throw new Error("Empfänger nicht gefunden.");

            // 2. Guthaben prüfen & abziehen
            if (type === 'token') {
                if ((sender.tokens || 0) < amount) throw new Error("Nicht genügend Tokens.");
                await usersCollection.updateOne({ _id: senderId }, { $inc: { tokens: -amount } }, { session });
                await usersCollection.updateOne({ _id: recipient._id }, { $inc: { tokens: amount } }, { session });
            } else {
                // Geld (USD)
                if (sender.balance < amount && !sender.infinityMoney && !sender.isAdmin) throw new Error("Nicht genügend Guthaben.");
                
                // Bei Infinity Money wird nichts abgezogen, aber beim Empfänger draufgerechnet
                if (!sender.infinityMoney && !sender.isAdmin) {
                    await usersCollection.updateOne({ _id: senderId }, { $inc: { balance: -amount } }, { session });
                }
                await usersCollection.updateOne({ _id: recipient._id }, { $inc: { balance: amount } }, { session });
            }

            // 3. Loggen
            await bankTransactionsCollection.insertOne({
                fromId: senderId,
                fromName: senderName,
                toId: recipient._id,
                toName: recipient.username,
                amount: parseFloat(amount),
                type: type, // 'money' | 'token'
                reason: reason || "Überweisung",
                timestamp: new Date()
            }, { session });
        });

        res.json({ message: "Überweisung erfolgreich!" });
    } catch (e) {
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

// =========================================================
// === LIMO NEWS NETWORK (LNN) MIT GEMINI AI ===
// =========================================================

const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const NEWS_INTERVAL_MS = 30 * 60 * 1000; // Alle 30 Minuten prüfen

// Hilfsfunktion: Prüfen, ob im System was los war (letzte 30 Min)
async function checkSystemActivity() {
    const threshold = new Date(Date.now() - NEWS_INTERVAL_MS);
    
    // Prüfe die wichtigsten Collections auf neue Einträge
    const [newRatings, newOrders, newMessages, newPosts] = await Promise.all([
        ratingsCollection.countDocuments({ timestamp: { $gt: threshold } }),
        ordersCollection.countDocuments({ date: { $gt: threshold } }),
        limMessagesCollection.countDocuments({ timestamp: { $gt: threshold } }),
        dontBlameMeCollection.countDocuments({ createdAt: { $gt: threshold } })
    ]);

    return {
        active: (newRatings + newOrders + newMessages + newPosts) > 0,
        stats: { newRatings, newOrders, newMessages, newPosts }
    };
}

// Hilfsfunktion: Kontext für die AI sammeln
async function gatherNewsContext() {
    // Hole die allerneuesten Ereignisse für den Prompt
    const lastRating = await ratingsCollection.find().sort({ timestamp: -1 }).limit(1).toArray();
    const lastOrder = await ordersCollection.find().sort({ date: -1 }).limit(1).toArray();
    const lastPost = await dontBlameMeCollection.find().sort({ createdAt: -1 }).limit(1).toArray();
    
    let context = "Hier sind die aktuellen Ereignisse im Limo-Universum (eine Online-Community mit Shop, Bank und Schule):";
    
    if (lastRating.length > 0) {
        // Human Name holen (Join wäre besser, aber wir machen es simpel)
        const human = await humansCollection.findOne({ _id: lastRating[0].humanId });
        if(human) context += `\n- Im Human Grades System wurde "${human.name}" bewertet.`;
    }
    if (lastOrder.length > 0) {
        context += `\n- User "${lastOrder[0].username}" hat im Shop eingekauft (Wert: $${lastOrder[0].total.toFixed(2)}).`;
    }
    if (lastPost.length > 0) {
        context += `\n- Im 'Don't Blame Me' Bereich gab es ein Geständnis: "${lastPost[0].reason}".`;
    }
    
    return context;
}

// AI News Generator Job
if (GEMINI_API_KEY) {
    setInterval(async () => {
        try {
            console.log(`${LOG_PREFIX_SERVER} [LNN] Prüfe Aktivität für News...`);
            const activity = await checkSystemActivity();

            if (!activity.active) {
                console.log(`${LOG_PREFIX_SERVER} [LNN] Keine Aktivität. Keine News generiert.`);
                return;
            }

            console.log(`${LOG_PREFIX_SERVER} [LNN] Aktivität erkannt! Generiere News...`);
            const contextData = await gatherNewsContext();

            // Prompt an Gemini
            const prompt = `
                ${contextData}
                
                Aufgabe: Schreibe einen kurzen, witzigen News-Artikel (max. 40 Wörter) als "Breaking News" für das "Limo News Network". 
                Stil: Boulevard-Zeitung, etwas übertrieben oder satirisch.
                Format: JSON mit den Feldern "headline" und "content".
                Sprache: Deutsch.
            `;

            const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${GEMINI_API_KEY}`;
            const response = await axios.post(url, {
                contents: [{ parts: [{ text: prompt }] }]
            });

            const textResponse = response.data.candidates[0].content.parts[0].text;
            
            // Versuchen, JSON aus dem Text zu extrahieren (falls Gemini Markdown ```json drumrum macht)
            let jsonString = textResponse.replace(/```json/g, '').replace(/```/g, '').trim();
            const article = JSON.parse(jsonString);

            // In DB speichern
            await newsCollection.insertOne({
                headline: article.headline,
                content: article.content,
                author: "LNN AI Bot",
                category: "Community",
                createdAt: new Date(),
                likes: 0
            });

            console.log(`${LOG_PREFIX_SERVER} [LNN] News veröffentlicht: "${article.headline}"`);

        } catch (err) {
            console.error(`${LOG_PREFIX_SERVER} [LNN] Fehler bei News-Generierung:`, err.message);
        }
    }, NEWS_INTERVAL_MS);
} else {
    console.warn(`${LOG_PREFIX_SERVER} [LNN] Kein GEMINI_API_KEY gefunden. Auto-News deaktiviert.`);
}

// --- API ENDPOINTS ---

// News abrufen
app.get('/api/news', async (req, res) => {
    try {
        const news = await newsCollection.find({}).sort({ createdAt: -1 }).limit(20).toArray();
        res.json({ news });
    } catch (e) { res.status(500).json({ error: "Fehler." }); }
});

// News Liken (Trinkgeld-Feature könnte man hier anbauen)
app.post('/api/news/:id/like', isAuthenticated, async (req, res) => {
    try {
        await newsCollection.updateOne({ _id: new ObjectId(req.params.id) }, { $inc: { likes: 1 } });
        res.json({ message: "Geliked!" });
    } catch (e) { res.status(500).json({ error: "Fehler." }); }
});

// Admin: News manuell schreiben
app.post('/api/admin/news', isAuthenticated, isAdmin, async (req, res) => {
    const { headline, content } = req.body;
    if(!headline || !content) return res.status(400).json({error: "Fehlende Daten"});
    
    await newsCollection.insertOne({
        headline, content,
        author: req.session.username, // Echter Admin Name
        category: "Offiziell",
        createdAt: new Date(),
        likes: 0
    });
    res.json({ message: "Artikel veröffentlicht." });
});

app.use((req, res) => {
    console.warn(`${LOG_PREFIX_SERVER} Unbekannter Endpoint aufgerufen: ${req.method} ${req.originalUrl} von IP ${req.ip}`);
    res.status(404).send('Endpoint nicht gefunden');
});