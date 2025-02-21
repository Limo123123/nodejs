const express = require('express');
const fs = require('fs');
const https = require('https');
const http = require('http');
const cors = require('cors');
const path = require('path');

const app = express();
const HTTP_PORT = 80;  // HTTP
const HTTPS_PORT = 443; // HTTPS

// Path to products.json in /var/www/
const PRODUCTS_FILE = '/var/www/products.json';

// Load SSL certificate
const privateKey = fs.readFileSync('/etc/letsencrypt/live/host.slimo.v6.rocks/privkey.pem', 'utf8');
const certificate = fs.readFileSync('/etc/letsencrypt/live/host.slimo.v6.rocks/fullchain.pem', 'utf8');
const credentials = { key: privateKey, cert: certificate };

// Middleware
app.use(cors({
  origin: ['https://git.slimo.v6.rocks', 'http://mexico-utilize.gl.at.ply.gg:18468'], // Allow frontend
  methods: ['GET', 'POST'], // Allow only necessary methods
  allowedHeaders: ['Content-Type']
}));

app.use(express.json()); // JSON body parser

// ✅ Ensure products.json exists
if (!fs.existsSync(PRODUCTS_FILE)) {
    fs.writeFileSync(PRODUCTS_FILE, JSON.stringify({ products: [] }, null, 2));
}

// ✅ Read products.json
const readProducts = () => JSON.parse(fs.readFileSync(PRODUCTS_FILE));

// ✅ Write to products.json
const writeProducts = (data) => fs.writeFileSync(PRODUCTS_FILE, JSON.stringify(data, null, 2));

// ✅ GET: Fetch all products
app.get('/api/products', (req, res) => {
    try {
        res.json(readProducts());
    } catch (error) {
        res.status(500).json({ error: "Error reading products file!" });
    }
});

// ✅ POST: Add a new product
app.post('/api/products', (req, res) => {
    const { name, image_url, price } = req.body;
    if (!name || !image_url || !price) {
        return res.status(400).json({ error: "All fields are required!" });
    }

    try {
        const data = readProducts();
        const newProduct = { id: Date.now(), name, image_url, price };
        data.products.push(newProduct);
        writeProducts(data);
        res.status(201).json({ message: "Product added!", product: newProduct });
    } catch (error) {
        res.status(500).json({ error: "Error writing to products file!" });
    }
});

// ✅ Start both HTTP & HTTPS servers
http.createServer(app).listen(HTTP_PORT, () => {
    console.log(`HTTP Server running on port ${HTTP_PORT}`);
});

https.createServer(credentials, app).listen(HTTPS_PORT, () => {
    console.log(`HTTPS Server running on port ${HTTPS_PORT}`);
});
