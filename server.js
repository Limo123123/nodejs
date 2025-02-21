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

async function deleteProductById() {
    const productId = document.getElementById('delete-product-id').value.trim();

    if (!productId.match(/^\d{6}$/)) {
        alert("Please enter a valid 6-digit Product ID.");
        return;
    }

    if (!confirm(`Are you sure you want to delete product with ID ${productId}?`)) return;

    try {
        const response = await fetch(`${getApiBaseUrl()}/products/${productId}`, {
            method: 'DELETE'
        });

        if (response.ok) {
            alert("Product deleted successfully!");
            document.getElementById('delete-product-id').value = ""; // Clear input field
            loadProducts(); // Refresh product list
        } else {
            alert("Product not found or could not be deleted.");
        }
    } catch (error) {
        console.error("Error deleting product:", error);
    }
}

app.delete('/api/products/:id', (req, res) => {
    const productId = parseInt(req.params.id); // Convert to number
    let data = readProducts();

    // ✅ Only allow deleting products with a 6-digit ID
    if (!/^\d{6}$/.test(req.params.id)) {
        return res.status(400).json({ error: "Invalid Product ID format!" });
    }

    // Find the product by ID
    const productIndex = data.products.findIndex(product => product.id === productId);
    if (productIndex === -1) {
        return res.status(404).json({ error: "Product not found!" });
    }

    // Remove the product from the list
    data.products.splice(productIndex, 1);
    writeProducts(data);

    res.json({ message: "Product deleted successfully!" });
});


// Middleware
app.use(cors()); // Enable CORS
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
    let { name, image_url, price } = req.body;

    if (!name || !image_url || !price) {
        return res.status(400).json({ error: "All fields are required!" });
    }

    // ✅ Ensure price starts with "$"
    price = price.trim();
    if (!price.startsWith('$')) {
        price = `$${price}`;
    }

    // ✅ Ensure price is a valid number (after removing "$")
    const numericPrice = parseFloat(price.replace(/[^0-9.]/g, ''));
    if (isNaN(numericPrice) || numericPrice < 0) {
        return res.status(400).json({ error: "Invalid price! Enter a valid number." });
    }

    const data = readProducts();

    // ✅ Generate a unique 6-digit ID
    let newId;
    do {
        newId = Math.floor(100000 + Math.random() * 900000); // Random 6-digit number
    } while (data.products.some(product => product.id === newId)); // Ensure ID is unique

    const newProduct = { id: newId, name, image_url, price }; // ✅ Save price with "$"

    data.products.push(newProduct);
    writeProducts(data);

    res.status(201).json({ message: "Product added!", product: newProduct });
});


// ✅ Start both HTTP & HTTPS servers
http.createServer(app).listen(HTTP_PORT, () => {
    console.log(`HTTP Server running on port ${HTTP_PORT}`);
});

https.createServer(credentials, app).listen(HTTPS_PORT, () => {
    console.log(`HTTPS Server running on port ${HTTPS_PORT}`);
});
