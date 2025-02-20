const express = require('express');
const app = express();
const fs = require('fs');
const https = require('https');
const http = require('http');
const port = 443;

// Lade das SSL-Zertifikat und den privaten Schlüssel 
const privateKey = fs.readFileSync('/etc/letsencrypt/live/host.slimo.v6.rocks/privkey.pem', 'utf8');
const certificate = fs.readFileSync('/etc/letsencrypt/live/host.slimo.v6.rocks/fullchain.pem', 'utf8');
const credentials = { key: privateKey, cert: certificate };

// Standardfarbe
let currentColor = '#FFFFFF'; 

// Middleware für JSON-Parsing
app.use(express.json());

// Endpunkt, um die aktuelle Farbe abzurufen
app.get('/api/limolights/color', (req, res) => {
  res.json({ color: currentColor });
});

// Endpunkt, um die Farbe zu setzen
app.post('/api/limolights/color', (req, res) => {
  const { color } = req.body;

  // Validierung des Hex-Farbcodes
  if (typeof color === 'string' && /^#[0-9A-F]{6}$/i.test(color)) {
    currentColor = color;
    res.json({ message: 'Farbe erfolgreich gesetzt!', color: currentColor });
  } else {
    res.status(400).json({ message: 'Ungültiger Farbcode. Verwende ein Hex-Code (z. B. #FF5733).' });
  }
});

// HTTPS Server starten
https.createServer(credentials, app).listen(port, () => {
  console.log(`Server läuft auf https://localhost:${port}`);
});

// HTTP Server für Weiterleitung auf HTTPS (optional)
http.createServer((req, res) => {
  res.writeHead(301, { "Location": `https://${req.headers.host}${req.url}` });
  res.end();
}).listen(80, () => {
  console.log('HTTP-Server läuft auf http://localhost:80');
});
