const express = require('express');
const app = express();
const port = 80;

let currentColor = '#FFFFFF'; // Standardfarbe: Weiß

// Middleware für JSON-Parsing
app.use(express.json());

// Endpunkt, um die aktuelle Farbe abzurufen
app.get('/api/limolights/color', (req, res) => {
  res.json({ color: currentColor });
});

// Endpunkt, um die Farbe zu setzen
app.post('/api/limolights/color', (req, res) => {
  const { color } = req.body;

  // Einfaches Validieren des Farbcodes (Hex-Code)
  if (typeof color === 'string' && /^#[0-9A-F]{6}$/i.test(color)) {
    currentColor = color;
    res.json({ message: 'Farbe erfolgreich gesetzt!', color: currentColor });
  } else {
    res.status(400).json({ message: 'Ungültiger Farbcode. Verwende ein Hex-Code (z. B. #FF5733).' });
  }
});

app.listen(port, () => {
  console.log(`Server läuft auf http://localhost:${port}`);
});
