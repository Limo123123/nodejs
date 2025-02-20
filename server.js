const express = require("express");
const app = express();

// Middleware für JSON-Parsing
app.use(express.json());

// Einfache API-Route
app.get("/api/hello", (req, res) => {
  res.json({ message: "👋 Hallo von der Node.js API!" });
});

// Starte den Server auf Port 3000
const PORT = 81;
app.listen(PORT, () => console.log(`🚀 API läuft auf http://localhost:${PORT}`));
