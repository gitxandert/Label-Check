
require('dotenv').config();
const express = require('express');
const cors = require('cors');

const passport = require('passport');

const app = express();

// Passport middleware
app.use(passport.initialize());

// Passport Config
require('./config/passport')(passport);

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Static file serving for images
app.use('/images/label', express.static('../Data/label'));
app.use('/images/macro', express.static('../Data/macro'));

// Basic health check route
app.get('/', (req, res) => {
  res.send('Label-Check Backend API is running...');
});

// API Routes
app.use('/api/auth', require('./routes/authRoutes'));
app.use('/api/queue', require('./routes/queueRoutes'));
app.use('/api/items', require('./routes/dataRoutes'));
app.use('/api/users', require('./routes/userRoutes'));

module.exports = app;
