require('dotenv').config();

const cors = require('cors');
const express = require('express');

const api_key_auth = require('./middleware/apiKeyAuth');
// const rt_auth = require('./controllers/auth');
const rt_apcms = require('./routes/apcms');

const port = process.env.PORT || 3021;
const app = express();

app.use(express.json());
app.use(cors());

// --- Test route for root path (New Health Check) ---
app.get('/', (req, res) => {
    res.status(200).json({
        status: 'OK',
        service: 'Backend API',
        message: 'The server is running successfully.'
    });
});

// Routes
app.use('/apcms', api_key_auth, rt_apcms);

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
