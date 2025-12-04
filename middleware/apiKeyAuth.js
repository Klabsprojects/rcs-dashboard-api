
const EXTERNAL_API_KEY = process.env.EXTERNAL_API_KEY; 

const apiKeyAuth = (req, res, next) => {
    // 1. Check for the header where the key is expected (e.g., 'x-api-key')
    const apiKey = req.header('x-api-key');
    
    // 2. Log or check if the key is present
    if (!apiKey) {
        return res.status(401).json({ 
            error: true, 
            message: 'Access Denied: API Key missing. Please provide an X-API-Key header.' 
        });
    }

    // 3. Compare the provided key with the expected secret key
    if (apiKey === EXTERNAL_API_KEY) {
        // Key is valid, proceed to the route handler
        next();
    } else {
        // Key is invalid
        return res.status(401).json({ 
            error: true, 
            message: 'Access Denied: Invalid API Key.' 
        });
    }
};

module.exports = apiKeyAuth;