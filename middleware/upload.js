const multer = require('multer');
const path = require('path');

// Define storage settings
const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        let uploadFolder = 'others/';

        // Set folder based on file type
        if (file.mimetype.startsWith('image')) uploadFolder = 'images/';
        else if (file.mimetype.startsWith('video')) 
            uploadFolder = 'videos/';
        else if (file.mimetype.startsWith('application')) 
            uploadFolder = 'docs/';
        
console.log(uploadFolder);
        cb(null, '/home/onlinetn/inspection/uploads/inspection/'+uploadFolder);
    },
    filename: (req, file, cb) => {
        const fileExt = path.extname(file.originalname);
        const fileName = `${Date.now()}-${file.originalname.replace(/\s/g, '_')}`;
        cb(null, fileName);
    }
});

// File filter to allow specific types
const fileFilter = (req, file, cb) => {
    const allowedTypes = ['image/jpeg', 'image/png', 'image/gif', 'video/mp4', 'video/mpeg', 'application/pdf'];
    if (allowedTypes.includes(file.mimetype)) {
        cb(null, true);
    } else {
        cb(new Error('Invalid file type. Only images, videos, and PDFs are allowed.'));
    }
};

const upload = multer({ storage, fileFilter });

module.exports = upload;
