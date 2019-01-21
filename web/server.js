const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const path = require('path');

// init parameters
const app = express();
app.use(cors());
app.use(bodyParser.json());

// routes
app.get('/', function (req, res) {
  res.sendFile(path.join(__dirname+'/main.html'));
})

app.listen(3000, () => console.log(`Express server running on port 3000`));
