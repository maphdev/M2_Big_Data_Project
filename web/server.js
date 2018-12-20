const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');

// init parameters
const app = express();
app.use(cors());
app.use(bodyParser.json());

// routes
app.get('/', function (req, res) {
  res.send('Hello World!')
})

app.listen(3000, () => console.log(`Express server running on port 3000`));
