const swaggerJSDoc = require('swagger-jsdoc');

const options = {
  definition: {
    openapi: '3.0.0',
    info: {
      title: 'Synthetix Cache API',
      version: '1.0.0',
      description: 'API documentation for the Synthetix Cache API',
    },
    servers: [
      {
        url: 'https://synthetix-cache-api-f7db01015ec1.herokuapp.com/'
      },
    ],
  },
  apis: ['./routes/*.js'], 
};

const specs = swaggerJSDoc(options);

module.exports = specs;
