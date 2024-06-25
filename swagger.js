const swaggerUi = require('swagger-ui-express');
const swaggerSpec = require('./swaggerConfig');

const setupSwagger = (app) => {
  app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerSpec));
  console.log('Swagger docs available at /api-docs');
};

module.exports = setupSwagger;
