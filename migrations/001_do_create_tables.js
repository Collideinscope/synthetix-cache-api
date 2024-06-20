const fs = require('fs');
const path = require('path');

const upSql = fs.readFileSync(path.join(__dirname, '001.do.create_tables.sql'), 'utf-8');

const downSql = fs.readFileSync(path.join(__dirname, '001.undo.create_tables.sql'), 'utf-8');

exports.up = function(knex) {
    return knex.raw(upSql);
};

exports.down = function(knex) {
    return knex.raw(downSql);
};
