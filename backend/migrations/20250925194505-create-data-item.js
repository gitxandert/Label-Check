'use strict';
/** @type {import('sequelize-cli').Migration} */
module.exports = {
  async up(queryInterface, Sequelize) {
    await queryInterface.createTable('DataItems', {
      id: {
        allowNull: false,
        autoIncrement: true,
        primaryKey: true,
        type: Sequelize.INTEGER
      },
      originalLine: {
        type: Sequelize.TEXT
      },
      identifier: {
        type: Sequelize.STRING
      },
      originalLabelText: {
        type: Sequelize.TEXT
      },
      originalMacroText: {
        type: Sequelize.TEXT
      },
      isComplete: {
        type: Sequelize.BOOLEAN,
        allowNull: false,
        defaultValue: false
      },
      accessionId: {
        type: Sequelize.STRING,
        index: true
      },
      imageIdentifier: {
        type: Sequelize.STRING
      },
      createdAt: {
        allowNull: false,
        type: Sequelize.DATE
      },
      updatedAt: {
        allowNull: false,
        type: Sequelize.DATE
      }
    });
  },
  async down(queryInterface, Sequelize) {
    await queryInterface.dropTable('DataItems');
  }
};