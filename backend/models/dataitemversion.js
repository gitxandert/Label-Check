'use strict';
const {
  Model
} = require('sequelize');
module.exports = (sequelize, DataTypes) => {
  class DataItemVersion extends Model {
    /**
     * Helper method for defining associations.
     * This method is not a part of Sequelize lifecycle.
     * The `models/index` file will call this method automatically.
     */
    static associate(models) {
      DataItemVersion.belongsTo(models.DataItem, { foreignKey: 'dataItemId' });
      DataItemVersion.belongsTo(models.User, { as: 'changedByUser', foreignKey: 'changedByUserId' });
    }
  }
  DataItemVersion.init({
    dataItemId: DataTypes.INTEGER,
    version: DataTypes.INTEGER,
    accessionId: DataTypes.STRING,
    stain: DataTypes.STRING,
    blockNumber: DataTypes.STRING,
    isComplete: DataTypes.BOOLEAN,
    changedByUserId: DataTypes.INTEGER
  }, {
    sequelize,
    modelName: 'DataItemVersion',
  });
  return DataItemVersion;
};