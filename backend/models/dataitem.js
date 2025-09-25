'use strict';
const {
  Model
} = require('sequelize');
module.exports = (sequelize, DataTypes) => {
  class DataItem extends Model {
    /**
     * Helper method for defining associations.
     * This method is not a part of Sequelize lifecycle.
     * The `models/index` file will call this method automatically.
     */
    static associate(models) {
      DataItem.hasMany(models.DataItemVersion, { foreignKey: 'dataItemId' });
      DataItem.hasOne(models.QueueItem, { foreignKey: 'dataItemId' });
    }
  }
  DataItem.init({
    originalLine: DataTypes.TEXT,
    identifier: DataTypes.STRING,
    originalLabelText: DataTypes.TEXT,
    originalMacroText: DataTypes.TEXT,
    accessionId: DataTypes.STRING,
    stain: DataTypes.STRING,
    blockNumber: DataTypes.STRING,
    isComplete: DataTypes.BOOLEAN,
    imageIdentifier: DataTypes.STRING
  }, {
    sequelize,
    modelName: 'DataItem',
  });
  return DataItem;
};