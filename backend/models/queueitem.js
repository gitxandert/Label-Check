'use strict';
const {
  Model
} = require('sequelize');
module.exports = (sequelize, DataTypes) => {
  class QueueItem extends Model {
    /**
     * Helper method for defining associations.
     * This method is not a part of Sequelize lifecycle.
     * The `models/index` file will call this method automatically.
     */
    static associate(models) {
      QueueItem.belongsTo(models.DataItem, { foreignKey: 'dataItemId' });
      QueueItem.belongsTo(models.User, { as: 'leasedBy', foreignKey: 'leasedById' });
      QueueItem.belongsTo(models.User, { as: 'completedBy', foreignKey: 'completedById' });
    }
  }
  QueueItem.init({
    dataItemId: {
      type: DataTypes.INTEGER,
      allowNull: false,
      unique: true,
    },
    status: {
      type: DataTypes.ENUM('pending', 'leased', 'completed'),
      allowNull: false,
      defaultValue: 'pending',
    },
    leasedById: DataTypes.INTEGER,
    leasedAt: DataTypes.DATE,
    completedById: DataTypes.INTEGER,
    completedAt: DataTypes.DATE
  }, {
    sequelize,
    modelName: 'QueueItem',
  });
  return QueueItem;
};