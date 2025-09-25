const { sequelize, User, DataItem, DataItemVersion, QueueItem } = require('../models');

describe('Database Models', () => {
  beforeAll(async () => {
    await sequelize.sync({ force: true });
  });

  afterAll(async () => {
    await sequelize.close();
  });

  describe('User Model', () => {
    it('should create a user with a hashed password', async () => {
      const user = await User.create({ username: 'testuser', password: 'password123' });
      expect(user.id).toBe(1);
      expect(user.username).toBe('testuser');
      expect(user.passwordHash).not.toBe('password123');
    });

    it('should verify a valid password', async () => {
      const user = await User.findOne({ where: { username: 'testuser' } });
      const isValid = await user.verifyPassword('password123');
      expect(isValid).toBe(true);
    });

    it('should have default values', async () => {
      const user = await User.findOne({ where: { username: 'testuser' } });
      expect(user.correctionCount).toBe(0);
      expect(user.isAdmin).toBe(false);
    });
  });

  describe('Model Associations', () => {
    let user, dataItem;

    it('should create associated models correctly', async () => {
      user = await User.create({ username: 'assoc_user', password: 'password123' });
      dataItem = await DataItem.create({ originalLine: 'test line' });

      expect(user.id).toBe(2);
      expect(dataItem.id).toBe(1);
    });

    it('should create a DataItemVersion associated with a DataItem and a User', async () => {
      const version = await DataItemVersion.create({
        dataItemId: dataItem.id,
        changedByUserId: user.id,
        version: 1,
        accessionId: 'ABC-123'
      });

      const retrievedVersion = await DataItemVersion.findOne({
        where: { id: version.id },
        include: ['changedByUser', DataItem]
      });

      expect(retrievedVersion.DataItem.id).toBe(dataItem.id);
      expect(retrievedVersion.changedByUser.id).toBe(user.id);
    });

    it('should create a QueueItem associated with a DataItem and Users', async () => {
      const queueItem = await QueueItem.create({
        dataItemId: dataItem.id,
        status: 'leased',
        leasedById: user.id,
        leasedAt: new Date(),
      });

      const retrievedQueueItem = await QueueItem.findOne({
        where: { id: queueItem.id },
        include: [DataItem, 'leasedBy']
      });

      expect(retrievedQueueItem.DataItem.id).toBe(dataItem.id);
      expect(retrievedQueueItem.leasedBy.id).toBe(user.id);
      expect(retrievedQueueItem.status).toBe('leased');
    });

    it('should enforce the one-to-one relationship between DataItem and QueueItem', async () => {
      // Attempting to create another queue item for the same dataItem should fail
      await expect(
        QueueItem.create({ dataItemId: dataItem.id })
      ).rejects.toThrow(); // Throws because of the unique constraint on dataItemId
    });
  });
});