import * as lib from './index';
import { Sequelize, Model, DataTypes } from 'sequelize';

const dbUrl = process.env.AURORA_TEST_DB_URL;

if (!process.env.CI) {
  console.log(dbUrl);
}

describe('Simulate raw postgres client', () => {
  test(
    'create table, insert and retrieve a record',
    async () => {
      const randomId = Math.random().toString(36).substr(2, 9);
      const client = new lib.Client(dbUrl);
      const options = client.dataApiClient.raw.postgresDataApiClientConfig();

      if (!process.env.CI) {
        console.log(options);
      }

      const sequelize = new Sequelize({
        ...(options as any),
        dialect: 'postgres',
        dialectModule: lib,
        dialectOptions: {
          statement_timeout: 2000,
          query_timeout: 2000,
        },
      });

      await sequelize.authenticate();

      class User extends Model {
        public id!: number; // Note that the `null assertion` `!` is required in strict mode.
        public name!: string;
        public age!: number | null; // for nullable fields
      }

      User.init(
        {
          id: {
            type: DataTypes.INTEGER.UNSIGNED,
            autoIncrement: true,
            primaryKey: true,
          },
          name: {
            type: new DataTypes.STRING(128),
            allowNull: false,
          },
          age: {
            type: new DataTypes.INTEGER(),
            allowNull: true,
          },
        },
        {
          tableName: 'users.' + randomId,
          freezeTableName: true,
          sequelize, // passing the `sequelize` instance is required
        },
      );

      await sequelize.sync({ force: true });

      // await User.destroy({ truncate: true });

      const newUser = await User.create({
        name: 'Johnny',
        age: 30,
      });
      console.log(newUser.id, newUser.name, newUser.age);

      const foundUser = await User.findOne({ where: { name: 'Johnny' } });
      expect(foundUser).toBeTruthy();
      expect(foundUser.name).toBe('Johnny');
      expect(foundUser.id).toBeGreaterThan(0);

      await sequelize.drop();
      await sequelize.close();
    },
    15 * 1000,
  );
});
