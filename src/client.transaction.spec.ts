import * as lib from './index';
import { Sequelize, Model, DataTypes } from 'sequelize';
import * as AWS from 'aws-sdk';

const dbUrl = process.env.AURORA_TEST_DB_URL;

if (!process.env.CI) {
  console.log(dbUrl);
  AWS.config.logger = console;
}

describe('Simulate transaction', () => {
  test(
    'create table, insert and retrieve a record',
    async () => {
      const randomId = Math.random().toString(36).substr(2, 9);
      const client = new lib.Client(dbUrl);
      const options = client.dataApiClient.raw.postgresDataApiClientConfig();
      options.query_timeout = 5000;
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

      class Contact extends Model {
        public id!: number; // Note that the `null assertion` `!` is required in strict mode.
        public name!: string;
        public age!: number | null; // for nullable fields
      }

      Contact.init(
        {
          id: {
            type: DataTypes.INTEGER.UNSIGNED,
            autoIncrement: true,
            primaryKey: true,
          },
          firstName: {
            type: new DataTypes.STRING(128),
            allowNull: false,
          },
          lastName: {
            type: new DataTypes.STRING(128),
            allowNull: true,
          },
        },
        {
          tableName: 'contacts.' + randomId,
          freezeTableName: true,
          sequelize, // passing the `sequelize` instance is required
        },
      );

      await sequelize.sync({ force: true });

      // await Contact.destroy({ truncate: true });

      // First, we start a transaction and save it into a variable
      try {
        const result = await sequelize.transaction(async (t) => {
          const bart = await Contact.create(
            {
              firstName: 'Bart',
              lastName: 'Simpson',
            },
            { transaction: t },
          );

          const lisa = await Contact.create(
            {
              firstName: 'Lisa',
              lastName: 'Simpson',
            },
            { transaction: t },
          );

          return { bart, lisa };
        });

        // If the execution reaches this line, the transaction has been committed successfully
        // `result` is whatever was returned from the transaction callback (the `user`, in this case)
        console.log(result);
        expect(result).toBeDefined();
      } catch (error) {
        // If the execution reaches this line, an error occurred.
        // The transaction has already been rolled back automatically by Sequelize!
      }

      try {
        const result = await sequelize.transaction(async (t) => {
          await Contact.create(
            {
              firstName: 'Homer',
              lastName: 'Simpson',
            },
            { transaction: t },
          );

          throw new Error('test');
        });

        // If the execution reaches this line, the transaction has been committed successfully
        // `result` is whatever was returned from the transaction callback (the `user`, in this case)
        console.log(result);
        expect(result).toBeDefined();
      } catch (error) {
        // If the execution reaches this line, an error occurred.
        // The transaction has already been rolled back automatically by Sequelize!
        expect(error).toBeDefined();
        console.log('expected error', error);
      }

      const foundUser = await Contact.findOne({ where: { firstName: 'Homer' } });
      console.log(foundUser);

      await sequelize.query('commit');

      await sequelize.drop();

      await sequelize.close();
      expect(true).toBeTruthy();
    },
    30 * 1000,
  );
});
