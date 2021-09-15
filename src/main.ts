import { MongoClient, MongoOptions } from 'mongodb';
import { randomBytes } from 'crypto';
export default class MongoQueueManager {
  private mongoClient: MongoClient;
  private mongoDb: any;
  private mongoCollection: any;

  private options: MongoQueueManagerOptions = {
    visibilityTimeout: 30000,
    delay: 0,
    maxRetries: 5,
    deadQueueMessage: null,
  };

  constructor(
    connection: {
      url: string;
      db: string;
      collection: string;
      options?: MongoOptions;
    },
    options?: MongoQueueManagerOptions,
  ) {
    if (options) {
      this.log('Updating options.');
      this.options = { ...this.options, ...options };
    }

    this.log('Constructor init.');
    this.mongoClient = new MongoClient(connection.url, connection.options);
    this.log('MongoClinet created.');

    this.mongoClient.connect(async (err, client) => {
      this.log('Mongo client connecting...');

      if (err) {
        this.log('MongoClinet error.');
        throw new Error(`MongoClientError: ${err}`);
      }
      this.log('MongoClinet connected.');
      this.mongoDb = await client.db(connection.db);
      this.log(`MongoClinet db ${connection.db} selected.`);

      this.mongoCollection = await this.mongoDb.collection(
        connection.collection,
      );

      this.log(`MongoClinet collection ${connection.collection} selected.`);
    });
    console.log(this.getId());
    console.log(this.getTimestamp());
    console.log(this.getTimestampDelay(this.options.delay));
  }

  private getId(): string {
    this.log('Generate new ID.');
    return randomBytes(16).toString('hex');
  }

  private getTimestamp(): number {
    this.log('Generate new Timestamp.');
    return +new Date();
  }

  private getTimestampDelay(delay: number): number {
    this.log('Generate new Timestamp with Delay.');
    return +new Date() + delay;
  }

  private log(msg: any) {
    console.log(`\x1b[36m[MongoQueueManager]\x1b[0m `, msg);
  }

  public async push(payload: any, delay?: number) {
    this.log('Push new task to queue.');
    try {
      const visibleTime = this.getTimestampDelay(delay || this.options.delay);
      const queueData = [];
      this.log(
        `visibleTime: ${visibleTime}, delay: ${delay || this.options.delay}`,
      );

      if (payload instanceof Array) {
        this.log('Payload is Array');

        if (payload.length === 0) {
          this.log('Payload is Empty Array!');

          throw `Payload is an array but it doesn't contain elements`;
        }
        this.log('Prepare many payloads to push.');

        await payload.map(async (payload) => {
          await queueData.push({
            visible: visibleTime,
            payload: payload,
          });
        });
      } else {
        this.log('Prepare one payload to push.');

        await queueData.push({
          visible: visibleTime,
          payload: payload,
        });
      }
      this.log('Data ready.');
      const options = { ordered: true };
      const result = await this.mongoCollection.insertMany(queueData, options);
      return result.value;
    } catch (error) {
      console.log(error);
    }
    return;
  }

  public async pull(rawVisibilityTimeout?: number) {
    try {
      const visibilityTimeout = this.getTimestampDelay(
        rawVisibilityTimeout || this.options.visibilityTimeout,
      );
      const newId = this.getId();
      const query = {
        deleted: null,
        visible: { $lte: this.getTimestamp() },
      };
      const sort = {
        _id: 1,
      };
      const update = {
        $inc: { tries: 1 },
        $set: {
          ack: newId,
          visible: this.getTimestampDelay(visibilityTimeout),
        },
      };

      const result = await this.mongoCollection.findOneAndUpdate(
        query,
        update,
        {
          sort: sort,
          returnOriginal: false,
        },
      );
      return { ...result.value, ...{ ack: newId } };
    } catch (error) {
      console.log(error);
    }
  }

  public async mark(ack: string) {
    try {
      const query = {
        ack: ack,
        visible: { $gt: this.getTimestamp() },
        deleted: null,
      };
      const update = {
        $set: {
          deleted: this.getTimestamp(),
        },
      };

      const result = await this.mongoCollection.findOneAndUpdate(
        query,
        update,
        {
          returnOriginal: false,
        },
      );
      // console.log(result);
      return result.value;
    } catch (error) {
      console.log(error);
    }
  }

  public async ping(ack: string, rawVisibilityTimeout?: number) {
    try {
      const visibilityTimeout = this.getTimestampDelay(
        rawVisibilityTimeout || this.options.visibilityTimeout,
      );
      const query = {
        ack: ack,
        visible: { $gt: this.getTimestamp() },
        deleted: null,
      };
      const update = {
        $set: {
          visible: this.getTimestampDelay(visibilityTimeout),
        },
      };

      const result = await this.mongoCollection.findOneAndUpdate(
        query,
        update,
        {
          returnOriginal: false,
        },
      );
      // console.log(result);
      return result.value;
    } catch (error) {
      console.log(error);
    }
  }

  public async count() {
    try {
      const queryDeleted = {
        deleted: { $exists: true },
      };
      const queryProcesseing = {
        ack: { $exists: true },
        visible: { $gt: this.getTimestamp() },
        deleted: null,
      };
      const queryWaiting = {
        deleted: null,
        visible: { $lte: this.getTimestamp() },
      };
      const totalTasks = await this.mongoCollection.countDocuments();
      const deletedTasks = await this.mongoCollection.countDocuments(
        queryDeleted,
      );
      const processingTasks = await this.mongoCollection.countDocuments(
        queryProcesseing,
      );
      const queuedTasks = await this.mongoCollection.countDocuments(
        queryWaiting,
      );

      return { totalTasks, deletedTasks, processingTasks, queuedTasks };
    } catch (error) {
      console.log(error);
    }
  }

  public async clear() {
    try {
      const query = {
        deleted: { $exists: true },
      };

      const result = await this.mongoCollection.deleteMany(query);
      // console.log(result);
      return;
    } catch (error) {
      console.log(error);
    }
  }
}
export interface MongoQueueManagerOptions {
  visibilityTimeout?: number;
  delay?: number;
  maxRetries?: number;
  deadQueueMessage?: string;
}
