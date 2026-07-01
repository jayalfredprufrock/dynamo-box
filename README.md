# dynamo-box

> [!WARNING] > **This project has been superseded by [dinah](https://github.com/jayalfredprufrock/dinah), a new and improved library.**
> There will be no further development in this repo. Please migrate to [dinah](https://github.com/jayalfredprufrock/dinah).

Dynamo-box leverages [TypeBox](https://github.com/sinclairzx81/typebox) to provide a strongly typed repository client for interacting with DynamoDb tables. **Remain safe in the box.**

This library also currently leverages [Dynamon](https://gitlab.com/monster-space-network/typemon/dynamon) for marshalling and building expressions, so be sure to visit their docs for additional usage information, especially in regards to conditional/update expressions.

```sh
npm i @jayalfredprufrock/dynamo-box typebox @typemon/dynamon
```

## Basic Usage

Consider the following schema that represents a user:

```typescript
import { Static, Type } from 'typebox';

export const UserSchema = Type.Object({
    id: Type.String(),
    name: Type.String(),
    email: Type.String(),
    createdAt: Type.Integer(),
    updatedAt: Type.Integer(),
});
```

Use `makeDdbRepository()` to create a fully-typed repository class:

```typescript
import { makeDdbRepository } from '@jayalfredprufrock/dynamo-box';

export class UserRepository = makeDdbRepository(UserSchema)({
    client: { region: 'us-east-1', endpoint: 'localhost' },
    tableName: 'users',
    partitionKey: 'id',
    transformInput: (input: UserInput) => {
        return {
            id: randomString(16),
            createdAt: Date.now(),
            ...input,
            updatedAt: Date.now(),
        };
    },
    gsis: {
        keysByEmail: {
            partitionKey: 'email',
            projection: 'KEYS',
        },
    },
});

const userRepo = new UserRepository();

// does a PUT, but throws if id already exists
const newUser = await userRepo.create({
    name: 'Dynamo Box',
    email: 'dynamo@box.com'
});

// last param of all methods allow advanced configuration
const user = await userRepo.getOrThrow({ id: newUser.id }, { consistentRead: true });

```

The class can also be extended:

```typescript
export class UserRepositoryExtended extends UserRepository {
    constructor() {
        super({
            tableName: usersTableName,
            client: new DynamoDBClient({ region: 'us-east-1', endpoint: 'localhost' }),
        });
    }

    async getByEmail(userEmail: string): Promise<User> {
        const email = userEmail.toLowerCase();

        const [userKeys] = await this.queryGsi('keysByEmail', { email });
        if (!userKeys) {
            throw new Error('User not found');
        }
        return this.getOrThrow({ id: userKeys.id });
    }
}
```

## Docs

Library is nearing 1.0 release, at which point full documentation will become available.
It is currently being used in production environments so I welcome brave community members to give it a spin in its current state. I don't suspect any major API changes at this point and I do my absolute best to respect SemVer.

---

### Wishlist

-   transact write/get items support
-   make special case for TTL field, support function that automatically sets it based on data
-   count() method that automatically projects partition key only and returns number
-   make it easier to override methods, potentially by not producing an anonymous class
-   automatically remove keys from update expressions? potentially anything "readonly" as well
-   getGsi() and getGsiOrThrow() that fetches one record
-   custom events (configurable logic, i.e. status change events)
-   safe "transform" operation that performs a GET + PUT (with transform function) that uses consistent read, conditional
    expression + automatic retry
