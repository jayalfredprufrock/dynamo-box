# dynamo-box

Dynamo-box leverages [TypeBox](https://github.com/sinclairzx81/typebox) to provide a strongly typed repository client for interacting with DynamoDb tables. **Remain safe in the box.**

This library also currently leverages [Dynamon](https://gitlab.com/monster-space-network/typemon/dynamon) for marshalling and building expressions, so be sure to visit their docs for additional usage information, especially in regards to conditional/update expressions.

```sh
npm i @jayalfredprufrock/dynamo-box @sinclair/typebox @typemon/dynamon
```

## Basic Usage

Consider the following schema that represents a user:

```typescript
import { Static, Type } from '@sinclair/typebox';

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

-   update() not transforming input, thus not getting updatedAt
-   create() not transforming output
-   expression based update() not transforming input. Are timestamps the only use case for that?
-   should a distinction be made between "defaults" and "generated/computed" values?
-   leverage typebox decoder/encoder for transform support
-   update() should take array of expressions and not require wrapping in dynamon update()
-   atomic field (optimistic lock) support that automatically generates condition expression based on something like updatedAt or version
-   transformer support projected GSIs
-   transact write/get items support
-   modify return type based on filter projection
-   strongly typed equality filter support
-   transformInput/Output async support
-   explore integrating mongodb-style conditional expressions
    for safer typings and less reliance on dynamon.
-   alternatively, provide strongly-typed "condition builder"
-   ability to create tables (or definitions) based on config
-   createOrUpdate() and make put() require _complete_ type object?
-   update() support nested path syntax for simple updates
-   utility to create projectionFilter from schema
-   use special schema field to indicate basic mapping type and generate mapping automatically
-   handle transformInput a little better so its obvious that the input is providing defaults.
    will probably involve specifying a transformInput schema (which can also provide runtime type safety)
    or switching to specifying default/readonly fields for create/update
-   make special case for TTL field, support function that automatically sets it based on data
-   remove undefined values from update expression object values
-   bulk operations that perform non-batch operations in parallell (for conditions and consistent logging interface)
-   allow typebox schemas for projection and filter conditions
-   count() method that automatically projects partition key only and returns number
-   isEmpty() method that counts (with limit of 1) and returns true/false
-   make it easier to override methods, potentially by not producing an anonymous class
-   paged versions of scanGsi and queryGsi
-   transformOutput should not be run when a projectionExpression is used
-   automatically remove keys from update expressions? potentially anything "readonly" as well
-   getGsi() and getGsiOrThrow() that fetches one record
-   get() (optionally gsi variants) that allow a simple condition expression and pretends like
    document doesn't exist if condition fails (useful for permission stuff)
-   should event item object be transformed?
