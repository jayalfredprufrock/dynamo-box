# dynamo-box

Dynamo-box leverages TypeBox to provide a strongly typed repository client for interacting with DynamoDb tables. **Remain safe in the box.**

```sh
npm i dynamo-box @sinclair/typebox @typemon/dynamon
```

## Usage

### Todo

-   transformer support projected GSIs
-   logging for batch operations
-   output transforms for batch write
-   transact write/get items support
-   modify return type based on filter projection
-   paginated variants of scan/query
-   strongly typed equality filter support
-   explore integrating mongodb-style conditional expressions
    for safer typings and less reliance on dynamon.
-   alternatively, provide strongly-typed "condition builder"
-   ability to create tables (or definitions) based on config
-   prevent specifying same property for both partition/sort key at compile time (is that even possible?)
-   prevent specifying extraneous properties in projection array
