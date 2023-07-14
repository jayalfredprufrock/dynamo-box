# dynamo-box

Dynamo-box leverages TypeBox to provide a strongly typed repository client for interacting with DynamoDb tables. **Remain safe in the box.**

```sh
npm i @jayalfredprufrock/dynamo-box @sinclair/typebox @typemon/dynamon
```

## Usage

### Todo

-   transformer support projected GSIs
-   transact write/get items support
-   modify return type based on filter projection
-   strongly typed equality filter support
-   explore integrating mongodb-style conditional expressions
    for safer typings and less reliance on dynamon.
-   alternatively, provide strongly-typed "condition builder"
-   ability to create tables (or definitions) based on config
-   createOrUpdate() and make put() require _complete_ type object?
-   utility to create projectionFilter from schema
