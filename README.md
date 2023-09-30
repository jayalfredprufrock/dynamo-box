# dynamo-box

Dynamo-box leverages TypeBox to provide a strongly typed repository client for interacting with DynamoDb tables. **Remain safe in the box.**

```sh
npm i @jayalfredprufrock/dynamo-box @sinclair/typebox @typemon/dynamon
```

## Usage

### Todo

-   update() not transforming input, thus not getting updatedAt
-   atomic field (optimistic lock) support that automatically generates condition expression based on something like updatedAt or version
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
-   use special schema field to indicate basic mapping type and generate mapping automatically
-   handle transformInput a little better so its obvious that the input is providing defaults.
    will probably involve specifying a transformInput schema (which can also provide runtime type safety)
    or switching to specifying default/readonly fields for create/update
-   make special case for TTL field, support function that automatically sets it based on da
-   should explictly passed undefined values to UPDATE be converted into REMOVE operations?
