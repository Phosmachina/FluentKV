# Changelog

## Vnext

### Feature

- In memory key management (in place of in-database).
  - Remove useless `CleanUnusedKey`: db is now analyzed at opening via `NewAbstractRelDB`
- Parallelized operations:
  - New helper to make a pool of tasks.
  - `Foreach` use it to permit for creation of `Collection` for example.

### Test

- Add `Test_AutoKey` to check auto key feature and analyze at the re-opening database.
- Add some benchmark for general operation:
  - `InsertGet_SimpleType`
  - `Foreach`
  - `NewCollection`

### Build

- Remove `golog` dependency.

## v0.1.3

> The initial tag published.
