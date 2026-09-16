# External sources

## smoltcp development branch

Develop and test smoltcp inside its own repository. Kotoconn consumes the pinned
revision as a dependency; its workspace checks cover the integration.

Before editing the fork, initialize it and attach the persistent `kotoconn`
branch of [merrkry/smoltcp](https://github.com/merrkry/smoltcp):

```sh
git submodule update --init --recursive
git -C external/smoltcp switch kotoconn
git config submodule.external/smoltcp.update rebase
```

Use `git submodule update --remote external/smoltcp` to rebase onto the configured
remote branch. Test fork changes there and push to `origin/kotoconn` before
committing the updated gitlink here. Retain upstream history.

To reproduce this repository's pinned revision in an existing workspace:

```sh
git submodule update --checkout external/smoltcp
```

This detaches HEAD. Switch back to `kotoconn` before editing the fork again.
