# External sources

## smoltcp development branch

Maintain `external/smoltcp` on the persistent `kotoconn` branch of
[merrkry/smoltcp](https://github.com/merrkry/smoltcp). `.gitmodules` selects this
branch and uses rebase updates. Fork behavior and tests belong in the
[consumer crate documentation](../crates/tun/README.md#smoltcp-fork).

Initialize the submodule and attach its development branch once per workspace:

```sh
git submodule update --init --recursive
git -C external/smoltcp switch kotoconn
```

Existing workspaces must also update their initialized local configuration;
`.gitmodules` does not overwrite it:

```sh
git config submodule.external/smoltcp.update rebase
```

Fetch the configured remote branch and rebase local commits onto it with:

```sh
git submodule update --remote external/smoltcp
```

Commit fork changes on `kotoconn` and push normally to `origin/kotoconn`. Publish
those commits before committing the updated `external/smoltcp` gitlink in the
parent repository. Retain upstream history and keep fork changes focused.

The gitlink records the exact dependency revision even though development tracks
a branch. Fresh initialization checks out that revision. To reproduce a recorded
revision in an existing workspace, explicitly request checkout:

```sh
git submodule update --checkout external/smoltcp
```

That command intentionally detaches HEAD. Switch back to `kotoconn` before
resuming fork development.
