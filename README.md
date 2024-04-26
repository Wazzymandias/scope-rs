# scope-rs

> "We are part of a universe that is a work of art. We are the notes of the music of the spheres."
> - Sol Weintraub, Hyperion

`scope-rs` is a command-line utility for searching and exploring [Hubs](https://docs.farcaster.xyz/learn/architecture/hubs) on the [Farcaster](https://www.farcaster.xyz/) protocol. 

## Build
```shell
cargo build --release
```

## Usage 
```shell
Usage: scope-rs [OPTIONS] [COMMAND]

Commands:
  info
  diff
  peers
  sync-metadata
  sync-snapshot
  help           Print this message or the help of the given subcommand(s)

Options:
      --http
      --https
      --port <PORT>  [default: 2283]
  -h, --help         Print help
```
