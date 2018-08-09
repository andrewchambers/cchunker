# cchunker

This is a command that does content defined chunking on data piped into stdin.
Content chunking has the special property is that chunks will be shared across similar
data, this makes these chunks suitable for deduplicating backup programs.
with cchunker, what to do with the chunk is determined by a subcommand passed to cchunker.


# credits

https://github.com/restic/chunker/