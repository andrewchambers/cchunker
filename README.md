# cchunker

This is a command that does content defined chunking on data piped into stdin.
Content chunking has the special property is that chunks will be shared across similar
data, this makes these chunks suitable for deduplicating backup programs.
with cchunker, what to do with the chunk is determined by a subcommand passed to cchunker.

# multicchunker

This command is similar to cchunker except it expects the subcommand to output one line per chunk processed, 
multicchunker is the equivalent of running cchunker on the data repeatedly on the previous
output streams until only a single line is printed. Each nested stream is prefixed with a single line with the iteration number.

using multicchunker you can collapse a large file into a single summary, built as a tree of
keys, it is intended as a building block for a backuptool.

# TODO

deduplicate documentation in readme and individual commands

# credits

https://github.com/restic/chunker/