package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"

	"github.com/restic/chunker"
)

func usage() {
	fmt.Fprintln(os.Stderr, "This is a command that does content defined chunking on data piped into stdin.")
	fmt.Fprintln(os.Stderr, "Content chunking has the special property is that chunks will be shared across similar")
	fmt.Fprintln(os.Stderr, "data, this makes these chunks suitable for deduplicating backup programs.")
	fmt.Fprintln(os.Stderr, "with cchunker, what to do with the chunk is determined by a subcommand passed to cchunker.")
	fmt.Fprintln(os.Stderr, "\n")
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "cchunker [-flags...] CHUNK PROCESSOR")
	fmt.Fprintln(os.Stderr, "CHUNK PROCESSOR is a command+arguments that reads the chunk data on stdin and does an arbitrary action.")
	fmt.Fprintln(os.Stderr, "The default are chunks with a min size 512 KiB, max size 16 MiB and and average of 4MiB")
	fmt.Fprintln(os.Stderr, "On any IO or subprocess errors, cchunker exits with a non zero exit code.")
	flag.PrintDefaults()
	os.Exit(1)
}

func main() {
	flag.Usage = usage

	newPolynomial := flag.Bool("new-polynomial", false, "generate a new chunking polynomial, print it on stdout and exit")
	checkPolynomial := flag.Bool("check-polynomial", false, "check if the given polynomial is suitable for content chunking")
	smallChunks := flag.Bool("small-chunks", false, "change to a min size 512 KiB, max size 16 MiB and and average of 4MiB")
	largeChunks := flag.Bool("large-chunks", false, "change to a min size 1 MiB, max size 32 MiB and and average of 8MiB")
	polynomialInt := flag.Uint64("polynomial", 0x3DA3358B4DC173, "polynomial to use for content defined chunking, should be generated via -new-polynomial")

	flag.Parse()

	polynomial := chunker.Pol(*polynomialInt)

	if *newPolynomial {
		p, err := chunker.RandomPolynomial()
		if err != nil {
			fmt.Fprintf(os.Stderr, "unable to generate polynomial: %s\n", err)
			os.Exit(1)
		}

		_, err = fmt.Printf("%d\n", uint64(p))
		if err != nil {
			fmt.Fprintf(os.Stderr, "unable to print polynomial: %s\n", err)
			os.Exit(1)
		}
		return
	}

	if *checkPolynomial {
		if !polynomial.Irreducible() {
			fmt.Fprintf(os.Stderr, "polynomial is not irreducible, it is not suitable for content chunking\n")
			os.Exit(1)
		}
		return
	}

	cmdArgs := flag.Args()

	if len(cmdArgs) == 0 {
		usage()
	}

	var cchunker *chunker.Chunker

	const (
		kiB = 1024
		miB = 1024 * kiB

		SmallMinSize = 512 * kiB
		SmallMaxSize = 8 * miB
		// This number is a bit mask that determins chunking with probabilty,
		// (assuming the fingerprint of bytes coming in are random)
		// >>> int('0b' + '1' * 20, base=2)
		// one out of every ~ 1 million will split.
		SmallBits = 20

		StandardMinSize = 512 * kiB
		StandardMaxSize = 16 * miB
		// This number is a bit mask that determins chunking with probabilty,
		// (assuming the fingerprint of bytes coming in are random)
		// >>> int('0b' + '1' * 22, base=2)
		// one out of every 4 million will split.
		StandardBits = 22

		LargeMinSize = 1024 * kiB
		LargeMaxSize = 32 * miB
		// This number is a bit mask that determins chunking with probabilty,
		// (assuming the fingerprint of bytes coming in are random)
		// >>> int('0b' + '1' * 22, base=2)
		// one out of every 8 million will split.
		LargeBits = 23

		chunkerBufSize = 512 * kiB
	)
	// reuse this buffer
	var buf []byte

	if *smallChunks {
		cchunker = chunker.NewWithBoundaries(os.Stdin, polynomial, SmallMinSize, SmallMaxSize)
		cchunker.SetAverageBits(SmallBits)
		buf = make([]byte, SmallMaxSize)
	} else if *largeChunks {
		cchunker = chunker.NewWithBoundaries(os.Stdin, polynomial, LargeMinSize, LargeMaxSize)
		cchunker.SetAverageBits(LargeBits)
		buf = make([]byte, LargeMaxSize)
	} else {
		cchunker = chunker.NewWithBoundaries(os.Stdin, polynomial, StandardMinSize, StandardMaxSize)
		cchunker.SetAverageBits(StandardBits)
		buf = make([]byte, StandardMaxSize)
	}

	for {
		chunk, err := cchunker.Next(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "error getting next data chunk: %s\n", err)
			os.Exit(1)
		}

		var cmd *exec.Cmd
		if len(cmdArgs) == 1 {
			cmd = exec.Command(cmdArgs[0])
		} else {
			cmd = exec.Command(cmdArgs[0], cmdArgs[1:]...)
		}

		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Stdin = bytes.NewReader(chunk.Data)

		err = cmd.Run()
		if err != nil {
			fmt.Fprintf(os.Stderr, "error running chunk processing command: %s\n", err)
			os.Exit(1)
		}
	}

}
