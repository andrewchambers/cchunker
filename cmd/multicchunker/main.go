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
	fmt.Fprintln(os.Stderr, "This is a command that iteratively does content defined chunking on data piped into stdin,")
	fmt.Fprintln(os.Stderr, "each subcommand prints a line per chunk, eventually the iteration will reduce the data to a single line")
	fmt.Fprintln(os.Stderr, "This command is intended to be used as part of a backup tool")
	fmt.Fprintln(os.Stderr, "\n")
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "multicchunker [-flags...] CHUNK PROCESSOR")
	fmt.Fprintln(os.Stderr, "CHUNK PROCESSOR is a command+arguments that reads the chunk data on stdin and does an arbitrary action, but")
	fmt.Fprintln(os.Stderr, "must only print a single line to stdout")
	fmt.Fprintln(os.Stderr, "The default are chunks with a min size 512 KiB, max size 16 MiB and and average of 4MiB")
	fmt.Fprintln(os.Stderr, "On any IO or subprocess errors, multicchunker exits with a non zero exit code.")
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

	// XXX TODO disk back if this becomes very large.
	// XXX TODO test with multi terrabytes of data.

	// Pointer so we can do summaryData.Bytes() in a loop
	// safely.
	summaryData := &bytes.Buffer{}
	var summaryLine bytes.Buffer
	var input io.Reader

	iteration := int64(0)
	input = os.Stdin

	for {
		_, err := fmt.Fprintf(summaryData, "%d\n", iteration)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error writing iteration number: %s\n", err)
			os.Exit(1)
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
			cchunker = chunker.NewWithBoundaries(input, polynomial, SmallMinSize, SmallMaxSize)
			cchunker.SetAverageBits(SmallBits)
			buf = make([]byte, SmallMaxSize)
		} else if *largeChunks {
			cchunker = chunker.NewWithBoundaries(input, polynomial, LargeMinSize, LargeMaxSize)
			cchunker.SetAverageBits(LargeBits)
			buf = make([]byte, LargeMaxSize)
		} else {
			cchunker = chunker.NewWithBoundaries(input, polynomial, StandardMinSize, StandardMaxSize)
			cchunker.SetAverageBits(StandardBits)
			buf = make([]byte, StandardMaxSize)
		}

		nChunks := 0

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

			summaryLine.Reset()
			cmd.Stdout = &summaryLine
			cmd.Stderr = os.Stderr
			cmd.Stdin = bytes.NewReader(chunk.Data)

			err = cmd.Run()
			if err != nil {
				fmt.Fprintf(os.Stderr, "error running chunk processing command: %s\n", err)
				os.Exit(1)
			}
			_, err = summaryData.Write(summaryLine.Bytes())
			if err != nil {
				fmt.Fprintf(os.Stderr, "error writing summary line: %s\n", err)
				os.Exit(1)
			}

			nChunks += 1
		}

		if nChunks == 0 || nChunks == 1 {
			break
		}

		input = summaryData
		summaryData = &bytes.Buffer{}
		iteration += 1
	}

	_, err := os.Stdout.Write(summaryData.Bytes())
	if err != nil {
		fmt.Fprintf(os.Stderr, "error writing summary line: %s\n", err)
		os.Exit(1)
	}
}
