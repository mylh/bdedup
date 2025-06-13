package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"

	"github.com/AndreasBriese/bbloom"
)

var (
	inputFile     string
	outputFile    string
	stateFile     string
	numValues     float64
	falsePositive float64
	returnSeen    bool
	concurrency   int
)

func init() {
	flag.StringVar(&inputFile, "input", "", "Input file (default: stdin)")
	flag.StringVar(&outputFile, "output", "", "Output file (default: stdout)")
	flag.StringVar(&stateFile, "state", "bloom.gz", "Bloom filter state file")
	flag.Float64Var(&numValues, "n", 1000000, "Expected number of values")
	flag.Float64Var(&falsePositive, "p", 0.01, "False positive probability")
	flag.BoolVar(&returnSeen, "seen", false, "Return only seen items (default: return new items)")
	flag.IntVar(&concurrency, "concurrency", runtime.NumCPU(), "Number of concurrent workers")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(),
			`Efficient command-line deduplication tool that uses a Bloom filter for high-performance duplicate detection in large datasets or streams.

Usage: %[1]s [options]

Options:
  -input         Input file (default: stdin)
  -output        Output file (default: stdout)
  -state         Bloom filter state file (default: bloom.gz)
  -n             Expected number of values (default: 1000000)
  -p             False positive probability (default: 0.01)
  -seen          Return only seen items (default: return new items)
  -concurrency   Number of concurrent workers (default: number of CPUs)
  -h, -?         Show this help and exit
  --help         Show this help and exit

Examples:
  cat data.txt | %[1]s -n 10000 -p 0.001 > deduped.txt
  %[1]s -input infile -output outfile -state mystate.gz

`, os.Args[0])
	}
}

func main() {
	// Support -?, -h, --help flags
	// for _, arg := range os.Args[1:] {
	// 	if arg == "-?" || arg == "-h" || arg == "--help" {
	// 		flag.Usage()
	// 		return
	// 	}
	// }
	flag.Parse()

	bf := loadBloomFilter()
	defer saveBloomFilter(bf)

	var input io.Reader = os.Stdin
	var output io.Writer = os.Stdout

	if inputFile != "" {
		file, err := os.Open(inputFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error opening input file: %v\n", err)
			os.Exit(1)
		}
		defer file.Close()
		input = file
	}

	if outputFile != "" {
		file, err := os.Create(outputFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating output file: %v\n", err)
			os.Exit(1)
		}
		defer file.Close()
		output = file
	}

	if inputFile != "" {
		processInParallel(input, output, &bf)
	} else {
		processStream(input, output, &bf)
	}
}

func loadBloomFilter() bbloom.Bloom {
	if _, err := os.Stat(stateFile); os.IsNotExist(err) {
		return bbloom.New(numValues, falsePositive)
	}

	file, err := os.Open(stateFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening state file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	gz, err := gzip.NewReader(file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating gzip reader: %v\n", err)
		os.Exit(1)
	}
	defer gz.Close()

	data, err := io.ReadAll(gz)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading state file: %v\n", err)
		os.Exit(1)
	}

	return bbloom.JSONUnmarshal(data)
}

func saveBloomFilter(bf bbloom.Bloom) {
	file, err := os.Create(stateFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating state file: %v\n", err)
		return
	}
	defer file.Close()

	gz := gzip.NewWriter(file)
	defer gz.Close()

	data := bf.JSONMarshal()
	if _, err := gz.Write(data); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing state file: %v\n", err)
	}
}

func processStream(input io.Reader, output io.Writer, bf *bbloom.Bloom) {
	scanner := bufio.NewScanner(input)
	for scanner.Scan() {
		line := scanner.Bytes()
		if returnSeen == bf.Has(line) {
			fmt.Fprintln(output, scanner.Text())
		}
		bf.Add(line)
	}
}

func processInParallel(input io.Reader, output io.Writer, bf *bbloom.Bloom) {
	var wg sync.WaitGroup
	lines := make(chan string)
	results := make(chan string)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go worker(&wg, lines, results, bf)
	}

	go func() {
		scanner := bufio.NewScanner(input)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		close(lines)
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	for result := range results {
		fmt.Fprintln(output, result)
	}
}

func worker(wg *sync.WaitGroup, lines <-chan string, results chan<- string, bf *bbloom.Bloom) {
	defer wg.Done()
	for line := range lines {
		if returnSeen == bf.HasTS([]byte(line)) {
			results <- line
		}
		bf.AddTS([]byte(line))
	}
}
