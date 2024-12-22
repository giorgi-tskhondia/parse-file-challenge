package main

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

func parse() (float64, float64, int64) {
	data, err := os.ReadFile("points.txt")
	if err != nil {
		panic("Failed to read file")
	}

	numCPU := runtime.NumCPU()
	chunkSize := len(data) / numCPU
	var wg sync.WaitGroup
	var mu sync.Mutex
	var sum1, sum2 float64
	var count int64

	worker := func(start, end int) {
		defer wg.Done()
		var chunkSum1, chunkSum2 float64 = 0, 0
		var chunkCount int64 = 0

		lines := strings.Split(string(data[start:end]), "\n")

		for i := 0; i < len(lines); i++ {
			if lines[i] == "" {
				continue
			}

			commaIndex := strings.IndexByte(lines[i], ',')
			if commaIndex == -1 {
				continue
			}

			val1, err1 := strconv.ParseFloat(strings.TrimSpace(lines[i][:commaIndex]), 64)
			val2, err2 := strconv.ParseFloat(strings.TrimSpace(lines[i][commaIndex+1:]), 64)
			if err1 != nil || err2 != nil {
				panic("Failed to parse floats")
			}

			chunkSum1 += val1
			chunkSum2 += val2
			chunkCount++
		}

		mu.Lock()
		sum1 += chunkSum1
		sum2 += chunkSum2
		count += chunkCount
		mu.Unlock()
	}

	start := 0
	end := 0
	for i := 0; i < numCPU; i++ {
		start = end
		end = start + chunkSize
		if i == numCPU-1 {
			end = len(data)
		} else {
			for data[end] != '\n' {
				end--
			}
		}

		wg.Add(1)
		go worker(start, end)
	}

	wg.Wait()

	sum1 = math.Round(sum1*100) / 100
	sum2 = math.Round(sum2*100) / 100
	// fmt.Print(sum1, sum2, count)
	return sum1, sum2, count
}

func compFloat(f1 float64, f2 float64) bool {
	i1 := int64(f1*100 + 0.5)
	i2 := int64(f1*100 + 0.5)
	return i1 == i2
}

func run() time.Duration {
	start := time.Now()
	s1, s2, lines := parse()
	elapsed := time.Since(start)

	data, err := os.ReadFile("points-verify.txt")
	if err != nil {
		panic(err)
	}

	parts := strings.Split(string(data[:len(data)-1]), ",")
	vl, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		panic(err)
	}
	f1, err := strconv.ParseFloat(parts[0], 8)
	if err != nil {
		panic(err)
	}
	f2, err := strconv.ParseFloat(parts[1], 8)
	if err != nil {
		panic(err)
	}

	if lines != vl {
		panic(fmt.Sprintf("Expected number of lines to be: %d got %d\n", vl, lines))
	}

	if !compFloat(s1, f1) {
		panic(fmt.Sprintf("Expected first number to be: %.2f got %.2f\n", f1, s1))
	}

	// if fmt.Sprintf("%.2f", s2) != fmt.Sprintf("%.2f", f2) {
	if !compFloat(s2, f2) {
		panic(fmt.Sprintf("Expected second number to be: %.2f got %.2f\n", f2, s2))
	}

	return elapsed
}

func main() {
	bestTime, err := time.ParseDuration("1h")
	if err != nil {
		panic(err)
	}

	for true {
		execTime := run()
		if execTime.Milliseconds() < bestTime.Milliseconds() {
			bestTime = execTime
			fmt.Printf("Execution time: %s\n", bestTime)
		}
	}
}
