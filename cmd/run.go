/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"bufio"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

var args struct {
	Url       string `arg:"required" help:"URL to fetch for HTTP/1.1"`
	Dist      string `arg:"required" help:"List of distribution types to test, e.g. fixed, exp"`
	Rates     string `arg:"required" help:"List if rates to test, e.g. 10,20,50"`
	Durations string `arg:"required" help:"List of durations to test, e.g. 10,20,50 (in seconds)"`
	Workers   int    `arg:"required" help:"Number of concurrent workers to use"`
	Output    string `arg:"-o,--output" help:"Output file to write results to (CSV format)" default:"out.csv"`
}

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "run the request workload generator",
	Run: func(cmd *cobra.Command, args []string) {
		Run()
	},
}

func init() {
	runCmd.Flags().StringVarP(&args.Url, "url", "u", "", "URL to fetch for HTTP/1.1")
	runCmd.Flags().StringVarP(&args.Dist, "dist", "d", "", "Distribution type to test: fixed or exp")
	runCmd.Flags().StringVarP(&args.Rates, "rates", "r", "", "Comma-separated list of rates to test, e.g. 10,20,50")
	runCmd.Flags().StringVarP(&args.Durations, "durations", "D", "", "Comma-separated list of durations (in seconds), e.g. 10,20,50")
	runCmd.Flags().IntVarP(&args.Workers, "workers", "w", 0, "Number of concurrent workers to use")
	runCmd.Flags().StringVarP(&args.Output, "output", "o", "out.csv", "Output file to write results to (CSV format)")

	if err := runCmd.MarkFlagRequired("url"); err != nil {
		panic(err)
	}
	if err := runCmd.MarkFlagRequired("dist"); err != nil {
		panic(err)
	}
	if err := runCmd.MarkFlagRequired("rates"); err != nil {
		panic(err)
	}
	if err := runCmd.MarkFlagRequired("durations"); err != nil {
		panic(err)
	}
	if err := runCmd.MarkFlagRequired("workers"); err != nil {
		panic(err)
	}
	rootCmd.AddCommand(runCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// runCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// runCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

type Sample struct {
	Url        string
	Latency    int64
	StatusCode int
	ErrStr     string
	Timestamp  time.Time
}

type Worker struct {
	Url      string
	ch       chan bool
	reportCh chan Sample
	client   *http.Client
}

func NewWorker(url string, ch chan bool, reportCh chan Sample, client *http.Client) *Worker {
	return &Worker{
		Url:      url,
		ch:       ch,
		reportCh: reportCh,
		client:   client,
	}
}

func (w *Worker) Start() {
	var sample Sample
	sample.Url = w.Url

	var resp *http.Response
	var err error
	var start time.Time

	for ok := range w.ch {
		if !ok {
			return
		}
		start = time.Now()
		resp, err = w.client.Get(w.Url)
		sample.Latency = time.Since(start).Microseconds()
		if err != nil {
			sample.StatusCode = 0
			sample.ErrStr = err.Error()
		} else {
			sample.StatusCode = resp.StatusCode
			// Drain and close the body so Transport can reuse the connection
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			sample.ErrStr = ""
		}
		sample.Timestamp = start
		w.reportCh <- sample
	}
}

type Collector struct {
	reportCh chan Sample
	Samples  []Sample
}

func NewCollector(reportCh chan Sample, size int) *Collector {
	return &Collector{
		reportCh: reportCh,
		Samples:  make([]Sample, 0, size),
	}
}

func (c *Collector) Start() {
	for sample := range c.reportCh {
		c.Samples = append(c.Samples, sample)
	}
	fmt.Printf("Collected %d samples\n", len(c.Samples))
	if args.Output != "" {
		fmt.Printf("Writing results to %s\n", args.Output)
		f, err := os.OpenFile(args.Output, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			panic(err)
		}
		defer f.Close()

		w := bufio.NewWriter(f)
		_, err = w.WriteString("url,latency,status_code,error,timestamp\n")
		if err != nil {
			panic(err)
		}
		for _, sample := range c.Samples {
			// url
			if _, err = w.WriteString(sample.Url); err != nil {
				panic(err)
			}
			_ = w.WriteByte(',')
			// latency
			if _, err = w.WriteString(strconv.FormatInt(sample.Latency, 10)); err != nil {
				panic(err)
			}
			_ = w.WriteByte(',')
			// status code
			if _, err = w.WriteString(strconv.Itoa(sample.StatusCode)); err != nil {
				panic(err)
			}
			_ = w.WriteByte(',')
			// error
			if _, err = w.WriteString(sample.ErrStr); err != nil {
				panic(err)
			}
			_ = w.WriteByte(',')
			// timestamp (RFC3339Nano)
			if _, err = w.WriteString(sample.Timestamp.Format(time.RFC3339Nano)); err != nil {
				panic(err)
			}
			_ = w.WriteByte('\n')
		}
		if err := w.Flush(); err != nil {
			panic(err)
		}
		fmt.Println("Done")
	} else {
		fmt.Println("No output file specified, not writing results")
	}
}

// preciseSleep sleeps until the provided deadline using a hybrid strategy:
// - For large remaining durations it uses time.Sleep to reduce CPU usage.
// - For short remaining durations it yields or busy-spins to reduce wake-up jitter.
// This achieves much lower jitter at microsecond scale while keeping CPU use reasonable.
func preciseSleep(deadline time.Time) {
	// thresholds tuned empirically: adjust for your platform.
	const yieldThreshold = 2 * time.Millisecond  // switch from sleep->yield
	const spinThreshold = 300 * time.Microsecond // busy-spin window

	for {
		now := time.Now()
		if !now.Before(deadline) {
			return
		}
		remaining := deadline.Sub(now)
		if remaining > yieldThreshold {
			// Sleep most of the remaining time, leave a margin for yield/spin
			time.Sleep(remaining - yieldThreshold)
			continue
		}

		// Now we're in the short-window region. Yield to other goroutines while
		// there's still some time, but if it's very small, busy-spin.
		if remaining > spinThreshold {
			// Yield the processor to allow other goroutines to run while we wait
			runtime.Gosched()
			continue
		}

		// Very small remaining time -> busy spin for lowest jitter.
		for time.Now().Before(deadline) {
		}
		return
	}
}

type SpinWait struct {
	deadline time.Time
}

func (sw *SpinWait) Wait(phaseWait float64) {
	sw.deadline = time.Now().Add(time.Duration(phaseWait*0.96) * time.Microsecond)
	for time.Now().Before(sw.deadline) {
	}
}

type WaitCalculator interface {
	GetWaitTime() float64
	UpdatePhaseRate(rate int)
}

type FixedWait struct {
	phaseWait float64
}

func (fw *FixedWait) GetWaitTime() float64 {
	return fw.phaseWait
}

func (fw *FixedWait) UpdatePhaseRate(rate int) {
	fw.phaseWait = float64(1000000 / rate)
}

type ExponentialWait struct {
	rate float64
}

func (ew *ExponentialWait) GetWaitTime() float64 {
	// generate a exponential random number with mean 1/rate
	return rand.ExpFloat64() / ew.rate * 1000000
}

func (ew *ExponentialWait) UpdatePhaseRate(rate int) {
	ew.rate = float64(rate)
}

func Run() {

	// Sanity checks on args
	if args.Workers <= 0 {
		panic("Workers must be > 0")
	}
	if len(args.Rates) == 0 {
		panic("At least one rate must be specified")
	}
	if args.Dist != "fixed" && args.Dist != "exp" {
		panic("Distribution must be 'fixed' or 'exp'")
	}

	// Parse rates and durations
	var rates, durations []int
	for _, r := range strings.Split(args.Rates, ",") {
		rate, err := strconv.Atoi(r)
		if err != nil {
			panic(fmt.Sprintf("Invalid rate: %s", r))
		}
		rates = append(rates, rate)
	}
	for _, d := range strings.Split(args.Durations, ",") {
		duration, err := strconv.Atoi(d)
		if err != nil {
			panic(fmt.Sprintf("Invalid duration: %s", d))
		}
		durations = append(durations, duration)
	}
	if len(rates) != len(durations) {
		panic("Rates and durations must have the same length")
	}
	// append a final phase with 1 rps for 1 second to flush out remaining requests
	/* rates = append(rates, 1)
	durations = append(durations, 1) */

	// Initializations
	numSamples := 0
	for i := 0; i < len(rates); i++ {
		numSamples += rates[i] * durations[i]
	}
	numSamples += 10000 // Padding
	workers := make([]*Worker, args.Workers)
	ch := make(chan bool)
	reportCh := make(chan Sample, args.Workers*50)

	for i := 0; i < args.Workers; i++ {
		tr := &http.Transport{
			MaxIdleConns:        1,
			MaxIdleConnsPerHost: 1,
			MaxConnsPerHost:     1,
			IdleConnTimeout:     1 * time.Second,
		}
		client := &http.Client{Transport: tr, Timeout: 1 * time.Second}
		workers[i] = NewWorker(args.Url, ch, reportCh, client)
		go workers[i].Start()
	}
	collector := NewCollector(reportCh, numSamples)
	go collector.Start()
	time.Sleep(10 * time.Millisecond) // Wait for workers and collector to start
	var cal WaitCalculator
	if args.Dist == "fixed" {
		cal = &FixedWait{}
	} else {
		cal = &ExponentialWait{}
	}
	phaseIndex := 0
	cal.UpdatePhaseRate(rates[phaseIndex])
	spinWait := &SpinWait{}
	fmt.Printf("Starting phase %d: rate=%d, duration=%d\n", phaseIndex, rates[phaseIndex], durations[phaseIndex])

	// Main loop
	fmt.Println("Starting the test")
	timer := time.NewTimer(time.Duration(durations[phaseIndex]) * time.Second)
	for {
		select {
		case <-timer.C:
			phaseIndex++
			if phaseIndex >= len(rates) {
				//fmt.Printf("GOMAXPROCS=%d\n", runtime.GOMAXPROCS(0))
				fmt.Println("All phases completed")
				close(ch)
				time.Sleep(1000 * time.Millisecond) // Wait for inflight requests to finish
				close(reportCh)
				time.Sleep(100 * time.Millisecond) // Wait for collector to finish
				return
			}
			timer.Reset(time.Duration(durations[phaseIndex]) * time.Second)
			cal.UpdatePhaseRate(rates[phaseIndex])
		case ch <- true:
		default:
			panic("Channel is full")
		}

		spinWait.Wait(cal.GetWaitTime())

	}

}
