/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/farzad1132/rwg/protobuf"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var args struct {
	Url       string `arg:"required" help:"URL to fetch for HTTP/1.1"`
	Dist      string `arg:"required" help:"List of distribution types to test, e.g. fixed, exp"`
	Rates     string `arg:"required" help:"List if rates to test, e.g. 10,20,50"`
	Durations string `arg:"required" help:"List of durations to test, e.g. 10,20,50 (in seconds)"`
	Workers   int    `arg:"required" help:"Number of concurrent workers to use"`
	Output    string `arg:"-o,--output" help:"Output file to write results to (CSV format)" default:"out.csv"`
	// Args allows passing arbitrary key=value pairs from the command line.
	// Use with --args key=value (can be repeated) or --args key1=val1,key2=val2
	Proto      string
	Args       map[string]string
	PrintStats bool
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
	// Accept arbitrary args key=value pairs
	runCmd.Flags().StringToStringVarP(&args.Args, "args", "a", nil, "Args key=value,key2=value2 pairs (can be repeated)")
	runCmd.Flags().StringVarP(&args.Proto, "proto", "p", "", "Protocol to use (http or grpc)")
	runCmd.Flags().BoolVarP(&args.PrintStats, "stats", "s", true, "Print stats at the end of the test")

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

type TransportInterface interface {
	Issue(*Sample)
	GetUrl() string
}

type HTTP11Transport struct {
	Url        string
	httpClient *http.Client
}

func (t *HTTP11Transport) Issue(sample *Sample) {
	resp, err := t.httpClient.Get(t.Url)
	if err != nil {
		sample.StatusCode = 0
		sample.ErrStr = err.Error()
		return
	}
	// Drain and close the body so Transport can reuse the connection
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	sample.StatusCode = resp.StatusCode
	sample.ErrStr = ""
}

func (t *HTTP11Transport) GetUrl() string {
	return t.Url
}

func NewHTTP11Transport(url string) *HTTP11Transport {
	tr := &http.Transport{
		IdleConnTimeout: 60 * time.Minute,
	}
	client := &http.Client{Transport: tr, Timeout: 60 * time.Minute}
	return &HTTP11Transport{
		Url:        url,
		httpClient: client,
	}
}

type GRPCTransport struct {
	call func() error
	Url  string
}

func (t *GRPCTransport) Issue(sample *Sample) {
	err := t.call()
	if err != nil {
		sample.StatusCode = int(status.Code(err))
		if sample.StatusCode == int(codes.ResourceExhausted) {
			sample.ErrStr = ""
		} else {
			sample.ErrStr = fmt.Sprintf("error code: %d", status.Code(err))
		}
		return
	}
	sample.StatusCode = 0
	sample.ErrStr = ""
}

func (t *GRPCTransport) GetUrl() string {
	return t.Url
}

func NewGRPCTransport(call func() error, url string) *GRPCTransport {
	return &GRPCTransport{
		call: call,
		Url:  url,
	}
}

type Worker struct {
	transport TransportInterface
	ch        chan bool
	reportCh  chan Sample
}

const MaxConcurrency = 100

func NewWorker(ch chan bool, reportCh chan Sample, transport TransportInterface) *Worker {
	return &Worker{
		transport: transport,
		ch:        ch,
		reportCh:  reportCh,
	}
}

func (w *Worker) Start() {
	var sample Sample
	sample.Url = w.transport.GetUrl()
	var start time.Time

	for ok := range w.ch {
		if !ok {
			return
		}
		start = time.Now()
		w.transport.Issue(&sample)
		sample.Latency = time.Since(start).Microseconds()
		sample.Timestamp = start
		w.reportCh <- sample
	}
}

type Collector struct {
	reportCh          chan Sample
	Samples           []Sample
	SkippedIterations int
	NumErrors         int
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
	if args.PrintStats {
		c.PrintStats()
	}
	if args.Output != "" {
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
		fmt.Println("Exported results to ", args.Output)
	} else {
		fmt.Println("No output file specified, not writing results")
	}
}

// This is executed at the end of the test and reports
// - number of errors
// number of requests with different status codes
// - rate of successfull requests (empty string for error)
// p50 and p95 latency
// total requests
// duration of test
func (c *Collector) PrintStats() {
	// duration is sum of durations of all phases
	totalDuration := 0
	for _, d := range strings.Split(args.Durations, ",") {
		duration, err := strconv.Atoi(d)
		if err != nil {
			panic(fmt.Sprintf("Invalid duration: %s", d))
		}
		totalDuration += duration
	}
	// number of errors and number of requests with different status codes
	statusCodeCounts := make(map[int]int)
	c.NumErrors = 0
	for _, sample := range c.Samples {
		if sample.ErrStr != "" {
			c.NumErrors++
		} else {
			statusCodeCounts[sample.StatusCode]++
		}
	}
	// number of skipped iterations (in red color)
	fmt.Printf("\033[31m| %-25s | %-12d | %-13.1f |\033[0m\n", "Skipped iterations", c.SkippedIterations, float64(c.SkippedIterations)/float64(totalDuration))
	// print number of requests with different status codes
	for statusCode, count := range statusCodeCounts {
		fmt.Printf("| %-25s | %-12d | %-13.1f |\n", "status code: "+strconv.Itoa(statusCode), count, float64(count)/float64(totalDuration))
	}
	// rate of successfull requests
	rateSuccess := float64(len(c.Samples)-c.NumErrors) / float64(totalDuration)
	// p50 and p95 latency
	sort.Slice(c.Samples, func(i, j int) bool {
		return c.Samples[i].Latency < c.Samples[j].Latency
	})
	p50Latency := c.Samples[len(c.Samples)/2].Latency
	p95Latency := c.Samples[len(c.Samples)*95/100].Latency
	minLatency := c.Samples[0].Latency
	maxLatency := c.Samples[len(c.Samples)-1].Latency
	// total requests
	totalRequests := len(c.Samples)
	// print stats in table format
	fmt.Println("+---------------------------+---------------------------+")
	fmt.Printf("\033[1;31m| %-25s | %-25v |\033[0m\n", "Number of errors", c.NumErrors)
	fmt.Printf("| %-25s | %-25.6f |\n", "Successfull requests", rateSuccess)
	fmt.Printf("| %-25s | %-25d |\n", "Min latency", minLatency)
	fmt.Printf("| %-25s | %-25d |\n", "P50 latency", p50Latency)
	fmt.Printf("| %-25s | %-25d |\n", "P95 latency", p95Latency)
	fmt.Printf("| %-25s | %-25d |\n", "Max latency", maxLatency)
	fmt.Printf("| %-25s | %-25d |\n", "Total requests", totalRequests)
	fmt.Printf("| %-25s | %-25d |\n", "Duration of test", totalDuration)
	fmt.Println("+---------------------------+---------------------------+")
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

func CheckArgsPresent(required ...string) {
	for _, key := range required {
		if _, ok := args.Args[key]; !ok {
			panic("Missing required argument: " + key)
		}
	}
}

// Url is in the format of package.service/method
func CreateGRPCTransport(url string, Args map[string]string) TransportInterface {
	conn, err := grpc.NewClient(url, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}

	cut := strings.Split(args.Proto, "/")
	service := cut[0]
	method := cut[1]

	switch service {
	case "protobuf.RajomonClient":
		client := protobuf.NewRajomonClientClient(conn)

		switch method {
		case "SearchHotels":
			CheckArgsPresent("lat", "lon", "InDate", "OutDate")
			lat, err := strconv.ParseFloat(Args["lat"], 64)
			if err != nil {
				panic("Invalid lat argument")
			}
			lon, err := strconv.ParseFloat(Args["lon"], 64)
			if err != nil {
				panic("Invalid lon argument")
			}
			return NewGRPCTransport(func() error {
				_, err := client.SearchHotels(
					context.Background(),
					&protobuf.SearchHotelsRequest{
						Lat:     float32(lat),
						Lon:     float32(lon),
						InDate:  Args["InDate"],
						OutDate: Args["OutDate"],
					},
				)
				return err
			}, url)
		case "FrontendReservation":
			CheckArgsPresent("HotelId", "CustomerName", "Username", "Password", "number", "InDate", "OutDate")
			number, err := strconv.Atoi(Args["number"])
			if err != nil {
				panic("Invalid number argument")
			}
			return NewGRPCTransport(func() error {
				_, err := client.FrontendReservation(
					context.Background(),
					&protobuf.FrontendReservationRequest{
						HotelId:      Args["HotelId"],
						CustomerName: Args["CustomerName"],
						Username:     Args["Username"],
						Password:     Args["Password"],
						Number:       int32(number),
						InDate:       Args["InDate"],
						OutDate:      Args["OutDate"],
					},
				)
				return err
			}, url)
		default:
			panic("Unknown method: " + method)
		}
	case "protobuf.GRPCServer":
		client := protobuf.NewGRPCServerClient(conn)
		CheckArgsPresent("Input")
		return NewGRPCTransport(func() error {
			_, err := client.Testendpoint(
				context.Background(),
				&protobuf.TestRequest{
					Input: Args["Input"],
				},
			)
			return err
		}, url)

	default:
		panic("Unknown service: " + service)
	}

}

func CreateTransport() TransportInterface {
	// Currently we only support HTTP/1.1 transports. In future we can
	// inspect the scheme and return a gRPC transport when needed.
	if strings.HasPrefix(args.Url, "http://") || strings.HasPrefix(args.Url, "https://") {
		return NewHTTP11Transport(args.Url)
	}
	return CreateGRPCTransport(args.Url, args.Args)
}

func VersionFinder() int {
	if strings.HasPrefix(args.Url, "http://") || strings.HasPrefix(args.Url, "https://") {
		return 1
	}
	return 2
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
	version := VersionFinder()
	index := 0
	workersWg := sync.WaitGroup{}
	var transport TransportInterface
	for i := 0; i < args.Workers; i++ {
		switch version {
		case 1:
			transport = CreateTransport()
		case 2:
			if index%MaxConcurrency == 0 {
				transport = CreateTransport()
			}
			index++
		}
		workers[i] = NewWorker(ch, reportCh, transport)
		workersWg.Add(1)
		go func() {
			defer workersWg.Done()
			workers[i].Start()
		}()
	}
	collector := NewCollector(reportCh, numSamples)
	collectorWg := sync.WaitGroup{}
	collectorWg.Add(1)
	go func() {
		defer collectorWg.Done()
		collector.Start()
	}()
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
	// print phases with rate and duration
	for i := 0; i < len(rates); i++ {
		fmt.Printf("Phase %d: rate=%d, duration=%d\n", i, rates[i], durations[i])
	}

	// Main loop
	fmt.Println("Starting the test")
	timer := time.NewTimer(time.Duration(durations[phaseIndex]) * time.Second)
	for {
		select {
		case <-timer.C:
			phaseIndex++
			if phaseIndex >= len(rates) {
				fmt.Println("All phases completed")
				close(ch)
				workersWg.Wait()
				close(reportCh)
				collectorWg.Wait()
				if collector.SkippedIterations > 0 || collector.NumErrors > 0 {
					os.Exit(1)
				}
				os.Exit(0)
			}
			timer.Reset(time.Duration(durations[phaseIndex]) * time.Second)
			cal.UpdatePhaseRate(rates[phaseIndex])
		case ch <- true:
		default:
			collector.SkippedIterations++
		}

		spinWait.Wait(cal.GetWaitTime())

	}

}
