/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
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
	// Args allows passing arbitrary key=value pairs from the command line.
	// Use with --args key=value (can be repeated) or --args key1=val1,key2=val2
	Proto      string
	Args       map[string]string
	PrintStats bool
	Timeout    int
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
	runCmd.Flags().IntVarP(&args.Timeout, "timeout", "t", 5, "Request timeout in seconds")

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
	IsTimeout  bool
}

type TransportInterface interface {
	Issue(*Sample) (int64, time.Time)
	GetUrl() string
}

type HTTP11Transport struct {
	Url        string
	httpClient *http.Client
	bufPool    *sync.Pool
}

func (t *HTTP11Transport) Issue(sample *Sample) (int64, time.Time) {
	start := time.Now()
	resp, err := t.httpClient.Get(t.Url)
	duration := time.Since(start).Microseconds()
	if err != nil {
		sample.StatusCode = 0
		sample.ErrStr = err.Error()
		if os.IsTimeout(err) {
			sample.IsTimeout = true
		}
		return duration, start
	}
	// Get buffer from pool
	bufPtr := t.bufPool.Get().(*[]byte)
	buf := *bufPtr

	// Drain and close the body so Transport can reuse the connection
	_, _ = io.CopyBuffer(io.Discard, resp.Body, buf)
	_ = resp.Body.Close()

	// Return buffer to pool
	t.bufPool.Put(bufPtr)

	sample.StatusCode = resp.StatusCode
	sample.ErrStr = ""
	return duration, start
}

func (t *HTTP11Transport) GetUrl() string {
	return t.Url
}

func NewHTTP11Transport(url string) *HTTP11Transport {
	tr := &http.Transport{
		IdleConnTimeout:     60 * time.Minute,
		MaxIdleConns:        10000,
		MaxIdleConnsPerHost: 10000,
		DisableKeepAlives:   false,
		ForceAttemptHTTP2:   false,
		TLSNextProto:        make(map[string]func(string, *tls.Conn) http.RoundTripper),
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}
	client := &http.Client{Transport: tr, Timeout: time.Duration(args.Timeout) * time.Second}
	return &HTTP11Transport{
		Url:        url,
		httpClient: client,
		bufPool: &sync.Pool{
			New: func() interface{} {
				s := make([]byte, 32*1024)
				return &s
			},
		},
	}
}

/* type GRPCTransport struct {
	call func() error
	Url  string
}

func (t *GRPCTransport) Issue(sample *Sample) {
	err := t.call()
	if err != nil {
		sample.StatusCode = int(status.Code(err))
		if sample.StatusCode == int(codes.DeadlineExceeded) {
			sample.IsTimeout = true
		}
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
} */

type Worker struct {
	transport TransportInterface
	ch        chan bool
	reportCh  chan Sample
}

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
	var duration int64
	var start time.Time

	for ok := range w.ch {
		if !ok {
			return
		}
		sample.IsTimeout = false
		duration, start = w.transport.Issue(&sample)
		sample.Latency = duration
		sample.Timestamp = start
		w.reportCh <- sample
	}
}

type Collector struct {
	reportCh          chan Sample
	Latencies         []int64
	statusCodeCounts  map[int]int
	SkippedIterations int
	NumErrors         int
	NumTimeouts       int
}

func NewCollector(reportCh chan Sample, size int) *Collector {
	c := &Collector{
		reportCh:         reportCh,
		Latencies:        make([]int64, 0, size),
		statusCodeCounts: make(map[int]int),
	}
	for i := range c.Latencies {
		c.Latencies[i] = -1
	}
	return c
}

func (c *Collector) RequestFinished() int64 {
	return int64(len(c.Latencies))
}

func (c *Collector) Start() {
	// Setup async writer if output is requested
	var writeCh chan *[]Sample
	var writerWg sync.WaitGroup
	var batchPool *sync.Pool

	const BatchSize = 3000

	if args.Output != "" {
		writeCh = make(chan *[]Sample, 100) // Buffer for 100 batches
		batchPool = &sync.Pool{
			New: func() interface{} {
				s := make([]Sample, 0, BatchSize)
				return &s
			},
		}

		writerWg.Add(1)
		go func() {
			defer writerWg.Done()
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
			defer func() {
				if err := w.Flush(); err != nil {
					panic(err)
				}
				fmt.Println("Exported results to ", args.Output)
			}()

			for batchPtr := range writeCh {
				batch := *batchPtr
				for _, sample := range batch {
					// url
					if _, err := w.WriteString(sample.Url); err != nil {
						panic(err)
					}
					_ = w.WriteByte(',')
					// latency
					if _, err := w.WriteString(strconv.FormatInt(sample.Latency, 10)); err != nil {
						panic(err)
					}
					_ = w.WriteByte(',')
					// status code
					if _, err := w.WriteString(strconv.Itoa(sample.StatusCode)); err != nil {
						panic(err)
					}
					_ = w.WriteByte(',')
					// error
					if _, err := w.WriteString(sample.ErrStr); err != nil {
						panic(err)
					}
					_ = w.WriteByte(',')
					// timestamp (RFC3339Nano)
					if _, err := w.WriteString(sample.Timestamp.Format(time.RFC3339Nano)); err != nil {
						panic(err)
					}
					_ = w.WriteByte('\n')
				}
				// Reset and reuse
				*batchPtr = batch[:0]
				batchPool.Put(batchPtr)
			}
		}()
	} else {
		fmt.Println("No output file specified, not writing results")
	}

	// Main collection loop
	var currentBatchPtr *[]Sample
	if batchPool != nil {
		currentBatchPtr = batchPool.Get().(*[]Sample)
	}

	for sample := range c.reportCh {
		// Update stats
		c.Latencies = append(c.Latencies, sample.Latency)
		if sample.ErrStr != "" {
			c.NumErrors++
			if sample.IsTimeout {
				c.NumTimeouts++
			}
		} else {
			c.statusCodeCounts[sample.StatusCode]++
		}

		// Handle batching
		if batchPool != nil {
			*currentBatchPtr = append(*currentBatchPtr, sample)
			if len(*currentBatchPtr) >= BatchSize {
				writeCh <- currentBatchPtr
				currentBatchPtr = batchPool.Get().(*[]Sample)
			}
		}
	}

	// Flush remaining
	if batchPool != nil {
		if len(*currentBatchPtr) > 0 {
			writeCh <- currentBatchPtr
		} else {
			// Not used
			batchPool.Put(currentBatchPtr)
		}
		close(writeCh)
		writerWg.Wait()
	}

	if args.PrintStats {
		c.PrintStats()
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

	// number of skipped iterations (in red color)
	fmt.Printf("\033[31m| %-25s | %-12d | %-13.1f |\033[0m\n", "Skipped iterations", c.SkippedIterations, float64(c.SkippedIterations)/float64(totalDuration))
	// print number of requests with different status codes
	for statusCode, count := range c.statusCodeCounts {
		fmt.Printf("| %-25s | %-12d | %-13.1f |\n", "status code: "+strconv.Itoa(statusCode), count, float64(count)/float64(totalDuration))
	}
	// rate of successfull requests
	rateSuccess := float64(len(c.Latencies)-c.NumErrors) / float64(totalDuration)
	// p50 and p95 latency
	sort.Slice(c.Latencies, func(i, j int) bool {
		return c.Latencies[i] < c.Latencies[j]
	})
	totalRequests := len(c.Latencies)
	var p50Latency, p95Latency, p99Latency, p999Latency, minLatency, maxLatency int64
	if totalRequests > 0 {
		p50Latency = c.Latencies[totalRequests/2]
		p95Latency = c.Latencies[totalRequests*95/100]
		p99Latency = c.Latencies[totalRequests*99/100]
		p999Latency = c.Latencies[totalRequests*999/1000]
		minLatency = c.Latencies[0]
		maxLatency = c.Latencies[totalRequests-1]
	}

	// print stats in table format
	fmt.Println("+---------------------------+---------------------------+")
	fmt.Printf("\033[1;31m| %-25s | %-25v |\033[0m\n", "Number of errors", c.NumErrors)
	fmt.Printf("\033[1;31m| %-25s | %-25v |\033[0m\n", "Number of timeouts", c.NumTimeouts)
	fmt.Printf("| %-25s | %-25.6f |\n", "Successfull requests", rateSuccess)
	fmt.Printf("| %-25s | %-25d |\n", "Min latency", minLatency)
	fmt.Printf("| %-25s | %-25d |\n", "P50 latency", p50Latency)
	fmt.Printf("| %-25s | %-25d |\n", "P95 latency", p95Latency)
	fmt.Printf("\033[1m| %-25s | %-25d |\033[0m\n", "P99 latency", p99Latency)
	fmt.Printf("\033[1m| %-25s | %-25d |\033[0m\n", "P99.9 latency", p999Latency)
	fmt.Printf("| %-25s | %-25d |\n", "Max latency", maxLatency)
	fmt.Printf("| %-25s | %-25d |\n", "Total requests", totalRequests)
	fmt.Printf("| %-25s | %-25d |\n", "Duration of test", totalDuration)
	fmt.Println("+---------------------------+---------------------------+")
}

type SpinWait struct {
	deadline time.Time
}

func (sw *SpinWait) Wait(phaseWait float64) {
	sw.deadline = time.Now().Add(time.Duration(phaseWait) * time.Microsecond)
	for time.Now().Before(sw.deadline) {
	}
}

type WaitCalculator interface {
	GetTotalRequests() int64
	GetWaitTime() float64
}

type FixedWait struct {
	WaitPeriods []float64
	Index       int64
	Total       int64
}

func NewFixedWait(rates, durations []int) *FixedWait {
	var phaseCount int64
	waiter := &FixedWait{Index: -1, Total: 0}
	waiter.WaitPeriods = make([]float64, 0)
	for i := 0; i < len(rates); i++ {
		phaseCount = int64(rates[i] * durations[i])
		waiter.Total += phaseCount
		for range phaseCount {
			waiter.WaitPeriods = append(waiter.WaitPeriods, float64(1000000/rates[i]))
		}
	}
	return waiter
}

func (fw *FixedWait) GetTotalRequests() int64 {
	return fw.Total
}

func (fw *FixedWait) GetWaitTime() float64 {
	fw.Index += 1
	if fw.Index == fw.Total {
		return -1
	}
	return fw.WaitPeriods[fw.Index]
}

type ExponentialWait struct {
	WaitPeriods []float64
	Index       int64
	Total       int64
}

func NewExpWait(rates, durations []int) *ExponentialWait {
	waiter := &ExponentialWait{
		Index:       0,
		Total:       0,
		WaitPeriods: make([]float64, 0),
	}
	total := 0
	for i := 0; i < len(rates); i++ {
		localDuration := float64(0)
		var val float64
		for localDuration < float64(durations[i]*1000000) {
			val = waiter.GenExpVariable(float64(rates[i]))
			waiter.WaitPeriods = append(waiter.WaitPeriods, val)
			localDuration += val
			total += 1
		}
	}

	waiter.Total = int64(total)
	return waiter
}

func (ew *ExponentialWait) GenExpVariable(rate float64) float64 {
	// generate a exponential random number with mean 1/rate
	return rand.ExpFloat64() / rate * 1000000
}

func (ew *ExponentialWait) GetWaitTime() float64 {
	ew.Index += 1
	if ew.Index == ew.Total {
		return -1
	}
	return ew.WaitPeriods[ew.Index]
}

func (ew *ExponentialWait) GetTotalRequests() int64 {
	return ew.Total
}

func CheckArgsPresent(required ...string) {
	for _, key := range required {
		if _, ok := args.Args[key]; !ok {
			panic("Missing required argument: " + key)
		}
	}
}

// Url is in the format of package.service/method
/* func CreateGRPCTransport(url string, Args map[string]string) TransportInterface {
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
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(args.Timeout)*time.Second)
				defer cancel()
				_, err := client.SearchHotels(
					ctx,
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
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(args.Timeout)*time.Second)
				defer cancel()
				_, err := client.FrontendReservation(
					ctx,
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
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(args.Timeout)*time.Second)
			defer cancel()
			_, err := client.Testendpoint(
				ctx,
				&protobuf.TestRequest{
					Input: Args["Input"],
				},
			)
			return err
		}, url)

	default:
		panic("Unknown service: " + service)
	}

} */

func CreateTransport() TransportInterface {
	// Currently we only support HTTP/1.1 transports. In future we can
	// inspect the scheme and return a gRPC transport when needed.
	if strings.HasPrefix(args.Url, "http://") || strings.HasPrefix(args.Url, "https://") {
		return NewHTTP11Transport(args.Url)
	} else {
		panic("gRPC not supported yet")
	}
	//return CreateGRPCTransport(args.Url, args.Args)
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
	MaxWorkers := args.Workers
	CurrentWorkers := 50
	workers := make([]*Worker, 0, MaxWorkers)
	ch := make(chan bool)
	reportCh := make(chan Sample, 10000)
	version := VersionFinder()
	//index := 0
	workersWg := sync.WaitGroup{}
	var transport TransportInterface

	// For HTTP, create a single shared transport instance
	if version == 1 {
		transport = CreateTransport()
	}
	for i := 0; i < CurrentWorkers; i++ {
		workers = append(workers, NewWorker(ch, reportCh, transport))
		workersWg.Add(1)
		go func() {
			defer workersWg.Done()
			workers[i].Start()
		}()
	}
	scaleFunc := func() bool {
		for i := CurrentWorkers; i < CurrentWorkers+CurrentWorkers; i++ {
			if i+1 >= MaxWorkers {
				CurrentWorkers = i + 1
				return true
			}
			workers = append(workers, NewWorker(ch, reportCh, transport))
			workersWg.Add(1)
			go func() {
				defer workersWg.Done()
				workers[i].Start()
			}()
		}
		CurrentWorkers *= 2
		return false
	}
	scaleInProgress := false
	var cal WaitCalculator
	if args.Dist == "fixed" {
		cal = NewFixedWait(rates, durations)
	} else {
		cal = NewExpWait(rates, durations)
	}
	collector := NewCollector(reportCh, int(cal.GetTotalRequests()))
	collectorWg := sync.WaitGroup{}
	collectorWg.Add(1)
	go func() {
		defer collectorWg.Done()
		collector.Start()
	}()
	time.Sleep(10 * time.Millisecond) // Wait for workers and collector to start
	spinWait := &SpinWait{}
	// print phases with rate and duration
	for i := 0; i < len(rates); i++ {
		fmt.Printf("Phase %d: rate=%d, duration=%d\n", i, rates[i], durations[i])
	}

	// Main loop
	fmt.Println("Starting the test")
	var waitTime float64
	var requestStarted int64
	var workerInUse int64
	maxWorkerInUse := int64(0)
	for {
		select {
		case ch <- true:
			requestStarted++
		default:
			collector.SkippedIterations++
		}
		workerInUse = requestStarted - collector.RequestFinished()
		if workerInUse > maxWorkerInUse {
			maxWorkerInUse = workerInUse
		}
		if float64(workerInUse) > 0.7*float64(CurrentWorkers) {
			if !scaleInProgress {
				scaleInProgress = true
				go func() {
					scaleInProgress = scaleFunc()
				}()
			}

		}
		waitTime = cal.GetWaitTime()
		if waitTime < 0 {
			fmt.Println("All phases completed")
			close(ch)
			workersWg.Wait()
			close(reportCh)
			collectorWg.Wait()
			if collector.SkippedIterations > 0 || collector.NumErrors > 0 {
				fmt.Printf("Max Worker Used: %d, Worker Pool Size: %d\n", maxWorkerInUse, CurrentWorkers)
				os.Exit(1)
			}
			fmt.Printf("Max Worker Used: %d, Worker Pool Size: %d\n", maxWorkerInUse, CurrentWorkers)
			os.Exit(0)
		} else {
			spinWait.Wait(waitTime)
		}
	}

}
