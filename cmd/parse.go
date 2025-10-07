package cmd

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/spf13/cobra"
)

// flags for parseCmd
var parseFlags struct {
	rwgOutput      string
	overallOutput  string
	warmup         int
	cooldown       int
	version        int
	slo            int
	realtimeOutput string
	freq           int
}

// parseCmd represents the parse command
var parseCmd = &cobra.Command{
	Use:   "parse",
	Short: "parse the result of a run (invokes analyzer.py)",
	Run: func(cmd *cobra.Command, args []string) {
		Parse()
	},
}

func init() {
	rootCmd.AddCommand(parseCmd)

	parseCmd.Flags().StringVar(&parseFlags.rwgOutput, "rwg_output", "", "Path to the rwg CSV output (required)")
	parseCmd.Flags().StringVar(&parseFlags.overallOutput, "overall_output", "", "Path to write overall report (JSON) (required)")
	parseCmd.Flags().IntVar(&parseFlags.warmup, "warmup", 0, "Warmup seconds to trim from start")
	parseCmd.Flags().IntVar(&parseFlags.cooldown, "cooldown", 0, "Cooldown seconds to trim from end")
	parseCmd.Flags().IntVar(&parseFlags.version, "version", 0, "HTTP version (1 or 2) (required)")
	parseCmd.Flags().IntVar(&parseFlags.slo, "slo", 0, "SLO in milliseconds (required)")
	parseCmd.Flags().StringVar(&parseFlags.realtimeOutput, "realtime_output", "", "Path to write realtime report (JSON)")
	parseCmd.Flags().IntVar(&parseFlags.freq, "freq", 0, "Frequency in milliseconds for realtime report")

	_ = parseCmd.MarkFlagRequired("rwg_output")
	//_ = parseCmd.MarkFlagRequired("overall_output")
	_ = parseCmd.MarkFlagRequired("version")
	_ = parseCmd.MarkFlagRequired("slo")
}

func Parse() {
	// Build command to run the analyzer.py script
	python := "python3"
	scriptPath := filepath.Join("analyzer.py")
	// If the script path isn't found relative to cmd/, try repository root
	if _, err := os.Stat(scriptPath); os.IsNotExist(err) {
		scriptPath = filepath.Join("..", "..", "analyzer.py")
	}

	args := []string{
		scriptPath,
		"--rwg_output", parseFlags.rwgOutput,
		"--overall_output", parseFlags.overallOutput,
		"--warmup", fmt.Sprintf("%d", parseFlags.warmup),
		"--cooldown", fmt.Sprintf("%d", parseFlags.cooldown),
		"--version", fmt.Sprintf("%d", parseFlags.version),
		"--slo", fmt.Sprintf("%d", parseFlags.slo),
		"--realtime_output", parseFlags.realtimeOutput,
		"--freq", fmt.Sprintf("%d", parseFlags.freq),
	}

	cmd := exec.Command(python, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin

	fmt.Printf("Running: %s %v\n", python, args)
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "analyzer.py failed: %v\n", err)
		os.Exit(1)
	}
}
