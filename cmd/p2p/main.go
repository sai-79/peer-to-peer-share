package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"p2p/pkg/discovery"
	"p2p/pkg/meta"
	"p2p/pkg/protocol"
	"p2p/pkg/storage"
	"p2p/pkg/swarm"
)

const version = "1.0.0"

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	subcommand := os.Args[1]
	args := os.Args[2:]

	var err error
	switch subcommand {
	case "share":
		err = handleShare(args)
	case "download":
		err = handleDownload(args)
	case "tracker":
		err = handleTracker(args)
	case "info":
		err = handleInfo(args)
	case "verify":
		err = handleVerify(args)
	case "version":
		fmt.Printf("p2p version %s\n", version)
	default:
		fmt.Fprintf(os.Stderr, "Unknown subcommand: %s\n\n", subcommand)
		printUsage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`P2P File Distribution Engine (Dragonfly/BitTorrent Hybrid)

Usage:
  p2p <command> [arguments]

Commands:
  share       Hash a file, generate .p2p manifest, and seed to swarm
  download    Download a file from peers using a .p2p manifest
  tracker     Run a standalone HTTP tracker coordinator
  info        Inspect a .p2p manifest file
  verify      Verify local file integrity against a manifest
  version     Show version

Run 'p2p <command> -h' for command-specific flags.`)
}

func handleShare(args []string) error {
	fs := flag.NewFlagSet("share", flag.ExitOnError)
	port := fs.Int("port", 6881, "TCP port to listen on for peer connections")
	trackerURL := fs.String("tracker", "", "Optional HTTP tracker announce URL")
	outManifest := fs.String("out", "", "Output path for .p2p manifest file (default <filename>.p2p)")
	pieceSizeKB := fs.Int("piece-size", 256, "Piece size in KB (default: 256)")

	_ = fs.Parse(args)
	if fs.NArg() < 1 {
		return fmt.Errorf("missing target file path. Usage: p2p share [options] <filepath>")
	}

	targetPath := fs.Arg(0)
	absPath, err := filepath.Abs(targetPath)
	if err != nil {
		return err
	}

	fmt.Printf("Analyzing and chunking file: %s ...\n", absPath)
	manifest, err := meta.CreateFromFile(absPath, *pieceSizeKB*1024, *trackerURL)
	if err != nil {
		return fmt.Errorf("chunking failed: %w", err)
	}

	manifestDest := *outManifest
	if manifestDest == "" {
		manifestDest = targetPath + ".p2p"
	}
	if err := manifest.Save(manifestDest); err != nil {
		return fmt.Errorf("failed saving manifest: %w", err)
	}

	fmt.Printf("\n✓ Manifest created successfully!\n")
	fmt.Printf("  File:       %s (%.2f MB)\n", manifest.FileName, float64(manifest.FileSize)/(1024*1024))
	fmt.Printf("  InfoHash:   %s\n", manifest.InfoHash)
	fmt.Printf("  Pieces:     %d (%d KB each)\n", manifest.PieceCount, *pieceSizeKB)
	fmt.Printf("  Manifest:   %s\n\n", manifestDest)

	// Start Seeder
	tf, err := storage.OpenOrCreate(manifest, absPath)
	if err != nil {
		return fmt.Errorf("failed opening storage: %w", err)
	}
	defer tf.Close()

	peerID := protocol.GeneratePeerID()
	sm, err := swarm.NewSwarmManager(peerID, tf)
	if err != nil {
		return err
	}
	defer sm.Close()

	actualPort, err := sm.StartListener(*port)
	if err != nil {
		return err
	}
	fmt.Printf("→ Seeder listening on TCP :%d (PeerID: %s)\n", actualPort, protocol.PeerIDString(peerID))

	// LAN Discovery Beacon
	lan := discovery.NewLANBroadcaster(protocol.PeerIDString(peerID), actualPort)
	lan.AddInfoHash(manifest.InfoHash)
	_ = lan.Start(func(infoHash, peerAddr, remotePeerID string) {
		if infoHash == manifest.InfoHash {
			_, _ = sm.ConnectPeer(peerAddr)
		}
	})
	defer lan.Stop()

	// Tracker Announce Loop
	if *trackerURL != "" {
		go func() {
			for {
				resp, err := discovery.Announce(*trackerURL, manifest.InfoHash, protocol.PeerIDString(peerID), actualPort, 0)
				if err == nil && resp != nil {
					for _, p := range resp.Peers {
						_, _ = sm.ConnectPeer(p.Address())
					}
				}
				time.Sleep(15 * time.Second)
			}
		}()
	}

	fmt.Println("✓ Seeding active. Press Ctrl+C to stop.")
	waitForSignal()
	return nil
}

func handleDownload(args []string) error {
	fs := flag.NewFlagSet("download", flag.ExitOnError)
	port := fs.Int("port", 6882, "TCP port to listen on for peer connections")
	outputDir := fs.String("dir", ".", "Directory to save downloaded file")
	workers := fs.Int("workers", 4, "Concurrent piece download workers")

	_ = fs.Parse(args)
	if fs.NArg() < 1 {
		return fmt.Errorf("missing manifest file. Usage: p2p download [options] <manifest.p2p>")
	}

	manifestPath := fs.Arg(0)
	manifest, err := meta.Load(manifestPath)
	if err != nil {
		return fmt.Errorf("failed loading manifest: %w", err)
	}

	destPath := filepath.Join(*outputDir, manifest.FileName)
	absDest, _ := filepath.Abs(destPath)

	fmt.Printf("Preparing download for: %s\n", manifest.FileName)
	fmt.Printf("  Target Size: %.2f MB (%d pieces)\n", float64(manifest.FileSize)/(1024*1024), manifest.PieceCount)
	fmt.Printf("  Destination: %s\n\n", absDest)

	tf, err := storage.OpenOrCreate(manifest, destPath)
	if err != nil {
		return fmt.Errorf("failed initializing storage: %w", err)
	}
	defer tf.Close()

	peerID := protocol.GeneratePeerID()
	sm, err := swarm.NewSwarmManager(peerID, tf)
	if err != nil {
		return err
	}
	defer sm.Close()

	actualPort, err := sm.StartListener(*port)
	if err != nil {
		return err
	}

	// LAN Discovery
	lan := discovery.NewLANBroadcaster(protocol.PeerIDString(peerID), actualPort)
	lan.AddInfoHash(manifest.InfoHash)
	_ = lan.Start(func(infoHash, peerAddr, remotePeerID string) {
		if infoHash == manifest.InfoHash {
			_, _ = sm.ConnectPeer(peerAddr)
		}
	})
	defer lan.Stop()

	// Tracker Discovery
	if manifest.TrackerURL != "" {
		go func() {
			for {
				left := manifest.FileSize - (int64(tf.Bitfield.Count()) * int64(manifest.PieceSize))
				if left < 0 {
					left = 0
				}
				resp, err := discovery.Announce(manifest.TrackerURL, manifest.InfoHash, protocol.PeerIDString(peerID), actualPort, left)
				if err == nil && resp != nil {
					for _, p := range resp.Peers {
						_, _ = sm.ConnectPeer(p.Address())
					}
				}
				time.Sleep(10 * time.Second)
			}
		}()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Live progress reporting
	go renderProgressBar(ctx, sm)

	// Start downloader
	err = sm.DownloadAll(ctx, *workers)
	if err != nil {
		return fmt.Errorf("download error: %w", err)
	}

	fmt.Printf("\n\n✓ Download complete! Verified all %d pieces.\n", manifest.PieceCount)
	fmt.Printf("  Saved to: %s\n", absDest)
	fmt.Println("→ Now seeding to swarm. Press Ctrl+C to exit.")

	waitForSignal()
	return nil
}

func handleTracker(args []string) error {
	fs := flag.NewFlagSet("tracker", flag.ExitOnError)
	port := fs.Int("port", 8080, "HTTP port for tracker")
	_ = fs.Parse(args)

	server := discovery.NewTrackerServer(*port)
	if err := server.Start(); err != nil {
		return err
	}
	defer server.Stop()

	fmt.Printf("✓ P2P HTTP Tracker running on http://0.0.0.0:%d/announce\n", *port)
	fmt.Printf("  Health check: http://0.0.0.0:%d/health\n", *port)
	fmt.Println("Press Ctrl+C to terminate.")

	waitForSignal()
	return nil
}

func handleInfo(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: p2p info <manifest.p2p>")
	}
	m, err := meta.Load(args[0])
	if err != nil {
		return err
	}

	fmt.Println("=== P2P File Manifest ===")
	fmt.Printf("File Name:    %s\n", m.FileName)
	fmt.Printf("File Size:    %d bytes (%.2f MB)\n", m.FileSize, float64(m.FileSize)/(1024*1024))
	fmt.Printf("InfoHash:     %s\n", m.InfoHash)
	fmt.Printf("Piece Count:  %d\n", m.PieceCount)
	fmt.Printf("Piece Size:   %d bytes (%d KB)\n", m.PieceSize, m.PieceSize/1024)
	fmt.Printf("Created At:   %s\n", m.CreatedAt.Format(time.RFC3339))
	if m.TrackerURL != "" {
		fmt.Printf("Tracker URL:  %s\n", m.TrackerURL)
	}
	if len(m.PieceHashes) > 0 {
		fmt.Printf("First Piece:  %s\n", m.PieceHashes[0])
		fmt.Printf("Last Piece:   %s\n", m.PieceHashes[len(m.PieceHashes)-1])
	}
	return nil
}

func handleVerify(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: p2p verify <manifest.p2p> <data-filepath>")
	}
	m, err := meta.Load(args[0])
	if err != nil {
		return err
	}

	tf, err := storage.OpenOrCreate(m, args[1])
	if err != nil {
		return err
	}
	defer tf.Close()

	valid := tf.Bitfield.Count()
	total := m.PieceCount

	fmt.Printf("Verification result for '%s':\n", args[1])
	fmt.Printf("  Valid Pieces:   %d / %d (%.1f%%)\n", valid, total, float64(valid)*100.0/float64(total))
	if valid == total {
		fmt.Println("✓ File is 100% complete and cryptographically intact.")
	} else {
		missing := tf.Bitfield.MissingPieces()
		fmt.Printf("✗ Missing or corrupted %d pieces: %v\n", len(missing), missing)
	}
	return nil
}

func renderProgressBar(ctx context.Context, sm *swarm.SwarmManager) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats := sm.Stats()
			pct := 0.0
			if stats.TotalPieces > 0 {
				pct = float64(stats.CompletedPieces) * 100.0 / float64(stats.TotalPieces)
			}

			barWidth := 30
			filled := int(pct / 100.0 * float64(barWidth))
			if filled > barWidth {
				filled = barWidth
			}
			bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)

			dlMB := float64(stats.DownloadedBytes) / (1024 * 1024)
			totalMB := float64(stats.TotalBytes) / (1024 * 1024)

			fmt.Printf("\r[%s] %.1f%% | %.2f/%.2f MB | Pieces: %d/%d | Peers: %d ",
				bar, pct, dlMB, totalMB, stats.CompletedPieces, stats.TotalPieces, stats.ActivePeers)

			if stats.CompletedPieces == stats.TotalPieces {
				return
			}
		}
	}
}

func waitForSignal() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan
	fmt.Println("\nShutdown signal received. Exiting gracefully...")
}
