// eth/tracers/live/usdt.go

package live

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"path/filepath"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/tracers"
	"gopkg.in/natefinch/lumberjack.v2"
)

func init() {
	tracers.LiveDirectory.Register("usdt", newUsdtTracer)
}

var (
	usdtContractAddress = common.HexToAddress("0x55d398326f99059ff775485246999027b3197955") // BSC USDT
	transferTopic       = common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
)

type usdtTransfer struct {
	BlockNumber uint64         `json:"blockNumber"`
	BlockHash   common.Hash    `json:"blockHash"`
	Timestamp   uint64         `json:"timestamp"`
	TxHash      common.Hash    `json:"txHash"`
	TxFrom      common.Address `json:"txFrom"`
	TxTo        common.Address `json:"txTo"`
	From        common.Address `json:"from"`
	To          common.Address `json:"to"`
	Amount      *big.Int       `json:"amount"`
	GasUsed     uint64         `json:"gasUsed"`
	GasPrice    uint64         `json:"gasPrice"`
}

// blockInfo stores block metadata for reorg detection.
// Each block's info is written to block_info_YYYYMMDD.jsonl file.
type blockInfo struct {
	BlockNumber uint64      `json:"blockNumber"`
	BlockHash   common.Hash `json:"blockHash"`
	ParentHash  common.Hash `json:"parentHash"`
	Timestamp   uint64      `json:"timestamp"`
}

type usdtTracerConfig struct {
	Path    string `json:"path"`
	MaxSize int    `json:"maxSize"`
}

// callFrame tracks the state of a single call frame during transaction execution.
// It uses a tree structure similar to supply.go to properly handle reverts.
type callFrame struct {
	Depth   int
	To      common.Address
	GasUsed uint64
	Logs    []*types.Log // logs emitted in this call frame
	calls   []callFrame  // child call frames
}

// usdtTracer tracks USDT transfer events during block processing.
type usdtTracer struct {
	mu               sync.Mutex
	logger           *lumberjack.Logger
	blockInfoLogger  *lumberjack.Logger // logger for block info (reorg detection)
	callStack        []*callFrame
	txHash           common.Hash
	txFrom           common.Address
	txTo             common.Address
	blockNum         uint64
	blockHash        common.Hash
	parentHash       common.Hash // parent block hash for reorg detection
	timestamp        uint64
	gasPrice         uint64
	blockTransfers   []*usdtTransfer
	pendingTransfers []*usdtTransfer
	currentDate      string
	configPath       string
	maxSize          int // max size for log rotation
}

func newUsdtTracer(cfg json.RawMessage) (*tracing.Hooks, error) {
	var config usdtTracerConfig
	if err := json.Unmarshal(cfg, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %v", err)
	}
	if config.Path == "" {
		return nil, errors.New("usdt tracer output path is required")
	}
	configPath := config.Path
	maxSize := config.MaxSize
	if maxSize <= 0 {
		maxSize = 100 // default 100MB
	}
	t := &usdtTracer{
		configPath: configPath,
		maxSize:    maxSize,
	}
	return &tracing.Hooks{
		OnBlockStart: t.onBlockStart,
		OnBlockEnd:   t.onBlockEnd,
		OnTxStart:    t.onTxStart,
		OnTxEnd:      t.onTxEnd,
		OnEnter:      t.onEnter,
		OnExit:       t.onExit,
		OnLog:        t.onLog,
		OnClose:      t.onClose,
	}, nil
}

func (t *usdtTracer) onBlockStart(ev tracing.BlockEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.blockNum = ev.Block.NumberU64()
	t.blockHash = ev.Block.Hash()
	t.parentHash = ev.Block.ParentHash()
	t.timestamp = ev.Block.Time()

	// Rotate log files by UTC date
	utc := time.Unix(int64(t.timestamp), 0).UTC()
	dateStr := utc.Format("20060102") // YYYYMMDD

	if t.logger == nil || t.blockInfoLogger == nil || t.currentDate != dateStr {
		// Close existing loggers
		if t.logger != nil {
			_ = t.logger.Close()
		}
		if t.blockInfoLogger != nil {
			_ = t.blockInfoLogger.Close()
		}

		// Create new transfer logger
		transferLogPath := filepath.Join(t.configPath, fmt.Sprintf("usdt_transfer_%s.jsonl", dateStr))
		t.logger = &lumberjack.Logger{
			Filename: transferLogPath,
			MaxSize:  t.maxSize,
		}

		// Create new block info logger for reorg detection
		blockInfoLogPath := filepath.Join(t.configPath, fmt.Sprintf("block_info_%s.jsonl", dateStr))
		t.blockInfoLogger = &lumberjack.Logger{
			Filename: blockInfoLogPath,
			MaxSize:  t.maxSize,
		}

		t.currentDate = dateStr
	}
}

func (t *usdtTracer) onBlockEnd(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Always write block info for reorg detection (even if no transfers)
	t.writeBlockInfo()

	// Write transfer records
	if len(t.blockTransfers) == 0 {
		return
	}
	for _, transfer := range t.blockTransfers {
		out, _ := json.Marshal(transfer)
		out = append(out, '\n')
		if _, err := t.logger.Write(out); err != nil {
			fmt.Println("failed to write to usdt tracer log file", err)
		}
	}
	t.blockTransfers = nil
}

// writeBlockInfo persists block metadata for reorg detection.
// This is called at the end of each block to record the block hash chain.
func (t *usdtTracer) writeBlockInfo() {
	if t.blockInfoLogger == nil {
		return
	}
	info := blockInfo{
		BlockNumber: t.blockNum,
		BlockHash:   t.blockHash,
		ParentHash:  t.parentHash,
		Timestamp:   t.timestamp,
	}
	out, _ := json.Marshal(info)
	out = append(out, '\n')
	if _, err := t.blockInfoLogger.Write(out); err != nil {
		fmt.Println("failed to write to block info log file", err)
	}
}

func (t *usdtTracer) onTxStart(vm *tracing.VMContext, tx *types.Transaction, from common.Address) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.callStack = nil
	t.txHash = tx.Hash()
	t.txFrom = from
	if tx.To() != nil {
		t.txTo = *tx.To()
	} else {
		t.txTo = common.Address{}
	}
	if tx.Type() == types.DynamicFeeTxType {
		baseFee := vm.BaseFee
		tip := tx.GasTipCap()
		feeCap := tx.GasFeeCap()
		effective := new(big.Int).Add(baseFee, tip)
		if effective.Cmp(feeCap) > 0 {
			effective.Set(feeCap)
		}
		t.gasPrice = effective.Uint64()
	} else {
		t.gasPrice = tx.GasPrice().Uint64()
	}
}

func (t *usdtTracer) onTxEnd(receipt *types.Receipt, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if receipt != nil && receipt.Status == types.ReceiptStatusSuccessful {
		if t.txTo == usdtContractAddress {
			for _, transfer := range t.pendingTransfers {
				transfer.GasUsed = receipt.GasUsed
			}
		}
		t.blockTransfers = append(t.blockTransfers, t.pendingTransfers...)
	}
	t.pendingTransfers = nil
}

func (t *usdtTracer) onEnter(depth int, typ byte, from, to common.Address, input []byte, gas uint64, value *big.Int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	frame := &callFrame{
		Depth: depth,
		To:    to,
		calls: make([]callFrame, 0),
	}
	t.callStack = append(t.callStack, frame)
}

func (t *usdtTracer) onExit(depth int, output []byte, gasUsed uint64, err error, reverted bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if depth == 0 {
		// Process the root frame if not reverted and callStack is not empty
		if !reverted && len(t.callStack) > 0 {
			t.internalTxsHandler(t.callStack[0])
		}
		// Clear callStack to prevent memory leak?
		t.callStack = nil
		return
	}

	if len(t.callStack) == 0 {
		return
	}

	// Pop the current frame
	frame := t.callStack[len(t.callStack)-1]
	t.callStack = t.callStack[:len(t.callStack)-1]
	frame.GasUsed = gasUsed

	// If reverted, drop this frame and all its children (they won't be added to parent)
	if reverted {
		return
	}

	// If not reverted, add this frame as a child of the parent frame
	if len(t.callStack) > 0 {
		t.callStack[len(t.callStack)-1].calls = append(t.callStack[len(t.callStack)-1].calls, *frame)
	}
}

func (t *usdtTracer) internalTxsHandler(frame *callFrame) {

	if frame == nil {
		return
	}

	// dfs to handle the internal calls
	for _, call := range frame.calls {
		callCopy := call
		t.internalTxsHandler(&callCopy)
	}

	// extract transfer from the call frame
	t.extractTransfer(frame)
}

func (t *usdtTracer) extractTransfer(frame *callFrame) {
	// Direct address comparison is more efficient than string comparison
	if frame.To != usdtContractAddress {
		return
	}

	for _, log := range frame.Logs {
		if log.Address != usdtContractAddress || len(log.Topics) == 0 || log.Topics[0] != transferTopic {
			continue
		}
		var from, to common.Address
		if len(log.Topics) > 1 {
			from = common.BytesToAddress(log.Topics[1].Bytes()[12:])
		}
		if len(log.Topics) > 2 {
			to = common.BytesToAddress(log.Topics[2].Bytes()[12:])
		}
		transfer := &usdtTransfer{
			BlockNumber: t.blockNum,
			BlockHash:   t.blockHash,
			Timestamp:   t.timestamp,
			TxHash:      t.txHash,
			TxFrom:      t.txFrom,
			TxTo:        t.txTo,
			From:        from,
			To:          to,
			Amount:      new(big.Int).SetBytes(log.Data),
			GasUsed:     frame.GasUsed,
			GasPrice:    t.gasPrice,
		}
		t.pendingTransfers = append(t.pendingTransfers, transfer)
	}
}

func (t *usdtTracer) onLog(log *types.Log) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// Append USDT transfer logs to the current call frame
	if len(t.callStack) > 0 && log.Address == usdtContractAddress && len(log.Topics) > 0 && log.Topics[0] == transferTopic {
		t.callStack[len(t.callStack)-1].Logs = append(t.callStack[len(t.callStack)-1].Logs, log)
	}
}

func (t *usdtTracer) onClose() {
	if t.logger != nil {
		if err := t.logger.Close(); err != nil {
			fmt.Println("failed to close usdt tracer log file", err)
		}
	}
	if t.blockInfoLogger != nil {
		if err := t.blockInfoLogger.Close(); err != nil {
			fmt.Println("failed to close block info log file", err)
		}
	}
}
