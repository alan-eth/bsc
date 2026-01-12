// eth/tracers/live/usdt.go

package live

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/eth/tracers"
	"math/big"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
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

type usdtTracerConfig struct {
	Path    string `json:"path"`
	MaxSize int    `json:"maxSize"`
}

type callFrame struct {
	Depth   int
	To      common.Address
	GasUsed uint64
	Logs    []*types.Log // 本层call产生的logs
}

type usdtTracer struct {
	mu               sync.Mutex
	logger           *lumberjack.Logger
	callStack        []*callFrame
	txHash           common.Hash
	txFrom           common.Address
	txTo             common.Address
	blockNum         uint64
	timestamp        uint64
	gasPrice         uint64
	blockTransfers   []*usdtTransfer
	pendingTransfers []*usdtTransfer
	currentDate      string
	configPath       string
	txReceiptGasUsed uint64
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
	logger := &lumberjack.Logger{
		Filename: filepath.Join(config.Path, "usdt_transfer.jsonl"),
	}
	if config.MaxSize > 0 {
		logger.MaxSize = config.MaxSize
	}
	t := &usdtTracer{
		logger:     logger,
		configPath: configPath,
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
	t.blockNum = ev.Block.NumberU64()
	t.timestamp = ev.Block.Time()

	utc := time.Unix(int64(t.timestamp), 0).UTC()
	dateStr := utc.Format("20060102") // YYYYMMDD
	if t.logger == nil || t.currentDate != dateStr {
		if t.logger != nil {
			_ = t.logger.Close()
		}
		logPath := filepath.Join(t.configPath, fmt.Sprintf("usdt_transfer_%s.jsonl", dateStr))
		t.logger = &lumberjack.Logger{
			Filename: logPath,
			MaxSize:  t.logger.MaxSize,
		}
		t.currentDate = dateStr
	}
}

func (t *usdtTracer) onBlockEnd(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.blockTransfers) == 0 {
		return
	}
	for _, transfer := range t.blockTransfers {
		out, _ := json.Marshal(transfer)
		if _, err := t.logger.Write(out); err != nil {
			fmt.Println("failed to write to usdt tracer log file", err)
		}
		if _, err := t.logger.Write([]byte{'\n'}); err != nil {
			fmt.Println("failed to write to usdt tracer log file", err)
		}
	}
	t.blockTransfers = nil
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
	}
	t.callStack = append(t.callStack, frame)
}

func (t *usdtTracer) onExit(depth int, output []byte, gasUsed uint64, err error, reverted bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.callStack) == 0 {
		return
	}
	frame := t.callStack[len(t.callStack)-1]
	t.callStack = t.callStack[:len(t.callStack)-1]
	frame.GasUsed = gasUsed

	// 只处理 USDT 合约
	if strings.EqualFold(frame.To.Hex(), usdtContractAddress.Hex()) && !reverted {
		// 遍历本层call产生的logs
		for _, log := range frame.Logs {
			if log.Address == usdtContractAddress && len(log.Topics) > 0 && log.Topics[0] == transferTopic {
				from := common.Address{}
				to := common.Address{}
				if len(log.Topics) > 1 {
					from.SetBytes(log.Topics[1].Bytes()[12:])
				}
				if len(log.Topics) > 2 {
					to.SetBytes(log.Topics[2].Bytes()[12:])
				}
				amount := new(big.Int).SetBytes(log.Data)
				transfer := usdtTransfer{
					BlockNumber: t.blockNum,
					Timestamp:   t.timestamp,
					TxHash:      t.txHash,
					TxFrom:      t.txFrom,
					TxTo:        t.txTo,
					From:        from,
					To:          to,
					Amount:      amount,
					GasUsed:     frame.GasUsed,
					GasPrice:    t.gasPrice,
				}
				t.pendingTransfers = append(t.pendingTransfers, &transfer)
			}
		}
	}
	frame.Logs = nil
}

func (t *usdtTracer) onLog(log *types.Log) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// 追加到当前callFrame的logs
	if len(t.callStack) > 0 && log.Address == usdtContractAddress && len(log.Topics) > 0 && log.Topics[0] == transferTopic {
		t.callStack[len(t.callStack)-1].Logs = append(t.callStack[len(t.callStack)-1].Logs, log)
	}
}

func (t *usdtTracer) onClose() {
	if err := t.logger.Close(); err != nil {
		fmt.Println("failed to close usdt tracer log file", err)
	}
}

func (t *usdtTracer) write(data any) {
	out, _ := json.Marshal(data)
	if _, err := t.logger.Write(out); err != nil {
		fmt.Println("failed to write to usdt tracer log file", err)
	}
	if _, err := t.logger.Write([]byte{'\n'}); err != nil {
		fmt.Println("failed to write to usdt tracer log file", err)
	}
}
