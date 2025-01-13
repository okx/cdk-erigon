// Copyright 2021 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package native

import (
	"encoding/json"
	"errors"
	"math/big"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/ledgerwatch/log/v3"

	"github.com/holiman/uint256"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/tracers"
)

//go:generate go run github.com/fjl/gencodec -type callFrame -field-override callFrameMarshaling -out gen_callframe_json.go

func init() {
	register("okTracer", NewOKTracer)
}

type okLog struct {
	Index   uint64            `json:"index"`
	Address libcommon.Address `json:"address"`
	Topics  []libcommon.Hash  `json:"topics"`
	Data    hexutility.Bytes  `json:"data"`
}

type okFrame struct {
	Type     vm.OpCode         `json:"-"`
	From     libcommon.Address `json:"from"`
	Gas      uint64            `json:"gas"`
	GasUsed  uint64            `json:"gasUsed"`
	To       libcommon.Address `json:"to,omitempty" rlp:"optional"`
	Input    []byte            `json:"input" rlp:"optional"`
	Output   []byte            `json:"output,omitempty" rlp:"optional"`
	Error    string            `json:"error,omitempty" rlp:"optional"`
	Revertal string            `json:"revertReason,omitempty"`
	Calls    []okFrame         `json:"calls,omitempty" rlp:"optional"`
	Logs     []okLog           `json:"logs,omitempty" rlp:"optional"`
	// Placed at end on purpose. The RLP will be decoded to 0 instead of
	// nil if there are non-empty elements after in the struct.
	Value *big.Int `json:"value,omitempty" rlp:"optional"`
}

func (f okFrame) TypeString() string {
	return f.Type.String()
}

func (f okFrame) failed() bool {
	return len(f.Error) > 0
}

func (f *okFrame) processOutput(output []byte, err error) {
	output = libcommon.CopyBytes(output)
	if err == nil {
		f.Output = output
		return
	}
	f.Error = err.Error()
	if f.Type == vm.CREATE || f.Type == vm.CREATE2 {
		f.To = libcommon.Address{}
	}
	if !errors.Is(err, vm.ErrExecutionReverted) || len(output) == 0 {
		return
	}
	f.Output = output
	if len(output) < 4 {
		return
	}
	if unpacked, err := abi.UnpackRevert(output); err == nil {
		f.Revertal = unpacked
	}
}

type okFrameMarshaling struct {
	TypeString string `json:"type"`
	Gas        hexutil.Uint64
	GasUsed    hexutil.Uint64
	Value      *hexutil.Big
	Input      hexutil.Bytes
	Output     hexutil.Bytes
}

type okTracer struct {
	noopTracer
	callstack []okFrame
	config    okTracerConfig
	gasLimit  uint64
	interrupt uint32 // Atomic flag to signal execution interruption
	reason    error  // Textual reason for the interruption
}

type okTracerConfig struct {
	OnlyTopCall bool `json:"onlyTopCall"` // If true, call tracer won't collect any subcalls
	WithLog     bool `json:"withLog"`     // If true, call tracer will collect event logs
}

// NewOKTracer returns a native go tracer which tracks
// call frames of a tx, and implements fakevm.EVMLogger.
func NewOKTracer(ctx *tracers.Context, cfg json.RawMessage) (tracers.Tracer, error) {
	var config okTracerConfig
	if cfg != nil {
		if err := json.Unmarshal(cfg, &config); err != nil {
			return nil, err
		}
	}
	// First callframe contains tx context info
	// and is populated on start and end.
	return &okTracer{callstack: make([]okFrame, 1), config: config}, nil
}

// CaptureStart implements the EVMLogger interface to initialize the tracing operation.
func (t *okTracer) CaptureStart(env *vm.EVM, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	toCopy := to
	t.callstack[0] = okFrame{
		Type:  vm.CALL,
		From:  from,
		To:    toCopy,
		Input: libcommon.CopyBytes(input),
		Gas:   gas,
	}
	if value != nil {
		t.callstack[0].Value = value.ToBig()
	}
	if create {
		t.callstack[0].Type = vm.CREATE
	}
}

// CaptureEnd is called after the call finishes to finalize the tracing.
func (t *okTracer) CaptureEnd(output []byte, gasUsed uint64, err error) {
	t.callstack[0].processOutput(output, err)
}

// CaptureState implements the EVMLogger interface to trace a single step of VM execution.
func (t *okTracer) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
	// Only logs need to be captured via opcode processing
	if !t.config.WithLog {
		return
	}
	// Avoid processing nested calls when only caring about top call
	if t.config.OnlyTopCall && depth > 0 {
		return
	}
	// Skip if tracing was interrupted
	if atomic.LoadUint32(&t.interrupt) > 0 {
		return
	}
	switch op {
	case vm.LOG0, vm.LOG1, vm.LOG2, vm.LOG3, vm.LOG4:
		size := int(op - vm.LOG0)

		stack := scope.Stack
		stackData := stack.Data

		// Don't modify the stack
		mStart := stackData[len(stackData)-1]
		mSize := stackData[len(stackData)-2]
		topics := make([]libcommon.Hash, size)
		for i := 0; i < size; i++ {
			topic := stackData[len(stackData)-2-(i+1)]
			topics[i] = libcommon.Hash(topic.Bytes32())
		}

		data := scope.Memory.GetCopy(int64(mStart.Uint64()), int64(mSize.Uint64()))
		log := okLog{Address: scope.Contract.Address(), Topics: topics, Data: hexutility.Bytes(data)}
		t.callstack[len(t.callstack)-1].Logs = append(t.callstack[len(t.callstack)-1].Logs, log)
	}
}

// CaptureEnter is called when EVM enters a new scope (via call, create or selfdestruct).
func (t *okTracer) CaptureEnter(typ vm.OpCode, from libcommon.Address, to libcommon.Address, precompile, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	if t.config.OnlyTopCall {
		return
	}
	// Skip if tracing was interrupted
	if atomic.LoadUint32(&t.interrupt) > 0 {
		return
	}

	toCopy := to
	call := okFrame{
		Type:  typ,
		From:  from,
		To:    toCopy,
		Input: libcommon.CopyBytes(input),
		Gas:   gas,
	}
	if value != nil {
		call.Value = value.ToBig()
	}
	t.callstack = append(t.callstack, call)
}

// CaptureExit is called when EVM exits a scope, even if the scope didn't
// execute any code.
func (t *okTracer) CaptureExit(output []byte, gasUsed uint64, err error) {
	if t.config.OnlyTopCall {
		return
	}
	size := len(t.callstack)
	if size <= 1 {
		return
	}
	// pop call
	call := t.callstack[size-1]
	t.callstack = t.callstack[:size-1]
	size -= 1

	call.GasUsed = gasUsed
	call.processOutput(output, err)
	t.callstack[size-1].Calls = append(t.callstack[size-1].Calls, call)
}

func (t *okTracer) CaptureTxStart(gasLimit uint64) {
	t.gasLimit = gasLimit
}

func (t *okTracer) CaptureTxEnd(restGas uint64) {
	t.callstack[0].GasUsed = t.gasLimit - restGas
	if t.config.WithLog {
		// Logs are not emitted when the call fails
		clearOKFailedLogs(&t.callstack[0], false)
	}
}

func inArray(target string, list []string) bool {
	for _, item := range list {
		if item == target {
			return true
		}
	}
	return false
}

func internalTxTraceToInnerTxs(tx okFrame) []*InnerTx {
	dfs := Dfs{}
	indexMap := make(map[int]int)
	indexMap[0] = 0
	var level = 0
	var index = 1
	isError := tx.Error != ""
	dfs.dfs(tx, level, index, indexMap, isError)
	return dfs.innerTxs
}

type Dfs struct {
	innerTxs []*InnerTx
}

func (d *Dfs) dfs(tx okFrame, level int, index int, indexMap map[int]int, isError bool) {
	if !inArray(strings.ToLower(tx.Type.String()), []string{"call", "create", "create2",
		"callcode", "delegatecall", "staticcall", "selfdestruct"}) {
		return
	}

	name := strings.ToLower(tx.Type.String())

	for i := 0; i < level; i++ {
		if indexMap[i] == 0 {
			continue
		}
		name = name + "_" + strconv.Itoa(indexMap[i])
	}
	innerTx := internalTxTraceToInnerTx(tx, name, level, index)
	if !isError {
		isError = innerTx.IsError
	} else {
		innerTx.IsError = isError
	}
	d.innerTxs = append(d.innerTxs, innerTx)
	index = 0
	for _, call := range tx.Calls {
		index++
		indexMap[level] = index
		d.dfs(call, level+1, index+1, indexMap, isError)
	}
	if len(tx.Calls) == 0 {
		return
	}
}

type InnerTx struct {
	Dept          big.Int `json:"dept"`
	InternalIndex big.Int `json:"internal_index"`
	From          string  `json:"from"`
	To            string  `json:"to"`
	Input         string  `json:"input"`
	Output        string  `json:"output"`

	IsError      bool   `json:"is_error"`
	GasUsed      uint64 `json:"gas_used"`
	Value        string `json:"value"`
	ValueWei     string `json:"value_wei"`
	CallValueWei string `json:"call_value_wei"`
	Error        string `json:"error"`
	Gas          uint64 `json:"gas"`
	//ReturnGas    uint64 `json:"return_gas"`

	CallType     string `json:"call_type"`
	Name         string `json:"name"`
	TraceAddress string `json:"trace_address"`
	CodeAddress  string `json:"code_address"`
}

func internalTxTraceToInnerTx(currentTx okFrame, name string, depth int, index int) *InnerTx {
	value := currentTx.Value
	if value == nil {
		value = big.NewInt(0)
	}
	var toAddress string
	if len(currentTx.To) != 0 {
		// 0x2ea4d738775e11d96c5e0c0810cb24e26e1af074c90de038e403858238dd972c
		toAddress = currentTx.To.String()
	}
	callTx := &InnerTx{
		Dept:         *big.NewInt(int64(depth)),
		From:         currentTx.From.String(),
		To:           toAddress,
		ValueWei:     value.String(),
		CallValueWei: hexutil.EncodeBig(value),
		CallType:     strings.ToLower(currentTx.Type.String()),
		Name:         name,
		Input:        hexutil.Encode(currentTx.Input),
		Output:       hexutil.Encode(currentTx.Output),
		Gas:          currentTx.Gas,
		GasUsed:      currentTx.GasUsed,
		IsError:      false,
		//ReturnGas:    currentTx.Gas - currentTx.GasUsed,
	}
	if index-2 >= 0 {
		callTx.InternalIndex = *big.NewInt(int64(index - 2))
	} else {
		callTx.InternalIndex = *big.NewInt(0)
		log.Error("internalTxTraceToInnerTx got -ve internal index.", "index", index, "calc idx", index-2)
	}
	if strings.ToLower(currentTx.Type.String()) == "callcode" {
		callTx.CodeAddress = currentTx.To.String()
	}
	if currentTx.Error != "" {
		callTx.Error = currentTx.Error
		callTx.IsError = true
	}
	return callTx
}

// GetResult returns the json-encoded nested list of call traces, and any
// error arising from the encoding or forceful termination (via `Stop`).
func (t *okTracer) GetResult() (json.RawMessage, error) {
	if len(t.callstack) != 1 {
		log.Error("incorrect number of top-level calls", "len", len(t.callstack))
		return json.RawMessage([]byte("[]")), nil
	}
	// Turn the okFrame into a list
	innerTxs := internalTxTraceToInnerTxs(t.callstack[0])
	if len(innerTxs) <= 1 {
		return json.RawMessage([]byte("[]")), nil
	}

	res, err := json.Marshal(innerTxs)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(res), t.reason
}

// Stop terminates execution of the tracer at the first opportune moment.
func (t *okTracer) Stop(err error) {
	t.reason = err
	atomic.StoreUint32(&t.interrupt, 1)
}

// clearOKFailedLogs clears the logs of a callframe and all its children
// in case of execution failure.
func clearOKFailedLogs(cf *okFrame, parentFailed bool) {
	failed := cf.failed() || parentFailed
	// Clear own logs
	if failed {
		cf.Logs = nil
	}
	for i := range cf.Calls {
		clearOKFailedLogs(&cf.Calls[i], failed)
	}
}
