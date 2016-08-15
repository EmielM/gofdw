package main

/*
#cgo CFLAGS: -I/usr/include/postgresql/9.5/server
#cgo LDFLAGS: -Wl,-unresolved-symbols=ignore-all

#include "postgres.h"
#include "access/reloptions.h"
#include "commands/defrem.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "utils/rel.h"
#include "utils/builtins.h"
*/
import "C"

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"log"
	"os/exec"
	"reflect"
	"unsafe"
)

//export FdwInit
func FdwInit() {
	log.Print("FdwInit")
}

//export FdwValidator
func FdwValidator(options *C.List, catalog C.Oid) {

	iterateList(options, func(ptr unsafe.Pointer) {
		elem := (*C.DefElem)(ptr)
		name := C.GoString(elem.defname)
		value := C.GoString(C.defGetString(elem))
		log.Print("option ", name, "=", value)
	})
}

type Cond interface{}

type PeriodCond struct {
	day int
	op  byte
}

type GrepCond struct {
	field string
	value string
}

type PlanState struct {
	conds []Cond
	meta  map[string]int
}

//export FdwGetRelSize
func FdwGetRelSize(root *C.PlannerInfo, baseRel *C.RelOptInfo, tableId C.Oid) {

	state := &PlanState{
		conds: []Cond{},
	}

	baseRel.fdw_private = cRetain(state)

	iterateList(baseRel.baserestrictinfo, func(ptr unsafe.Pointer) {
		clause := ((*C.RestrictInfo)(ptr)).clause
		tag := getNodeTag(unsafe.Pointer(clause))
		if tag == C.T_OpExpr {
			log.Print("OP EXPR!!")
		} else {
			log.Print("other expr ", int(tag))
		}
		state.conds = append(state.conds, &PeriodCond{day: 1, op: '>'})
	})

	// todo:
	// - read $dir/meta
	// - sum tupleCounts
	tupleCount := 0
	selectivity := float64(C.clauselist_selectivity(root, baseRel.baserestrictinfo, 0, C.JOIN_INNER, nil))
	baseRel.rows = C.clamp_row_est(C.double(float64(tupleCount) * selectivity))

	log.Print("FdwGetRelSize ", " rows=", int(baseRel.rows))

	// todo: baseRel.width
}

//export FdwGetPaths
func FdwGetPaths(root *C.PlannerInfo, baseRel *C.RelOptInfo, tableId C.Oid) {

	state := cRetained(baseRel.fdw_private).(*PlanState)

	log.Print("FdwGetPaths ctx=", state)

	// add a single access path
	startupCost := float64(baseRel.baserestrictcost.startup)
	totalCost := startupCost + float64(C.cpu_tuple_cost*baseRel.rows)
	foreignPath := C.create_foreignscan_path(root, baseRel, baseRel.rows, C.Cost(startupCost), C.Cost(totalCost), nil, nil, nil, nil)

	C.add_path(baseRel, &foreignPath.path)
}

//export FdwGetPlan
func FdwGetPlan(root *C.PlannerInfo, baseRel *C.RelOptInfo, tableId C.Oid, bestPath *C.ForeignPath, tlist *C.List, scanClauses *C.List, outerPlan *C.Plan) *C.ForeignScan {

	state := cRetained(baseRel.fdw_private).(*PlanState)

	log.Print("FdwGetPlan ctx=", state)

	scanClauses = C.extract_actual_clauses(scanClauses, C.false)

	baseRel.fdw_private = nil
	cRelease(state)

	// pass data down to FdwBegin: wrap it in a *C.List
	// var private *C.List
	// private = C.lappend(private, baseRel.fdw_private)
	return C.make_foreignscan(tlist, scanClauses, baseRel.relid, scanClauses, nil, nil, nil, outerPlan)
}

type ExecState struct {
	dir   string
	files []string
	cmds  []*exec.Cmd
	out   *bufio.Reader
	count int
}

//export FdwBegin
func FdwBegin(node *C.ForeignScanState, eflags C.int) {
	rel := node.ss.ss_currentRelation // *C.RelationData
	tableId := rel.rd_id

	state := &ExecState{}
	node.fdw_state = cRetain(state)
	// node.fdw_state = C.list_nth(fscan.fdw_private, 0) // copy context pointer from fscan.fdw_private to node.fdw_state

	state.dir = getDir(tableId)

	// todo: check *Plan node.ss.ps.plan is indeed of type ForeignScan?
	fscan := (*C.ForeignScan)(unsafe.Pointer(node.ss.ps.plan))

	iterateList(fscan.fdw_exprs, func(ptr unsafe.Pointer) {
		expr := (*C.Expr)(ptr)
		log.Print("  expr!", int(expr._type), "  ", C.T_OpExpr)
	})

	tupDesc := rel.rd_att
	n := int(tupDesc.natts)

	log.Print("  n=", n)

	cmd := exec.Command("/bin/cat", state.dir+"/data100m.json")

	log.Print("  cmd=", cmd)

	state.cmds = []*exec.Cmd{}

	for {
		state.cmds = append(state.cmds, cmd)
		r, err := cmd.StdoutPipe()
		if err == nil {
			log.Print("start")
			err = cmd.Start()
		}
		if err != nil {
			log.Print(err)
			return
		}
		if len(state.cmds) == 1 {
			state.out = bufio.NewReader(r)
			break
		}

		cmd = exec.Command("/bin/grep", "\"user\":52")
		cmd.Stdin = r
	}

	log.Print("FdwBegin ctx=", state)
}

//export FdwIterate
func FdwIterate(node *C.ForeignScanState) *C.TupleTableSlot {

	state := cRetained(node.fdw_state).(*ExecState)

	slot := node.ss.ss_ScanTupleSlot
	C.ExecClearTuple(slot)

	data, err := state.out.ReadBytes('\n')
	if err != nil {
		if err != io.EOF {
			log.Print(err)
		}
		return slot
	}

	data = data[0 : len(data)-1]

	// log.Print("line: ", string(data))

	tupDesc := slot.tts_tupleDescriptor // type C.TupDesc
	n := int(tupDesc.natts)
	// 1024 should be enough for anyone
	if n > 1024 {
		n = 1024
	}

	values := (*[1024]C.Datum)(unsafe.Pointer(slot.tts_values))
	nulls := (*[1024]C.bool)(unsafe.Pointer(slot.tts_isnull))
	descs := (*[1024]C.Form_pg_attribute)(unsafe.Pointer(tupDesc.attrs))

	var decoded map[string]interface{}
	var decoder *json.Decoder

	for i := 0; i < n; i++ {
		col := C.GoString(&descs[i].attname.data[0])

		var value string
		if col == "data" {
			value = string(data)
		} else if col == "file" {
			value = "todo"
		} else {
			if decoder == nil {
				decoder = json.NewDecoder(bytes.NewBuffer(data))
				decoder.UseNumber()
				err := decoder.Decode(&decoded)
				if err != nil {
					log.Print(err)
					return slot
				}
			}

			v, ok := decoded[col]
			if !ok {
				nulls[i] = C.true
				continue
			}

			switch vv := v.(type) {
			case string:
				value = vv
			case json.Number:
				value = vv.String()
			case bool:
				if vv {
					value = "T"
				} else {
					value = "F"
				}
			}
		}

		typ := descs[i].atttypid  // C.Oid
		mod := descs[i].atttypmod // C.uint32
		values[i] = makeDatum(value, typ, mod)
		if values[i] == (C.Datum)(0) {
			nulls[i] = C.true
		} else {
			nulls[i] = C.false
		}
	}

	state.count++

	C.ExecStoreVirtualTuple(slot)

	return slot
}

//export FdwReScan
func FdwReScan(node *C.ForeignScanState) {
	log.Print("FdwReScan")
}

//export FdwEnd
func FdwEnd(node *C.ForeignScanState) {
	state := cRetained(node.fdw_state).(*ExecState)
	log.Print("FdwEnd ctx=", state)

	for _, cmd := range state.cmds {
		log.Print("  kill ", cmd.Process.Pid)
		cmd.Process.Kill()
		cmd.Wait()
	}

	node.fdw_state = nil
	cRelease(state)
}

func getNodeTag(ptr unsafe.Pointer) C.NodeTag {
	return ((*C.Node)(ptr))._type
}

// golang somehow types C.PgFunction as *[0]byte
type pgFunc *[0]byte

func makeDatum(data string, typ C.Oid, mod C.int32) C.Datum {

	cData := C.CString(data)
	defer C.free(unsafe.Pointer(cData))

	datum := (C.Datum)(uintptr(unsafe.Pointer(cData)))
	// this is now a c-string (0-terminated) pg Datum

	switch typ {
	case 16: // BOOLID
		return C.DirectFunctionCall1Coll((pgFunc)(C.boolin), C.InvalidOid, datum)
	case 21: // INT2OID
		return C.DirectFunctionCall1Coll((pgFunc)(C.int2in), C.InvalidOid, datum)
	case 23: // INT4OID
		return C.DirectFunctionCall1Coll((pgFunc)(C.int4in), C.InvalidOid, datum)
	case 25: // TEXTOID
		// alternative: convert via C.cstring_to_text(C.CString(data))
		return C.DirectFunctionCall1Coll((pgFunc)(C.textin), C.InvalidOid, datum)
	default:
		return C.Datum(0)
	}

}

// iterates a *C.List
func iterateList(list *C.List, cb func(unsafe.Pointer)) {
	if list == nil {
		return
	}

	for cell := list.head; cell != nil; cell = cell.next {
		// &cell.data[0] is the address of the (data in) cell, dereference to get the value
		cb(*(*unsafe.Pointer)(unsafe.Pointer(&cell.data[0])))
	}
}

func getDir(tableId C.Oid) string {

	var ret string

	cb := func(ptr unsafe.Pointer) {
		elem := (*C.DefElem)(ptr)
		name := C.GoString(elem.defname)
		value := C.GoString(C.defGetString(elem))
		if name == "directory" {
			ret = value
		}
	}

	table := C.GetForeignTable(tableId)
	iterateList(C.GetForeignServer(table.serverid).options, cb)
	iterateList(table.options, cb)

	return ret
}

// retain stuff for gc so we can pass it through C

var cMap = make(map[uintptr]interface{})

// should be passed a pointer!
func cRetain(x interface{}) unsafe.Pointer {
	ptr := reflect.ValueOf(x).Pointer()
	cMap[ptr] = x
	return unsafe.Pointer(ptr)
}

func cRetained(ptr unsafe.Pointer) interface{} {
	return cMap[uintptr(ptr)]
}

func cRelease(x interface{}) {
	ptr := reflect.ValueOf(x).Pointer()
	delete(cMap, ptr)
}

func main() {}
