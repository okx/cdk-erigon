package smt

import (
	"context"
	"fmt"
	"sync"

	"github.com/dgravesa/go-parallel/parallel"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	"github.com/ledgerwatch/erigon/zk"
)

func (s *SMT) InsertBatch(ctx context.Context, logPrefix string, nodeKeys []*utils.NodeKey, nodeValues []*utils.NodeValue8, nodeValuesHashes []*[4]uint64, rootNodeHash *utils.NodeKey) (*SMTResponse, error) {
	s.clearUpMutex.Lock()
	defer s.clearUpMutex.Unlock()

	var maxInsertingNodePathLevel = 0
	var size int = len(nodeKeys)
	var err error
	var smtBatchNodeRoot *smtBatchNode
	nodeHashesForDelete := make(map[uint64]map[uint64]map[uint64]map[uint64]*utils.NodeKey)

	//start a progress checker
	preprocessStage := uint64(size) / 10
	finalizeStage := preprocessStage
	progressChan, stopProgressPrinter := zk.ProgressPrinterWithoutValues(fmt.Sprintf("[%s] SMT incremental progress", logPrefix), uint64(size)+preprocessStage+finalizeStage)
	defer stopProgressPrinter()

	err = validateDataLengths(nodeKeys, nodeValues, &nodeValuesHashes)
	if err != nil {
		return nil, err
	}

	err = removeDuplicateEntriesByKeys(&size, &nodeKeys, &nodeValues, &nodeValuesHashes)
	if err != nil {
		return nil, err
	}

	err = calculateNodeValueHashesIfMissing(s, nodeValues, &nodeValuesHashes)
	if err != nil {
		return nil, err
	}

	progressChan <- uint64(preprocessStage)

	err = calculateRootNodeHashIfNil(s, &rootNodeHash)
	if err != nil {
		return nil, err
	}

	for i := 0; i < size; i++ {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf(fmt.Sprintf("[%s] Context done", logPrefix))
		default:
		}
		progressChan <- preprocessStage + uint64(i)

		insertingNodeKey := nodeKeys[i]
		insertingNodeValue := nodeValues[i]
		insertingNodeValueHash := nodeValuesHashes[i]

		insertingNodePathLevel, insertingNodePath, insertingPointerToSmtBatchNode, visitedNodeHashes, err := findInsertingPoint(s, insertingNodeKey, rootNodeHash, &smtBatchNodeRoot, insertingNodeValue.IsZero())
		if err != nil {
			return nil, err
		}
		updateNodeHashesForDelete(nodeHashesForDelete, visitedNodeHashes)

		// special case if root does not exists yet
		if (*insertingPointerToSmtBatchNode) == nil {
			if !insertingNodeValue.IsZero() {
				*insertingPointerToSmtBatchNode = newSmtBatchNodeLeaf(insertingNodeKey, (*utils.NodeKey)(insertingNodeValueHash), nil)
			}
			// else branch would be for deleting a value but the root does not exists => there is nothing to delete
			continue
		}

		insertingRemainingKey := utils.RemoveKeyBits(*insertingNodeKey, insertingNodePathLevel)
		if !insertingNodeValue.IsZero() {
			if !((*insertingPointerToSmtBatchNode).isLeaf()) {
				if insertingPointerToSmtBatchNode, err = (*insertingPointerToSmtBatchNode).createALeafInEmptyDirection(insertingNodePath, insertingNodePathLevel, insertingNodeKey); err != nil {
					return nil, err
				}
				insertingRemainingKey = *((*insertingPointerToSmtBatchNode).nodeLeftHashOrRemainingKey)
				insertingNodePathLevel++
			}

			if !((*insertingPointerToSmtBatchNode).nodeLeftHashOrRemainingKey.IsEqualTo(insertingRemainingKey)) {
				currentTreeNodePath := utils.JoinKey(insertingNodePath[:insertingNodePathLevel], *(*insertingPointerToSmtBatchNode).nodeLeftHashOrRemainingKey).GetPath()
				for insertingNodePath[insertingNodePathLevel] == currentTreeNodePath[insertingNodePathLevel] {
					insertingPointerToSmtBatchNode = (*insertingPointerToSmtBatchNode).expandLeafByAddingALeafInDirection(insertingNodePath, insertingNodePathLevel)
					insertingNodePathLevel++
				}

				(*insertingPointerToSmtBatchNode).expandLeafByAddingALeafInDirection(currentTreeNodePath, insertingNodePathLevel)

				if insertingPointerToSmtBatchNode, err = (*insertingPointerToSmtBatchNode).createALeafInEmptyDirection(insertingNodePath, insertingNodePathLevel, insertingNodeKey); err != nil {
					return nil, err
				}
				// EXPLAIN THE LINE BELOW: there is no need to update insertingRemainingKey because it is not needed anymore therefore its value is incorrect if used after this line
				// insertingRemainingKey = *((*insertingPointerToSmtBatchNode).nodeLeftKeyOrRemainingKey)
				insertingNodePathLevel++
			}

			// EXPLAIN THE LINE BELOW: cannot delete the old values because it might be used as a value of an another node
			// updateNodeHashesForDelete(nodeHashesForDelete, []*utils.NodeKey{(*insertingPointerToSmtBatchNode).nodeRightHashOrValueHash})
			(*insertingPointerToSmtBatchNode).nodeRightHashOrValueHash = (*utils.NodeKey)(insertingNodeValueHash)
		} else {
			if (*insertingPointerToSmtBatchNode).nodeLeftHashOrRemainingKey.IsEqualTo(insertingRemainingKey) {
				// EXPLAIN THE LINE BELOW: cannot delete the old values because it might be used as a value of an another node
				// updateNodeHashesForDelete(nodeHashesForDelete, []*utils.NodeKey{(*insertingPointerToSmtBatchNode).nodeRightHashOrValueHash})

				parentAfterDelete := &((*insertingPointerToSmtBatchNode).parentNode)
				*insertingPointerToSmtBatchNode = nil
				insertingPointerToSmtBatchNode = parentAfterDelete
				if (*insertingPointerToSmtBatchNode) != nil {
					(*insertingPointerToSmtBatchNode).updateHashesAfterDelete()
				}
				insertingNodePathLevel--
				// EXPLAIN THE LINE BELOW: there is no need to update insertingRemainingKey because it is not needed anymore therefore its value is incorrect if used after this line
				// insertingRemainingKey = utils.RemoveKeyBits(*insertingNodeKey, insertingNodePathLevel)
			}

			for {
				// the root node has been deleted so we can safely break
				if *insertingPointerToSmtBatchNode == nil {
					break
				}

				// a leaf (with mismatching remaining key) => nothing to collapse
				if (*insertingPointerToSmtBatchNode).isLeaf() {
					break
				}

				// does not have a single leaf => nothing to collapse
				theSingleNodeLeaf, theSingleNodeLeafDirection := (*insertingPointerToSmtBatchNode).getTheSingleLeafAndDirectionIfAny()
				if theSingleNodeLeaf == nil {
					break
				}

				insertingPointerToSmtBatchNode = (*insertingPointerToSmtBatchNode).collapseLeafByRemovingTheSingleLeaf(insertingNodePath, insertingNodePathLevel, theSingleNodeLeaf, theSingleNodeLeafDirection)
				insertingNodePathLevel--
			}
		}

		if maxInsertingNodePathLevel < insertingNodePathLevel {
			maxInsertingNodePathLevel = insertingNodePathLevel
		}
	}

	s.updateDepth(maxInsertingNodePathLevel)

	for _, mapLevel0 := range nodeHashesForDelete {
		for _, mapLevel1 := range mapLevel0 {
			for _, mapLevel2 := range mapLevel1 {
				for _, nodeHash := range mapLevel2 {
					s.Db.DeleteByNodeKey(*nodeHash)
					s.Db.DeleteHashKey(*nodeHash)
				}
			}
		}
	}
	for i, nodeValue := range nodeValues {
		if !nodeValue.IsZero() {
			_, err = s.hashSave(nodeValue.ToUintArray(), utils.BranchCapacity, *nodeValuesHashes[i])
			if err != nil {
				return nil, err
			}
		}
	}

	if smtBatchNodeRoot == nil {
		rootNodeHash = &utils.NodeKey{0, 0, 0, 0}
	} else {
		calculateAndSaveHashesDfs(s, smtBatchNodeRoot, make([]int, 256), 0)
		rootNodeHash = (*utils.NodeKey)(smtBatchNodeRoot.hash)
	}
	if err := s.setLastRoot(*rootNodeHash); err != nil {
		return nil, err
	}

	progressChan <- preprocessStage + uint64(size) + finalizeStage

	return &SMTResponse{
		Mode:          "batch insert",
		NewRootScalar: rootNodeHash,
	}, nil
}

func validateDataLengths(nodeKeys []*utils.NodeKey, nodeValues []*utils.NodeValue8, nodeValuesHashes *[]*[4]uint64) error {
	var size int = len(nodeKeys)

	if len(nodeValues) != size {
		return fmt.Errorf("mismatch nodeValues length, expected %d but got %d", size, len(nodeValues))
	}

	if (*nodeValuesHashes) == nil {
		(*nodeValuesHashes) = make([]*[4]uint64, size)
	}
	if len(*nodeValuesHashes) != size {
		return fmt.Errorf("mismatch nodeValuesHashes length, expected %d but got %d", size, len(*nodeValuesHashes))
	}

	return nil
}

func removeDuplicateEntriesByKeys(size *int, nodeKeys *[]*utils.NodeKey, nodeValues *[]*utils.NodeValue8, nodeValuesHashes *[]*[4]uint64) error {
	storage := make(map[uint64]map[uint64]map[uint64]map[uint64]int)

	resultNodeKeys := make([]*utils.NodeKey, 0, *size)
	resultNodeValues := make([]*utils.NodeValue8, 0, *size)
	resultNodeValuesHashes := make([]*[4]uint64, 0, *size)

	for i, nodeKey := range *nodeKeys {
		setNodeKeyMapValue(storage, nodeKey, i)
	}

	for i, nodeKey := range *nodeKeys {
		latestIndex, found := getNodeKeyMapValue(storage, nodeKey)
		if !found {
			return fmt.Errorf("key not found")
		}

		if latestIndex == i {
			resultNodeKeys = append(resultNodeKeys, nodeKey)
			resultNodeValues = append(resultNodeValues, (*nodeValues)[i])
			resultNodeValuesHashes = append(resultNodeValuesHashes, (*nodeValuesHashes)[i])
		}
	}

	*nodeKeys = resultNodeKeys
	*nodeValues = resultNodeValues
	*nodeValuesHashes = resultNodeValuesHashes

	*size = len(*nodeKeys)

	return nil
}

func calculateNodeValueHashesIfMissing(s *SMT, nodeValues []*utils.NodeValue8, nodeValuesHashes *[]*[4]uint64) error {
	var globalError error
	size := len(nodeValues)
	cpuNum := parallel.DefaultNumGoroutines()

	if size > (cpuNum << 2) {
		var wg sync.WaitGroup
		itemsPerCpu := (size + cpuNum - 1) / cpuNum

		wg.Add(cpuNum)

		for cpuIndex := 0; cpuIndex < cpuNum; cpuIndex++ {
			go func(cpuIndexInThread int) {
				defer wg.Done()
				startIndex := cpuIndexInThread * itemsPerCpu
				endIndex := startIndex + itemsPerCpu
				if endIndex > size {
					endIndex = size
				}
				err := calculateNodeValueHashesIfMissingInInterval(s, nodeValues, nodeValuesHashes, startIndex, endIndex)
				if err != nil {
					globalError = err
				}
			}(cpuIndex)
		}

		wg.Wait()
	} else {
		globalError = calculateNodeValueHashesIfMissingInInterval(s, nodeValues, nodeValuesHashes, 0, len(nodeValues))
	}

	return globalError
}

func calculateNodeValueHashesIfMissingInInterval(s *SMT, nodeValues []*utils.NodeValue8, nodeValuesHashes *[]*[4]uint64, startIndex, endIndex int) error {
	for i := startIndex; i < endIndex; i++ {
		if (*nodeValuesHashes)[i] != nil {
			continue
		}

		nodeValueHashObj, err := s.hashcalc(nodeValues[i].ToUintArray(), utils.BranchCapacity)
		if err != nil {
			return err
		}

		(*nodeValuesHashes)[i] = &nodeValueHashObj
	}

	return nil
}

func calculateRootNodeHashIfNil(s *SMT, root **utils.NodeKey) error {
	if (*root) == nil {
		oldRootObj, err := s.getLastRoot()
		if err != nil {
			return err
		}
		(*root) = &oldRootObj
	}
	return nil
}

func findInsertingPoint(s *SMT, insertingNodeKey, insertingPointerNodeHash *utils.NodeKey, insertingPointerToSmtBatchNode **smtBatchNode, fetchDirectSiblings bool) (int, []int, **smtBatchNode, []*utils.NodeKey, error) {
	var err error
	var insertingNodePathLevel int = -1
	var insertingPointerToSmtBatchNodeParent *smtBatchNode

	var visitedNodeHashes = make([]*utils.NodeKey, 0, 256)

	var nextInsertingPointerNodeHash *utils.NodeKey
	var nextInsertingPointerToSmtBatchNode **smtBatchNode

	insertingNodePath := insertingNodeKey.GetPath()

	for {
		if (*insertingPointerToSmtBatchNode) == nil { // update in-memory structure from db
			if !insertingPointerNodeHash.IsZero() {
				*insertingPointerToSmtBatchNode, err = fetchNodeDataFromDb(s, insertingPointerNodeHash, insertingPointerToSmtBatchNodeParent)
				if err != nil {
					return -2, []int{}, insertingPointerToSmtBatchNode, visitedNodeHashes, err
				}
				visitedNodeHashes = append(visitedNodeHashes, insertingPointerNodeHash)
			} else {
				if insertingNodePathLevel != -1 {
					return -2, []int{}, insertingPointerToSmtBatchNode, visitedNodeHashes, fmt.Errorf("nodekey is zero at non-root level")
				}
			}
		}

		if (*insertingPointerToSmtBatchNode) == nil {
			if insertingNodePathLevel != -1 {
				return -2, []int{}, insertingPointerToSmtBatchNode, visitedNodeHashes, fmt.Errorf("working smt pointer is nil at non-root level")
			}
			break
		}

		insertingNodePathLevel++

		if (*insertingPointerToSmtBatchNode).isLeaf() {
			break
		}

		if fetchDirectSiblings {
			// load direct siblings of a non-leaf from the DB
			if (*insertingPointerToSmtBatchNode).leftNode == nil {
				(*insertingPointerToSmtBatchNode).leftNode, err = fetchNodeDataFromDb(s, (*insertingPointerToSmtBatchNode).nodeLeftHashOrRemainingKey, (*insertingPointerToSmtBatchNode))
				if err != nil {
					return -2, []int{}, insertingPointerToSmtBatchNode, visitedNodeHashes, err
				}
				visitedNodeHashes = append(visitedNodeHashes, (*insertingPointerToSmtBatchNode).nodeLeftHashOrRemainingKey)
			}
			if (*insertingPointerToSmtBatchNode).rightNode == nil {
				(*insertingPointerToSmtBatchNode).rightNode, err = fetchNodeDataFromDb(s, (*insertingPointerToSmtBatchNode).nodeRightHashOrValueHash, (*insertingPointerToSmtBatchNode))
				if err != nil {
					return -2, []int{}, insertingPointerToSmtBatchNode, visitedNodeHashes, err
				}
				visitedNodeHashes = append(visitedNodeHashes, (*insertingPointerToSmtBatchNode).nodeRightHashOrValueHash)
			}
		}

		insertDirection := insertingNodePath[insertingNodePathLevel]
		nextInsertingPointerNodeHash = (*insertingPointerToSmtBatchNode).getNextNodeHashInDirection(insertDirection)
		nextInsertingPointerToSmtBatchNode = (*insertingPointerToSmtBatchNode).getChildInDirection(insertDirection)
		if nextInsertingPointerNodeHash.IsZero() && (*nextInsertingPointerToSmtBatchNode) == nil {
			break
		}

		insertingPointerNodeHash = nextInsertingPointerNodeHash
		insertingPointerToSmtBatchNodeParent = *insertingPointerToSmtBatchNode
		insertingPointerToSmtBatchNode = nextInsertingPointerToSmtBatchNode
	}

	return insertingNodePathLevel, insertingNodePath, insertingPointerToSmtBatchNode, visitedNodeHashes, nil
}

func updateNodeHashesForDelete(nodeHashesForDelete map[uint64]map[uint64]map[uint64]map[uint64]*utils.NodeKey, visitedNodeHashes []*utils.NodeKey) {
	for _, visitedNodeHash := range visitedNodeHashes {
		if visitedNodeHash == nil {
			continue
		}

		setNodeKeyMapValue(nodeHashesForDelete, visitedNodeHash, visitedNodeHash)
	}
}

func calculateAndSaveHashesDfs(s *SMT, smtBatchNode *smtBatchNode, path []int, level int) error {
	if smtBatchNode.isLeaf() {
		hashObj, err := s.hashcalcAndSave(utils.ConcatArrays4(*smtBatchNode.nodeLeftHashOrRemainingKey, *smtBatchNode.nodeRightHashOrValueHash), utils.LeafCapacity)
		if err != nil {
			return err
		}

		nodeKey := utils.JoinKey(path[:level], *smtBatchNode.nodeLeftHashOrRemainingKey)
		s.Db.InsertHashKey(hashObj, *nodeKey)

		smtBatchNode.hash = &hashObj
		return nil
	}

	var totalHash utils.NodeValue8

	if smtBatchNode.leftNode != nil {
		path[level] = 0
		calculateAndSaveHashesDfs(s, smtBatchNode.leftNode, path, level+1)
		totalHash.SetHalfValue(*smtBatchNode.leftNode.hash, 0)
	} else {
		totalHash.SetHalfValue(*smtBatchNode.nodeLeftHashOrRemainingKey, 0)
	}

	if smtBatchNode.rightNode != nil {
		path[level] = 1
		calculateAndSaveHashesDfs(s, smtBatchNode.rightNode, path, level+1)
		totalHash.SetHalfValue(*smtBatchNode.rightNode.hash, 1)
	} else {
		totalHash.SetHalfValue(*smtBatchNode.nodeRightHashOrValueHash, 1)
	}

	hashObj, err := s.hashcalcAndSave(totalHash.ToUintArray(), utils.BranchCapacity)
	if err != nil {
		return err
	}
	smtBatchNode.hash = &hashObj
	return nil
}

type smtBatchNode struct {
	nodeLeftHashOrRemainingKey *utils.NodeKey
	nodeRightHashOrValueHash   *utils.NodeKey
	leaf                       bool
	parentNode                 *smtBatchNode // if nil => this is the root node
	leftNode                   *smtBatchNode
	rightNode                  *smtBatchNode
	hash                       *[4]uint64
}

func newSmtBatchNodeLeaf(nodeLeftHashOrRemainingKey, nodeRightHashOrValueHash *utils.NodeKey, parentNode *smtBatchNode) *smtBatchNode {
	return &smtBatchNode{
		nodeLeftHashOrRemainingKey: nodeLeftHashOrRemainingKey,
		nodeRightHashOrValueHash:   nodeRightHashOrValueHash,
		leaf:                       true,
		parentNode:                 parentNode,
		leftNode:                   nil,
		rightNode:                  nil,
		hash:                       nil,
	}
}

func fetchNodeDataFromDb(s *SMT, nodeHash *utils.NodeKey, parentNode *smtBatchNode) (*smtBatchNode, error) {
	if nodeHash.IsZero() {
		return nil, nil
	}

	dbNodeValue, err := s.Db.Get(*nodeHash)
	if err != nil {
		return nil, err
	}

	nodeLeftHashOrRemainingKey := utils.NodeKeyFromBigIntArray(dbNodeValue[0:4])
	nodeRightHashOrValueHash := utils.NodeKeyFromBigIntArray(dbNodeValue[4:8])
	return &smtBatchNode{
		parentNode:                 parentNode,
		nodeLeftHashOrRemainingKey: &nodeLeftHashOrRemainingKey,
		nodeRightHashOrValueHash:   &nodeRightHashOrValueHash,
		leaf:                       dbNodeValue.IsFinalNode(),
	}, nil
}

func (sbn *smtBatchNode) isLeaf() bool {
	return sbn.leaf
}

func (sbn *smtBatchNode) getTheSingleLeafAndDirectionIfAny() (*smtBatchNode, int) {
	if sbn.leftNode != nil && sbn.rightNode == nil && sbn.leftNode.isLeaf() {
		return sbn.leftNode, 0
	}
	if sbn.leftNode == nil && sbn.rightNode != nil && sbn.rightNode.isLeaf() {
		return sbn.rightNode, 1
	}
	return nil, -1
}

func (sbn *smtBatchNode) getNextNodeHashInDirection(direction int) *utils.NodeKey {
	if direction == 0 {
		return sbn.nodeLeftHashOrRemainingKey
	} else {
		return sbn.nodeRightHashOrValueHash
	}
}

func (sbn *smtBatchNode) getChildInDirection(direction int) **smtBatchNode {
	if direction == 0 {
		return &sbn.leftNode
	} else {
		return &sbn.rightNode
	}
}

func (sbn *smtBatchNode) updateHashesAfterDelete() {
	if sbn.leftNode == nil {
		sbn.nodeLeftHashOrRemainingKey = &utils.NodeKey{0, 0, 0, 0}
	}
	if sbn.rightNode == nil {
		sbn.nodeRightHashOrValueHash = &utils.NodeKey{0, 0, 0, 0}
	}
}

func (sbn *smtBatchNode) createALeafInEmptyDirection(insertingNodePath []int, insertingNodePathLevel int, insertingNodeKey *utils.NodeKey) (**smtBatchNode, error) {
	direction := insertingNodePath[insertingNodePathLevel]
	childPointer := sbn.getChildInDirection(direction)
	if (*childPointer) != nil {
		return nil, fmt.Errorf("branch has already been taken")
	}
	remainingKey := utils.RemoveKeyBits(*insertingNodeKey, insertingNodePathLevel+1)
	*childPointer = newSmtBatchNodeLeaf(&remainingKey, nil, sbn)
	return childPointer, nil
}

func (sbn *smtBatchNode) expandLeafByAddingALeafInDirection(insertingNodeKey []int, insertingNodeKeyLevel int) **smtBatchNode {
	direction := insertingNodeKey[insertingNodeKeyLevel]
	insertingNodeKeyUpToLevel := insertingNodeKey[:insertingNodeKeyLevel]

	childPointer := sbn.getChildInDirection(direction)

	nodeKey := utils.JoinKey(insertingNodeKeyUpToLevel, *sbn.nodeLeftHashOrRemainingKey)
	remainingKey := utils.RemoveKeyBits(*nodeKey, insertingNodeKeyLevel+1)

	*childPointer = newSmtBatchNodeLeaf(&remainingKey, sbn.nodeRightHashOrValueHash, sbn)
	sbn.nodeLeftHashOrRemainingKey = &utils.NodeKey{0, 0, 0, 0}
	sbn.nodeRightHashOrValueHash = &utils.NodeKey{0, 0, 0, 0}
	sbn.leaf = false

	return childPointer
}

func (sbn *smtBatchNode) collapseLeafByRemovingTheSingleLeaf(insertingNodeKey []int, insertingNodeKeyLevel int, theSingleLeaf *smtBatchNode, theSingleNodeLeafDirection int) **smtBatchNode {
	insertingNodeKeyUpToLevel := insertingNodeKey[:insertingNodeKeyLevel+1]
	insertingNodeKeyUpToLevel[insertingNodeKeyLevel] = theSingleNodeLeafDirection
	nodeKey := utils.JoinKey(insertingNodeKeyUpToLevel, *theSingleLeaf.nodeLeftHashOrRemainingKey)
	remainingKey := utils.RemoveKeyBits(*nodeKey, insertingNodeKeyLevel)

	sbn.nodeLeftHashOrRemainingKey = &remainingKey
	sbn.nodeRightHashOrValueHash = theSingleLeaf.nodeRightHashOrValueHash
	sbn.leaf = true
	sbn.leftNode = nil
	sbn.rightNode = nil

	return &sbn.parentNode
}

func setNodeKeyMapValue[T int | *utils.NodeKey](nodeKeyMap map[uint64]map[uint64]map[uint64]map[uint64]T, nodeKey *utils.NodeKey, value T) {
	mapLevel0, found := nodeKeyMap[nodeKey[0]]
	if !found {
		mapLevel0 = make(map[uint64]map[uint64]map[uint64]T)
		nodeKeyMap[nodeKey[0]] = mapLevel0
	}

	mapLevel1, found := mapLevel0[nodeKey[1]]
	if !found {
		mapLevel1 = make(map[uint64]map[uint64]T)
		mapLevel0[nodeKey[1]] = mapLevel1
	}

	mapLevel2, found := mapLevel1[nodeKey[2]]
	if !found {
		mapLevel2 = make(map[uint64]T)
		mapLevel1[nodeKey[2]] = mapLevel2
	}

	mapLevel2[nodeKey[3]] = value
}

func getNodeKeyMapValue[T int | *utils.NodeKey](nodeKeyMap map[uint64]map[uint64]map[uint64]map[uint64]T, nodeKey *utils.NodeKey) (T, bool) {
	var notExistingValue T

	mapLevel0, found := nodeKeyMap[nodeKey[0]]
	if !found {
		return notExistingValue, false
	}

	mapLevel1, found := mapLevel0[nodeKey[1]]
	if !found {
		return notExistingValue, false
	}

	mapLevel2, found := mapLevel1[nodeKey[2]]
	if !found {
		return notExistingValue, false
	}

	value, found := mapLevel2[nodeKey[3]]
	return value, found
}
