/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{
// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string& relationName,
                       std::string& outIndexName,
                       BufMgr *bufMgrIn,
                       const int attrByteOffset,
                       const Datatype attrType) {
    this->bufMgr        = bufMgrIn;
    this->scanExecuting = false;

    this->leafOccupancy = INTARRAYLEAFSIZE;
    this->nodeOccupancy = INTARRAYNONLEAFSIZE;

    // this->leafOccupancy = 10;
    // this->nodeOccupancy = 10;

    this->attributeType  = attrType;
    this->attrByteOffset = attrByteOffset;

    std::ostringstream idxStr;
    idxStr << relationName << '.' << attrByteOffset;
    std::string indexName = idxStr.str();

    outIndexName = indexName;

    if (File::exists(outIndexName)) {
        this->file = new BlobFile(outIndexName, false);

        Page *metaPage;

        headerPageNum = file->getFirstPageNo();
        bufMgr->readPage(file, headerPageNum, metaPage);

        IndexMetaInfo *indexMetaInfo = (IndexMetaInfo *) metaPage;

        if ((indexMetaInfo->attrType != attrType) ||
            (indexMetaInfo->attrByteOffset != attrByteOffset) ||
            (indexMetaInfo->relationName != relationName)) {
            throw BadIndexInfoException("Error: Index meta attributes don't match!");
        }

        rootPageNum = indexMetaInfo->rootPageNo;

        rootIsLeaf = rootPageNum == 2;

        this->bufMgr->unPinPage(this->file, headerPageNum, false);
    }
    else {
        rootIsLeaf = true;

        file = new BlobFile(outIndexName, true);
        Page *indexMetaInfoPage;
        this->bufMgr->allocPage(this->file, headerPageNum, indexMetaInfoPage);

        struct IndexMetaInfo *metaInfo = (struct IndexMetaInfo *) indexMetaInfoPage;
        metaInfo->attrByteOffset = attrByteOffset;
        metaInfo->attrType       = attrType;
        strncpy(metaInfo->relationName, relationName.c_str(),
                sizeof(metaInfo->relationName));

        Page *rootPage;
        bufMgr->allocPage(this->file, rootPageNum, rootPage);
        LeafNodeInt *root = (LeafNodeInt *) rootPage;

        for (int idx = 0; idx < leafOccupancy; idx++) {
            (root->ridArray[idx]).page_number = 0;
        }
        root->rightSibPageNo = 0;

        metaInfo->rootPageNo = rootPageNum; // Starts at 2

        bufMgr->unPinPage(file, headerPageNum, true);
        bufMgr->unPinPage(file, rootPageNum, true);

        RecordId  curr_rid;
        FileScan *fs = new FileScan(relationName, bufMgr);

        try {
            while (true) {
                //// scan entry, insert entries here ////
                fs->scanNext(curr_rid);
                std::string recordStr      = fs->getRecord();
                const char *recordToInsert = recordStr.c_str();
                insertEntry(recordToInsert + attrByteOffset, curr_rid);
            }
        } catch (EndOfFileException e) {
        }

        bufMgr->flushFile(file);
        delete fs;
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex() {
    bufMgr->flushFile(this->file);
    scanExecuting = false;
    delete file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
    RIDKeyPair <int> ridkey_entry;
    ridkey_entry.set(rid, *(int *) key);

    SplitData <int> *splitData;

    if (rootIsLeaf) {
        splitData = insertLeafEntry(rootPageNum, &ridkey_entry);
    }
    else {
        splitData = insertNonLeafEntry(rootPageNum, &ridkey_entry);
    }

    if (splitData) {
        Page * newRootPage;
        PageId newPageId;

        bufMgr->allocPage(file, newPageId, newRootPage);

        struct NonLeafNodeInt *newRoot = (struct NonLeafNodeInt *) newRootPage;
        for (int i = 0; i <= nodeOccupancy; i++) {
            newRoot->pageNoArray[i] = 0;
        }

        newRoot->keyArray[0]    = splitData->key;
        newRoot->pageNoArray[0] = rootPageNum;
        newRoot->pageNoArray[1] = splitData->newPageId;

        newRoot->level = rootIsLeaf ? 1 : 0;

        rootIsLeaf  = false;
        rootPageNum = newPageId;

        Page *metaPage;
        bufMgr->readPage(file, headerPageNum, metaPage);

        struct IndexMetaInfo *metaInfo = (struct IndexMetaInfo *) metaPage;
        metaInfo->rootPageNo = rootPageNum;

        delete splitData;

        bufMgr->unPinPage(file, headerPageNum, true);
        bufMgr->unPinPage(file, rootPageNum, true);
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::getLastFullIndex
// -----------------------------------------------------------------------------

const int BTreeIndex::getLastFullIndex(Page *node, bool isLeaf) {
    if (isLeaf) {
        LeafNodeInt *leafNode = (LeafNodeInt *) node;

        int idx;
        for (idx = 0; idx < leafOccupancy && (leafNode->ridArray[idx]).page_number != 0; idx++);
        return idx - 1;
    }
    else {
        NonLeafNodeInt *nonLeafNode = (NonLeafNodeInt *) node;

        int idx;
        for (idx = 0; idx <= nodeOccupancy && nonLeafNode->pageNoArray[idx] != 0; idx++);
        return idx - 1;
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertLeafEntry
// -----------------------------------------------------------------------------

SplitData <int> *BTreeIndex::insertLeafEntry(PageId leafNum, RIDKeyPair <int> *ridKeyPair) {
    Page *leafPage;

    bufMgr->readPage(file, leafNum, leafPage);
    struct LeafNodeInt *leafNode = (struct LeafNodeInt *) leafPage;

    int lastFullIndex = getLastFullIndex(leafPage, true);

    if (lastFullIndex >= leafOccupancy - 1) {
        SplitData <int> *splitData = splitLeafNode(leafNode, ridKeyPair);
        bufMgr->unPinPage(file, leafNum, true);
        return splitData;
    }


    insertToLeaf(leafNode, ridKeyPair, getLastFullIndex(leafPage, true));
    bufMgr->unPinPage(file, leafNum, true);
    return NULL;
}

// -----------------------------------------------------------------------------
// BTreeIndex::splitLeafNode
// -----------------------------------------------------------------------------

SplitData <int> *BTreeIndex::splitLeafNode(LeafNodeInt *leafNode, RIDKeyPair <int> *ridKeyPair) {
    Page * newLeafPage;
    PageId newLeafId;

    bufMgr->allocPage(file, newLeafId, newLeafPage);
    struct LeafNodeInt *newLeaf = (struct LeafNodeInt *) newLeafPage;
    for (int i = 0; i < leafOccupancy; i++) {
        newLeaf->ridArray[i].page_number = 0;
    }
    newLeaf->rightSibPageNo  = leafNode->rightSibPageNo;
    leafNode->rightSibPageNo = newLeafId;

    int key  = ridKeyPair->key;
    int pIdx = 0;

    for (pIdx = 0; pIdx < leafOccupancy && key >= leafNode->keyArray[pIdx]; pIdx++);

    int mid = (leafOccupancy + 1) / 2;

    if (pIdx < mid) {
        int midKey = leafNode->keyArray[mid - 1];

        int lIdx = 0;
        for (int i = mid - 1; i < leafOccupancy; i++, lIdx++) {
            newLeaf->keyArray[lIdx]             = leafNode->keyArray[i];
            newLeaf->ridArray[lIdx].page_number = leafNode->ridArray[i].page_number;
            newLeaf->ridArray[lIdx].slot_number = leafNode->ridArray[i].slot_number;
            leafNode->ridArray[i].page_number   = 0;
        }

        insertToLeaf(leafNode, ridKeyPair, mid - 2);

        SplitData <int> *splitData = new SplitData <int> ();
        splitData->set(newLeafId, midKey);

        bufMgr->unPinPage(file, newLeafId, true);
        return splitData;
    }
    else {
        int lIdx = 0;
        for (int i = mid; i < leafOccupancy; i++, lIdx++) {
            newLeaf->keyArray[lIdx]             = leafNode->keyArray[i];
            newLeaf->ridArray[lIdx].page_number = leafNode->ridArray[i].page_number;
            newLeaf->ridArray[lIdx].slot_number = leafNode->ridArray[i].slot_number;
            leafNode->ridArray[i].page_number   = 0;
        }

        insertToLeaf(newLeaf, ridKeyPair, lIdx - 1);

        int midKey = newLeaf->keyArray[0];

        SplitData <int> *splitData = new SplitData <int> ();
        splitData->set(newLeafId, midKey);

        bufMgr->unPinPage(file, newLeafId, true);
        return splitData;
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertToLeaf
// -----------------------------------------------------------------------------

const void BTreeIndex::insertToLeaf(LeafNodeInt *leafNode, RIDKeyPair <int> *ridKeyPair, int lastFullIndex) {
    int key = ridKeyPair->key;
    int idx = 0;

    for (idx = 0; idx <= lastFullIndex && key >= leafNode->keyArray[idx]; idx++);

    for (int i = lastFullIndex; i >= idx; i--) {
        leafNode->keyArray[i + 1] = leafNode->keyArray[i];
        leafNode->ridArray[i + 1] = leafNode->ridArray[i];
    }

    leafNode->keyArray[idx] = key;
    leafNode->ridArray[idx] = ridKeyPair->rid;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertNonLeafEntry
// -----------------------------------------------------------------------------

SplitData <int> *BTreeIndex::insertNonLeafEntry(PageId nodeNum, RIDKeyPair <int> *ridKeyPair) {
    Page *nodePage;

    bufMgr->readPage(file, nodeNum, nodePage);
    struct NonLeafNodeInt *node = (struct NonLeafNodeInt *) nodePage;

    int key           = ridKeyPair->key;
    int idx           = 0;
    int lastFullIndex = getLastFullIndex(nodePage, false);

    for (idx = 0; idx < lastFullIndex && key >= node->keyArray[idx]; idx++);
    PageId nextPage = node->pageNoArray[idx];
    int    level    = node->level;

    bufMgr->unPinPage(file, nodeNum, false);

    SplitData <int> *splitData;

    if (level) {
        splitData = insertLeafEntry(nextPage, ridKeyPair);
    }
    else {
        splitData = insertNonLeafEntry(nextPage, ridKeyPair);
    }

    if (splitData) {
        bufMgr->readPage(file, nodeNum, nodePage);
        node = (struct NonLeafNodeInt *) nodePage;
        SplitData <int> *data;

        if (lastFullIndex >= nodeOccupancy) {
            data = splitNonLeafNode(node, splitData);
        }
        else {
            insertToNonLeaf(node, splitData, lastFullIndex);
            data = NULL;
        }

        bufMgr->unPinPage(file, nodeNum, true);
        delete splitData;

        return data;
    }

    return NULL;
}

// -----------------------------------------------------------------------------
// BTreeIndex::splitNonLeafNode
// -----------------------------------------------------------------------------

SplitData <int> *BTreeIndex::splitNonLeafNode(NonLeafNodeInt *node, SplitData <int> *splitData) {
    Page * newNodePage;
    PageId newPageId;

    bufMgr->allocPage(file, newPageId, newNodePage);
    struct NonLeafNodeInt *newNode = (struct NonLeafNodeInt *) newNodePage;
    for (int i = 0; i <= nodeOccupancy; i++) {
        newNode->pageNoArray[i] = 0;
    }
    newNode->level = node->level;

    int key = splitData->key;
    int idx = 0;

    for (idx = 0; idx < nodeOccupancy && key >= node->keyArray[idx]; idx++);

    int mid = (nodeOccupancy + 1) / 2;

    if (idx < mid) {
        int midKey = node->keyArray[mid - 1];
        newNode->pageNoArray[0] = node->pageNoArray[mid];
        node->pageNoArray[mid]  = 0;

        int nIdx = 0;
        for (int i = mid; i < nodeOccupancy; i++, nIdx++) {
            newNode->keyArray[nIdx]        = node->keyArray[i];
            newNode->pageNoArray[nIdx + 1] = node->pageNoArray[i + 1];
            node->pageNoArray[i + 1]       = 0;
        }

        insertToNonLeaf(node, splitData, mid - 1);

        SplitData <int> *data = new SplitData <int> ();
        data->set(newPageId, midKey);

        bufMgr->unPinPage(file, newPageId, true);
        return data;
    }
    else if (idx == mid) {
        int midKey = splitData->key;
        newNode->pageNoArray[0] = splitData->newPageId;

        int nIdx = 0;
        for (int i = mid; i < nodeOccupancy; i++, nIdx++) {
            newNode->keyArray[nIdx]        = node->keyArray[i];
            newNode->pageNoArray[nIdx + 1] = node->pageNoArray[i + 1];
            node->pageNoArray[i + 1]       = 0;
        }

        SplitData <int> *data = new SplitData <int> ();
        data->set(newPageId, midKey);

        bufMgr->unPinPage(file, newPageId, true);
        return data;
    }
    else {
        int midKey = node->keyArray[mid];
        newNode->pageNoArray[0]    = node->pageNoArray[mid + 1];
        node->pageNoArray[mid + 1] = 0;

        int nIdx = 0;
        for (int i = mid + 1; i < nodeOccupancy; i++, nIdx++) {
            newNode->keyArray[nIdx]        = node->keyArray[i];
            newNode->pageNoArray[nIdx + 1] = node->pageNoArray[i + 1];
            node->pageNoArray[i + 1]       = 0;
        }

        insertToNonLeaf(newNode, splitData, nIdx);

        SplitData <int> *data = new SplitData <int> ();
        data->set(newPageId, midKey);

        bufMgr->unPinPage(file, newPageId, true);
        return data;
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertToNonLeaf
// -----------------------------------------------------------------------------

const void BTreeIndex::insertToNonLeaf(NonLeafNodeInt *node, SplitData <int> *splitData, int lastFullIndex) {
    int idx = 0;
    int key = splitData->key;

    for (idx = 0; idx < lastFullIndex && key >= node->keyArray[idx]; idx++);

    for (int i = lastFullIndex - 1; i >= idx; i--) {
        node->keyArray[i + 1]    = node->keyArray[i];
        node->pageNoArray[i + 2] = node->pageNoArray[i + 1];
    }

    node->keyArray[idx]        = key;
    node->pageNoArray[idx + 1] = splitData->newPageId;
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void *lowValParm,
                                 const Operator lowOpParm,
                                 const void *highValParm,
                                 const Operator highOpParm) {
    if (*(int *) lowValParm > *(int *) highValParm) {
        scanExecuting = false;
        throw BadScanrangeException();
    }

    if (lowOpParm != GT && lowOpParm != GTE) {
        scanExecuting = false;
        throw BadOpcodesException();
    }

    if (highOpParm != LT && highOpParm != LTE) {
        scanExecuting = false;
        throw BadOpcodesException();
    }

    if (scanExecuting) {
        endScan();
    }

    scanExecuting = true;

    lowValInt  = *(int *) lowValParm;
    highValInt = *(int *) highValParm;
    lowOp      = lowOpParm;
    highOp     = highOpParm;

    nextEntry = 0;

    bool isLeaf = rootIsLeaf;
    currentPageNum = rootPageNum;

    while (!isLeaf) {
        Page *page;
        bufMgr->readPage(file, currentPageNum, page);

        int lastFullIndex = getLastFullIndex(page, false);

        struct NonLeafNodeInt *node = (struct NonLeafNodeInt *) page;

        int idx = 0;
        for (idx = 0; idx < lastFullIndex && lowValInt >= node->keyArray[idx]; idx++);
        isLeaf = node->level;
        PageId prev_page = currentPageNum;
        currentPageNum = node->pageNoArray[idx];

        bufMgr->unPinPage(file, prev_page, false);
    }

    bool stop = false;

    while (!stop) {
        if (!currentPageNum) {
            stop = true;
        }
        else {
            Page *page;
            bufMgr->readPage(file, currentPageNum, page);

            int lastFullIndex        = getLastFullIndex(page, true);
            struct LeafNodeInt *leaf = (struct LeafNodeInt *) page;

            int idx = 0;
            if (lowOp == GT) {
                for (idx = 0; idx <= lastFullIndex && lowValInt >= leaf->keyArray[idx]; idx++);
            }
            else {
                for (idx = 0; idx <= lastFullIndex && lowValInt > leaf->keyArray[idx]; idx++);
            }

            if (idx > lastFullIndex) {
                PageId next_page = leaf->rightSibPageNo;
                bufMgr->unPinPage(file, currentPageNum, false);
                currentPageNum = next_page;
            }
            else {
                currentPageData = page;
                stop            = true;
                nextEntry       = idx;
            }
        }
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) {
    if (!scanExecuting) {
        throw ScanNotInitializedException();
    }

    if (this->currentPageNum == 0) {
        throw IndexScanCompletedException();
    }

    struct LeafNodeInt *leaf = (struct LeafNodeInt *) currentPageData;
    if (highValInt > leaf->keyArray[nextEntry] && highOp == LT) {
        outRid = leaf->ridArray[nextEntry++];
    }
    else if (highValInt >= leaf->keyArray[nextEntry] && highOp == LTE) {
        outRid = leaf->ridArray[nextEntry++];
    }
    else {
        bufMgr->unPinPage(file, currentPageNum, false);
        throw IndexScanCompletedException();
    }

    if (nextEntry >= leafOccupancy || leaf->ridArray[nextEntry].page_number == 0) {
        PageId nextPage = leaf->rightSibPageNo;
        nextEntry = 0;
        bufMgr->unPinPage(file, currentPageNum, false);
        currentPageNum = nextPage;
        if (!currentPageNum) {
            throw IndexScanCompletedException();
        }
        bufMgr->readPage(file, currentPageNum, currentPageData);
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() {
    if (!scanExecuting) {
        throw ScanNotInitializedException();
    }

    scanExecuting   = false;
    currentPageNum  = 0;
    currentPageData = NULL;
}
}
