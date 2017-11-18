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
#include "exceptions/page_not_pinned_exception.h"
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
    //// init fields for Btree ////
    this->bufMgr        = bufMgrIn; //bufManager instance
    this->leafOccupancy = INTARRAYLEAFSIZE;
    this->nodeOccupancy = INTARRAYNONLEAFSIZE;

    this->attributeType  = attrType;
    this->attrByteOffset = attrByteOffset;
    this->scanExecuting  = false;

    //// constructing index file name ////
    std::ostringstream idxStr;
    idxStr << relationName << '.' << attrByteOffset;
    std::string indexName = idxStr.str(); //indexName : name of index file created

    //// return the name of index file after determining it above ////
    outIndexName = indexName;

    //// debug checks ////
    std::cout << "Index File Name: " << indexName << std::endl;


    //// if index file exists, open the file ////

    if (File::exists(outIndexName)) {
        //// open file if exists ////
        this->file = new BlobFile(outIndexName, false);

        //// get meta page info (always the first page) ////
        Page *metaPage;
        this->headerPageNum = file->getFirstPageNo();
        this->bufMgr->readPage(this->file, headerPageNum, metaPage);

        //// cast meta page to IndexMetaInfo structure ////
        IndexMetaInfo *indexMetaInfo = (IndexMetaInfo *) metaPage;

        //// unpin the header page, as we don't use it after constructor ////
        this->bufMgr->unPinPage(this->file, headerPageNum, false);

        //// VALIDATION CHECK ////
        // @throws  BadIndexInfoException
        // If the index file already exists for the corresponding attribute,
        // but values in metapage(relationName, attribute byte offset, attribute type etc.)
        // do not match with values received through constructor parameters.
        if ((indexMetaInfo->attrType != attrType) ||
            (indexMetaInfo->attrByteOffset != attrByteOffset) ||
            (indexMetaInfo->relationName != relationName)) {
            throw BadIndexInfoException("Error: Index meta attributes don't match!");
        }

        //// page number of root page of B+ tree inside index file. ////
        this->rootPageNum = indexMetaInfo->rootPageNo;

        rootIsLeaf = (rootPageNum == 2);
    }
    else {
        std::cout << "Creating index file ...\n";
        rootIsLeaf = true;

        //// create index file if doesn't exist + meta pg + root pg ////
        this->file = new BlobFile(outIndexName, true);
        Page *indexMetaInfoPage;

        //// meta info init ////
        this->bufMgr->allocPage(this->file, headerPageNum, indexMetaInfoPage);
        struct IndexMetaInfo *metaInfo = (struct IndexMetaInfo *) indexMetaInfoPage;
        metaInfo->attrByteOffset = attrByteOffset;
        metaInfo->attrType       = attrType;
        strncpy(metaInfo->relationName, relationName.c_str(), sizeof(metaInfo->relationName));

        //// root page init ////
        // Initially, root is a leaf node.
        // A non leaf root is added only when the leaf is full and splits.
        Page *rootPage;
        this->bufMgr->allocPage(this->file, rootPageNum, rootPage);
        LeafNodeInt *root = (LeafNodeInt *) rootPage;

        // Initialize all rids to 0.
        // This will help us keep a track of when a node is full
        for (int idx = 0; idx < INTARRAYLEAFSIZE; idx++) {
            (root->ridArray[idx]).page_number = 0;
        }
        root->rightSibPageNo = -1;

        // How certain are we of this ?
        // What keeps it at 2? Why can it not be a different number?
        metaInfo->rootPageNo = this->rootPageNum = 2; // Root page starts as page 2


        //// unpin headerPage and rootPage ////
        bufMgr->unPinPage(file, headerPageNum, true);
        bufMgr->unPinPage(file, rootPageNum, true);

        //   insert entries for every tuple in the base relation using FileScan class. ////
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
        } catch (EndOfFileException e) { delete fs; }

        bufMgr->flushFile(file);
        //delete fs;
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex() {
    //// flushing the index file ////
    bufMgr->flushFile(file);

    //// destructor of file class called ////
    file->~File();

    //// del file and bufMgr instance ////
    delete file;
    delete bufMgr;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

// This method inserts a new entry into the index using the pair <key, rid>
// const void* key A pointer to the value (integer) we want to insert.
// const RecordId& rid The corresponding record id of the tuple in the base relation

/**
 * Insert a new entry using the pair <value,rid>.
 * Start from root to recursively find out the leaf to insert the entry in.
 * The insertion may cause splitting of leaf node.
 * This splitting will require addition of new leaf page number entry into the parent non-leaf,
 * which may in-turn get split.
 * This may continue all the way upto the root causing the root to get split.
 * If root gets split, metapage needs to be changed accordingly.
 * Make sure to unpin pages as soon as you can.
 **/
const void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
    RIDKeyPair <int> ridkey_entry;
    ridkey_entry.set(rid, *(int *) key);

    //if root is leaf, special case
    if (this->rootIsLeaf) {
        // BTreeIndex::insertRootEntry(ridkey_entry);
        BTreeIndex::insertEntry(rootPageNum, &ridkey_entry, true);
    }
    else {
        //traverse and insert non-root
    }
}

const int BTreeIndex::getLastFullIndex(Page *node, bool isLeaf) {
    if (isLeaf) {
        LeafNodeInt *leafNode = (LeafNodeInt *) node;

        int idx;
        for (idx = 0; idx < INTARRAYLEAFSIZE && (leafNode->ridArray[idx]).page_number != 0; idx++);
        return idx - 1;
    }
    else {
        NonLeafNodeInt *nonLeafNode = (NonLeafNodeInt *) node;

        int idx;
        for (idx = 0; idx <= INTARRAYNONLEAFSIZE && nonLeafNode->pageNoArray[idx] != 0; idx++);
        return idx - 1;
    }
}

SplitData <int> *BTreeIndex::insertEntry(PageId pageNum, RIDKeyPair <int> *ridKeyPair, bool isLeaf) {
    if (isLeaf) {
        // Logic for adding to leaf nodes
        Page *leafPage;
        bufMgr->readPage(file, pageNum, leafPage);
        struct LeafNodeInt *leafNode = (struct LeafNodeInt *) leafPage;

        int lastfullIndex = getLastFullIndex(leafPage, true);

        if (lastfullIndex + 1 == INTARRAYLEAFSIZE) {
            SplitData <int> *splitData = splitLeafNode(leafNode, ridKeyPair);
            bufMgr->unPinPage(file, pageNum, true);
            return splitData;
        }
        else {
            insertLeafEntry(leafNode, ridKeyPair, lastfullIndex);
            bufMgr->unPinPage(file, pageNum, true);
            return NULL;
        }

        // These statements should never be reached
        bufMgr->unPinPage(file, pageNum, true);
        return NULL;
    }

    // Logic to go down the tree
    int   key = ridKeyPair->key;
    Page *nonLeafPage;
    bufMgr->readPage(file, pageNum, nonLeafPage);
    struct NonLeafNodeInt *nonLeafNode = (struct NonLeafNodeInt *) nonLeafPage;
    int lastFullIndex = getLastFullIndex(nonLeafPage, false);
    int index;

    int lastIndex = lastFullIndex - 1;;

    for (index = 0; index <= lastIndex && key >= nonLeafNode->keyArray[index]; index++);

    bool isNextLeaf = nonLeafNode->level == 1;

    bufMgr->unPinPage(file, pageNum, false);

    SplitData <int> *splitPointer = insertEntry(nonLeafNode->pageNoArray[index], ridKeyPair, isNextLeaf);

    if (splitPointer) {
        if (lastFullIndex == INTARRAYNONLEAFSIZE) {
            SplitData <int> *splitData = splitNonLeafNode(pageNum, splitPointer);
            delete splitPointer;
            return splitData;
        }
        else {
            Page *nonLeafPage;
            bufMgr->readPage(file, pageNum, nonLeafPage);
            struct NonLeafNodeInt *nonLeafNode = (struct NonLeafNodeInt *) nonLeafPage;

            int splitKey = splitPointer->key;
            int insertIndex;

            for (insertIndex = 0; insertIndex <= lastIndex && splitKey >= nonLeafNode->keyArray[insertIndex]; insertIndex++);

            for (int i = lastIndex, j = lastFullIndex; i >= insertIndex; i--, j--) {
                nonLeafNode->keyArray[i + 1]    = nonLeafNode->keyArray[i];
                nonLeafNode->pageNoArray[j + 1] = nonLeafNode->pageNoArray[j];
            }

            nonLeafNode->keyArray[insertIndex]        = splitKey;
            nonLeafNode->pageNoArray[insertIndex + 1] = splitPointer->newPageId;

            delete splitPointer;
            bufMgr->unPinPage(file, pageNum, true);
            return NULL;
        }
    }

    delete splitPointer;
    bufMgr->unPinPage(file, pageNum, false);
    return NULL;
}

SplitData <int> *BTreeIndex::splitNonLeafNode(PageId pageNum, SplitData <int> *splitPointer) {
    Page *nonLeafPage;

    bufMgr->readPage(file, pageNum, nonLeafPage);
    struct NonLeafNodeInt *nonLeafNode = (struct NonLeafNodeInt *) nonLeafPage;

    int pIdx;
    int key = splitPointer->key;

    for (pIdx = 0; pIdx < INTARRAYNONLEAFSIZE && key >= nonLeafNode->keyArray[pIdx]; pIdx++);

    int halfIndex = (INTARRAYNONLEAFSIZE + 1) / 2;


    PageId newPageId;
    Page * newPage;

    bufMgr->allocPage(file, newPageId, newPage);
    struct NonLeafNodeInt *newNode = (struct NonLeafNodeInt *) newPage;
    newNode->level = nonLeafNode->level;
    for (int idx = 0; idx < INTARRAYNONLEAFSIZE + 1; idx++) {
        newNode->pageNoArray[idx] = 0;
    }

    if (pIdx < halfIndex) {
        int sendUpKey = nonLeafNode->keyArray[halfIndex - 1];
        newNode->pageNoArray[0] = nonLeafNode->pageNoArray[halfIndex];

        nonLeafNode->pageNoArray[halfIndex] = 0;

        for (int i = halfIndex, j = 0; i < INTARRAYNONLEAFSIZE; i++, j++) {
            newNode->keyArray[j]            = nonLeafNode->keyArray[i];
            newNode->pageNoArray[j + 1]     = nonLeafNode->pageNoArray[j + 1];
            nonLeafNode->pageNoArray[j + 1] = 0;
        }

        for (int i = halfIndex - 1; i > pIdx; i--) {
            nonLeafNode->keyArray[i]        = nonLeafNode->keyArray[i - 1];
            nonLeafNode->pageNoArray[i + 1] = nonLeafNode->pageNoArray[i];
        }

        nonLeafNode->keyArray[pIdx]    = key;
        nonLeafNode->pageNoArray[pIdx] = splitPointer->newPageId;

        SplitData <int> *newNodeData = new SplitData <int>();
        newNodeData->set(newPageId, sendUpKey);
        bufMgr->unPinPage(file, pageNum, true);
        bufMgr->unPinPage(file, newPageId, true);
        return newNodeData;
    }
    else if (pIdx == halfIndex) {
        int sendUpKey = splitPointer->key;
        newNode->pageNoArray[0] = splitPointer->newPageId;

        for (int i = halfIndex, j = 0; i < INTARRAYNONLEAFSIZE; i++, j++) {
            newNode->keyArray[j]            = nonLeafNode->keyArray[i];
            newNode->pageNoArray[j + 1]     = nonLeafNode->pageNoArray[j + 1];
            nonLeafNode->pageNoArray[j + 1] = 0;
        }

        SplitData <int> *newNodeData = new SplitData <int>();
        bufMgr->unPinPage(file, pageNum, true);
        bufMgr->unPinPage(file, newPageId, true);
        return newNodeData;
    }
    else {
        int sendUpKey = nonLeafNode->keyArray[halfIndex];
        newNode->pageNoArray[0] = nonLeafNode->pageNoArray[halfIndex + 1];

        nonLeafNode->pageNoArray[halfIndex + 1] = 0;

        bool newAdded = false;
        for (int i = halfIndex + 1, j = 0; i < INTARRAYNONLEAFSIZE; i++, j++) {
            if (!newAdded) {
                if (i == pIdx) {
                    newNode->keyArray[j]    = key;
                    newNode->pageNoArray[j] = splitPointer->newPageId;
                    i--;
                    newAdded = true;
                }
                else {
                    newNode->keyArray[j]        = nonLeafNode->keyArray[i];
                    newNode->pageNoArray[j]     = nonLeafNode->pageNoArray[i];
                    nonLeafNode->pageNoArray[i] = 0;
                }
            }
            else {
                newNode->pageNoArray[j]     = nonLeafNode->pageNoArray[i];
                newNode->pageNoArray[j]     = nonLeafNode->pageNoArray[i];
                nonLeafNode->pageNoArray[i] = 0;
            }
        }

        SplitData <int> *newNodeData = new SplitData <int>();
        newNodeData->set(newPageId, sendUpKey);
        bufMgr->unPinPage(file, pageNum, true);
        bufMgr->unPinPage(file, newPageId, true);
        return newNodeData;
    }

    // This should never be executed
    bufMgr->unPinPage(file, pageNum, true);
    return NULL;
}

SplitData <int> *BTreeIndex::splitLeafNode(struct LeafNodeInt *leafNode, RIDKeyPair <int> *ridKeyPair) {
    // TODO: All splitting logic here
    // Logic for splitting

    int key = ridKeyPair->key;

    int pIdx;

    for (pIdx = 0; pIdx < INTARRAYLEAFSIZE && key >= leafNode->keyArray[pIdx]; pIdx++);

    int    halfIndex = (INTARRAYLEAFSIZE + 1) / 2;
    PageId newPageId;
    Page * newPage;

    bufMgr->allocPage(file, newPageId, newPage);
    struct LeafNodeInt *newLeaf = (struct LeafNodeInt *) newPage;
    for (int idx = 0; idx < INTARRAYLEAFSIZE; idx++) {
        (newLeaf->ridArray[idx]).page_number = 0;
    }


    if (pIdx < halfIndex) {
        for (int i = halfIndex - 1, j = 0; i < INTARRAYLEAFSIZE; i++, j++) {
            newLeaf->keyArray[j] = leafNode->keyArray[i];
            newLeaf->ridArray[j] = leafNode->ridArray[i];
            (leafNode->ridArray[i]).page_number = 0;
        }

        int inpIdx;

        for (int i = halfIndex - 1; i > pIdx; i--) {
            leafNode->keyArray[i] = leafNode->keyArray[i - 1];
            leafNode->ridArray[i] = leafNode->ridArray[i - 1];
        }

        leafNode->keyArray[pIdx] = key;
        leafNode->ridArray[pIdx] = ridKeyPair->rid;

        newLeaf->rightSibPageNo  = leafNode->rightSibPageNo;
        leafNode->rightSibPageNo = newPageId;

        SplitData<int> *splitData = new SplitData<int>();
        splitData->set(newPageId, newLeaf->keyArray[0]);
        return splitData;
    }
    else {
        bool newAdded = false;
        for (int i = halfIndex, j = 0; i < INTARRAYLEAFSIZE; i++, j++) {
            if (!newAdded) {
                if (i == pIdx) {
                    newLeaf->keyArray[j] = key;
                    newLeaf->ridArray[j] = ridKeyPair->rid;
                    i--;
                    newAdded = true;
                }
                else {
                    newLeaf->keyArray[j] = leafNode->keyArray[i];
                    newLeaf->ridArray[j] = leafNode->ridArray[i];
                    (leafNode->ridArray[i]).page_number = 0;
                }
            }
            else {
                newLeaf->keyArray[j] = leafNode->keyArray[i];
                newLeaf->ridArray[j] = leafNode->ridArray[i];
                (leafNode->ridArray[i]).page_number = 0;
            }
        }

        newLeaf->rightSibPageNo  = leafNode->rightSibPageNo;
        leafNode->rightSibPageNo = newPageId;

        SplitData<int> *splitData = new SplitData<int>();
        splitData->set(newPageId, newLeaf->keyArray[0]);
        return splitData;
    }

    // This statement should never be reached
    return NULL;
}

/*const void BTreeIndex::insertNonLeafEntry(NonLeafNodeInt *nonLeafNode, PageKeyPair <int> pkEntry) {
    int pos = 0;

    int idx = 0;

    //find current pos in the page to insert
    while ((pos < nodeOccupancy) && (nonLeafNode->pageNoArray[pos] != 0)) {
        if (nonLeafNode->keyArray[pos] >= pkEntry.key) {
            break; //found
        }
        pos++;
    }

    // shift other entries to right
    for (idx = nodeOccupancy - 1; idx > pos; idx--) {
        nonLeafNode->pageNoArray[idx + 1] = nonLeafNode->pageNoArray[idx];

        nonLeafNode->keyArray[idx] = nonLeafNode->keyArray[idx - 1];
    }


    int pageNoIdx = 0;
    int keyIdx    = 0;

    if (nonLeafNode->pageNoArray[idx] == 0) {
        // last position
        pageNoIdx = idx;
        keyIdx    = idx - 1;
    }
    else {
        // on position
        pageNoIdx = idx + 1;
        keyIdx    = idx;
    }

    // insertion
    nonLeafNode->pageNoArray[pageNoIdx] = pkEntry.pageNo;
    nonLeafNode->keyArray[keyIdx]       = pkEntry.key;
}*/

/**
 * Assumes leaf node has empty space
 */
const void BTreeIndex::insertLeafEntry(LeafNodeInt *leafNode, RIDKeyPair <int> *kpEntry, int lastFullIndex) {
    int inputIndex;
    int key = kpEntry->key;

    for (inputIndex = 0; inputIndex <= lastFullIndex && key >= leafNode->keyArray[inputIndex]; inputIndex++);

    for (int i = lastFullIndex + 1; i > inputIndex; i--) {
        leafNode->keyArray[i] = leafNode->keyArray[i - 1];
        leafNode->ridArray[i] = leafNode->ridArray[i - 1];
    }

    leafNode->keyArray[inputIndex] = key;
    leafNode->ridArray[inputIndex] = kpEntry->rid;
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void *lowValParm,
                                 const Operator lowOpParm,
                                 const void *highValParm,
                                 const Operator highOpParm) {
    // I'm not quite sure of all the functionality of this method. The pdf seems to
    // imply that this sets global variables and then calls the scan next method
    // I don't see why the use of global variables is even necessary as this function
    // could store the state of the scan in member variables

    // First use iterative traversal to find the leftmost node that could point to
    // values in the parameter range. this is done by using the lower bounds
    // and the given operator either GT or GTE and comparing to the value in the
    // non leaf key array. If it fails go onto the next index entry, otherwise
    // find the child node. If the current node has level = 1 that means the child
    // is a leaf, otherwise it's another non leaf

    // Okay, I think I have a better idea of what's going on in this function.
    // It doesn't actually call the scanNext function, it's only finding the first
    // node that matches the criteria and setting up the gloabal vars that
    // scanNext will use to return the recordIDs, an external call to scanNext()
    // will be made when needed.

    // Be pinning pages!

    ////  Range sanity check ////
    // The range parameters are void pointers, so they must first be cast to
    // integer pointers and then dereferenced to get the actual values.
    if (*(int *) lowValParm > *(int *) highValParm) {
        throw BadScanrangeException();
    }

    // Opcode check
    if (lowOpParm != GT && lowOpParm != GTE) {
        throw BadOpcodesException();
    }
    if (highOpParm != LT && lowOpParm != LTE) {
        throw BadOpcodesException();
    }


    // Stop any current scans, might need to do more here
    if (scanExecuting) {
        endScan();
    }

    // Set up global vars for the scan constraints
    scanExecuting = true;

    lowValInt  = *(int *) lowValParm;
    highValInt = *(int *) highValParm;
    lowOp      = lowOpParm;
    highOp     = highOpParm;

    // Index of next entry in current leaf to be scanned
    nextEntry = 0;

    // Start at the root node of the B+ index to traverse for key values
    // Each non leaf node has an array of key values, and associated child nodes
    // The nodes are pages, and once I have the page I can cast the pointer to
    // a struct pointer to get the datamembers?

    // Start by getting the root page number, get it's page, make a pointer
    // to that page address and then cast it to the NonLeafNodeInt struct pointer
    PageId index_root_pageID    = rootPageNum;
    Page   current_page         = file->readPage(index_root_pageID);
    Page * current_page_pointer = &current_page;

    struct NonLeafNodeInt *cur_node_ptr = (struct NonLeafNodeInt *) current_page_pointer;

    PageId min_pageID;

    //Traverse the tree until the level = 1, this is the last level before leafs
    while (cur_node_ptr->level != 1) {
        bool found_range = false;

        // Find the leftmost non leaf child with key values matching the search,
        for (int i = 0; i < nodeOccupancy && !found_range; i++) {
            int key_value = cur_node_ptr->keyArray[i];
            // There might be issue with 0 key values
            // Since the lower bound must be contained in the child that defines
            // a range larger than it if the current key is larger than the lower
            // bound we will find the value in the corresponding child index

            // If the key_value is zero we need to make sure that the page is points
            // to is valid, if not then that key isn't occupied
            if (lowValInt < key_value) {
                min_pageID = cur_node_ptr->pageNoArray[i];
                Page min_page = file->readPage(min_pageID);

                Page *min_page_ptr = &min_page;
                cur_node_ptr = (struct NonLeafNodeInt *) min_page_ptr;
            }
            // All the key values have been compared and the lower bound is larger
            // than all of them, thus we take the rightmost child of the node
            // Though this might be different if the nodes aren't fully occupied!
            else if (i == nodeOccupancy) {
                //is this bad form, to have 1 offset here?
                min_pageID = cur_node_ptr->pageNoArray[i + 1];
                Page min_page = file->readPage(min_pageID);

                Page *min_page_ptr = &min_page;
                cur_node_ptr = (struct NonLeafNodeInt *) min_page_ptr;
            }
        }
    }
    // Once the first matching node is found then the rest of the scan
    // related global variables can be setup, this is also where the distinction
    // between the GTE vs GT might come into play, though it might be in the
    // scan next method that we worry about that


    // Current page number (pageId)
    currentPageNum = min_pageID;
    // Current page pointer
    currentPageData = current_page_pointer;
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

/**
 * Fetch the record id of the next index entry that matches the scan.
 * Return the next record from current page being scanned.
 * If current page has been scanned to its entirety, move on to the right sibling of current page,
 * if any exists, to start scanning that page. Make sure to unpin any pages that are no longer required.
 * @param outRid    RecordId of next record found that satisfies the scan criteria returned in this
 * @throws ScanNotInitializedException If no scan has been initialized.
 * @throws IndexScanCompletedException If no more records, satisfying the scan criteria, are left to be scanned.
 **/
const void BTreeIndex::scanNext(RecordId& outRid) {
    //This method fetches the record id of the next
    //tuple that matches the scan criteria.

    if (!scanExecuting) {
        throw ScanNotInitializedException();
    }

    // should we include this here ?
    if (this->currentPageNum == 0) {
        throw IndexScanCompletedException();
    }

    // get the current node/page being scanned as a leafNode
    LeafNodeInt *currentPageLeaf = (LeafNodeInt *) (this->currentPageData);

    // int key for next entry in the current scanned page
    int currKey = currentPageLeaf->keyArray[nextEntry];

    // if we reached limit, completed scan
    if ((this->highValInt <= currKey && highOp == LT) ||
        (this->highValInt < currKey && highOp == LTE)) {
        throw IndexScanCompletedException();
    }

    // go through records, fetch the record id of the next index entry that matches the scan.
    if ((lowOp == GTE && currKey < this->lowValInt) ||
        (lowOp == GT && currKey <= this->lowValInt)) {
        outRid = currentPageLeaf->ridArray[nextEntry];
        nextEntry++;
    }

    //move on to the right sibling when end of array
    if (nextEntry >= leafOccupancy || currentPageLeaf->ridArray[nextEntry].page_number == 0) {
        //get page id of right sibling
        PageId nextSiblingPage = currentPageLeaf->rightSibPageNo;

        if (nextSiblingPage == 0) {
            throw IndexScanCompletedException();
        }

        // unpin current page/node
        bufMgr->unPinPage(file, currentPageNum, false);

        // reset next entry in node
        this->nextEntry = 0;

        // set current page num as sibling page & pin the page
        this->currentPageNum = nextSiblingPage;
        bufMgr->readPage(file, currentPageNum, currentPageData);
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() {
    //@TODO
    //Terminate the current scan. Unpin any pinned pages. Reset scan specific variables.
    //@throws ScanNotInitializedException If no scan has been initialized.

    if (!scanExecuting) {
        throw ScanNotInitializedException();
    }

    scanExecuting = false;

    try {
        bufMgr->unPinPage(this->file, this->currentPageNum, false);
    }

    catch (PageNotPinnedException e) { }
}
}
