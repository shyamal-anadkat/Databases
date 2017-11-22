/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
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

namespace badgerdb {
// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string&relationName,
                       std::string&outIndexName, BufMgr *bufMgrIn,
                       const int attrByteOffset, const Datatype attrType) {
    //// init fields for Btree ////
    this->bufMgr = bufMgrIn; // bufManager instance
    // this->leafOccupancy = INTARRAYLEAFSIZE;
    // this->nodeOccupancy = INTARRAYNONLEAFSIZE;

    this->leafOccupancy = 10;
    this->nodeOccupancy = 10;

    this->attributeType  = attrType;
    this->attrByteOffset = attrByteOffset;
    this->scanExecuting  = false;

    //// constructing index file name ////
    std::ostringstream idxStr;
    idxStr << relationName << '.' << attrByteOffset;
    std::string indexName =
        idxStr.str(); // indexName : name of index file created

    //// return the name of index file after determining it above ////
    outIndexName = indexName;

    //// debug checks ////
    // std::cout << "Index File Name: " << indexName << std::endl;

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
        // but values in metapage(relationName, attribute byte offset, attribute
        // type etc.)
        // do not match with values received through constructor parameters.
        if ((indexMetaInfo->attrType != attrType) ||
            (indexMetaInfo->attrByteOffset != attrByteOffset) ||
            (indexMetaInfo->relationName != relationName)) {
            throw BadIndexInfoException("Error: Index meta attributes don't match!");
        }

        //// page number of root page of B+ tree inside index file. ////
        this->rootPageNum = indexMetaInfo->rootPageNo;

        // if root is the only page, then it is also a root
        rootIsLeaf = (rootPageNum == 2);
    }
    else {
        rootIsLeaf = true;

        //// create index file if doesn't exist + meta pg + root pg ////
        this->file = new BlobFile(outIndexName, true);
        Page *indexMetaInfoPage;

        //// meta info init ////
        std::cout << file << "\n";
        this->bufMgr->allocPage(this->file, headerPageNum, indexMetaInfoPage);
        std::cout << "Alloc : " << headerPageNum << "\n";
        struct IndexMetaInfo *metaInfo = (struct IndexMetaInfo *) indexMetaInfoPage;
        metaInfo->attrByteOffset = attrByteOffset;
        metaInfo->attrType       = attrType;
        strncpy(metaInfo->relationName, relationName.c_str(),
                sizeof(metaInfo->relationName));

        //// root page init ////
        // Initially, root is a leaf node.
        // A non leaf root is added only when the leaf is full and splits.
        Page *rootPage;
        std::cout << file << "\n";
        this->bufMgr->allocPage(this->file, rootPageNum, rootPage);
        std::cout << "Alloc : " << rootPageNum << "\n";
        LeafNodeInt *root = (LeafNodeInt *) rootPage;

        // Initialize all rids to 0.
        // This will help us keep a track of when a node is full
        for (int idx = 0; idx < leafOccupancy; idx++) {
            (root->ridArray[idx]).page_number = 0;
        }
        root->rightSibPageNo = 0;

        // How certain are we of this ?
        // What keeps it at 2? Why can it not be a different number?
        metaInfo->rootPageNo = this->rootPageNum = 2; // Root page starts as page 2

        //// unpin headerPage and rootPage ////
        // try {
        bufMgr->unPinPage(file, headerPageNum, true);
        bufMgr->unPinPage(file, rootPageNum, true);
        // } catch (PageNotPinnedException e) { }

        // insert entries for every tuple in the base relation using FileScan class.
        // ////
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
            delete fs;
        }

        // unpin any pinned pages
        bufMgr->flushFile(file);
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex() {
    if (scanExecuting) {
        endScan();
    }

    //// flushing the index file ////
    if (file) {
        bufMgr->flushFile(file);
    }

    //// destructor of file class called ////

    //// del file and bufMgr instance ////
    delete file;

    // BufMgr was given to us. We do not delete it
    // delete bufMgr;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

// This method inserts a new entry into the index using the pair <key, rid>
// const void* key A pointer to the value (integer) we want to insert.
// const RecordId& rid The corresponding record id of the tuple in the base
// relation

/**
 * Insert a new entry using the pair <value,rid>.
 * Start from root to recursively find out the leaf to insert the entry in.
 * The insertion may cause splitting of leaf node.
 * This splitting will require addition of new leaf page number entry into the
 * parent non-leaf,
 * which may in-turn get split.
 * This may continue all the way upto the root causing the root to get split.
 * If root gets split, metapage needs to be changed accordingly.
 * Make sure to unpin pages as soon as you can.
 **/
const void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
    // Set up the ridKeyPair to be sent to the helper function
    RIDKeyPair <int> ridkey_entry;
    ridkey_entry.set(rid, *(int *) key);

    // splitData holds a value if root was split
    // NULL otherwise`
    SplitData <int> *splitData;

    /// if root is leaf, special case
    if (this->rootIsLeaf) {
        /// insert entry and tell it that root is the leaf ///
        splitData = BTreeIndex::insertEntry(rootPageNum, &ridkey_entry, true);
    }
    else {
        /// traverse and insert non-leaf root entry ///
        splitData = BTreeIndex::insertEntry(rootPageNum, &ridkey_entry, false);
    }

    // If the root was split
    if (splitData) {
        // Create a new NonLeafNode to act as the root
        Page * newPage;
        PageId newPageId;
        std::cout << file << "\n";
        bufMgr->allocPage(file, newPageId, newPage);
        std::cout << "Alloc : " << newPageId << "\n";

        // Initialize the root to contain nothing
        struct NonLeafNodeInt *newNode = (struct NonLeafNodeInt *) newPage;
        for (int i = 0; i <= nodeOccupancy; i++) {
            newNode->pageNoArray[i] = 0;
        }

        newNode->keyArray[0]    = splitData->key;
        newNode->pageNoArray[0] = rootPageNum;
        newNode->pageNoArray[1] = splitData->newPageId;
        newNode->level          = rootIsLeaf ? 1 : 0;

        // root is no longer a leaf
        rootIsLeaf = false;

        // update the root to point to the new node
        rootPageNum = newPageId;

        // Open the metadata page
        Page *metaPage;
        bufMgr->readPage(file, headerPageNum, metaPage);

        struct IndexMetaInfo *metaInfo = (struct IndexMetaInfo *) metaPage;

        // Update root page in metadata page
        metaInfo->rootPageNo = rootPageNum;

        delete splitData;

        // unpin both pinned pages
        bufMgr->unPinPage(file, headerPageNum, true);
        bufMgr->unPinPage(file, newPageId, true);
    }
}

/**
 * Helper method to get the index of last filled element in leaf or non-leaf node.
 *
 * @param node Node to check
 * @param isLeaf true if node is a leaf, false otherwise
 * @return index that contains the last filled element in node
 */
const int BTreeIndex::getLastFullIndex(Page *node, bool isLeaf) {
    if (isLeaf) {
        LeafNodeInt *leafNode = (LeafNodeInt *) node;

        int idx;
        for (idx = 0;
             idx < leafOccupancy && (leafNode->ridArray[idx]).page_number != 0;
             idx++)
            ;
        return idx - 1;
    }
    else {
        NonLeafNodeInt *nonLeafNode = (NonLeafNodeInt *) node;

        int idx;
        for (idx = 0;
             idx <= nodeOccupancy && nonLeafNode->pageNoArray[idx] != 0;
             idx++)
            ;
        return idx - 1;
    }
}

/**
 * Helper method to recursively add and data to B+ tree, and split if needed.
 *
 * @param pageNum page number of the current node
 * @param ridKeyPair contains the key and pecord id that needs to be inserted
 * @param isLeaf true if node is a leaf, false otherwise
 *
 * @return NULL if node was not split, otherwise sends the key, and page number
 *       of the newly created leaf
 */
SplitData <int> *BTreeIndex::insertEntry(PageId pageNum,
                                         RIDKeyPair <int> *ridKeyPair,
                                         bool isLeaf) {
    // Leaf nodes have to be handled differently
    if (isLeaf) {
        /// Logic for adding to leaf nodes ///

        // Grab the leaf node
        Page *leafPage;
        bufMgr->readPage(file, pageNum, leafPage);
        struct LeafNodeInt *leafNode = (struct LeafNodeInt *) leafPage;

        // Get the last full index in current leaf
        int lastfullIndex = getLastFullIndex(leafPage, true);

        // Is the leaf is full, we need to split it
        if (lastfullIndex + 1 == leafOccupancy) {
            // split leaf, add key, and return the relevant information
            SplitData <int> *splitData = splitLeafNode(leafNode, ridKeyPair);
            // unpin current page
            bufMgr->unPinPage(file, pageNum, true);
            return splitData;
        }
        // otherwise, simply add to the existing leaf
        else {
            /// insert entry in the leaf ////
            insertLeafEntry(leafNode, ridKeyPair, lastfullIndex);
            bufMgr->unPinPage(file, pageNum, true);

            /// no splitting needed to return null pntr for splitdata ///
            return NULL;
        }

        /// These statements should never be reached ///
        // Statements only present to handle if something goes wrong
        bufMgr->unPinPage(file, pageNum, true);
        return NULL;
    }

    /// else block for non-leaf insert entry ///
    /// Logic to go down the tree ///

    int key = ridKeyPair->key;

    // Get the non-leaf node from blob-file
    Page *nonLeafPage;
    bufMgr->readPage(file, pageNum, nonLeafPage);

    /// cast page to nonLeafNode ///
    struct NonLeafNodeInt *nonLeafNode = (struct NonLeafNodeInt *) nonLeafPage;

    // Find index of last entry on the page
    int lastFullIndex = getLastFullIndex(nonLeafPage, false);
    int index;

    // last index of the key
    // NOTE: each non-leaf node must have at least 1 key
    int lastIndex = lastFullIndex - 1;

    /// find index to insert ///
    for (index = 0; index <= lastIndex && key >= nonLeafNode->keyArray[index];
         index++)
        ;

    // Check if current node is parent of leaf nodes
    bool isNextLeaf = nonLeafNode->level == 1;

    // unpin page before traversing the tree
    // page will need to be repinned if split is necessary
    bufMgr->unPinPage(file, pageNum, false);

    /// recursive insert ///
    SplitData <int> *splitPointer =
        insertEntry(nonLeafNode->pageNoArray[index], ridKeyPair, isNextLeaf);

    // If the child of current node was split
    if (splitPointer) {
        // If the current node is also full
        if (lastFullIndex == nodeOccupancy) {
            // split current node, add new key, and links
            SplitData <int> *splitData = splitNonLeafNode(pageNum, splitPointer);
            // delete split data of the previous split
            delete splitPointer;
            // return the split data
            return splitData;
        }
        // current node is not full
        else {
            /// insert non-leaf entry ///
            Page *nonLeafPage;
            bufMgr->readPage(file, pageNum, nonLeafPage);
            struct NonLeafNodeInt *nonLeafNode = (struct NonLeafNodeInt *) nonLeafPage;

            int splitKey = splitPointer->key;

            // calculate index where the new key should be
            int insertIndex;
            for (insertIndex = 0; insertIndex <= lastIndex &&
                 splitKey >= nonLeafNode->keyArray[insertIndex];
                 insertIndex++)
                ;

            // Shift things to the right to make space for the new key
            for (int i = lastIndex, j = lastFullIndex; i >= insertIndex; i--, j--) {
                nonLeafNode->keyArray[i + 1]    = nonLeafNode->keyArray[i];
                nonLeafNode->pageNoArray[j + 1] = nonLeafNode->pageNoArray[j];
            }

            // add new key, and reference to the current node
            nonLeafNode->keyArray[insertIndex]        = splitKey;
            nonLeafNode->pageNoArray[insertIndex + 1] = splitPointer->newPageId;

            // delete split data of the previous split
            delete splitPointer;

            // unpin current node before finishing
            bufMgr->unPinPage(file, pageNum, true);

            // Since no split was performed, return NULL
            return NULL;
        }
    }

    // Split was not performed in lower level
    // delete is purely defensive. Should do nothing
    delete splitPointer;

    return NULL;
}

/**
 * Helper function to split, and add data to a non-leaf node
 *
 * @param pageNum page number of the node to be split
 * @param splitPointer pointer to data of previous split.
 *
 * @return pointer containing the key on which split was performed and page number
 *       of the newly created node
 */
SplitData <int> *BTreeIndex::splitNonLeafNode(PageId pageNum,
                                              SplitData <int> *splitPointer) {
    // Read page from the BlobFile
    Page *nonLeafPage;

    bufMgr->readPage(file, pageNum, nonLeafPage);
    struct NonLeafNodeInt *nonLeafNode = (struct NonLeafNodeInt *) nonLeafPage;

    int pIdx;
    int key = splitPointer->key;

    // calculate where the new key needs to be inserted
    for (pIdx = 0;
         pIdx < nodeOccupancy && key >= nonLeafNode->keyArray[pIdx]; pIdx++)
        ;

    // get the index at which the nod will be split
    int halfIndex = (nodeOccupancy + 1) / 2;
    // create a new node
    PageId newPageId;
    Page * newPage;

    std::cout << file << "\n";
    bufMgr->allocPage(file, newPageId, newPage);
    std::cout << "New Non Leaf: " << newPageId << std::endl;

    struct NonLeafNodeInt *newNode = (struct NonLeafNodeInt *) newPage;

    // level of the new node will be same as its sibling
    newNode->level = nonLeafNode->level;
    // initialize new node to contain nothing
    for (int idx = 0; idx <= nodeOccupancy + 1; idx++) {
        newNode->pageNoArray[idx] = 0;
    }

    // if new key is in the first half of the node
    if (pIdx < halfIndex) {
        // save the key that will be propagated up
        int sendUpKey = nonLeafNode->keyArray[halfIndex - 1];
        // copy over the corresponding link to newly created node
        newNode->pageNoArray[0] = nonLeafNode->pageNoArray[halfIndex];

        // clean current node's last empty pointer
        nonLeafNode->pageNoArray[halfIndex] = 0;

        // Move over the second half of the node to the new node
        for (int i = halfIndex, j = 0; i < nodeOccupancy; i++, j++) {
            newNode->keyArray[j]            = nonLeafNode->keyArray[i];
            newNode->pageNoArray[j + 1]     = nonLeafNode->pageNoArray[i + 1];
            nonLeafNode->pageNoArray[i + 1] = 0;
        }

        // shift all elements right to make space for new key
        for (int i = halfIndex - 1; i > pIdx; i--) {
            nonLeafNode->keyArray[i]        = nonLeafNode->keyArray[i - 1];
            nonLeafNode->pageNoArray[i + 1] = nonLeafNode->pageNoArray[i];
        }

        // Add key to the current nodd
        nonLeafNode->keyArray[pIdx]        = key;
        nonLeafNode->pageNoArray[pIdx + 1] = splitPointer->newPageId;

        // Save data about the split
        SplitData <int> *newNodeData = new SplitData <int>();
        newNodeData->set(newPageId, sendUpKey);

        // Unpin the page before returning
        bufMgr->unPinPage(file, pageNum, true);
        bufMgr->unPinPage(file, newPageId, true);

        // return the split data
        return newNodeData;
    }
    // If the split happens at the new key
    else if (pIdx == halfIndex) {
        int sendUpKey = splitPointer->key;
        newNode->pageNoArray[0] = splitPointer->newPageId;
        // copy over the second half to the new node
        for (int i = halfIndex, j = 0; i < nodeOccupancy; i++, j++) {
            newNode->keyArray[j]            = nonLeafNode->keyArray[i];
            newNode->pageNoArray[j + 1]     = nonLeafNode->pageNoArray[i + 1];
            nonLeafNode->pageNoArray[i + 1] = 0;
        }

        // store the relavant split data
        SplitData <int> *newNodeData = new SplitData <int>();
        newNodeData->set(sendUpKey, newPageId);
        // unpin the pages before returning
        bufMgr->unPinPage(file, pageNum, true);
        bufMgr->unPinPage(file, newPageId, true);
        return newNodeData;
    }
    // new key goes in the newly created node
    else {
        // store the key at which split occurs
        int sendUpKey = nonLeafNode->keyArray[halfIndex];
        newNode->pageNoArray[0] = nonLeafNode->pageNoArray[halfIndex + 1];

        nonLeafNode->pageNoArray[halfIndex + 1] = 0;

        // track if the new key was added
        bool newAdded = false;
        int  idx      = 0;
        // add keys to the new node at the appropriate index
        for (int i = halfIndex + 1; i < nodeOccupancy; i++, idx++) {
            if (!newAdded) {
                if (i == pIdx) {
                    newNode->keyArray[idx]        = key;
                    newNode->pageNoArray[idx + 1] = splitPointer->newPageId;
                    i--;
                    newAdded = true;
                }
                else {
                    newNode->keyArray[idx]          = nonLeafNode->keyArray[i];
                    newNode->pageNoArray[idx + 1]   = nonLeafNode->pageNoArray[i + 1];
                    nonLeafNode->pageNoArray[i + 1] = 0;
                }
            }
            else {
                newNode->pageNoArray[idx]       = nonLeafNode->pageNoArray[i];
                newNode->pageNoArray[idx + 1]   = nonLeafNode->pageNoArray[i + 1];
                nonLeafNode->pageNoArray[i + 1] = 0;
            }
        }

        // If the new key was not added in the loop, it was the last element
        // add it at the appropriate index
        if (!newAdded) {
            newNode->keyArray[idx]        = key;
            newNode->pageNoArray[idx + 1] = splitPointer->newPageId;
        }

        SplitData <int> *newNodeData = new SplitData <int>();
        newNodeData->set(newPageId, sendUpKey);

        bufMgr->unPinPage(file, pageNum, true);
        bufMgr->unPinPage(file, newPageId, true);

        return newNodeData;
    }

    // This should never be executed
    bufMgr->unPinPage(file, pageNum, true);
    bufMgr->unPinPage(file, newPageId, true);
    return NULL;
}

SplitData <int> *BTreeIndex::splitLeafNode(struct LeafNodeInt *leafNode,
                                           RIDKeyPair <int> *ridKeyPair) {
    /// Logic for splitting
    std::cout << "--> Start leaf split\n";
    // std::cout << pageNum << "\n";
    int key = ridKeyPair->key;
    std::cout << key << "\n";
    int pIdx;

    /// find correct spot, respecting the key order, to insert the entry in leaf
    /// ///
    for (pIdx = 0; pIdx < leafOccupancy && key >= leafNode->keyArray[pIdx];
         pIdx++)
        ;

    // std::cout << leafNode << " " << ridKeyPair->key << " " << ridKeyPair->rid.page_number << "/" << ridKeyPair->rid.slot_number << "\n";
    /// halfIndex: where to split, maintaining 50% occupancy ///
    int halfIndex = (leafOccupancy + 1) / 2;

    /// new node that will be returned as split-data ///
    PageId newPageId;
    Page * newPage;
    std::cout << file << "\n";
    bufMgr->allocPage(file, newPageId, newPage);
    // std::cout << "Alloc : " << newPageId << "\n";
    std::cout << newPageId;
    /// initialize rid-array within the new leaf node, all of them are empty rn
    /// ///
    struct LeafNodeInt *newLeaf = (struct LeafNodeInt *) newPage;
    for (int idx = 0; idx < leafOccupancy; idx++) {
        (newLeaf->ridArray[idx]).page_number = 0;
    }

    /// actual splitting logic ///
    if (pIdx < halfIndex) {
        for (int i = halfIndex - 1, j = 0; i < leafOccupancy; i++, j++) {
            newLeaf->keyArray[j] = leafNode->keyArray[i];
            newLeaf->ridArray[j] = leafNode->ridArray[i];
            (leafNode->ridArray[i]).page_number = 0;
        }

        for (int i = halfIndex - 1; i > pIdx; i--) {
            leafNode->keyArray[i] = leafNode->keyArray[i - 1];
            leafNode->ridArray[i] = leafNode->ridArray[i - 1];
        }

        leafNode->keyArray[pIdx] = key;
        leafNode->ridArray[pIdx] = ridKeyPair->rid;

        /// set pointer to the adjacent leaf node ///
        newLeaf->rightSibPageNo  = leafNode->rightSibPageNo;
        leafNode->rightSibPageNo = newPageId;

        /// construct return val - split-data ///
        SplitData <int> *splitData = new SplitData <int>();
        splitData->set(newPageId, newLeaf->keyArray[0]);
        bufMgr->unPinPage(file, newPageId, true);
        std::cout << "    End leaf split\n";
        return splitData;
    }
    else {
        bool newAdded = false;
        int  idx      = 0;
        for (int i = halfIndex; i < leafOccupancy; i++, idx++) {
            if (!newAdded) {
                if (i == pIdx) {
                    newLeaf->keyArray[idx] = key;
                    newLeaf->ridArray[idx] = ridKeyPair->rid;
                    i--;
                    newAdded = true;
                }
                else {
                    newLeaf->keyArray[idx] = leafNode->keyArray[i];
                    newLeaf->ridArray[idx] = leafNode->ridArray[i];
                    (leafNode->ridArray[i]).page_number = 0;
                }
            }
            else {
                newLeaf->keyArray[idx] = leafNode->keyArray[i];
                newLeaf->ridArray[idx] = leafNode->ridArray[i];
                (leafNode->ridArray[i]).page_number = 0;
            }
        }

        if (!newAdded) {
            newLeaf->keyArray[idx] = key;
            newLeaf->ridArray[idx] = ridKeyPair->rid;
        }

        newLeaf->rightSibPageNo  = leafNode->rightSibPageNo;
        leafNode->rightSibPageNo = newPageId;

        SplitData <int> *splitData = new SplitData <int>();
        splitData->set(newPageId, newLeaf->keyArray[0]);
        bufMgr->unPinPage(file, newPageId, true);
        std::cout << "    End leaf split\n";
        return splitData;
    }

    /// This statement should never be reached ///
    return NULL;
}

/**
 * Assumes leaf node has empty space, finds right spot, shifts and inserts
 */
const void BTreeIndex::insertLeafEntry(LeafNodeInt *leafNode,
                                       RIDKeyPair <int> *kpEntry,
                                       int lastFullIndex) {
    int inputIndex;
    int key = kpEntry->key;

    /// respecting the ordering, find valid spot to insert entry in the leaf node
    /// ///
    for (inputIndex = 0;
         inputIndex <= lastFullIndex && key >= leafNode->keyArray[inputIndex];
         inputIndex++)
        ;

    /// shift other entries to the right ///
    for (int i = lastFullIndex + 1; i > inputIndex; i--) {
        leafNode->keyArray[i] = leafNode->keyArray[i - 1];
        leafNode->ridArray[i] = leafNode->ridArray[i - 1];
    }

    /// insert at the correct spot ///
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
    if (*(int *) lowValParm > *(int *) highValParm) {
        scanExecuting = false;
        throw BadScanrangeException();
    }

    // Opcode check
    if (lowOpParm != GT && lowOpParm != GTE) {
        scanExecuting = false;
        throw BadOpcodesException();
    }
    if (highOpParm != LT && highOpParm != LTE) {
        scanExecuting = false;
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

    bool stop       = 0;
    int  prev_level = rootIsLeaf ? 1 : 0;
    currentPageNum = rootPageNum;

    // Finding the proper leaf
    while (!stop) {
        if (prev_level) {
            stop = true;
            break;
        }

        Page *curr_page;
        bufMgr->readPage(file, currentPageNum, curr_page);
        struct NonLeafNodeInt *node = (struct NonLeafNodeInt *) curr_page;

        int nIdx;
        for (nIdx = 0; nIdx < nodeOccupancy && node->pageNoArray[nIdx + 1] != 0 &&
             lowValInt >= node->keyArray[nIdx];
             nIdx++)
            ;

        int nextPage = node->pageNoArray[nIdx];
        prev_level = node->level;

        bufMgr->unPinPage(file, currentPageNum, false);
        currentPageNum = nextPage;
    }

    stop = false;

    while (!stop) {
        Page *leafPage;
        bufMgr->readPage(file, currentPageNum, leafPage);
        currentPageData = leafPage;
        struct LeafNodeInt *leaf = (struct LeafNodeInt *) leafPage;
        int sIdx;
        stop = 0;

        if (lowOp == GT) {
            for (sIdx = 0; sIdx < leafOccupancy && lowValInt >= leaf->keyArray[sIdx];
                 sIdx++)
                ;
        }
        else {
            for (sIdx = 0; sIdx <leafOccupancy && lowValInt> leaf->keyArray[sIdx];
                 sIdx++)
                ;
        }

        if (sIdx == leafOccupancy) {
            int nPage = leaf->rightSibPageNo;
            if (!nPage) {
                stop            = true;
                nextEntry       = leafOccupancy - 1;
                currentPageNum  = 0;
                currentPageData = NULL;
            }
            else {
                bufMgr->unPinPage(file, currentPageNum, false);
                currentPageNum = nPage;
            }
        }
        else {
            nextEntry = sIdx;
            stop      = true;
        }
    }
}

/*
 * const void BTreeIndex::startScan(const void *lowValParm,
 *                               const Operator lowOpParm,
 *                               const void *highValParm,
 *                               const Operator highOpParm)
 * {
 * // I'm not quite sure of all the functionality of this method. The pdf seems
 * to
 * // imply that this sets global variables and then calls the scan next method
 * // I don't see why the use of global variables is even necessary as this
 * function
 * // could store the state of the scan in member variables
 *
 * // First use iterative traversal to find the leftmost node that could point to
 * // values in the parameter range. this is done by using the lower bounds
 * // and the given operator either GT or GTE and comparing to the value in the
 * // non leaf key array. If it fails go onto the next index entry, otherwise
 * // find the child node. If the current node has level = 1 that means the child
 * // is a leaf, otherwise it's another non leaf
 *
 * // Okay, I think I have a better idea of what's going on in this function.
 * // It doesn't actually call the scanNext function, it's only finding the first
 * // node that matches the criteria and setting up the gloabal vars that
 * // scanNext will use to return the recordIDs, an external call to scanNext()
 * // will be made when needed.
 *
 * // Be pinning pages!?
 *
 * ////  Range sanity check ////
 * // The range parameters are void pointers, so they must first be cast to
 * // integer pointers and then dereferenced to get the actual values.
 * if (*(int *) lowValParm > *(int *) highValParm)
 * {
 *  throw BadScanrangeException();
 * }
 *
 * // Opcode check
 * if (lowOpParm != GT && lowOpParm != GTE)
 * {
 *  throw BadOpcodesException();
 * }
 * if (highOpParm != LT && lowOpParm != LTE)
 * {
 *  throw BadOpcodesException();
 * }
 *
 * // Stop any current scans, might need to do more here
 * if (scanExecuting)
 * {
 *  endScan();
 * }
 *
 * // Set up global vars for the scan constraints
 * scanExecuting = true;
 *
 * lowValInt  = *(int *) lowValParm;
 * highValInt = *(int *) highValParm;
 * lowOp      = lowOpParm;
 * highOp     = highOpParm;
 *
 * // Index of next entry in current leaf to be scanned
 * nextEntry = 0;
 *
 * // Start at the root node of the B+ index to traverse for key values
 * // Each non leaf node has an array of key values, and associated child nodes
 * // The nodes are pages, and once I have the page I can cast the pointer to
 * // a struct pointer to get the datamembers?
 *
 * // Start by getting the root page number, get it's page, make a pointer
 * // to that page address and then cast it to the NonLeafNodeInt struct pointer
 * PageId index_root_pageID = rootPageNum;
 * Page current_page = file->readPage(index_root_pageID);
 * Page* current_page_pointer = &current_page;
 *
 * PageId min_pageID = 0;
 * struct NonLeafNodeInt* cur_node_ptr;
 * Page* min_leaf_ptr;
 *
 * if (rootIsLeaf)
 * {
 *  min_pageID = index_root_pageID;
 *  min_leaf_ptr = current_page_pointer;
 * }
 * else
 * {
 *  cur_node_ptr = (struct NonLeafNodeInt *) current_page_pointer;
 *
 *  while (cur_node_ptr->level != 1)
 *  {
 *    bool found_range = false;
 *
 *
 *    // Find the leftmost non leaf child with key values matching the search,
 *    for (int i = 0; i < nodeOccupancy && !found_range; i++)
 *    {
 *      int key_value = cur_node_ptr->keyArray[i];
 *
 *      // There might be issue with 0 key values
 *      // If the key_value is zero we need to make sure that the page is points
 *      // to is valid, if not then that key isn't valid
 *      // Either the key is supposed to be 0 or the key is "null"
 *      // If the key is supposed to be 0 the first condition will handle it
 *
 *      // Since the lower bound must be contained in the child that defines
 *      // a range larger than it if the current key is larger than the lower
 *      // bound we will find the value in the corresponding child index
 *      if (lowValInt < key_value)
 *      {
 *        min_pageID = cur_node_ptr->pageNoArray[i];
 *        Page min_page = file->readPage(min_pageID);
 *
 *        Page* min_page_ptr = &min_page;
 *        cur_node_ptr = (struct NonLeafNodeInt *) min_page_ptr;
 *
 *        found_range = true;
 *      }
 *      // "Null key" condition check
 *      else if (key_value == 0)
 *      {
 *        // If the key value is supposed to be 0, then there will be a
 *        // valid page number at i+1 in the pageNoArry
 *        // If the key is supposed to be null there won't
 *        // Can access the i+1 element because even if i is at the end of
 *        // the key array the page number array is one larger than it
 *        min_pageID = cur_node_ptr->pageNoArray[i + 1];
 *
 *        // If the key is null we know that the page number in the i index
 *        // will be the range we are looking for as this is the
 *        // range that is larger than the last valid key value
 *        if (min_pageID == 0)
 *        {
 *          min_pageID = cur_node_ptr->pageNoArray[i];
 *          Page min_page = file->readPage(min_pageID);
 *
 *          Page* min_page_ptr = &min_page;
 *          cur_node_ptr = (struct NonLeafNodeInt *) min_page_ptr;
 *
 *          found_range = true;
 *        }
 *      }
 *      // All the key values have been compared and the lower bound is larger
 *      // than all of them, thus we take the rightmost child of the node
 *      // Though this might be different if the nodes aren't fully occupied!
 *      //   The previous check for key_value = 0 should cover non full nodes
 *      else if (i == nodeOccupancy)
 *      {
 *        //is this bad form, to have 1 offset here?
 *        min_pageID = cur_node_ptr->pageNoArray[i + 1];
 *        Page min_page = file->readPage(min_pageID);
 *
 *        Page* min_page_ptr = &min_page;
 *        cur_node_ptr = (struct NonLeafNodeInt *) min_page_ptr;
 *
 *        found_range = true;
 *      }
 *    }
 *  }
 * }
 * //Traverse down the tree until the level = 1, this last level before leafs
 *
 * // Once the first matching node is found then the rest of the scan
 * // related global variables can be setup, this is also where the distinction
 * // between the GTE vs GT might come into play, though it might be in the
 * // scan next method that we worry about that
 *
 * // Looks like we want to deal with that here instead of scanNext, so now
 * // that we have the parent of the leafnodes we have to find the correct
 * // leaf node to start the scan on.
 * bool found_leaf = false;
 *
 * for (int i = 0; i < nodeOccupancy && !found_leaf; i++)
 * {
 *  int key_value = cur_node_ptr->keyArray[i];
 *
 *  // Look for the leaf node that contains entries of this range
 *  if (lowValInt < key_value)
 *  {
 *    min_pageID = cur_node_ptr->pageNoArray[i];
 *    Page min_page = file->readPage(min_pageID);
 *
 *    min_leaf_ptr = &min_page;
 *
 *    found_leaf = true;
 *  }
 *  // "Null key" condition check
 *  else if (key_value == 0)
 *  {
 *      // If the key value is supposed to be 0, then there will be a
 *      // valid page number at i+1 in the pageNoArry
 *      // If the key is supposed to be null there won't
 *      min_pageID = cur_node_ptr->pageNoArray[i + 1];
 *
 *      // If the key is null we know that the page number in the i index
 *      // will be the range we are looking for as this is the
 *      // range that is larger than the last valid key value
 *      if (min_pageID == 0)
 *      {
 *      min_pageID = cur_node_ptr->pageNoArray[i];
 *      Page min_page = file->readPage(min_pageID);
 *
 *      min_leaf_ptr = &min_page;
 *
 *      found_leaf = true;
 *      }
 *  }
 *  // All the key values have been compared and the lower bound is larger
 *  // than all of them, thus we take the rightmost child of the node
 *  // Though this might be different if the nodes aren't fully occupied!
 *  //   The previous check for key_value = 0 should cover non full nodes
 *  else if (i == nodeOccupancy)
 *  {
 *    min_pageID = cur_node_ptr->pageNoArray[i + 1];
 *    Page min_page = file->readPage(min_pageID);
 *
 *    min_leaf_ptr = &min_page;
 *  }
 * }
 *
 * // Current page number (pageId)
 * currentPageNum = min_pageID;
 * // Current page pointer, last non-leaf node, lv = 1
 * currentPageData = min_leaf_ptr;
 * }
 */

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

/**
 * Fetch the record id of the next index entry that matches the scan.
 * Return the next record from current page being scanned.
 * If current page has been scanned to its entirety, move on to the right
 * sibling of current page,
 * if any exists, to start scanning that page. Make sure to unpin any pages that
 * are no longer required.
 * @param outRid    RecordId of next record found that satisfies the scan
 * criteria returned in this
 * @throws ScanNotInitializedException If no scan has been initialized.
 * @throws IndexScanCompletedException If no more records, satisfying the scan
 * criteria, are left to be scanned.
 **/
const void BTreeIndex::scanNext(RecordId&outRid) {
    // This method fetches the record id of the next
    // tuple that matches the scan criteria.

    if (!scanExecuting) {
        throw ScanNotInitializedException();
    }

    if (this->currentPageNum == 0) {
        throw IndexScanCompletedException();
    }

    // get the current node/page being scanned as a leafNode
    LeafNodeInt *currentPageLeaf = (LeafNodeInt *) (this->currentPageData);

    if (currentPageLeaf->keyArray[nextEntry] < highValInt && highOp == LT) {
        outRid = currentPageLeaf->ridArray[nextEntry];
        nextEntry++;
    }
    else if (currentPageLeaf->keyArray[nextEntry] <= highValInt &&
             highOp == LTE) {
        outRid = currentPageLeaf->ridArray[nextEntry];
        nextEntry++;
    }
    else {
        // Unpin page before completing scan
        bufMgr->unPinPage(file, currentPageNum, false);

        throw IndexScanCompletedException();
    }

    // move on to the right sibling when end of array
    if (nextEntry >= leafOccupancy ||
        currentPageLeaf->ridArray[nextEntry].page_number == 0) {
        // get page id of right sibling
        PageId nextSiblingPage = currentPageLeaf->rightSibPageNo;

        // unpin current page/node
        bufMgr->unPinPage(file, currentPageNum, false);

        if (nextSiblingPage == 0) {
            throw IndexScanCompletedException();
        }

        // reset next entry in node
        this->nextEntry = 0;

        // set current page num as sibling page & pin the page
        this->currentPageNum = nextSiblingPage;
        // if a sibling exists, go to the sibling. otherwise, set to null
        // if (currentPageNum) {
        // currentPageData = NULL;
        // } else {
        bufMgr->readPage(file, currentPageNum, currentPageData);
        // }
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() {
    //@TODO
    // Terminate the current scan. Unpin any pinned pages. Reset scan specific
    // variables.
    //@throws ScanNotInitializedException If no scan has been initialized.

    if (!scanExecuting) {
        throw ScanNotInitializedException();
    }

    scanExecuting   = false;
    currentPageNum  = 0;
    currentPageData = NULL;

    // should we still unpin here ??
}
}
