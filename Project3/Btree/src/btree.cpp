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

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{

	// init fields for Btree
	this->bufMgr = bufMgrIn; //bufManager instance
	this->leafOccupancy = INTARRAYLEAFSIZE;
	this->nodeOccupancy = INTARRAYNONLEAFSIZE;

	this->attributeType = attrType;
	this->attrByteOffset = attrByteOffset;
	this->scanExecuting = false;

	// constructing index file name 
	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	std::string indexName = idxStr.str(); //indexName : name of index file created

	// return the name of index file after determining it above
	outIndexName = indexName;

	// debug checks
	std::cout << "Index File Name: " << indexName << std::endl;


	// if index file exists, open the file
	 
	if(File::exists(outIndexName)) {

		// open file if exists 
		this->file = new BlobFile(outIndexName, false);
		
		// get meta page info (always the first page)
		Page *metaPage;
		this->headerPageNum = file->getFirstPageNo();
		this->bufMgr->readPage(this->file, headerPageNum, metaPage);

		// cast meta page to IndexMetaInfo structure
		IndexMetaInfo * indexMetaInfo = (IndexMetaInfo *)metaPage;
		
		// unpin the header page, as we don't use it after constructor 
		this->bufMgr->unPinPage(this->file, headerPageNum, false);

		//// VALIDATION CHECK ////
        // @throws  BadIndexInfoException     
		// If the index file already exists for the corresponding attribute,
		// but values in metapage(relationName, attribute byte offset, attribute type etc.) 
		// do not match with values received through constructor parameters.
		if( (indexMetaInfo->attrType != attrType) || 
			(indexMetaInfo->attrByteOffset != attrByteOffset) ||
			(indexMetaInfo->relationName != relationName) ) 
		{

			throw BadIndexInfoException("Error: Index meta attributes don't match!");
		}
		// page number of root page of B+ tree inside index file.
		this->rootPageNum = indexMetaInfo->rootPageNo;
	}

	else {

		std::cout << "Creating index file ...\n";

		// create index file if doesn't exist + meta pg + root pg
		this->file = new BlobFile(outIndexName, true);
		Page* indexMetaInfoPage;

		//// meta info init ////
		this->bufMgr->allocPage(this->file, headerPageNum, indexMetaInfoPage);
		struct IndexMetaInfo * metaInfo = (struct IndexMetaInfo *)indexMetaInfoPage;
		metaInfo->attrByteOffset = attrByteOffset;
		metaInfo->attrType = attrType;
		strncpy(metaInfo->relationName, relationName.c_str(), sizeof(metaInfo->relationName));

		//// root page init ////
		Page* rootPage;
		this->bufMgr->allocPage(this->file, rootPageNum, rootPage);
		NonLeafNodeInt* root = (NonLeafNodeInt*) rootPage;
		metaInfo->rootPageNo = this->rootPageNum = 2; // Root page starts as page 2
		root->level = 1;

		// unpin headerPage and rootPage
		bufMgr->unPinPage(file, headerPageNum, true);
		bufMgr->unPinPage(file, rootPageNum,   true);

		//insert entries for every tuple in the base relation using FileScan class.
		RecordId curr_rid;
		FileScan * fs = new FileScan(relationName, bufMgr);
		try {
			while(true) {
				//// scan entry, insert entries here ////
				fs->scanNext(curr_rid);
				std::string recordStr = fs->getRecord();
				const char* recordToInsert = recordStr.c_str();
				insertEntry(recordToInsert + attrByteOffset, curr_rid);
			}
		} catch(EndOfFileException e) { delete fs;}

		bufMgr->flushFile(file);
		//delete fs;
	}
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	// flushing the index file
	bufMgr->flushFile(file);
	// destructor of file class called
	file->~File();
	delete file;
	delete bufMgr;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

// This method inserts a new entry into the index using the pair <key, rid>
// const void* key A pointer to the value (integer) we want to insert.
// const RecordId& rid The corresponding record id of the tuple in the base relation

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{

	if (!scanExecuting) {
			throw ScanNotInitializedException();	
	}

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{
	if (!scanExecuting) {
			throw ScanNotInitializedException();	
	}

}

}
