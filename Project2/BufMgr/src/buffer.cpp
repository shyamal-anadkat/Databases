/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb {
BufMgr::BufMgr(std::uint32_t bufs) : numBufs(bufs)
{
    bufDescTable = new BufDesc[bufs];

    for (FrameId i = 0; i < bufs; i++)
    {
        bufDescTable[i].frameNo = i;
        bufDescTable[i].valid   = false;
    }

    bufPool = new Page[bufs];

    int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
    hashTable = new BufHashTbl(htsize); // allocate the buffer hash table

    clockHand = bufs - 1;
}

BufMgr::~BufMgr()
{
    // TODO: flush all dirty pages (iterate)
    // deallocates the buffer pool and the BufDesc table.

    delete[] bufPool;
    delete[] bufDescTable;
    delete hashTable;
}

void BufMgr::advanceClock()
{
    // modulo to get index (frame ID)
    clockHand = (clockHand + 1) % numBufs;
}

void BufMgr::allocBuf(FrameId& frame)
{
    // allocate a free frame
    BufMgr::advanceClock();

    // if necessary, writing a dirty page back to disk.
    // Throws BufferExceededException if all buffer frames are pinned
    // Make sure that if the buffer frame allocated has a valid page in it,
    // you remove the appropriate entry from the hash table.
}

void BufMgr::readPage(File *file, const PageId pageNo, Page *& page)
{
    try {
        // set refBit to true, increment pinCount
        // return pointer to frame
        FrameId frameNum;
        hashTable->lookup(file, pageNo, frameNum);
        bufDescTable[frameNum].refbit = true;
        bufDescTable[frameNum].pinCnt++;
        page = &bufPool[frameNum];
    }

    catch (HashNotFoundException& hnfe) {
        // new frame allocated from buffer pool for reading page
        // insert
        FrameId frameNum;
        allocBuf(frameNum);
        bufPool[frameNum] = file->readPage(pageNo);
        hashTable->insert(file, pageNo, frameNum);
        bufDescTable[frameNum].Set(file, pageNo);
        page = &bufPool[frameNum];

        // HashTableException (optional) ?
    }
}

void BufMgr::unPinPage(File *file, const PageId pageNo, const bool dirty)
{
    /* Decrements the pinCnt of the frame containing (file, PageNo) and, if
     * dirty == true, sets
     * the dirty bit. Throws PAGENOTPINNED if the pin count is already 0. Does
     * nothing if
     * page is not found in the hash table lookup*/
    FrameId frameNum;

    try {
        hashTable->lookup(file, pageNo, frameNum);

        if (bufDescTable[frameNum].pinCnt == 0)
        {
            throw PageNotPinnedException(file->filename(), pageNo, frameNum);
        }
        bufDescTable[frameNum].pinCnt--;

        if (bufDescTable[frameNum].pinCnt == 0) {
            bufDescTable[frameNum].refbit = true;
        }

        if (dirty) {
            bufDescTable[frameNum].dirty = true;
        }
    }

    catch (HashNotFoundException hnfe) {
        // No corresponding frame found.
        // do nothing
    }
}

void BufMgr::flushFile(const File *file)
{
    for (FrameId i = 0; i < numBufs; i++)
    {
        BufDesc *bd = &bufDescTable[i];

        if ((bd->file) == file)
        {
            if (bd->pinCnt > 0)
            {
                throw PagePinnedException(bd->file->filename(), bd->pageNo, i);
            }

            if (!bd->valid)
            {
                throw BadBufferException(i, bd->dirty, false, bd->refbit);
            }

            if (bd->dirty)
            {
                bd->file->writePage(bufPool[i]);
            }

            hashTable->remove(bd->file, bd->pageNo);
            bd->Clear();
        }
    }
}

void BufMgr::allocPage(File *file, PageId& pageNo, Page *& page)
{
    *page = file->allocatePage();

    FrameId id;
    allocBuf(id);

    hashTable->insert(file, pageNo, id);
    bufDescTable[id].Set(file, pageNo);
}

void BufMgr::disposePage(File *file, const PageId PageNo)
{
    try {
        FrameId frameNum;

        hashTable->lookup(file, PageNo, frameNum);
        bufDescTable[frameNum].Clear();
        hashTable->remove(file, PageNo);
    } catch (HashNotFoundException e) {
        // Page not in the buffer.
        // Nothing more to be done to the buffer.
        // Ignore the exception.
    }

    file->deletePage(PageNo);
}

void BufMgr::printSelf(void)
{
    BufDesc *tmpbuf;
    int validFrames = 0;

    for (std::uint32_t i = 0; i < numBufs; i++)
    {
        tmpbuf = &(bufDescTable[i]);
        std::cout << "FrameNo:" << i << " ";
        tmpbuf->Print();

        if (tmpbuf->valid == true)
        {
            validFrames++;
        }
    }

    std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}
}
