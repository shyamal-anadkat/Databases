/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 * 
 */

/**
 * The code should be well-documented, using Doxygen style comments. Each file should start with
 * your name and student id, and should explain the purpose of the file. Each function
 * should be preceded by a few lines of comments describing the function and explaining
 * the input and output parameters and return values.
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

/**
 * \brief Buffer Manager descructor
 * Calling flushes all dirty pages, deallocates buffer pool and BufDesc table
 */

BufMgr::~BufMgr()
{
  // WB all dirty pages (iterate)
  for(FrameId i = 0; i < numBufs; i++) {
  		if(bufDescTable[i].dirty && bufDescTable[i].valid) {
  			bufDescTable[i].file -> 
  			writePage(bufPool[i]);
  			bufDescTable[i].dirty = false;
  		}
  }
  

  delete[] bufPool;
  delete[] bufDescTable;
  delete hashTable;
}

/**
 *\brief Advance clock to next frame in the buffer pool.
 */
void BufMgr::advanceClock()
{
  // Modulo on number of buffer frames to get index (frame ID)
  clockHand = (clockHand + 1) % numBufs;
}

/**
 * @brief Allocates a free frame
 *
 * @param frame - address of the FrameID to be allocated ?
 * @throws BufferExceededException if all buffer frames are pinned
 *
 * When allocating a free frame dirty pages are written to disk. 
 * If the frame to be allocated has a valid page in it, the page is removed
 * from the appropriate entry in the hash table.
 */
void BufMgr::allocBuf(FrameId& frame)
{
  std::uint32_t numPinned = 0;

  bool found = false;

  do 
  {
    BufDesc* frame_info = &bufDescTable[clockHand];

    if (!frame_info->valid) 
    {
      found = true; // found what? a free frame?
      frame = clockHand;
    } 
    // if current frame is already pinned increment pincount, why?
    else if (frame_info->pinCnt) 
    {
      numPinned++;
      advanceClock();
    } 
    else if (frame_info->refbit) 
    {
      frame_info->refbit = 0;
      advanceClock();
    }
    else // case of valid frame be written to disk, why not check this first?
    {
      hashTable->remove(frame_info->file, frame_info->pageNo);
      
      if (frame_info->dirty) 
      {
        frame_info->file->writePage(bufPool[clockHand]);
      }

      frame_info->Clear();
      found = true;
    }
  }while (!found && numPinned < numBufs);

  // If all buffer frames are pinned throw exception
  // SHOULD this be first before the do loop?
  if (numPinned == numBufs) 
  {
    throw BufferExceededException();
  }

  frame = clockHand;
}

/**
 * @brief Attempts to read a page in the buffer
 *
 * Calling this method will attempt to read a page in the buffer pool, this will
 * throw HashNotFoundException when page is not in the buffer pool
 *
 * If the page is present pin count is incremented and reference bit is set to
 * true. (Case 2)
 *
 * If page is not present in buffer pool a frame will be allocated, the page 
 * will be read from disk to the buffer frame. An entry is inserted into the 
 * hash table and pin count is one and reference bit is set to true. (Case 1)
 *
 * @param
 * @param
 * @param
 * @returns a pointer to the frame containing the page via the page parameter
 */
void BufMgr::readPage(File *file, const PageId pageNo, Page *& page)
{
  // Case 2: Page is present in buffer pool
  try 
  {
    FrameId read_target_frame_number;
    
    // if present in hashTable read_target_frame_number gets frame number
    // otherwise a HashNotFoundException will be thrown
    hashTable->lookup(file, pageNo, read_target_frame_number);
    
    // set refBit to true because this page is being referenced
    bufDescTable[read_target_frame_number].refbit = true; 
    
    // increment pinCount because page is being read
    bufDescTable[read_target_frame_number].pinCnt++;
    
    // return pointer to frame where page is stored
    page = &bufPool[read_target_frame_number];
  }
  // Case 1: Page is not in buffer pool
  catch (HashNotFoundException& hnfe) 
  {
    // New frame allocated from buffer pool and page stored in new frame
    FrameId read_target_frame_number;
    allocBuf(read_target_frame_number);    
    bufPool[read_target_frame_number] = file->readPage(pageNo);
    
    // Instert frame into hashTable
    hashTable->insert(file, pageNo, read_target_frame_number);
    bufDescTable[read_target_frame_number].Set(file, pageNo);
    
    // return pointer to frame where page is stored
    page = &bufPool[read_target_frame_number];
    
    // HashTableException (optional) ?
  }
}

/** 
  * @brief Unpins a page and updates dirty bit
  *
  * Decrements the pin count of the frame containing (file, PageNo) and, if
  * the page is dirty as, sets the dirty bit in buffer description.
  * 
  * @throws PAGENOTPINNED if the pin count is already 0. Does nothing if
  * page is not found in the hash table lookup
  * @param
  * @param
  * @param
  */   
void BufMgr::unPinPage(File *file, const PageId pageNo, const bool dirty)
{
    FrameId unpinned_frame_number;

  try 
  {
    hashTable->lookup(file, pageNo, unpinned_frame_number);

    // Page has no pins, throw exception
    if (bufDescTable[unpinned_frame_number].pinCnt == 0)
    {
      throw PageNotPinnedException(file->filename(), pageNo, unpinned_frame_number);
    }

    // Decrement pin count
    bufDescTable[unpinned_frame_number].pinCnt--;

    // WHY is the refbit true only if new pinCnt = 0? why not <= 0?
    if (bufDescTable[unpinned_frame_number].pinCnt == 0) 
    {
      bufDescTable[unpinned_frame_number].refbit = true;
    }

    if (dirty) 
    {
      bufDescTable[unpinned_frame_number].dirty = true;
    }
  }
  catch (HashNotFoundException hnfe) 
  {
    // No corresponding frame found.
    // do nothing
  }
}


/**
* @brief Allocates an empty page in a given file and adds it to the buffer pool
*
* Allocate an empty page in the specified file. This gives a newly allocated 
* page. Then allocBuf() is obtains a buffer pool frame. This frame is inserted 
* into the hash table and Set() is invoked on the frame to set it up properly. 
* The method returns both the page number of the newly allocated page to the 
* caller via the pageNo parameter and a pointer to the buffer frame allocated
* for the page via the page parameter
*
* @param file File obj
* @param pageNo Page Number
* @page  ref to page pointer
* @returns pageNo page number of the newly allocated page
* @returns page a pointer to the buffer frame allocated for the page
*/
void BufMgr::allocPage(File *file, PageId& pageNo, Page *& page)
{
  // std::cout << "Here 3\n";
  Page *new_page = new Page();
  *new_page = file->allocatePage();
  pageNo = new_page->page_number();
  FrameId id;

  allocBuf(id);
  hashTable->insert(file, pageNo, id);
  bufDescTable[id].Set(file, pageNo);
  bufPool[id] = *new_page;    page = &bufPool[id];
}


/**
* This method deletes a particular page from file. Before deleting the page from file, it
* makes sure that if the page to be deleted is allocated a frame in the buffer pool, that frame
* is freed and correspondingly entry from hash table is also removed.
*
* @param file File obj
* @param PageNo Page Number
*/
void BufMgr::disposePage(File *file, const PageId PageNo)
{
  try 
  {
    FrameId frameNum;
    hashTable->lookup(file, PageNo, frameNum);
    BufDesc *bd = &bufDescTable[frameNum];
    
    if(bufDescTable[frameNum].pinCnt != 0) {
      throw PagePinnedException
      (bd->file->filename(), bd->pageNo, frameNum);
    }

    bufDescTable[frameNum].Clear();
    hashTable->remove(file, PageNo);  
  } 
  catch (HashNotFoundException e) 
  {
    // Page not in the buffer.
    // Nothing more to be done to the buffer.
    // Ignore the exception.
  }

  file->deletePage(PageNo);
}

/*
Should scan bufTable for pages belonging to the file. For each page encountered it should:
(a) if the page is dirty, call file->writePage() to flush the page to disk and then set the dirty
bit for the page to false, (b) remove the page from the hashtable (whether the page is clean
or dirty) and (c) invoke the Clear() method of BufDesc for the page frame.
Throws PagePinnedException if some page of the file is pinned. Throws BadBuffer-
Exception if an invalid page belonging to the file is encountered
*/
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