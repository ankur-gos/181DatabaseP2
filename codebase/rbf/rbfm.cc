#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <string>
#include <algorithm>
#include <stdlib.h>

#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = NULL;
PagedFileManager *RecordBasedFileManager::_pf_manager = NULL;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
    // Initialize the internal PagedFileManager instance
    _pf_manager = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) 
{
    // Creating a new paged file.
    if (_pf_manager->createFile(fileName))
        return RBFM_CREATE_FAILED;

    // Setting up the first page.
    void * firstPageData = calloc(PAGE_SIZE, 1);
    if (firstPageData == NULL)
        return RBFM_MALLOC_FAILED;
    newRecordBasedPage(firstPageData);

    // Adds the first record based page.
    FileHandle handle;
    if (_pf_manager->openFile(fileName.c_str(), handle))
        return RBFM_OPEN_FAILED;
    if (handle.appendPage(firstPageData))
        return RBFM_APPEND_FAILED;
    _pf_manager->closeFile(handle);

    free(firstPageData);

    return SUCCESS;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) 
{
    return _pf_manager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) 
{
    return _pf_manager->openFile(fileName.c_str(), fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) 
{
    return _pf_manager->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) 
{
    // Gets the size of the record.
    unsigned recordSize = getRecordSize(recordDescriptor, data);

    // Cycles through pages looking for enough free space for the new entry.
    void *pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;
    bool pageFound = false;
    unsigned i;
    unsigned numPages = fileHandle.getNumberOfPages();
    for (i = 0; i < numPages; i++)
    {
        if (fileHandle.readPage(i, pageData))
            return RBFM_READ_FAILED;

        // When we find a page with enough space (accounting also for the size that will be added to the slot directory), we stop the loop.
        if (getPageFreeSpaceSize(pageData) >= sizeof(SlotDirectoryRecordEntry) + recordSize)
        {
            pageFound = true;
            break;
        }
    }

    // If we can't find a page with enough space, we create a new one
    if(!pageFound)
    {
        newRecordBasedPage(pageData);
    }

    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);

    // Setting the return RID.
    rid.pageNum = i;
    rid.slotNum = slotHeader.recordEntriesNumber;

    // Adding the new record reference in the slot directory.
    SlotDirectoryRecordEntry newRecordEntry;
    newRecordEntry.length = recordSize;
    newRecordEntry.offset = slotHeader.freeSpaceOffset - recordSize;
    setSlotDirectoryRecordEntry(pageData, rid.slotNum, newRecordEntry);

    // Updating the slot directory header.
    slotHeader.freeSpaceOffset = newRecordEntry.offset;
    slotHeader.recordEntriesNumber += 1;
    setSlotDirectoryHeader(pageData, slotHeader);

    // Adding the record data.
    setRecordAtOffset (pageData, newRecordEntry.offset, recordDescriptor, data);

    // Writing the page to disk.
    if (pageFound)
    {
        if (fileHandle.writePage(i, pageData))
            return RBFM_WRITE_FAILED;
    }
    else
    {
        if (fileHandle.appendPage(pageData))
            return RBFM_APPEND_FAILED;    }

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) 
{
    // Retrieve the specific page
    void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    if(slotHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);

    if(recordEntry.length <= 0 || recordEntry.offset <= 0){
        //we should fix this to work with forwarding files
        return -1;
    }

    // Retrieve the actual entry data
    getRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, data);

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) 
{
    // Parse the null indicator into an array
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, data, nullIndicatorSize);
    
    // We've read in the null indicator, so we can skip past it now
    unsigned offset = nullIndicatorSize;

    cout << "----" << endl;
    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        cout << setw(10) << left << recordDescriptor[i].name << ": ";
        // If the field is null, don't print it
        bool isNull = fieldIsNull(nullIndicator, i);
        if (isNull)
        {
            cout << "NULL" << endl;
            continue;
        }
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                uint32_t data_integer;
                memcpy(&data_integer, ((char*) data + offset), INT_SIZE);
                offset += INT_SIZE;

                cout << "" << data_integer << endl;
            break;
            case TypeReal:
                float data_real;
                memcpy(&data_real, ((char*) data + offset), REAL_SIZE);
                offset += REAL_SIZE;

                cout << "" << data_real << endl;
            break;
            case TypeVarChar:
                // First VARCHAR_LENGTH_SIZE bytes describe the varchar length
                uint32_t varcharSize;
                memcpy(&varcharSize, ((char*) data + offset), VARCHAR_LENGTH_SIZE);
                offset += VARCHAR_LENGTH_SIZE;

                // Gets the actual string.
                char *data_string = (char*) malloc(varcharSize + 1);
                if (data_string == NULL)
                    return RBFM_MALLOC_FAILED;
                memcpy(data_string, ((char*) data + offset), varcharSize);

                // Adds the string terminator.
                data_string[varcharSize] = '\0';
                offset += varcharSize;

                cout << data_string << endl;
                free(data_string);
            break;
        }
    }
    cout << "----" << endl;

    return SUCCESS;
}

// Private helper methods

// Configures a new record based page, and puts it in "page".
void RecordBasedFileManager::newRecordBasedPage(void * page)
{
    memset(page, 0, PAGE_SIZE);
    // Writes the slot directory header.
    SlotDirectoryHeader slotHeader;
    slotHeader.freeSpaceOffset = PAGE_SIZE;
    slotHeader.recordEntriesNumber = 0;
    setSlotDirectoryHeader(page, slotHeader);
}

SlotDirectoryHeader RecordBasedFileManager::getSlotDirectoryHeader(void * page)
{
    // Getting the slot directory header.
    SlotDirectoryHeader slotHeader;
    memcpy (&slotHeader, page, sizeof(SlotDirectoryHeader));
    return slotHeader;
}

void RecordBasedFileManager::setSlotDirectoryHeader(void * page, SlotDirectoryHeader slotHeader)
{
    // Setting the slot directory header.
    memcpy (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

SlotDirectoryRecordEntry RecordBasedFileManager::getSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber)
{
    // Getting the slot directory entry data.
    SlotDirectoryRecordEntry recordEntry;
    memcpy  (
            &recordEntry,
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            sizeof(SlotDirectoryRecordEntry)
            );

    return recordEntry;
}

void RecordBasedFileManager::setSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber, SlotDirectoryRecordEntry recordEntry)
{
    // Setting the slot directory entry data.
    memcpy  (
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            &recordEntry,
            sizeof(SlotDirectoryRecordEntry)
            );
}

// Computes the free space of a page (function of the free space pointer and the slot directory size).
unsigned RecordBasedFileManager::getPageFreeSpaceSize(void * page) 
{
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);
    return slotHeader.freeSpaceOffset - slotHeader.recordEntriesNumber * sizeof(SlotDirectoryRecordEntry) - sizeof(SlotDirectoryHeader);
}

unsigned RecordBasedFileManager::getRecordSize(const vector<Attribute> &recordDescriptor, const void *data) 
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Offset into *data. Start just after null indicator
    unsigned offset = nullIndicatorSize;
    // Running count of size. Initialize to size of header
    unsigned size = sizeof (RecordLength) + (recordDescriptor.size()) * sizeof(ColumnOffset) + nullIndicatorSize;

    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        // Skip null fields
        if (fieldIsNull(nullIndicator, i))
            continue;
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                size += INT_SIZE;
                offset += INT_SIZE;
            break;
            case TypeReal:
                size += REAL_SIZE;
                offset += REAL_SIZE;
            break;
            case TypeVarChar:
                uint32_t varcharSize;
                // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                memcpy(&varcharSize, (char*) data + offset, VARCHAR_LENGTH_SIZE);
                size += varcharSize;
                offset += varcharSize + VARCHAR_LENGTH_SIZE;
            break;
        }
    }

    return size;
}

// Calculate actual bytes for nulls-indicator for the given field counts
int RecordBasedFileManager::getNullIndicatorSize(int fieldCount) 
{
    return int(ceil((double) fieldCount / CHAR_BIT));
}

bool RecordBasedFileManager::fieldIsNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

void RecordBasedFileManager::setRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, const void *data)
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset (nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Points to start of record
    char *start = (char*) page + offset;

    // Offset into *data
    unsigned data_offset = nullIndicatorSize;
    // Offset into page header
    unsigned header_offset = 0;

    RecordLength len = recordDescriptor.size();
    memcpy(start + header_offset, &len, sizeof(len));
    header_offset += sizeof(len);

    memcpy(start + header_offset, nullIndicator, nullIndicatorSize);
    header_offset += nullIndicatorSize;

    // Keeps track of the offset of each record
    // Offset is relative to the start of the record and points to the END of a field
    ColumnOffset rec_offset = header_offset + (recordDescriptor.size()) * sizeof(ColumnOffset);

    unsigned i = 0;
    for (i = 0; i < recordDescriptor.size(); i++)
    {
        if (!fieldIsNull(nullIndicator, i))
        {
            // Points to current position in *data
            char *data_start = (char*) data + data_offset;

            // Read in the data for the next column, point rec_offset to end of newly inserted data
            switch (recordDescriptor[i].type)
            {
                case TypeInt:
                    memcpy (start + rec_offset, data_start, INT_SIZE);
                    rec_offset += INT_SIZE;
                    data_offset += INT_SIZE;
                break;
                case TypeReal:
                    memcpy (start + rec_offset, data_start, REAL_SIZE);
                    rec_offset += REAL_SIZE;
                    data_offset += REAL_SIZE;
                break;
                case TypeVarChar:
                    unsigned varcharSize;
                    // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                    memcpy(&varcharSize, data_start, VARCHAR_LENGTH_SIZE);
                    memcpy(start + rec_offset, data_start + VARCHAR_LENGTH_SIZE, varcharSize);
                    // We also have to account for the overhead given by that integer.
                    rec_offset += varcharSize;
                    data_offset += VARCHAR_LENGTH_SIZE + varcharSize;
                break;
            }
        }
        // Copy offset into record header
        // Offset is relative to the start of the record and points to END of field
        memcpy(start + header_offset, &rec_offset, sizeof(ColumnOffset));
        header_offset += sizeof(ColumnOffset);
    }
}

// Support header size and null indicator. If size is less than recordDescriptor size, then trailing records are null
// Memset null indicator as 1?
void RecordBasedFileManager::getRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, void *data)
{
    // Pointer to start of record
    char *start = (char*) page + offset;

    // Allocate space for null indicator. The returned null indicator may be larger than
    // the null indicator in the table has had fields added to it
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);

    // Get number of columns and size of the null indicator for this record
    RecordLength len = 0;
    memcpy (&len, start, sizeof(RecordLength));
    int recordNullIndicatorSize = getNullIndicatorSize(len);

    // Read in the existing null indicator
    memcpy (nullIndicator, start + sizeof(RecordLength), recordNullIndicatorSize);

    // If this new recordDescriptor has had fields added to it, we set all of the new fields to null
    for (unsigned i = len; i < recordDescriptor.size(); i++)
    {
        int indicatorIndex = (i+1) / CHAR_BIT;
        int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
        nullIndicator[indicatorIndex] |= indicatorMask;
    }
    // Write out null indicator
    memcpy(data, nullIndicator, nullIndicatorSize);

    // Initialize some offsets
    // rec_offset: points to data in the record. We move this forward as we read data from our record
    unsigned rec_offset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
    // data_offset: points to our current place in the output data. We move this forward as we write data to data.
    unsigned data_offset = nullIndicatorSize;
    // directory_base: points to the start of our directory of indices
    char *directory_base = start + sizeof(RecordLength) + recordNullIndicatorSize;
    
    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        if (fieldIsNull(nullIndicator, i))
            continue;
        
        // Grab pointer to end of this column
        ColumnOffset endPointer;
        memcpy(&endPointer, directory_base + i * sizeof(ColumnOffset), sizeof(ColumnOffset));

        // rec_offset keeps track of start of column, so end-start = total size
        uint32_t fieldSize = endPointer - rec_offset;

        // Special case for varchar, we must give data the size of varchar first
        if (recordDescriptor[i].type == TypeVarChar)
        {
            memcpy((char*) data + data_offset, &fieldSize, VARCHAR_LENGTH_SIZE);
            data_offset += VARCHAR_LENGTH_SIZE;
        }
        // Next we copy bytes equal to the size of the field and increase our offsets
        memcpy((char*) data + data_offset, start + rec_offset, fieldSize);
        rec_offset += fieldSize;
        data_offset += fieldSize;
    }
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, 
                 const RID &rid, const string &attributeName, void *data)
{
    //read page given by filehandle
    //look at readRecord for example on parsing the data!
    //read in record size;
    //read in null indicator
    //check if attr is null; what do we do if it is null?
    int index;
    AttrType type;
    void * page = malloc(PAGE_SIZE);

    for(int i = 0; i<recordDescriptor.size(); i++){
        if(recordDescriptor[i].name == attributeName){
            type = recordDescriptor[i].type;
            index = i;
        }
    }

    SlotDirectoryRecordEntry sdre = _rbf_manager->getSlotDirectoryRecordEntry(page, rid.pageNum);

    int offset_to_record = sdre.offset;

    int nullIndicatorSize = _rbf_manager->getNullIndicatorSize(recordDescriptor.size());

    //read into data from offset_to_record of size nullIndicator size
    char* nullBytes = (char*) malloc(nullIndicatorSize);
    memcpy(nullBytes, (char*)page+offset_to_record, nullIndicatorSize);


    // with only 1 record we only need 1 byte to represent nulls
    int recordNullIndicatorSize = 1; 

    //copy null byte into data if necessary
    if(fieldIsNull(nullBytes, index)){
        int indicatorIndex = (0+1) / CHAR_BIT;
        int indicatorMask  = 1 << (CHAR_BIT - 1 - (0 % CHAR_BIT));
        ((char*) data)[indicatorIndex] |= indicatorMask;
        //if null, we don't have any other data to return so we can exit now
        return 0;
    }

    int offset_to_null_indicator = offset_to_record + sizeof(RecordLength);

    int offset_to_field_dir = offset_to_null_indicator + nullIndicatorSize;

    //I'm assuming offset is relative to start of record :) 
    //The offset definitely does point to the end of the field
    ColumnOffset startPointer; //what if index == 0?
    memcpy(&startPointer, (char*)page + offset_to_field_dir + (index-1) * sizeof(ColumnOffset), sizeof(ColumnOffset));
    ColumnOffset endPointer;
    memcpy(&endPointer, (char*)page + offset_to_field_dir + index * sizeof(ColumnOffset), sizeof(ColumnOffset));

    // rec_offset keeps track of start of column, so end-start = total size
    uint32_t fieldSize = endPointer - startPointer;

    unsigned data_offset = recordNullIndicatorSize;
    // Special case for varchar, we must give data the size of varchar first
    if (recordDescriptor[index].type == TypeVarChar)
    {
        memcpy((char*) data+data_offset, &fieldSize, VARCHAR_LENGTH_SIZE);
        data_offset += VARCHAR_LENGTH_SIZE;
    }
    //
    int field_dir_len = recordDescriptor.size()*sizeof(ColumnOffset);
    int attr_offset = offset_to_record + startPointer;
    memcpy((void *)((char*) data+(size_t)data_offset), (void *)((char*)page + attr_offset), fieldSize);
    //read page given by filehandle
    //getSlotE
    return 0;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
                                const vector<Attribute> &recordDescriptor,
                                const string &conditionAttribute,
                                const CompOp compOp,                  // comparision type such as "<" and "="
                                const void *value,                    // used in the comparison
                                const vector<string> &attributeNames, // a list of projected attributes
                                RBFM_ScanIterator &rbfm_ScanIterator)
                                {
                                    //how do we determine which rids to read from? We should figure it out at this level.
                                    //also need to filter initialize with filter information
                                    rbfm_ScanIterator.fileHandle = fileHandle;
                                    rbfm_ScanIterator.recordDescriptor = recordDescriptor;
                                    rbfm_ScanIterator.conditionAttribute = conditionAttribute;
                                    rbfm_ScanIterator.compOp = compOp;

                                    rbfm_ScanIterator.value = value;

                                    rbfm_ScanIterator.attributeNames = attributeNames;
                                    rbfm_ScanIterator._rbfm = _rbf_manager;

                                    void* page = malloc(PAGE_SIZE);

                                    if(fileHandle.readPage(0, page) == -1)
                                        return -1;
                                    SlotDirectoryHeader header = _rbf_manager->getSlotDirectoryHeader(page);
                                    free(page);
                                    rbfm_ScanIterator.numberEntriesOnPage = header.recordEntriesNumber;
                                    return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid)
{
    //get offset
    //determine record size
    //get all records <offset
    //move last record forward by deletedRecord length
    //repeat for all records
    //delete slot entry
    //set free space -= deletedRecord length
    //set free space offset -= deletedRecord length

    void * page = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);
    //get header and slot table entry for our record
    SlotDirectoryHeader header = _rbf_manager->getSlotDirectoryHeader(page); 
    SlotDirectoryRecordEntry entry = _rbf_manager->getSlotDirectoryRecordEntry(page, rid.slotNum);
    //remove the entry for our record
    SlotDirectoryRecordEntry *empty_entry = (SlotDirectoryRecordEntry*) malloc(sizeof(SlotDirectoryRecordEntry));
    empty_entry->offset=0;
    empty_entry->length=0;
    setSlotDirectoryRecordEntry(page, rid.slotNum, *empty_entry);

    int offset = entry.offset;
    int length = entry.length;//confirm this is the records size on page including nullbytes, etc.

    //update the header 
    header.freeSpaceOffset -= length;
    header.recordEntriesNumber -= 1;
    setSlotDirectoryHeader(page, header);

    vector<SlotDirectoryRecordEntry> records_to_move;
    int recordsFound = 0;
    for(int index = 0; recordsFound < header.recordEntriesNumber; index++){
        //confirm recordNumberEntries is really the number of records on page.
        SlotDirectoryRecordEntry entry = _rbf_manager->getSlotDirectoryRecordEntry(page, index);
        if(entry.offset == 0 && entry.length == 0){
            continue; //record was deleted, should not be counted in recordsFound
        }

        recordsFound ++;

        if(entry.offset < offset && entry.length > 0){
            //deletion will affect this record. negative length means this RID is forwarded.

            //update the slot entry with new offset. 
            //we can't move the record itself yet b/c we need to move them in a specific
            //order to avoid accidentally overwritting any records
            SlotDirectoryRecordEntry new_entry = _rbf_manager->getSlotDirectoryRecordEntry(page, index);
            new_entry.offset += length; 
            _rbf_manager->setSlotDirectoryRecordEntry(page, index, new_entry);


            records_to_move.insert(records_to_move.end(), entry);
            //free new_entry;
        }else{
            //The record we want is not on this page but we can determine its new RID from the
            // slot table entry
            RID *forwarded_rid = (RID*)malloc(sizeof(RID));
            forwarded_rid->pageNum = entry.offset;
            forwarded_rid->slotNum = 0 - entry.length;
            return _rbf_manager->deleteRecord(fileHandle, recordDescriptor, *forwarded_rid);
        }
    }
    //sort records to move based on offset. greatest offset is closest to entry to delete 
    // & there fore should move first. This should sort with greatest offset first. 
    std::sort(records_to_move.begin(), records_to_move.end(), compareByOffset);

    //get record at entry.offset, set record at entry.offset + length
    for(int i =0; i<records_to_move.size(); i++){
        SlotDirectoryRecordEntry record_to_move = records_to_move[0];
        //should these be chars???
        memmove((char*)page+record_to_move.offset, (char*)page+record_to_move.offset+length, record_to_move.length);
    }

    fileHandle.writePage(rid.pageNum, page);
    return 0;
}
// http://stackoverflow.com/questions/4892680/sorting-a-vector-of-structs
bool compareByOffset(const SlotDirectoryRecordEntry &a, const SlotDirectoryRecordEntry &b)
{
    return a.offset > b.offset;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid){
    //delete record, insert record, if new rid != oldrid then update oldrid with forwarding.
    
    RID * new_rid = (RID*) malloc(sizeof(RID));

    _rbf_manager->deleteRecord(fileHandle, recordDescriptor, rid);
    _rbf_manager->insertRecord(fileHandle, recordDescriptor, data, *new_rid);

    if(rid.pageNum != new_rid->pageNum || rid.slotNum != new_rid->pageNum){
        void * page = malloc(sizeof(PAGE_SIZE));
        fileHandle.readPage(rid.pageNum, page);
        SlotDirectoryRecordEntry entry = getSlotDirectoryRecordEntry(page, rid.slotNum);
        //entry should be 0-ed out here
        entry.length = new_rid->pageNum;
        entry.offset = 0 - new_rid->slotNum;

        setSlotDirectoryRecordEntry(page, rid.slotNum, entry);
    }
    return 0;
}

// already defined in rbfm.h may need to redefine
RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data)  
	{
    //determine what rid is next
    //see if record at that rid satisfies conditional
    //read all fields specified
    //set null bytes
    //if needed increment page, reset page data
    void* page = malloc(PAGE_SIZE);
    // fprintf(stderr, "NEP: %d\n",numberEntriesOnPage);
    // fprintf(stderr, "ERP: %d\n", entriesReadOnPage);
    if(numberEntriesOnPage >= entriesReadOnPage){
      currentPage++;
      if(currentPage>(fileHandle.getNumberOfPages()-1)){
        return -1; //EOF
      }
      if(fileHandle.readPage(currentPage, page) == -1)
        return -1;
      SlotDirectoryHeader header = _rbfm->getSlotDirectoryHeader(page);
      numberEntriesOnPage=header.recordEntriesNumber;//what if its 0
      nextSlotNum=0;
      entriesReadOnPage=0;
    }
    while(true){
      //find non tombstoned rid
      rid.slotNum = nextSlotNum;
      rid.pageNum = currentPage;

      SlotDirectoryRecordEntry entry = _rbfm->getSlotDirectoryRecordEntry(page, rid.slotNum);
      if(entry.length == 0 && entry.offset == 0){
        //tombstone, not real record
        nextSlotNum++;
      }else{
        entriesReadOnPage++;
        nextSlotNum++;
        break;
      }
    }

    int conditional_index = 0;
    for(int i = 0; i< recordDescriptor.size(); i++){
      if(recordDescriptor[i].name == conditionAttribute){
        conditional_index = i;
        break;
      }
    }
    void* temp_data = malloc(PAGE_SIZE);
    memset(temp_data, 0, PAGE_SIZE);
    if(_rbfm->readAttribute(fileHandle, recordDescriptor, rid, conditionAttribute, temp_data) == -1)
      return -1;

    int comparison = 0;    
    if(recordDescriptor[conditional_index].type == TypeVarChar){
      int * recordSize = (int*)malloc(sizeof(int));
      memcpy(recordSize, (char*)temp_data+1, sizeof(int));
      char * record = (char *) malloc((*recordSize)+1);
      //set 0 to insure it is null terminated
      memset(record, 0, *recordSize+1);
      memcpy(record, (char*)temp_data+1+sizeof(int), *recordSize);
      comparison = strcmp(record, (char*)value);

    }else if(recordDescriptor[conditional_index].type == TypeInt){
      int* record = (int *) malloc(sizeof(int));
      memcpy(record, (char*)temp_data+1, sizeof(int));
      if(*record < *(int*) value){
        comparison = -1;
      }else if(*record > *(int*) value){
        comparison = 1;
      }
    }else{
      float* record = (float *) malloc(sizeof(float));
      memcpy(record, (char*)temp_data+1, sizeof(float));
      if(*record < *(float*) value){
        comparison = -1;
      }else if(*record > *(float*) value){
        comparison = 1;
      }
    }
    if(compOp == EQ_OP && comparison != 0){
      getNextRecord(rid, data);
    }
    if(compOp == LE_OP && comparison >= 0){
      getNextRecord(rid, data);
    }
    if(compOp == EQ_OP && comparison <= 0){
      getNextRecord(rid, data);
    }
    int nullSize = _rbfm->getNullIndicatorSize(attributeNames.size());
    void * nullBytes = malloc(nullSize);

    int offset_to_data = nullSize;

    for(int i = 0; i<attributeNames.size(); i++){
      int temp_data_offset = 1; //null indicator will be 1 bytes
      //memset temp_data 0
      _rbfm->readAttribute(fileHandle, recordDescriptor, rid, attributeNames[i], temp_data);

      if (_rbfm->fieldIsNull((char *)temp_data, 0)){
        int indicatorIndex = (i+1) / CHAR_BIT;
        int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
        ((char *)nullBytes)[indicatorIndex] |= indicatorMask;
      }else{
        int attr_index = 0;
        for(int j = 0; j< recordDescriptor.size(); j++){
          if(recordDescriptor[j].name == attributeNames[i]){
            attr_index = j;
            break;
          }
        }
        //get size of field
        //copy into data+offsettodata
        //edit offset
        memcpy((char*) data + offset_to_data, (char*)temp_data+temp_data_offset, VARCHAR_LENGTH_SIZE);

        if (recordDescriptor[attr_index].type == TypeVarChar)
        {
            int* field_size = (int*)malloc(sizeof(int));
            memcpy(field_size, (char*)temp_data+temp_data_offset, VARCHAR_LENGTH_SIZE);

            offset_to_data += VARCHAR_LENGTH_SIZE;
            temp_data_offset += VARCHAR_LENGTH_SIZE;
            memcpy((char*) data + offset_to_data, (char*)temp_data+temp_data_offset, *field_size);
        }
      }
      free(temp_data);
    }
    //get attribute pointed at by conditionAttribute
    //maybe no call to readRecord? Just sequential calls to readAttribute. 
    //we have to build up the data correctly though.
    //for nulls: readAttr will indicate null
    //for all nulls, generate null bytes
    //add null bytes + rest of records IN PROPER ORDER 
    //point data to new data
    free(page);
    return 0;
  }
/*
RC RBFM_ScanIterator::close() 
{ 
	return -1; 
} */
