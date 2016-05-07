
#include "rm.h"

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
    return -1;
}

RC RelationManager::deleteCatalog()
{
    return -1;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{

    return -1;
}

RC RelationManager::deleteTable(const string &tableName)
{
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    FileHandle fh;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC err;
    err = rbfm->openFile(tableName, fh);
    if(err == -1)
        return err;
    vector<Attribute> attrs;
    err = this->getAttributes(tableName, attrs);
    if(err == -1)
        return err;
    err = rbfm->insertRecord(fh, attrs, data, rid);
    if(err == -1)
        return err;
    return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    FileHandle fh;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC err;
    err = rbfm->openFile(tableName, fh);
    if(err == -1)
        return err;
    vector<Attribute> attrs;
    err = this->getAttributes(tableName, attrs);
    if(err == -1)
        return err;
    err = rbfm->deleteRecord(fh, attrs, rid);
    if(err == -1)
        return err;
    return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    FileHandle fh;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC err;
    err = rbfm->openFile(tableName, fh);
    if(err == -1)
        return err;
    vector<Attribute> attrs;
    err = this->getAttributes(tableName, attrs);
    if(err == -1)
        return err;
    err = rbfm->updateRecord(fh, attrs, data, rid);
    if(err == -1)
        return err;
    return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    FileHandle fh;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC err;
    err = rbfm->openFile(tableName, fh);
    if(err == -1)
        return err;
    vector<Attribute> attrs;
    err = this->getAttributes(tableName, attrs);
    if(err == -1)
        return err;
    err = rbfm->readRecord(fh, attrs, rid, data);
    if(err == -1)
        return err;
    return 0;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	return rbfm->printAttributes(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    FileHandle fh;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC err;
    err = rbfm->openFile(tableName, fh);
    if(err == -1)
        return err;
    vector<Attribute> attrs;
    err = this->getAttributes(tableName, attrs);
    if(err == -1)
        return err;
    err = rbfm->updateRecord(fh, attrs, rid, attributeName, data);
    if(err == -1)
        return err;
    return 0;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    return -1;
}



